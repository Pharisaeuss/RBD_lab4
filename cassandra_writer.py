# Споживач вихідних топіків Kafka для запису стану та подій панелей у Cassandra
import argparse
import json
import time
from datetime import datetime

from cassandra.concurrent import execute_concurrent_with_args
from confluent_kafka import Consumer

import core_app as c


def build_kafka_consumer(group_id: str) -> Consumer:
    # Створює транзакційно-безпечного споживача з ручним фіксуванням offset
    consumer = Consumer({
        "bootstrap.servers": c.BOOTSTRAP,
        "group.id": group_id,
        "enable.auto.commit": False,
        "isolation.level": "read_committed",
        "auto.offset.reset": "earliest",
        "max.poll.interval.ms": 300000,
    })
    consumer.subscribe([c.T_EVENTS, c.T_STATES, c.T_WINDOWS])
    return consumer


def persist_batch(session, statements_and_rows: list[tuple]) -> None:
    # Виконує ідемпотентний запис підготовлених пачок у таблиці keyspace solar_kyiv
    for stmt, rows in statements_and_rows:
        if not rows:
            continue
        results = execute_concurrent_with_args(
            session,
            stmt,
            rows,
            concurrency=64,
            raise_on_first_error=False,
        )
        for ok, err in results:
            if not ok:
                raise RuntimeError(f"Помилка запису в Cassandra: {err}")


def run_writer(group_id: str, idle_timeout: float | None = None, batch_size: int = 500, silent: bool = False) -> dict:
    cluster, session = c.connect()

    stmt_event = session.prepare(
        "INSERT INTO event_log (panel_id, day, event_ts, event_id, event_type, zone, "
        "window_start, window_end, seq, energy_kwh, payload) VALUES (?,?,?,?,?,?,?,?,?,?,?)"
    )
    stmt_state = session.prepare(
        "INSERT INTO device_state (panel_id, zone, windows_count, samples_count, avg_power_kw, "
        "min_power_kw, max_power_kw, total_energy_kwh, avg_temperature_c, avg_efficiency_pct, "
        "avg_irradiance_wm2, threshold_exceeded_count, last_window_start, last_event_type, "
        "last_seq, last_event_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    )
    stmt_window = session.prepare(
        "INSERT INTO window_aggregates (panel_id, day, window_start, window_end, zone, "
        "avg_power_kw, min_power_kw, max_power_kw, total_energy_kwh, avg_temperature_c, "
        "avg_efficiency_pct, avg_irradiance_wm2, readings) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)"
    )

    consumer = build_kafka_consumer(group_id)
    stats = {"events": 0, "states": 0, "windows": 0, "commits": 0}
    last_activity = time.time()

    try:
        while True:
            records = consumer.consume(num_messages=batch_size, timeout=1.0)
            records = [msg for msg in records if msg is not None and msg.error() is None]
            if not records:
                if idle_timeout is not None and (time.time() - last_activity) > idle_timeout:
                    break
                continue

            last_activity = time.time()
            event_batch = []
            state_batch = []
            window_batch = []

            for msg in records:
                data = json.loads(msg.value())
                topic_name = msg.topic()

                if topic_name == c.T_EVENTS:
                    ev_ts = float(data["event_ts"])
                    event_batch.append((
                        data["panel_id"],
                        c.day_of(ev_ts),
                        c.to_dt(ev_ts),
                        data["event_id"],
                        data["event_type"],
                        data["zone"],
                        c.to_dt(float(data["window_start"])),
                        c.to_dt(float(data["window_end"])),
                        int(data["seq"]),
                        float(data["energy_kwh"]),
                        json.dumps(data["payload"], ensure_ascii=False),
                    ))
                elif topic_name == c.T_STATES:
                    state_batch.append((
                        data["panel_id"],
                        data["zone"],
                        int(data["windows_count"]),
                        int(data["samples_count"]),
                        float(data["avg_power_kw"]),
                        float(data["min_power_kw"]),
                        float(data["max_power_kw"]),
                        float(data["total_energy_kwh"]),
                        float(data["avg_temperature_c"]),
                        float(data["avg_efficiency_pct"]),
                        float(data["avg_irradiance_wm2"]),
                        int(data["threshold_exceeded_count"]),
                        c.to_dt(float(data["last_window_start"])),
                        data["last_event_type"],
                        int(data["last_seq"]),
                        c.to_dt(float(data["last_event_ts"])),
                    ))
                elif topic_name == c.T_WINDOWS:
                    w_start = float(data["window_start"])
                    window_batch.append((
                        data["panel_id"],
                        c.day_of(w_start),
                        c.to_dt(w_start),
                        c.to_dt(float(data["window_end"])),
                        data["zone"],
                        float(data["avg_power_kw"]),
                        float(data["min_power_kw"]),
                        float(data["max_power_kw"]),
                        float(data["total_energy_kwh"]),
                        float(data["avg_temperature_c"]),
                        float(data["avg_efficiency_pct"]),
                        float(data["avg_irradiance_wm2"]),
                        int(data["readings"]),
                    ))

            persist_batch(session, [
                (stmt_event, event_batch),
                (stmt_state, state_batch),
                (stmt_window, window_batch),
            ])

            # Фіксуємо зміщення в Kafka лише після успішного збереження всіх рядків у БД
            consumer.commit(asynchronous=False)

            stats["events"] += len(event_batch)
            stats["states"] += len(state_batch)
            stats["windows"] += len(window_batch)
            stats["commits"] += 1

            if not silent and stats["commits"] % 20 == 0:
                print(f"[{datetime.now():%H:%M:%S}] writer: {stats}", flush=True)
    finally:
        consumer.close()
        cluster.shutdown()

    if not silent:
        print(f"Запис у Cassandra завершено: {stats}")
    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Запис подій та стану сонячних панелей у Cassandra")
    parser.add_argument("--group", default="solar-kyiv-writer")
    parser.add_argument("--idle-exit", type=float, default=None)
    args = parser.parse_args()
    run_writer(args.group, args.idle_exit)