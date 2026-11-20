#Потоковий обробник телеметрії 500 сонячних панелей (5-хв tumbling windows та Event Sourcing).
import json
import os
import uuid

import faust

import core_app as c

GUARANTEE_MODE = os.environ.get("PROCESSING_GUARANTEE", "exactly_once")
INSTANCE_TAG = os.environ.get("RUN_ID", "default")
COMMIT_SEC = float(os.environ.get("COMMIT_INTERVAL", "1.0"))
SKIP_DUPLICATE_SEQ = os.environ.get("DEDUPE", "1") == "1"

app = faust.App(
    f"solar-kyiv-{GUARANTEE_MODE}-{INSTANCE_TAG}",
    broker=f"kafka://{c.BOOTSTRAP}",
    store="memory://",
    processing_guarantee=GUARANTEE_MODE,
    broker_commit_interval=COMMIT_SEC,
    consumer_auto_offset_reset="earliest",
    topic_partitions=c.PARTITIONS,
    topic_replication_factor=1,
    web_enabled=False,
)


class PanelTelemetry(faust.Record, serializer="json"):
    panel_id: str
    zone: str
    seq: int
    event_ts: float
    power_output: float
    voltage: float
    current: float
    temperature: float
    efficiency: float
    irradiance: float


class WindowBucket(faust.Record, serializer="json"):
    #Поточне 5-хвилинне вікно агрегації для окремої панелі.
    window_start: int = -1
    zone: str = ""
    obs_count: int = 0
    power_sum: float = 0.0
    power_min: float = 1e18
    power_max: float = 0.0
    energy_sum_kwh: float = 0.0
    temp_sum: float = 0.0
    eff_sum: float = 0.0
    irr_sum: float = 0.0


class PanelAccumulator(faust.Record, serializer="json"):
    #Накопичувальний стан панелі між 5-хвилинними вікнами.
    zone: str = ""
    completed_windows: int = 0
    total_observations: int = 0
    total_power_sum: float = 0.0
    avg_power_kw: float = 0.0
    min_power_kw: float = 0.0
    max_power_kw: float = 0.0
    total_energy_kwh: float = 0.0
    total_temp_sum: float = 0.0
    avg_temperature_c: float = 0.0
    total_eff_sum: float = 0.0
    avg_efficiency_pct: float = 0.0
    total_irr_sum: float = 0.0
    avg_irradiance_wm2: float = 0.0
    over_threshold_events: int = 0
    last_window_start: float = 0.0
    last_event_type: str = ""
    last_seq: int = -1
    last_event_ts: float = 0.0


telemetry_stream = app.topic(c.T_TELEMETRY, key_type=bytes, value_type=PanelTelemetry, partitions=c.PARTITIONS)
domain_events_out = app.topic(c.T_EVENTS, key_type=bytes, value_type=bytes, partitions=c.PARTITIONS)
window_aggs_out = app.topic(c.T_WINDOWS, key_type=bytes, value_type=bytes, partitions=c.PARTITIONS)
panel_states_out = app.topic(c.T_STATES, key_type=bytes, value_type=bytes, partitions=c.PARTITIONS)

active_windows = app.Table("open_5m_window", default=WindowBucket, value_type=WindowBucket, partitions=c.PARTITIONS)
panel_store = app.Table("device_state_store", default=PanelAccumulator, value_type=PanelAccumulator, partitions=c.PARTITIONS)


def to_json_bytes(payload: dict) -> bytes:
    return json.dumps(payload, ensure_ascii=False).encode("utf-8")


async def close_window_and_emit(
    pid: str,
    bucket: WindowBucket,
    acc: PanelAccumulator,
    latest_seq: int,
    latest_ts: float,
) -> None:
    #Формує метрики закритого 5-хв вікна, події Event Sourcing та оновлює стан панелі.
    if bucket.obs_count <= 0 or bucket.window_start < 0:
        return

    w_start = bucket.window_start
    w_end = w_start + c.WINDOW_S
    p_avg = bucket.power_sum / bucket.obs_count
    p_min = bucket.power_min
    p_max = bucket.power_max
    e_kwh = bucket.energy_sum_kwh
    t_avg = bucket.temp_sum / bucket.obs_count
    eff_avg = bucket.eff_sum / bucket.obs_count
    irr_avg = bucket.irr_sum / bucket.obs_count

    window_summary = {
        "panel_id": pid,
        "zone": bucket.zone,
        "window_start": w_start,
        "window_end": w_end,
        "avg_power_kw": round(p_avg, 4),
        "min_power_kw": round(p_min, 4),
        "max_power_kw": round(p_max, 4),
        "total_energy_kwh": round(e_kwh, 6),
        "avg_temperature_c": round(t_avg, 2),
        "avg_efficiency_pct": round(eff_avg, 2),
        "avg_irradiance_wm2": round(irr_avg, 2),
        "readings": bucket.obs_count,
    }

    created_event = {
        "event_id": str(uuid.uuid4()),
        "event_type": c.EV_AGGREGATE_CREATED,
        "panel_id": pid,
        "zone": bucket.zone,
        "event_ts": float(w_end),
        "window_start": float(w_start),
        "window_end": float(w_end),
        "seq": latest_seq,
        "energy_kwh": round(e_kwh, 6),
        "payload": {
            "avg_power_kw": round(p_avg, 4),
            "min_power_kw": round(p_min, 4),
            "max_power_kw": round(p_max, 4),
            "total_energy_kwh": round(e_kwh, 6),
            "avg_temperature_c": round(t_avg, 2),
            "avg_efficiency_pct": round(eff_avg, 2),
            "avg_irradiance_wm2": round(irr_avg, 2),
            "readings": bucket.obs_count,
        },
    }

    emitted_events = [created_event]
    breached_limit = p_max > c.POWER_THRESHOLD_KW or p_avg > c.POWER_THRESHOLD_KW
    if breached_limit:
        emitted_events.append({
            "event_id": str(uuid.uuid4()),
            "event_type": c.EV_THRESHOLD_EXCEEDED,
            "panel_id": pid,
            "zone": bucket.zone,
            "event_ts": float(w_end),
            "window_start": float(w_start),
            "window_end": float(w_end),
            "seq": latest_seq,
            "energy_kwh": round(e_kwh, 6),
            "payload": {
                "threshold_kw": c.POWER_THRESHOLD_KW,
                "max_power_kw": round(p_max, 4),
                "avg_power_kw": round(p_avg, 4),
                "total_energy_kwh": round(e_kwh, 6),
            },
        })

    # Оновлюємо накопичувальну проєкцію стану сонячної панелі
    acc.zone = bucket.zone
    acc.completed_windows += 1
    acc.total_observations += bucket.obs_count
    acc.total_power_sum += bucket.power_sum
    acc.avg_power_kw = round(acc.total_power_sum / acc.total_observations, 4)
    acc.min_power_kw = round(p_min if acc.completed_windows == 1 else min(acc.min_power_kw, p_min), 4)
    acc.max_power_kw = round(max(acc.max_power_kw, p_max), 4)
    acc.total_energy_kwh = round(acc.total_energy_kwh + e_kwh, 6)
    acc.total_temp_sum += bucket.temp_sum
    acc.avg_temperature_c = round(acc.total_temp_sum / acc.total_observations, 2)
    acc.total_eff_sum += bucket.eff_sum
    acc.avg_efficiency_pct = round(acc.total_eff_sum / acc.total_observations, 2)
    acc.total_irr_sum += bucket.irr_sum
    acc.avg_irradiance_wm2 = round(acc.total_irr_sum / acc.total_observations, 2)
    if breached_limit:
        acc.over_threshold_events += 1
    acc.last_window_start = float(w_start)
    acc.last_event_type = c.EV_THRESHOLD_EXCEEDED if breached_limit else c.EV_AGGREGATE_CREATED
    acc.last_seq = latest_seq
    acc.last_event_ts = latest_ts

    msg_key = pid.encode("utf-8")
    for item in emitted_events:
        await domain_events_out.send(key=msg_key, value=to_json_bytes(item))
    await window_aggs_out.send(key=msg_key, value=to_json_bytes(window_summary))
    await panel_states_out.send(key=msg_key, value=to_json_bytes({
        "panel_id": pid,
        "zone": acc.zone,
        "windows_count": acc.completed_windows,
        "samples_count": acc.total_observations,
        "avg_power_kw": acc.avg_power_kw,
        "min_power_kw": acc.min_power_kw,
        "max_power_kw": acc.max_power_kw,
        "total_energy_kwh": acc.total_energy_kwh,
        "avg_temperature_c": acc.avg_temperature_c,
        "avg_efficiency_pct": acc.avg_efficiency_pct,
        "avg_irradiance_wm2": acc.avg_irradiance_wm2,
        "threshold_exceeded_count": acc.over_threshold_events,
        "last_window_start": acc.last_window_start,
        "last_event_type": acc.last_event_type,
        "last_seq": acc.last_seq,
        "last_event_ts": acc.last_event_ts,
    }))


@app.agent(telemetry_stream)
async def consume_panel_telemetry(stream):
    async for reading in stream:
        pid = reading.panel_id
        acc = panel_store[pid]
        if SKIP_DUPLICATE_SEQ and acc.last_seq >= 0 and reading.seq <= acc.last_seq:
            continue

        bucket = active_windows[pid]
        win_start = c.window_start(reading.event_ts)

        # Перехід у наступне 5-хвилинне вікно закриває попереднє вікно панелі
        if bucket.window_start != -1 and win_start > bucket.window_start:
            await close_window_and_emit(pid, bucket, acc, reading.seq, reading.event_ts)
            panel_store[pid] = acc
            bucket = WindowBucket(window_start=win_start, zone=reading.zone)
        elif bucket.window_start == -1:
            bucket.window_start = win_start
            bucket.zone = reading.zone

        bucket.obs_count += 1
        bucket.power_sum += reading.power_output
        bucket.power_min = min(bucket.power_min, reading.power_output)
        bucket.power_max = max(bucket.power_max, reading.power_output)
        bucket.energy_sum_kwh += c.energy_kwh(reading.power_output, c.INTERVAL_S)
        bucket.temp_sum += reading.temperature
        bucket.eff_sum += reading.efficiency
        bucket.irr_sum += reading.irradiance
        active_windows[pid] = bucket