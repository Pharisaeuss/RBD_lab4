# Producer телеметрії: 500 сонячних панелей, показ кожні 30 с (~16.7 msg/s)
import argparse
import json
import time
from pathlib import Path

from confluent_kafka import Producer

import core_app as c

OUT = Path(__file__).with_name("out")


def run(cycles: int, speedup: float = 1.0, base_ts: int | None = None, seq_start: int = 0,
        throttle: bool = True, n_panels: int = c.N_PANELS, topic: str | None = None,
        seed: int = 42, quiet: bool = False, info_file: str = "producer_last.json"):
    topic = topic or c.T_TELEMETRY
    OUT.mkdir(exist_ok=True)
    fleet = c.SolarFleet(n_panels, seed=seed)
    prod = Producer({
        "bootstrap.servers": c.BOOTSTRAP,
        "acks": "all",
        "enable.idempotence": True,
        "linger.ms": 5,
        "compression.type": "lz4",
    })
    base_ts = base_ts if base_ts is not None else int(time.time() // c.WINDOW_S * c.WINDOW_S)
    errors = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    sent, t_wall0 = 0, time.time()
    total_ticks = cycles * c.INTERVAL_S
    for tick in range(total_ticks):
        if throttle:
            delay = t_wall0 + (tick / speedup) - time.time()
            if delay > 0:
                time.sleep(delay)
        ts = base_ts + seq_start * c.INTERVAL_S + tick
        seq = seq_start + tick // c.INTERVAL_S
        # Рівномірний розподіл 500 панелей по 30 кроках (~16-17 повідомлень за 1 с)
        for i in range(tick % c.INTERVAL_S, n_panels, c.INTERVAL_S):
            msg = fleet.reading(i, seq, ts)
            while True:
                try:
                    prod.produce(topic, key=msg["panel_id"], value=json.dumps(msg, ensure_ascii=False),
                                 on_delivery=on_delivery)
                    break
                except BufferError:
                    prod.poll(0.05)
            sent += 1
        prod.poll(0)
        if not quiet and (tick + 1) % c.INTERVAL_S == 0:
            el = max(time.time() - t_wall0, 1e-6)
            print(f"  цикл {(tick + 1) // c.INTERVAL_S}/{cycles}: {sent:,} повідомлень, {sent / el:,.1f} msg/s")

    prod.flush(60)
    el = max(time.time() - t_wall0, 1e-6)
    info = {
        "sent": sent,
        "errors": len(errors),
        "seconds": el,
        "msg_per_s": sent / el,
        "base_ts": base_ts,
        "seq_start": seq_start,
        "cycles": cycles,
        "speedup": speedup,
        "spikes_injected": fleet.spikes_injected,
    }
    (OUT / info_file).write_text(json.dumps(info, ensure_ascii=False, indent=2), encoding="utf-8")
    if not quiet:
        print(f"Надіслано {sent:,} повідомлень за {el:.1f} с ({sent / el:,.1f} msg/s), помилок: {len(errors)}")
    return info


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--cycles", type=int, default=20, help="кількість 30-секундних циклів")
    ap.add_argument("--speedup", type=float, default=1.0)
    ap.add_argument("--no-throttle", action="store_true")
    ap.add_argument("--seq-start", type=int, default=0)
    ap.add_argument("--base-ts", type=int, default=None)
    ap.add_argument("--panels", type=int, default=c.N_PANELS)
    ap.add_argument("--topic", default=None)
    ap.add_argument("--info-file", default="producer_last.json")
    a = ap.parse_args()
    run(a.cycles, a.speedup, a.base_ts, a.seq_start, not a.no_throttle, a.panels, a.topic, info_file=a.info_file)