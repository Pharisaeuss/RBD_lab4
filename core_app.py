# Підключення до Cassandra (lab4) та застосування schema.cql
import time
from datetime import datetime, timezone
from pathlib import Path
import math
import os
import random
from zoneinfo import ZoneInfo

from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT



BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP", "kafka:9092")
CASSANDRA_HOST = os.environ.get("CASSANDRA_HOST", "cassandra")
KEYSPACE = "solar_kyiv"

PREFIX = os.environ.get("TOPIC_PREFIX", "solar")
T_TELEMETRY = f"{PREFIX}.telemetry"
T_BY_PANEL = f"{PREFIX}.telemetry.by_panel"
T_EVENTS = f"{PREFIX}.events"
T_WINDOWS = f"{PREFIX}.windows"
T_STATES = f"{PREFIX}.states"
PARTITIONS = 6

N_PANELS = 500
INTERVAL_S = 30          # показ кожні 30 с -> 500 / 30 ≈ 16.7 msg/sec (~17 msg/sec)
WINDOW_S = 300           # 5-хвилинні неперекриваючі (tumbling) вікна

ZONES = ["Бориспільська", "Броварська", "Фастівська", "Вишгородська", "Білоцерківська", "Ірпінська"]

EV_AGGREGATE_CREATED = "AGGREGATE_CREATED"
EV_THRESHOLD_EXCEEDED = "THRESHOLD_EXCEEDED"

POWER_THRESHOLD_KW = 4.6  # поріг пікової/середньої потужності панелі (кВт) для події THRESHOLD_EXCEEDED

_KYIV = ZoneInfo("Europe/Kyiv")


def panel_id(i: int) -> str:
    return f"P{i:05d}"


def zone_of(i: int) -> str:
    return ZONES[i % len(ZONES)]


def day_of(ts: float) -> str:
    return datetime.fromtimestamp(ts, timezone.utc).strftime("%Y-%m-%d")


def window_start(ts: float) -> int:
    return int(ts // WINDOW_S * WINDOW_S)


def solar_profile(ts: float) -> tuple[float, float]:
    # Повертає (irradiance_wm2, ambient_temp_c) за київським часом
    dt = datetime.fromtimestamp(ts, _KYIV)
    hour = dt.hour + dt.minute / 60.0 + dt.second / 3600.0
    daylight = max(0.0, math.cos(((hour - 12.5) / 6.5) * (math.pi / 2.0))) ** 1.4
    irradiance = daylight * 980.0
    ambient_temp = 10.0 + daylight * 22.0
    return irradiance, ambient_temp


class SolarFleet:
    # Парк із 500 сонячних панелей

    def __init__(self, n: int = N_PANELS, seed: int = 42, spike_prob: float = 0.03):
        self.n = n
        self.rng = random.Random(seed)
        self.capacity_kw = [random.Random(i + 1000).uniform(3.5, 5.2) for i in range(n)]
        self.base_eff = [random.Random(i + 2000).uniform(18.0, 22.5) for i in range(n)]
        self.spike_prob = spike_prob
        self.spikes_injected = 0

    def reading(self, i: int, seq: int, ts: float) -> dict:
        rng = self.rng
        irr_base, amb_t = solar_profile(ts)
        irradiance = max(0.0, irr_base + rng.gauss(0.0, 20.0 if irr_base > 0 else 1.0))
        efficiency = min(25.0, max(10.0, self.base_eff[i] + rng.gauss(0.0, 0.3)))
        temperature = amb_t + (irradiance / 1000.0) * 18.0 + rng.gauss(0.0, 1.2)

        # Нормальна генерація або локальний сплеск для перевірки THRESHOLD_EXCEEDED
        mult = 1.0
        if rng.random() < self.spike_prob and irradiance > 200.0:
            mult = rng.uniform(1.15, 1.35)
            self.spikes_injected += 1

        power_output = max(0.0, min(self.capacity_kw[i] * 1.4, (irradiance / 1000.0) * self.capacity_kw[i] * (efficiency / 20.0) * mult))
        voltage = rng.gauss(650.0, 18.0) if power_output > 0.01 else 0.0
        current = (power_output * 1000.0 / voltage) if voltage > 1.0 else 0.0

        return {
            "panel_id": panel_id(i),
            "zone": zone_of(i),
            "seq": seq,
            "event_ts": float(ts),
            "power_output": round(power_output, 4),
            "voltage": round(voltage, 2),
            "current": round(current, 3),
            "temperature": round(temperature, 2),
            "efficiency": round(efficiency, 2),
            "irradiance": round(irradiance, 2),
        }


def energy_kwh(power_kw: float, dt_s: float = INTERVAL_S) -> float:
    return power_kw * (dt_s / 3600.0)

SCHEMA = Path(__file__).with_name("schema.cql")


def to_dt(ts):
    # epoch-секунди -> naive UTC datetime (драйвер трактує naive як UTC)
    return datetime.fromtimestamp(ts, timezone.utc).replace(tzinfo=None)


def connect(keyspace=KEYSPACE, retries=30):
    last = None
    for _ in range(retries):
        try:
            profile = ExecutionProfile(request_timeout=30)
            cluster = Cluster([CASSANDRA_HOST], execution_profiles={EXEC_PROFILE_DEFAULT: profile})
            session = cluster.connect()
            if keyspace:
                try:
                    session.set_keyspace(keyspace)
                except Exception:
                    pass
            return cluster, session
        except Exception as e:  # Cassandra ще стартує
            last = e
            time.sleep(5)
    raise RuntimeError(f"Cassandra недоступна: {last}")


def apply_schema(session):
    text = "\n".join(l for l in SCHEMA.read_text(encoding="utf-8").splitlines() if not l.strip().startswith("--"))
    for stmt in [s.strip() for s in text.split(";") if s.strip()]:
        session.execute(stmt)
    session.set_keyspace(KEYSPACE)


def truncate_all(session):
    for t in ("event_log", "device_state", "window_aggregates"):
        session.execute(f"TRUNCATE {t}")
