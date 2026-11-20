CREATE KEYSPACE IF NOT EXISTS solar_kyiv
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE solar_kyiv;

-- Append-only Event Store
CREATE TABLE IF NOT EXISTS event_log (
    panel_id text,
    day text,
    event_ts timestamp,
    event_id text,
    event_type text,
    zone text,
    window_start timestamp,
    window_end timestamp,
    seq int,
    energy_kwh double,
    payload text,
    PRIMARY KEY ((panel_id, day), event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts ASC, event_id ASC);

-- Накопичувальна статистика по кожній панелі (State Store)
CREATE TABLE IF NOT EXISTS device_state (
    panel_id text PRIMARY KEY,
    zone text,
    windows_count bigint,
    samples_count bigint,
    avg_power_kw double,
    min_power_kw double,
    max_power_kw double,
    total_energy_kwh double,
    avg_temperature_c double,
    avg_efficiency_pct double,
    avg_irradiance_wm2 double,
    threshold_exceeded_count bigint,
    last_window_start timestamp,
    last_event_type text,
    last_seq int,
    last_event_ts timestamp
);

-- Агреговані 5-хвилинні вікна
CREATE TABLE IF NOT EXISTS window_aggregates (
    panel_id text,
    day text,
    window_start timestamp,
    window_end timestamp,
    zone text,
    avg_power_kw double,
    min_power_kw double,
    max_power_kw double,
    total_energy_kwh double,
    avg_temperature_c double,
    avg_efficiency_pct double,
    avg_irradiance_wm2 double,
    readings int,
    PRIMARY KEY ((panel_id, day), window_start)
) WITH CLUSTERING ORDER BY (window_start DESC);