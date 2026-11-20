#REST API для перегляду історії подій, накопичувального стану та replay сонячних панелей.
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse

import core_app as c
import replay

BASE_DIR = Path(__file__).parent
app = FastAPI(title="Kyiv Oblast Solar Telemetry: Events, State & Replay API")

_cassandra_cluster = None
_cassandra_session = None


def get_db_session():
    #Повертає активну сесію Cassandra (лінива ініціалізація).
    global _cassandra_cluster, _cassandra_session
    if _cassandra_session is None:
        _cassandra_cluster, _cassandra_session = c.connect()
    return _cassandra_session


def parse_timestamp_param(raw_value: str | None, fallback: float | None) -> float | None:
    #Перетворює epoch-секунди або ISO-8601 рядок у timestamp (UTC).
    if raw_value is None:
        return fallback
    try:
        return float(raw_value)
    except ValueError:
        parsed_dt = datetime.fromisoformat(raw_value.replace("Z", "+00:00"))
        aware_dt = parsed_dt if parsed_dt.tzinfo else parsed_dt.replace(tzinfo=timezone.utc)
        return aware_dt.timestamp()


def format_utc_iso(dt_obj):
    if dt_obj is None:
        return None
    return dt_obj.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")


@app.get("/")
def serve_dashboard():
    return FileResponse(BASE_DIR / "dashboard.html")


@app.get("/api/events/{panel_id}")
def list_panel_events(
    panel_id: str,
    start: str | None = Query(None, alias="from"),
    end: str | None = Query(None, alias="to"),
    event_type: str | None = None,
    limit: int = 200,
):
    #Повертає події AGGREGATE_CREATED та THRESHOLD_EXCEEDED з append-only таблиці event_log.
    session = get_db_session()
    t_from, t_to = replay.resolve_time_bounds(
        session,
        parse_timestamp_param(start, None),
        parse_timestamp_param(end, None),
    )
    records = replay.fetch_panel_events(session, panel_id, t_from, t_to)
    if event_type:
        records = [rec for rec in records if rec.event_type == event_type]

    selected = records[-max(1, limit):]
    events_payload = [
        {
            "event_id": rec.event_id,
            "event_type": rec.event_type,
            "panel_id": panel_id,
            "zone": rec.zone,
            "event_ts": format_utc_iso(rec.event_ts),
            "window_start": format_utc_iso(rec.window_start),
            "window_end": format_utc_iso(rec.window_end),
            "seq": rec.seq,
            "energy_kwh": rec.energy_kwh,
            "payload": rec.payload,
        }
        for rec in selected
    ]
    return {"panel_id": panel_id, "count": len(events_payload), "events": events_payload}


@app.get("/api/state/{panel_id}")
def read_panel_state(panel_id: str):
    #Повертає поточну накопичувальну проєкцію панелі з таблиці device_state.
    row = get_db_session().execute(
        "SELECT * FROM device_state WHERE panel_id=%s",
        (panel_id,),
    ).one()
    if row is None:
        raise HTTPException(status_code=404, detail=f"Запис стану для панелі {panel_id} відсутній")

    state_dict = row._asdict()
    state_dict["last_window_start"] = format_utc_iso(state_dict.get("last_window_start"))
    state_dict["last_event_ts"] = format_utc_iso(state_dict.get("last_event_ts"))
    return state_dict


@app.post("/api/replay/{panel_id}")
def trigger_panel_replay(
    panel_id: str,
    start: str | None = Query(None, alias="from"),
    end: str | None = Query(None, alias="to"),
    persist: bool = True,
):
    #Перераховує накопичувальний стан панелі з event_log та за потреби оновлює device_state.
    session = get_db_session()
    t_from, t_to = replay.resolve_time_bounds(
        session,
        parse_timestamp_param(start, None),
        parse_timestamp_param(end, None),
    )
    replay_result = replay.replay_panel_history(session, panel_id, t_from, t_to, persist=persist)
    if not replay_result["events"]:
        raise HTTPException(status_code=404, detail=f"Подій для replay панелі {panel_id} не знайдено")

    replay_result["from"] = format_utc_iso(c.to_dt(t_from))
    replay_result["to"] = format_utc_iso(c.to_dt(t_to))
    return replay_result