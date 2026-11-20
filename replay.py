# Відновлення стану панелей та перевірка агрегатів через replay журналу event_log
import argparse
import json
from datetime import datetime, timedelta, timezone

import core_app as c


def iter_utc_days(start_ts: float, end_ts: float) -> list[str]:
    # Повертає перелік дат у форматі YYYY-MM-DD (UTC) для діапазону часових міток
    first_day = datetime.fromtimestamp(start_ts, timezone.utc).date()
    last_day = datetime.fromtimestamp(end_ts, timezone.utc).date()
    span_days = (last_day - first_day).days
    return [(first_day + timedelta(days=offset)).strftime("%Y-%m-%d") for offset in range(span_days + 1)]


def fetch_panel_events(session, panel_id: str, start_ts: float, end_ts: float) -> list:
    # Зчитує події панелі з event_log у хронологічному порядку.# 
    query = session.prepare(
        "SELECT event_ts, event_id, event_type, zone, window_start, window_end, seq, energy_kwh, payload "
        "FROM event_log WHERE panel_id=? AND day=? AND event_ts>=? AND event_ts<=?"
    )
    dt_from = c.to_dt(start_ts)
    dt_to = c.to_dt(end_ts)
    collected = []

    for day_str in iter_utc_days(start_ts, end_ts):
        collected.extend(session.execute(query, (panel_id, day_str, dt_from, dt_to)))

    collected.sort(key=lambda row: (row.event_ts, row.event_id))
    return collected


def rebuild_state_from_events(
    panel_id: str,
    events: list,
    write_state: bool = False,
    session=None,
) -> dict:
    # Згортає послідовність подій панелі у накопичувальну статистику device_state
    processed_ids = set()
    skipped_duplicates = 0
    window_total = 0
    sample_total = 0
    power_weighted_sum = 0.0
    power_min_val = None
    power_max_val = 0.0
    energy_total_kwh = 0.0
    temp_weighted_sum = 0.0
    eff_weighted_sum = 0.0
    irr_weighted_sum = 0.0
    threshold_hits = 0

    zone_name = ""
    last_win_start = None
    last_ev_type = None
    last_seq_num = -1
    last_ev_ts = None
    counts_by_type: dict[str, int] = {}
    event_trace = []

    for ev in events:
        if ev.event_id in processed_ids:
            skipped_duplicates += 1
            continue
        processed_ids.add(ev.event_id)

        counts_by_type[ev.event_type] = counts_by_type.get(ev.event_type, 0) + 1
        zone_name = ev.zone or zone_name
        last_seq_num = ev.seq
        last_ev_ts = ev.event_ts
        last_ev_type = ev.event_type
        last_win_start = ev.window_start

        details = json.loads(ev.payload) if isinstance(ev.payload, str) else (ev.payload or {})
        event_trace.append({
            "event_id": ev.event_id,
            "event_type": ev.event_type,
            "event_ts": ev.event_ts.isoformat(),
            "window_start": ev.window_start.isoformat() if ev.window_start else None,
            "window_end": ev.window_end.isoformat() if ev.window_end else None,
            "payload": details,
        })

        if ev.event_type == c.EV_AGGREGATE_CREATED:
            window_total += 1
            readings_in_win = max(1, int(details.get("readings", 1)))
            w_avg_p = float(details.get("avg_power_kw", 0.0))
            w_min_p = float(details.get("min_power_kw", w_avg_p))
            w_max_p = float(details.get("max_power_kw", w_avg_p))
            w_energy = float(details.get("total_energy_kwh", ev.energy_kwh or 0.0))

            sample_total += readings_in_win
            power_weighted_sum += w_avg_p * readings_in_win
            power_min_val = w_min_p if power_min_val is None else min(power_min_val, w_min_p)
            power_max_val = max(power_max_val, w_max_p)
            energy_total_kwh += w_energy
            temp_weighted_sum += float(details.get("avg_temperature_c", 0.0)) * readings_in_win
            eff_weighted_sum += float(details.get("avg_efficiency_pct", 0.0)) * readings_in_win
            irr_weighted_sum += float(details.get("avg_irradiance_wm2", 0.0)) * readings_in_win

        elif ev.event_type == c.EV_THRESHOLD_EXCEEDED:
            threshold_hits += 1

    rebuilt_state = {
        "panel_id": panel_id,
        "zone": zone_name,
        "windows_count": window_total,
        "samples_count": sample_total,
        "avg_power_kw": round(power_weighted_sum / sample_total, 4) if sample_total else 0.0,
        "min_power_kw": round(power_min_val, 4) if power_min_val is not None else 0.0,
        "max_power_kw": round(power_max_val, 4),
        "total_energy_kwh": round(energy_total_kwh, 6),
        "avg_temperature_c": round(temp_weighted_sum / sample_total, 2) if sample_total else 0.0,
        "avg_efficiency_pct": round(eff_weighted_sum / sample_total, 2) if sample_total else 0.0,
        "avg_irradiance_wm2": round(irr_weighted_sum / sample_total, 2) if sample_total else 0.0,
        "threshold_exceeded_count": threshold_hits,
        "last_window_start": last_win_start.isoformat() if last_win_start else None,
        "last_event_type": last_ev_type,
        "last_seq": last_seq_num,
        "last_event_ts": last_ev_ts.isoformat() if last_ev_ts else None,
    }

    if write_state and session is not None and last_ev_ts is not None:
        upsert_state = session.prepare(
            "INSERT INTO device_state (panel_id, zone, windows_count, samples_count, avg_power_kw, "
            "min_power_kw, max_power_kw, total_energy_kwh, avg_temperature_c, avg_efficiency_pct, "
            "avg_irradiance_wm2, threshold_exceeded_count, last_window_start, last_event_type, "
            "last_seq, last_event_ts) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
        )
        session.execute(upsert_state, (
            panel_id,
            rebuilt_state["zone"],
            rebuilt_state["windows_count"],
            rebuilt_state["samples_count"],
            rebuilt_state["avg_power_kw"],
            rebuilt_state["min_power_kw"],
            rebuilt_state["max_power_kw"],
            rebuilt_state["total_energy_kwh"],
            rebuilt_state["avg_temperature_c"],
            rebuilt_state["avg_efficiency_pct"],
            rebuilt_state["avg_irradiance_wm2"],
            rebuilt_state["threshold_exceeded_count"],
            last_win_start,
            rebuilt_state["last_event_type"],
            rebuilt_state["last_seq"],
            last_ev_ts,
        ))

    return {
        "panel_id": panel_id,
        "events_processed": len(events) - skipped_duplicates,
        "duplicates_skipped": skipped_duplicates,
        "event_counts": counts_by_type,
        "state": rebuilt_state,
        "events": event_trace,
    }


def replay_panel_history(session, panel_id: str, start_ts: float, end_ts: float, persist: bool = False) -> dict:
    # Виконує повний replay журналу подій для вказаної сонячної панелі
    raw_events = fetch_panel_events(session, panel_id, start_ts, end_ts)
    return rebuild_state_from_events(panel_id, raw_events, write_state=persist, session=session)


def resolve_time_bounds(session, start_ts: float | None, end_ts: float | None) -> tuple[float, float]:
    # Визначає часові межі запиту на основі подій у БД або поточного часу
    if start_ts is None or end_ts is None:
        sample_row = session.execute("SELECT event_ts FROM event_log LIMIT 1").one()
        anchor = (
            sample_row.event_ts.replace(tzinfo=timezone.utc).timestamp()
            if sample_row
            else datetime.now(timezone.utc).timestamp()
        )
        return start_ts if start_ts is not None else anchor - 86400.0, end_ts if end_ts is not None else anchor + 86400.0
    return start_ts, end_ts


if __name__ == "__main__":
    cli = argparse.ArgumentParser(description="Replay подій панелі з event_log для відновлення device_state")
    cli.add_argument("--panel", required=True, help="Ідентифікатор панелі, наприклад P00001")
    cli.add_argument("--from", dest="start_ts", type=float, help="Початок інтервалу (epoch seconds)")
    cli.add_argument("--to", dest="end_ts", type=float, help="Кінець інтервалу (epoch seconds)")
    cli.add_argument("--persist", action="store_true", help="Записати відновлений стан у device_state")
    parsed = cli.parse_args()

    cluster, session = c.connect()
    t_start, t_end = resolve_time_bounds(session, parsed.start_ts, parsed.end_ts)
    output = replay_panel_history(session, parsed.panel, t_start, t_end, persist=parsed.persist)
    print(json.dumps(output, ensure_ascii=False, indent=2))
    cluster.shutdown()