import argparse
import json
import os
import statistics
import subprocess
import sys
import time
from pathlib import Path

from confluent_kafka import Consumer, KafkaError, Producer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

import core_app as c
import producer as prod_mod

HERE = Path(__file__).parent
OUT = HERE / "out"
LOGS = OUT / "logs"

MODES = ("at_most_once", "at_least_once", "exactly_once")
BENCH_IN_TOPIC = "solar.bench.in"
SINK_SCRIPT = "cassandra_writer.py"  


def env_for(prefix: str, guarantee: str, run_id: str, dedupe: bool = True) -> dict:
    e = os.environ.copy()
    e.update(
        TOPIC_PREFIX=prefix,
        PROCESSING_GUARANTEE=guarantee,
        RUN_ID=run_id,
        PYTHONUNBUFFERED="1",
        DEDUPE="1" if dedupe else "0",
    )
    return e


def spawn(cmd: list[str], env: dict, logname: str) -> subprocess.Popen:
    LOGS.mkdir(parents=True, exist_ok=True)
    return subprocess.Popen(
        cmd,
        env=env,
        cwd=HERE,
        stdout=open(LOGS / logname, "w", encoding="utf-8"),
        stderr=subprocess.STDOUT,
    )


def start_faust(env: dict, logname: str):
    return spawn([sys.executable, "-m", "faust", "-A", "app", "worker", "-l", "info"], env, logname), logname


def wait_ready(proc: subprocess.Popen, logname: str, timeout: int = 180) -> float:
    t0 = time.time()
    path = LOGS / logname
    while time.time() - t0 < timeout:
        if proc.poll() is not None:
            raise RuntimeError(f"Faust завершився з кодом {proc.returncode}, див. {path}")
        if path.exists() and "Ready" in path.read_text(encoding="utf-8", errors="replace"):
            return time.time()
        time.sleep(1)
    raise RuntimeError(f"Faust не став Ready за {timeout} с, див. {path}")


def committed_sum(group: str, topic: str) -> int:
    cons = Consumer({"bootstrap.servers": c.BOOTSTRAP, "group.id": group, "enable.auto.commit": False})
    res = cons.committed([TopicPartition(topic, p) for p in range(c.PARTITIONS)], timeout=15)
    cons.close()
    return sum(max(tp.offset, 0) for tp in res)


def wait_drained(group: str, topic: str, expected: int, timeout: int = 600) -> float | None:
    t0 = time.time()
    while time.time() - t0 < timeout:
        if committed_sum(group, topic) >= expected:
            return time.time()
        time.sleep(1)
    return None


def stop(proc: subprocess.Popen | None):
    if proc and proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(20)
        except subprocess.TimeoutExpired:
            proc.kill()


def ensure_topics(names: list[str], partitions: int = c.PARTITIONS) -> list[str]:
    admin = AdminClient({"bootstrap.servers": c.BOOTSTRAP})
    existing = set(admin.list_topics(timeout=15).topics)
    new = [NewTopic(n, partitions, 1) for n in names if n not in existing]
    for f in admin.create_topics(new).values():
        f.result()
    return names


def pipeline_topics(prefix: str, guarantee: str, run_id: str) -> list[str]:
    app_id = f"solar-kyiv-{guarantee}-{run_id}"
    return [f"{prefix}.{n}" for n in ("telemetry", "events", "windows", "states")] + [
        f"{app_id}-{t}-changelog" for t in ("open_5m_window", "device_state_store")
    ]


def producer_cmd(cycles: int, speedup: float | None = None, throttle: bool = True, info_file: str = "producer_last.json") -> list[str]:
    cmd = [
        sys.executable,
        "producer.py",
        "--cycles",
        str(cycles),
        "--panels",
        str(c.N_PANELS),
        "--info-file",
        info_file,
    ]
    if speedup:
        cmd += ["--speedup", str(speedup)]
    if not throttle:
        cmd += ["--no-throttle"]
    return cmd


# --- Backlog і Fault ---
def backlog(guarantee: str, cycles: int = 20) -> dict:
    run_id = str(int(time.time()))
    prefix = f"bl{run_id}{guarantee[:3]}"
    env = env_for(prefix, guarantee, run_id)
    ensure_topics(pipeline_topics(prefix, guarantee, run_id))
    subprocess.run(
        producer_cmd(cycles, throttle=False, info_file=f"producer_{prefix}.json"),
        env=env,
        cwd=HERE,
        check=True,
        capture_output=True,
    )
    sent = json.loads((OUT / f"producer_{prefix}.json").read_text(encoding="utf-8"))["sent"]
    proc, log = start_faust(env, f"faust_{prefix}.log")
    try:
        wait_ready(proc, log)
        t0 = time.time()
        t1 = wait_drained(f"solar-kyiv-{guarantee}-{run_id}", f"{prefix}.telemetry", sent)
    finally:
        stop(proc)
    res = {
        "guarantee": guarantee,
        "messages": sent,
        "seconds": (t1 - t0) if t1 else None,
        "msg_per_s": sent / (t1 - t0) if t1 else None,
    }
    print(
        f"  Faust backlog {guarantee:<14} {sent:,} повідомлень: "
        + (f"{res['seconds']:.1f} с, {res['msg_per_s']:,.1f} msg/s" if t1 else "НЕ вичерпано за таймаут")
    )
    return res


def cassandra_summary(session) -> dict:
    unique_agg = set()
    unique_thr = set()
    for r in session.execute("SELECT event_type, event_id FROM event_log"):
        if r.event_type == c.EV_AGGREGATE_CREATED:
            unique_agg.add(r.event_id)
        elif r.event_type == c.EV_THRESHOLD_EXCEEDED:
            unique_thr.add(r.event_id)

    window_readings = sum(r.readings for r in session.execute("SELECT readings FROM window_aggregates"))
    state_samples = sum(r.samples_count for r in session.execute("SELECT samples_count FROM device_state"))
    state_panels = sum(1 for _ in session.execute("SELECT panel_id FROM device_state"))

    return {
        "cassandra_aggregate_events": len(unique_agg),
        "cassandra_threshold_events": len(unique_thr),
        "window_readings_sum": window_readings,
        "state_samples_sum": state_samples,
        "state_panels_count": state_panels,
    }


def fault(name: str, guarantee: str, kill: bool, dedupe: bool = True, cycles: int = 24, speedup: int = 10, kill_after: int = 15) -> dict:
    run_id = str(int(time.time()))
    prefix = f"ft{run_id}{name}"
    env = env_for(prefix, guarantee, run_id, dedupe)
    cluster, session = c.connect()
    c.apply_schema(session)
    c.truncate_all(session)
    ensure_topics(pipeline_topics(prefix, guarantee, run_id))

    faust, flog = start_faust(env, f"faust_{prefix}_1.log")
    wait_ready(faust, flog)
    sink_proc = spawn([sys.executable, SINK_SCRIPT, "--group", f"sink-{prefix}"], env, f"sink_{prefix}.log")
    t_start = time.time()
    prod = spawn(producer_cmd(cycles, speedup, info_file=f"producer_{prefix}.json"), env, f"producer_{prefix}.log")

    restart_seconds = None
    if kill:
        time.sleep(kill_after)
        faust.kill()
        t_kill = time.time()
        print(f"  [{name}] Faust-воркер зупинено (SIGKILL) на {t_kill - t_start:.0f} с")
        faust.wait()
        time.sleep(3)
        faust, flog = start_faust(env, f"faust_{prefix}_2.log")
        restart_seconds = wait_ready(faust, flog) - t_kill
        print(f"  [{name}] воркер перезапущено, Ready через {restart_seconds:.1f} с після збою")

    prod.wait()
    info = json.loads((OUT / f"producer_{prefix}.json").read_text(encoding="utf-8"))
    sent = info["sent"]
    t_drained = wait_drained(f"solar-kyiv-{guarantee}-{run_id}", f"{prefix}.telemetry", sent)

    last, stable = -1, 0
    while stable < 10 and time.time() - t_start < 900:
        n = (
            sum(1 for _ in session.execute("SELECT event_id FROM event_log"))
            + sum(r.readings for r in session.execute("SELECT readings FROM window_aggregates"))
        )
        stable = stable + 1 if n == last else 0
        last = n
        time.sleep(1)

    stop(sink_proc)
    stop(faust)

    kafka = verify_events(f"{prefix}.events", sent)
    cass = cassandra_summary(session)
    cluster.shutdown()

    res = {
        "name": name,
        "guarantee": guarantee,
        "killed": kill,
        "dedupe": dedupe,
        "sent": sent,
        "restart_ready_seconds": restart_seconds,
        "drained": bool(t_drained),
        "kafka_events": kafka,
        "cassandra": cass,
        "window_readings_error": cass["window_readings_sum"] - sent,
    }
    print(
        f"  [{name}] {guarantee}: Kafka втрачено={kafka['lost']}, дублікатів={kafka['duplicates']}; "
        f"AGGREGATE_CREATED={cass['cassandra_aggregate_events']}, "
        f"THRESHOLD_EXCEEDED={cass['cassandra_threshold_events']}; "
        f"вікна Σreadings={cass['window_readings_sum']} (похибка {res['window_readings_error']:+d})"
    )
    return res


# --- Бенчмарк семантик ---
def process_bench_msg(raw: bytes, window_state: dict) -> list[tuple[str, dict]]:
    r = json.loads(raw)
    ws = c.window_start(r["event_ts"])
    kwh = c.energy_kwh(r["power_output"], c.INTERVAL_S)
    key = (r["panel_id"], ws)
    w = window_state.setdefault(key, {"count": 0, "sum_p": 0.0, "min_p": 1e18, "max_p": 0.0, "kwh": 0.0})
    w["count"] += 1
    w["sum_p"] += r["power_output"]
    w["min_p"] = min(w["min_p"], r["power_output"])
    w["max_p"] = max(w["max_p"], r["power_output"])
    w["kwh"] += kwh

    ev_id = f"{r['panel_id']}-w{ws}-s{r['seq']}-AGG"
    out = [(
        r["panel_id"],
        {
            "event_id": ev_id,
            "event_type": c.EV_AGGREGATE_CREATED,
            "panel_id": r["panel_id"],
            "zone": r["zone"],
            "window_start": ws,
            "window_end": ws + c.WINDOW_S,
            "energy_kwh": kwh,
            "event_ts": r["event_ts"],
        },
    )]
    if r["power_output"] > c.POWER_THRESHOLD_KW:
        out.append((
            r["panel_id"],
            {
                "event_id": f"{r['panel_id']}-w{ws}-s{r['seq']}-THR",
                "event_type": c.EV_THRESHOLD_EXCEEDED,
                "panel_id": r["panel_id"],
                "zone": r["zone"],
                "window_start": ws,
                "window_end": ws + c.WINDOW_S,
                "power_output": r["power_output"],
                "event_ts": r["event_ts"],
            },
        ))
    return out


def sem_worker(mode: str, in_topic: str, out_topic: str, group: str, batch: int = 500, crash_at: int | None = None) -> dict:
    cons = Consumer({
        "bootstrap.servers": c.BOOTSTRAP,
        "group.id": group,
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
    })
    cons.subscribe([in_topic])

    pconf = {"bootstrap.servers": c.BOOTSTRAP, "linger.ms": 5, "compression.type": "lz4"}
    if mode == "at_most_once":
        pconf.update({"acks": 0, "enable.idempotence": False})
    elif mode == "at_least_once":
        pconf.update({"acks": "all", "enable.idempotence": False, "retries": 5})
    else:
        pconf.update({"transactional.id": f"solar-bench-{group}", "enable.idempotence": True, "acks": "all"})

    producer = Producer(pconf)
    if mode == "exactly_once":
        producer.init_transactions()

    carry = []
    while not cons.assignment():
        m = cons.poll(0.2)
        if m is not None and m.error() is None:
            carry.append(m)

    parts = cons.assignment()
    hw = {tp.partition: cons.get_watermark_offsets(tp, timeout=15)[1] for tp in parts}
    nxt = {tp.partition: max(tp.offset, 0) for tp in cons.committed(parts, timeout=15)}

    def finished():
        return all(nxt[p] >= hw[p] for p in hw)

    window_state, lat, processed, b = {}, [], 0, 0
    t_start = None
    while True:
        msgs = carry + [m for m in cons.consume(batch, timeout=1.0) if m.error() is None]
        carry = []
        if not msgs:
            if finished():
                break
            continue
        t0 = time.perf_counter()
        t_start = t_start or t0
        b += 1
        crash = crash_at is not None and b == crash_at
        offs = {}
        for m in msgs:
            offs[m.partition()] = m.offset() + 1
        tps = [TopicPartition(in_topic, p, o) for p, o in offs.items()]

        if mode == "at_most_once":
            cons.commit(offsets=tps, asynchronous=False)
            if crash:
                os._exit(17)
        if mode == "exactly_once":
            producer.begin_transaction()

        for m in msgs:
            for key, ev in process_bench_msg(m.value(), window_state):
                while True:
                    try:
                        producer.produce(out_topic, key=key, value=json.dumps(ev, ensure_ascii=False))
                        break
                    except BufferError:
                        producer.poll(0.05)

        if mode == "at_least_once":
            producer.flush()
            if crash:
                os._exit(17)
            cons.commit(offsets=tps, asynchronous=False)
        elif mode == "exactly_once":
            producer.send_offsets_to_transaction(tps, cons.consumer_group_metadata())
            if crash:
                os._exit(17)
            producer.commit_transaction()
        else:
            producer.flush()

        lat.append((time.perf_counter() - t0) * 1000)
        processed += len(msgs)
        for p, o in offs.items():
            nxt[p] = o

    elapsed = time.perf_counter() - t_start if t_start else 0.0
    cons.close()
    lat_sorted = sorted(lat)
    pct = lambda q: lat_sorted[min(len(lat_sorted) - 1, int(q * len(lat_sorted)))] if lat_sorted else 0.0
    return {
        "mode": mode,
        "processed": processed,
        "seconds": elapsed,
        "msg_per_s": processed / elapsed if elapsed else 0.0,
        "batches": len(lat),
        "batch_ms_avg": statistics.mean(lat) if lat else 0.0,
        "batch_ms_p50": pct(0.50),
        "batch_ms_p95": pct(0.95),
        "batch_ms_p99": pct(0.99),
    }


def verify_events(out_topic: str, expected: int) -> dict:
    cons = Consumer({
        "bootstrap.servers": c.BOOTSTRAP,
        "group.id": f"verify-{out_topic}-{time.time()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
        "isolation.level": "read_committed",
        "enable.partition.eof": True,
    })
    cons.subscribe([out_topic])
    early = []
    while not cons.assignment():
        m = cons.poll(0.2)
        if m is not None:
            early.append(m)
    pending = {tp.partition for tp in cons.assignment()}
    total, ids = 0, set()
    deadline, idle = time.time() + 120, 0
    while (pending or idle < 4 or early) and time.time() < deadline:
        if early:
            m = early.pop(0)
        else:
            m = cons.poll(1.0)
        if m is None:
            idle += 1
            continue
        if m.error():
            if m.error().code() == KafkaError._PARTITION_EOF:
                pending.discard(m.partition())
            continue
        idle = 0
        d = json.loads(m.value())
        if d["event_type"] == c.EV_AGGREGATE_CREATED:
            total += 1
            ids.add(d["event_id"])
    cons.close()
    return {
        "aggregate_events": total,
        "unique": len(ids),
        "duplicates": total - len(ids),
        "lost": max(0, expected - len(ids)),
        "expected": expected,
    }


def run_semantics(cycles: int = 20, reps: int = 3, crash_batch: int = 5) -> dict:
    OUT.mkdir(exist_ok=True)
    run_id = int(time.time())
    in_topic = f"{BENCH_IN_TOPIC}.{run_id}"
    ensure_topics([in_topic])
    info = prod_mod.run(cycles, throttle=False, n_panels=c.N_PANELS, topic=in_topic, quiet=True)
    n = info["sent"]
    result = {"input_messages": n, "performance": {}, "crash": {}}

    for mode in MODES:
        runs = []
        for rep in range(reps):
            out = f"solar.bench.out.{mode}.{run_id}.perf{rep}"
            ensure_topics([out])
            runs.append(sem_worker(mode, in_topic, out, f"solar-bench-{mode}-{run_id}-perf{rep}"))
        avg = lambda k: statistics.mean(r[k] for r in runs)
        result["performance"][mode] = {
            "runs": runs,
            "msg_per_s": avg("msg_per_s"),
            "batch_ms_p50": avg("batch_ms_p50"),
            "batch_ms_p95": avg("batch_ms_p95"),
            "batch_ms_p99": avg("batch_ms_p99"),
        }

    for mode in MODES:
        out = f"solar.bench.out.{mode}.{run_id}.crash"
        group = f"solar-bench-{mode}-{run_id}-crash"
        ensure_topics([out])
        cmd = [
            sys.executable,
            __file__,
            "--sem-worker",
            mode,
            "--in-topic",
            in_topic,
            "--out-topic",
            out,
            "--group",
            group,
            "--crash-at",
            str(crash_batch),
        ]
        first = subprocess.run(cmd, capture_output=True, text=True)
        second = subprocess.run([x for x in cmd if x not in ("--crash-at", str(crash_batch))], capture_output=True, text=True)
        v = verify_events(out, n)
        v.update({"first_exit_code": first.returncode, "restart_exit_code": second.returncode})
        result["crash"][mode] = v

    (OUT / "semantics.json").write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")
    return result


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("what", choices=["setup", "backlog", "fault", "semantics", "all"], nargs="?", default="all")
    ap.add_argument("--only", default=None)
    ap.add_argument("--out", default="experiment.json")
    ap.add_argument("--sem-worker", choices=MODES)
    ap.add_argument("--in-topic")
    ap.add_argument("--out-topic")
    ap.add_argument("--group")
    ap.add_argument("--crash-at", type=int)
    ap.add_argument("--cycles", type=int, default=20)
    ap.add_argument("--reps", type=int, default=3)
    a = ap.parse_args()

    if a.sem_worker:
        print(json.dumps(sem_worker(a.sem_worker, a.in_topic, a.out_topic, a.group, crash_at=a.crash_at)))
        return

    OUT.mkdir(exist_ok=True)
    cluster, session = c.connect(keyspace=None)
    c  .apply_schema(session)
    cluster.shutdown()

    if a.what == "setup":
        print("Схему solar_kyiv створено")
        return

    path = OUT / a.out
    result = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {}

    if a.what in ("backlog", "all"):
        result["backlog"] = [backlog("exactly_once"), backlog("at_least_once")]
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    if a.what in ("fault", "all"):
        scenarios = {
            "alo_kill": ("at_least_once", True, False),
            "eos_kill": ("exactly_once", True, False),
            "eos_kill_dedupe": ("exactly_once", True, True),
            "eos_clean": ("exactly_once", False, False),
        }
        names = a.only.split(",") if a.only else list(scenarios)
        merged = {f["name"]: f for f in result.get("fault", [])}
        for n in names:
            merged[n] = fault(n, *scenarios[n])
        order = list(scenarios)
        result["fault"] = sorted(merged.values(), key=lambda f: order.index(f["name"]) if f["name"] in order else 99)
        path.write_text(json.dumps(result, ensure_ascii=False, indent=2), encoding="utf-8")

    if a.what in ("semantics", "all"):
        run_semantics(a.cycles, a.reps)


if __name__ == "__main__":
    main()