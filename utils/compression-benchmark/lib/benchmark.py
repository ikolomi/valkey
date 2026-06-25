"""Loader process management + connection/TPS split math.

A3 implements the pure split math (R5 / Q8e). Process spawning, FIFO barriers, and
the record-start signal land in Phase C. Stdlib only for the A3 part.
"""

from __future__ import annotations

import math
import os
import re
import subprocess
import time


def split_processes(commands, connections_total, max_clients_per_process, target_tps):
    """Split a command mix into loader processes (R5).

    Each command *c* with ratio *r* gets ``round(r*connections_total)`` connections
    (min 1) and ``r*target_tps`` rps; that share is spread over
    ``ceil(connections_c / max_clients_per_process)`` single-threaded processes,
    connections distributed as evenly as possible, each process driven at
    ``tps_c / procs`` rps.

    Returns a list of ``{"command", "n_procs", "processes": [{"connections","rps"}]}``.
    """
    out = []
    for c in commands:
        ratio = c.ratio
        conns_c = max(1, round(ratio * connections_total))
        tps_c = ratio * target_tps
        procs = max(1, math.ceil(conns_c / max_clients_per_process))
        base, rem = divmod(conns_c, procs)
        rps = tps_c / procs
        processes = [
            {"connections": base + (1 if k < rem else 0), "rps": rps}
            for k in range(procs)
        ]
        out.append({"command": c.type, "n_procs": procs, "processes": processes})
    return out


# --------------------------------------------------------------------------- #
# Loader argv builders (pure) — compose existing valkey-benchmark flags.
# Populate uses exact coverage (`--sequential -r N -n N`); the open-loop measure
# uses `--rps`/`--duration`/`-c`. No new benchmark flags (B0/B2 already in-tree).
# --------------------------------------------------------------------------- #

_VALUE_BEARING = {"set", "mset"}


def populate_argv(benchmark, host, port, key_count, datasize=3, value_corpus=None):
    """Exact, full keyspace populate: SET keys 0..key_count-1 each once (R8.2).

    With ``value_corpus`` (a corpus file path) the values come from the corpus
    (compressible, variable-length) via ``--value-data corpus:FILE`` (B1) instead
    of fixed ``-d`` random bytes."""
    argv = [
        benchmark, "-h", str(host), "-p", str(port),
        "-t", "set", "-r", str(key_count), "-n", str(key_count), "--sequential",
    ]
    if value_corpus:
        argv += ["--value-data", "corpus:%s" % value_corpus]
    else:
        argv += ["-d", str(datasize)]
    argv += ["-q"]
    return argv


def loader_argv(benchmark, host, port, command, connections, rps, duration,
                key_count, datasize=3, pipeline=1, value_corpus=None,
                key_dist=None, record_start_signal=None):
    """One open-loop, duration-bounded loader process for a single command type.

    Optional compression-mode knobs: ``value_corpus`` (corpus-backed values for
    value-bearing commands, B1), ``key_dist`` = ``("zipf", theta)`` for a hotset
    (B3), ``record_start_signal`` (windowed recording — the measured window starts
    on the signal, B4; ``--duration`` then bounds it)."""
    argv = [
        benchmark, "-h", str(host), "-p", str(port),
        "-t", command, "-r", str(key_count), "-c", str(connections),
        "-P", str(pipeline),
    ]
    if value_corpus and command in _VALUE_BEARING:
        argv += ["--value-data", "corpus:%s" % value_corpus]
    else:
        argv += ["-d", str(datasize)]
    if key_dist and key_dist[0] == "zipf":
        argv += ["--key-distribution", "zipf", "--zipf-theta", str(key_dist[1])]
    if record_start_signal:
        argv += ["--record-start-signal", str(int(record_start_signal))]
    argv += ["--rps", str(max(1, int(round(rps)))), "--duration", str(duration), "-q"]
    return argv


def build_load_argvs(benchmark, host, port, split_result, duration, key_count,
                     datasize=3, pipeline=1, value_corpus=None, key_dist=None,
                     record_start_signal=None):
    """Flatten a :func:`split_processes` result into per-process loader specs."""
    out = []
    for entry in split_result:
        for idx, proc in enumerate(entry["processes"]):
            out.append({
                "command": entry["command"],
                "index": idx,
                "argv": loader_argv(benchmark, host, port, entry["command"],
                                    proc["connections"], proc["rps"], duration,
                                    key_count, datasize, pipeline,
                                    value_corpus=value_corpus, key_dist=key_dist,
                                    record_start_signal=record_start_signal),
            })
    return out



# --------------------------------------------------------------------------- #
# Loader orchestration: FIFO start-barrier + spawn + artifact collection
# (R5.3, R6.4, §3.3). amz-orc pattern: each loader blocks on a FIFO read, then
# execs the benchmark; one write per loader releases them ~simultaneously.
# --------------------------------------------------------------------------- #

_RPS_RE = re.compile(r"([0-9]+\.?[0-9]*)\s+requests per second")


def parse_achieved_rps(text: str):
    """Parse the achieved rps from valkey-benchmark `-q` output, or None."""
    m = _RPS_RE.search(text or "")
    return float(m.group(1)) if m else None


def spawn_loaders(loader_specs, work_dir, fifo_name="start.fifo", barrier_delay=0.3):
    """Spawn all loaders behind a FIFO start-barrier and release them ~simultaneously.

    Returns a list of per-loader handles (dicts). For the off path the caller
    immediately calls :func:`collect_loaders`. For windowed (compression-on)
    measurement the caller releases here, polls the plateau, calls
    :func:`record_start_loaders` to begin the measured window, then collects.
    """
    os.makedirs(work_dir, exist_ok=True)
    fifo_path = os.path.join(work_dir, fifo_name)
    if os.path.exists(fifo_path):
        os.remove(fifo_path)
    os.mkfifo(fifo_path)

    spawned = []
    for spec in loader_specs:
        base = f"loader-{spec['command']}-{spec['index']}"
        out_path = os.path.join(work_dir, base + ".stdout")
        err_path = os.path.join(work_dir, base + ".stderr")
        out_fh = open(out_path, "w")
        err_fh = open(err_path, "w")
        # sh wrapper: block on the FIFO, then exec the loader (so the proc PID is the
        # benchmark itself — record_start_loaders can signal it directly).
        cmd = ["sh", "-c", 'read _ < "$1"; shift; exec "$@"', "sh", fifo_path] + spec["argv"]
        proc = subprocess.Popen(cmd, stdout=out_fh, stderr=err_fh)
        spawned.append({"spec": spec, "proc": proc, "out_path": out_path,
                        "err_path": err_path, "out_fh": out_fh, "err_fh": err_fh})

    # Let every loader reach its blocking read, then release the barrier.
    time.sleep(barrier_delay)
    with open(fifo_path, "w") as w:
        for _ in loader_specs:
            w.write("go\n")
        w.flush()
    os.remove(fifo_path)
    return spawned


def record_start_loaders(spawned, signum):
    """Fire the record-start signal at every loader so they reset their histograms
    and begin the measured window ~simultaneously (B4 windowed recording)."""
    for s in spawned:
        try:
            s["proc"].send_signal(int(signum))
        except Exception:
            pass


def collect_loaders(spawned, timeout_slack=15):
    """Wait for each loader to self-exit (duration-bounded) and collect artifacts."""
    results = []
    for s in spawned:
        proc = s["proc"]
        a = s["spec"]["argv"]
        dur = int(a[a.index("--duration") + 1]) if "--duration" in a else 0
        try:
            proc.wait(timeout=dur + timeout_slack)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        s["out_fh"].close()
        s["err_fh"].close()
        with open(s["out_path"], "r") as f:
            stdout_text = f.read()
        results.append({
            "command": s["spec"]["command"],
            "index": s["spec"]["index"],
            "returncode": proc.returncode,
            "stdout_path": s["out_path"],
            "stderr_path": s["err_path"],
            "achieved_rps": parse_achieved_rps(stdout_text),
        })
    return results


def run_loaders(loader_specs, work_dir, fifo_name="start.fifo",
                barrier_delay=0.3, timeout_slack=15):
    """Off path: spawn behind the FIFO start-barrier, release, wait for self-exit
    (duration-bounded), and collect per-loader artifacts. No record-start signal."""
    spawned = spawn_loaders(loader_specs, work_dir, fifo_name, barrier_delay)
    return collect_loaders(spawned, timeout_slack)
