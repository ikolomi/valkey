"""B4 Tier-2 test for R8.3 (windowed recording via ``--record-start-signal``).

The measurement window must start on a signal (the orchestrator sends it at the
compression plateau), not on a fixed timer — the plateau time is unpredictable.
With ``--record-start-signal <SIGNUM>`` the loader starts in warmup and stays there
until the signal arrives; then it resets stats and measures for ``--duration``
seconds. This test verifies the gating behaviorally: the process does NOT finish
on ``--duration`` while still in warmup, and finishes ~``--duration`` after the
signal. Needs valkey-server + valkey-benchmark (with the B4 change).
"""

import signal
import socket
import subprocess
import time

import pytest

from lib import env, server

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def test_record_start_signal_gates_measurement_window(tmp_path):
    bench = env.benchmark_binary_path()
    binpath = env.server_binary_path()
    sig = int(signal.SIGUSR1)
    duration = 2

    with server.Server(binpath, str(tmp_path), "rec", _free_port()) as srv:
        srv.flushall()
        proc = subprocess.Popen(
            [bench, "-h", "127.0.0.1", "-p", str(srv.port), "-t", "set",
             "-r", "1000", "--rps", "5000",
             "--record-start-signal", str(sig), "--duration", str(duration), "-q"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
        )
        try:
            # Signal mode: it must stay in warmup (running) past --duration, waiting
            # for the signal — i.e., the measured window has not started yet.
            time.sleep(duration + 1.5)
            assert proc.poll() is None, (
                "benchmark exited before the record-start signal — window not gated "
                "(stderr: %s)" % (proc.stderr.read() if proc.stderr else "")
            )

            # Fire the signal: the measured window begins and runs for --duration.
            t0 = time.time()
            proc.send_signal(signal.SIGUSR1)
            out, err = proc.communicate(timeout=duration + 20)
            elapsed = time.time() - t0

            assert proc.returncode == 0, err
            # Finished ~duration after the signal (not instantly, not much longer):
            # proves the window started at the signal and was bounded by --duration.
            assert elapsed >= duration - 0.5, "finished too soon after signal: %.2fs" % elapsed
            assert elapsed <= duration + 10, "did not finish near --duration after signal: %.2fs" % elapsed
        finally:
            if proc.poll() is None:
                proc.kill()
                proc.communicate()


def test_warmup_and_record_start_signal_are_mutually_exclusive():
    """They are two ways to define the same warmup->measure boundary; combining
    them is contradictory, so it must be rejected (cf. -n / --duration)."""
    bench = env.benchmark_binary_path()
    sig = str(int(signal.SIGUSR1))
    for args in (
        ["--warmup", "5", "--record-start-signal", sig],
        ["--record-start-signal", sig, "--warmup", "5"],
    ):
        r = subprocess.run([bench] + args + ["-t", "set", "-n", "1"],
                           capture_output=True, text=True, timeout=30)
        assert r.returncode != 0, "expected rejection for args %r" % args
        assert "mutually exclusive" in r.stderr.lower(), r.stderr
