"""P1 Tier-1 tests for lib/provenance.py (R9.1)."""

import hashlib

from lib import provenance


def test_sha256_file(tmp_path):
    p = tmp_path / "x.bin"
    p.write_bytes(b"hello world" * 1000)
    expected = "sha256:" + hashlib.sha256(p.read_bytes()).hexdigest()
    assert provenance.sha256_file(str(p)) == expected


def test_machine_info():
    info = provenance.machine_info()
    assert info["cpus"] and info["cpus"] >= 1
    assert info["platform"]
    assert "mem_total_bytes" in info


def test_build_provenance(tmp_path):
    server = tmp_path / "valkey-server"
    bench = tmp_path / "valkey-benchmark"
    corpus = tmp_path / "corpus.bin"
    for f, data in [(server, b"S"), (bench, b"B"), (corpus, b"C" * 50)]:
        f.write_bytes(data)

    prov = provenance.build_provenance(
        server_binary=str(server), benchmark_binary=str(bench),
        seed=1234, corpus_path=str(corpus), config_echo={"description": "x"},
    )
    assert prov["seed"] == 1234
    assert prov["binary_checksums"]["valkey-server"] == provenance.sha256_file(str(server))
    assert prov["binary_checksums"]["valkey-benchmark"] == provenance.sha256_file(str(bench))
    assert prov["corpus"]["sha256"] == provenance.sha256_file(str(corpus))
    assert prov["machine"]["cpus"] >= 1
    assert prov["config"] == {"description": "x"}
    assert "timestamp" in prov


def test_start_mpstat_absent_returns_none(monkeypatch, tmp_path):
    monkeypatch.setattr(provenance.shutil, "which", lambda _name: None)
    assert provenance.start_mpstat(str(tmp_path / "mpstat.log")) is None
    provenance.stop_mpstat(None)  # no-op, must not raise
