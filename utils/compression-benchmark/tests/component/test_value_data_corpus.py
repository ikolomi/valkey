"""B1 Tier-2 test for R8.1 (``--value-data corpus:FILE``).

Corpus mode is a separate, mutually-exclusive command-generation path: when
``--value-data corpus:FILE`` is given, valkey-benchmark loads the corpus once
(orchestrator format ``[4-byte BE len][bytes]*``) and, per SET request, builds the
command from the next corpus entry (round-robin) instead of the baked fixed-size
value. This test verifies, against the built binary, that the values actually
stored come from the corpus (varied, not one baked value) and are compressible.
Needs valkey-server + valkey-benchmark (with the B1 change).
"""

import socket
import subprocess
import zlib

import pytest

from lib import config, corpus, env, server

pytestmark = [pytest.mark.needs_server, pytest.mark.needs_benchmark]


def _free_port():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _dm():
    return config.DataModel(
        value_shape="kv",
        value_size_distribution=("constant", 512),
        value_size_min=1,
        value_size_max=16384,
        seed=1234,
        corpus_entries=200,
        key_count=500,
        key_distribution=("uniform",),
    )


def test_value_data_corpus_stores_corpus_members(tmp_path):
    binpath = env.server_binary_path()
    bench = env.benchmark_binary_path()
    dm = _dm()
    n = dm.key_count

    blobs = corpus.generate_blobs(dm)
    members = {b.decode("utf-8") for b in blobs}
    corpus_file = tmp_path / "corpus.bin"
    corpus.write_corpus(blobs, str(corpus_file))

    with server.Server(binpath, str(tmp_path), "corpus", _free_port()) as srv:
        srv.flushall()
        r = subprocess.run(
            [bench, "-h", "127.0.0.1", "-p", str(srv.port), "-t", "set",
             "-r", str(n), "-n", str(n), "--sequential",
             "--value-data", "corpus:%s" % corpus_file, "-q"],
            capture_output=True, text=True, timeout=120,
        )
        assert r.returncode == 0, r.stderr
        assert srv.dbsize() == n, "sequential corpus SET must still cover the keyspace exactly"

        # Sample stored values across the keyspace; each must be a corpus member,
        # and we must see VARIETY (not a single baked value).
        seen = set()
        sample_keys = [0, 1, 2, n // 2, n - 2, n - 1]
        for i in sample_keys:
            v = srv.cli("get", "key:%012d" % i)
            assert v in members, "stored value at key %d is not a corpus member" % i
            seen.add(v)
        assert len(seen) > 1, "all sampled values identical — corpus variety not applied"

        # The corpus is compressible (kv/json text) — at least one sample shrinks.
        sample = srv.cli("get", "key:%012d" % 0).encode("utf-8")
        assert len(zlib.compress(sample)) < len(sample), "corpus value not compressible"
