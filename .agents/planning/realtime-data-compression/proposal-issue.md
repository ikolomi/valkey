**The problem/use-case that the feature addresses**

Memory cost is a major constraint for a meaningful subset of Valkey workloads. While many deployments are not memory-bound, there are production workloads where resident dataset size is the primary scaling limiter and where reducing memory footprint could directly improve cost efficiency and dataset density.

This is becoming more important in the context of rising RAM costs, which increase the operational penalty of running large in-memory datasets. As observed during our (AWS ElastiCache) internal analysis, the economic cost of memory has become a stronger motivator for techniques that improve memory efficiency, especially for workloads already operating close to memory limits.

Internal AWS analysis of large production datasets indicates that some memory-constrained workloads have substantial compression savings of roughly 74%, suggesting that similar data shapes often contain significant redundancy.

Today, users who want to reduce memory usage must rely on application-level compression, which has several drawbacks:

- it complicates application logic
- it reduces transparency and debuggability
- it prevents server-side awareness of the compressed representation
- it is difficult to apply consistently across existing applications and clients

This creates a gap for workloads that would benefit from a native datastore-level compression capability.

**Description of the feature**

We would like Valkey to support optional inline compression of in-memory values for suitable workloads.

At a high level, the feature would allow values to be stored internally in compressed form and transparently decompressed when accessed or modified. The goal is to reduce memory usage while preserving the existing application-facing data model and command semantics as much as possible.

The initial scope could focus on workload shapes where compression is most likely to provide meaningful benefit, rather than trying to compress everything indiscriminately. For example, the implementation may choose to apply compression only above certain object-size thresholds or only for selected internal object types.

Important properties of the feature would include:

- transparent operation from the application point of view
- opt-in behavior, so users can enable it only for appropriate workloads
- bounded CPU overhead, with clear tradeoffs between memory savings and latency
- observability, including metrics for compressed bytes, uncompressed bytes, compression ratio, and compression/decompression activity
- compatibility with persistence and replication behavior, or at minimum clearly documented interaction with them

The purpose is not to replace application-level compression in every case, but to provide a native option for workloads where memory efficiency is more important than the additional CPU cost.

**Alternatives you've considered**

1. **Application-level or client-side compression**

This is the main alternative used today. In this model, the application or client library compresses values before writing them to Valkey and decompresses them after reading them.

Some client libraries and frameworks already provide building blocks for this approach. For example, PhpRedis supports compression options such as LZF, LZ4, and ZSTD when compiled with the corresponding support. Spring Data Valkey also exposes serialization abstractions that allow applications to transform values to and from byte arrays, which can be used to implement compressed value serializers.

This approach is useful and should remain a valid option, but it is not a complete replacement for native server-side compression:

  * It pushes compression policy into every application and every language client. In heterogeneous fleets, this makes behavior harder to deploy consistently.
  * It reduces transparency. Values stored in Valkey are opaque compressed blobs from the server point of view, making debugging, inspection, interoperability, and migration more difficult.
  * It prevents Valkey from making compression-aware decisions. The server cannot observe the original value size, compression ratio, object type, hotness, memory accounting, or decompression cost.
  * It does not compose well with server-side features that operate on native data structures or values. Once the client stores compressed blobs, Valkey can no longer efficiently apply normal semantics to the original value without application-specific knowledge.
  * Dictionary management is difficult to centralize. A client-side dictionary approach requires consistent dictionary distribution, versioning, compatibility handling, and rollout coordination across clients and applications.
  * It is difficult to introduce transparently for existing applications, because every writer and reader must agree on the same compression format and configuration.

For these reasons, client-side compression is a useful workaround for some applications, but it does not provide the same operational model as a native Valkey feature that can apply compression transparently and consistently inside the server.

2. **Tiered storage / flash-backed storage**

Another alternative is tiered storage, where keys remain in memory while values can be moved to a lower-cost storage tier such as local SSD or Flash. This model exists in managed Redis-compatible offerings. For example, AWS ElastiCache data tiering uses R6gd nodes to tier data between memory and local SSD, and Azure Enterprise Flash uses both RAM and NVMe Flash storage.

Tiered storage is related to inline compression because both try to reduce the effective cost of large datasets, but it is not a substitute for native in-memory compression:

  * Tiered storage is not currently a Valkey OSS feature. Relying on it would require users to use a specific managed-service implementation or a downstream fork rather than a portable upstream Valkey capability.
  * It changes the memory/latency tradeoff differently. Tiered storage can reduce DRAM pressure by moving values to SSD, but accessing cold values requires bringing them back from storage, which can introduce materially higher latency than in-memory access.
  * It is best suited for workloads with a clear hot/cold split. Workloads with uniform access patterns, write-heavy behavior, or frequently accessed large working sets are less ideal for tiered storage.
  * It does not reduce the in-memory size of hot values. Hot values that must remain resident in DRAM still consume their full uncompressed memory footprint.
  * It usually depends on specific hardware or instance families with local SSD / Flash capacity. Inline compression can be useful on standard memory-backed deployments where no storage tier exists.
  * It is orthogonal to compression. A deployment could potentially use tiered storage and compression together: tiering decides whether a value is placed in DRAM or a lower storage tier, while compression reduces the number of bytes needed to store the value.

Therefore, tiered storage addresses a different part of the memory-cost problem. It can be an excellent option for cold-data-heavy workloads, but it does not remove the need for an in-memory compression capability for workloads where values should remain in RAM but can be represented more compactly.

3. **External memory reduction strategies**

Users can reduce memory consumption by resizing values, changing schemas, shortening TTLs, deleting unused data, or scaling out to more shards. These approaches can help, but they do not solve the specific problem of highly compressible datasets that still need to remain in memory and preserve their current data model.


**Risks and limitations**

Inline compression is not expected to be a universal win. A design should explicitly account for the following limitations:

  * Compression efficiency is workload-dependent. Compression ratio depends on value size, data shape, redundancy, object type, selected algorithm, and whether dictionaries are used. Valkey should not guarantee a specific compression ratio.
  * Compression introduces CPU overhead. Compressing values on write/background paths and decompressing values on read/modify paths consumes CPU and can increase latency, especially under high load.
  * The feature is primarily useful for memory-constrained workloads. If a deployment is CPU-bound, network-bound, or has little compressible data, inline compression may provide limited benefit or may reduce performance.
  * Small values may not benefit enough to justify compression overhead. A practical implementation will likely need minimum-size thresholds and/or object-type eligibility rules.
  * Hot-value handling is important. Frequently accessed values may need to remain uncompressed, be temporarily cached in decompressed form, or be skipped by compression policy to avoid repeated decompression cost.
  * Observability is required. Users need metrics such as compressed bytes, estimated uncompressed bytes, compression ratio, compression/decompression counts, skipped values, CPU impact, and latency impact in order to decide whether the feature is beneficial for their workload.
  * Persistence, replication, eviction, memory accounting, active defragmentation, and module interactions require careful design. The feature should preserve Valkey command semantics and should document any non-obvious behavior.


**Possible implementation phases**

Bringing this issue forward with the current state of the [`detailed-design.md`](https://github.com/ikolomi/valkey/blob/unstable/.agents/planning/realtime-data-compression/design/detailed-design.md)
we propose a 2-stage delivery: v1 covers the basic functionality and proceeds conservatively, while v2 targets optimizations and more ambitious extensions:

| Capability | v1 | v2 (planned, non-breaking additions) |
|---|---|---|
| Compression for `OBJ_STRING` values (RAW encoding) | ✓ | — |
| Compression for HASH / ZSET / SET / LIST / STREAM | — | ✓ (encoding-tag design supports) |
| Algorithm: ZSTD + trained dictionary | ✓ | — |
| Algorithm-tag header (forward-compat for additional backends) | ✓ reserved | LZ4 / snappy / hardware emitted |
| Synchronous decompression on main thread | ✓ | ✓ (mandatory for scripts / EXEC / replication feed / AOF) |
| Asynchronous decompression | — | ✓ opt-in via `compression-async-decompress-threshold` |
| Dictionary self-training (background, on `bio` thread) | ✓ | — |
| Preshared dictionary import / export | ✓ (`COMPRESSION DICT IMPORT/EXPORT`) | cluster-wide dictionary gossip |
| Disk RDB compressed | ✓ (`RDB_ENC_COMPRESSED`) | — |
| Full-sync replication RDB | uncompressed | compressed via `REPLCONF compression yes` negotiation |
| AOF | uncompressed RESP | uncompressed RESP (no plan to change) |
| `DUMP` / `RESTORE` / `MIGRATE` | decompress before emit | compressed-in-place with dict prelude |
| Adaptive kill-switch (auto-disable on negative net savings) | — (pure observability) | opt-in via `compression-kill-switch` |
| Decompressed-view cache for hot compressed keys | — | opt-in extension |

Defaults are tuned for the common case sweet spot. Operators outside that profile can disable the feature, narrow eligibility via `compression-min-value-size` / `compression-max-value-size`, or wait for v2 capabilities. The full v1 ↔ v2 split is also captured in the design doc Appendix D.

**The case for synchronous decompression in v1**

A concern that comes up early in any in-memory compression design is that wide multi-key commands (`MGET` / `SORT` / scripts touching many compressed values) could pay a cumulative main-thread decompression stall. The concern is fair; v1 deliberately ships sync-only because:

1. **POC math.** Worker round-trip is ~5–15 µs vs. ZSTD-with-dict decompression at ~1 µs/KB. Async is a net loss below ~8 KB, which is the documented sweet spot for the dictionary-based approach.

2. **Sync is mandatory in four code paths regardless** — scripts (`EVAL` / `EVALSHA` / `FCALL` / `FCALL_RO`), transactions (queued `MULTI` / `EXEC`), the replication feed, and the AOF writer. Async would be additional code *on top of*, not replacing, sync. Two code paths means two test matrices, a client-yielding state machine, and worker-pool priority scheduling for sweep vs. compress vs. decompress.

3. **Worst case per value is bounded; per command depends on eligibility.** `compression-max-value-size` (default `131072` = 128 KiB) caps per-value sync decompression at ~128 µs. There is no per-command cap by design — a per-command cap would either break transparency (mid-command error after some keys had already decompressed) or defeat itself (block the main thread until everything decompresses anyway). Multi-key commands therefore pay the sum of per-value costs at ~1 µs/KB per compressed value. Operators concerned about a specific wide-multi-key workload have two levers: **lower** `compression-max-value-size` to keep large values uncompressed (typically the dominant contributor to per-command totals), or **raise** `compression-min-value-size` so the small values populating those reads stay uncompressed and contribute zero decompression cost. Both levers trade some memory benefit for tighter latency. Workloads where all eligible values are also frequently read in wide multi-key commands aren't a v1 target — see point 4.

4. **The feature is opt-in.** Workloads that read many large values per command are not a v1 target and would not enable it; their `INFO compression` shows zero compressed objects. The v1 target is the common case identified by the analysis: memory-constrained workloads with compressible values in the small-to-medium size range, rather than workloads dominated by wide multi-key reads over large values.

Async decompression is **planned for v2**, reserved as a non-breaking addition: a new config `compression-async-decompress-threshold` with default `0` (disabled). Sync below threshold, async above. The threshold can then be tuned from production data rather than POC speculation. Existing operators see no behavior change.


**Summary**

This request is motivated by both technical and economic considerations.

From a technical perspective, some production workloads appear to contain substantial compression headroom, and compression results from related production data paths suggest that the opportunity is practical rather than merely theoretical.

From an economic perspective, rising RAM costs make memory-efficiency improvements increasingly valuable for deployments where resident dataset size is the primary scaling limiter.
The intended target is not every Valkey workload. The target is memory-constrained deployments where values are sufficiently compressible and where reducing RAM footprint is more valuable than the additional CPU and latency cost.

The proposed direction is to explore native inline memory compression as a first-class Valkey capability, starting with a conservative opt-in v1 and leaving broader object coverage, async decompression, compressed transfer paths, and hot-key optimizations as possible non-breaking follow-up work.