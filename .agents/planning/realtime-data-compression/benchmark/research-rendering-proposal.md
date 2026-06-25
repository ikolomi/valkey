A good visualization should answer one user-facing question:

**“For my fixed workload, which compression config gives me acceptable latency cost for enough memory savings?”**

So I would design the results around **tradeoffs**, not around raw benchmark internals.

## 1. Main graph: memory savings vs latency penalty

This should be the headline graph.

**X-axis:** memory saved, for example:

```text
Memory reduction vs baseline (%)
```

**Y-axis:** latency penalty vs baseline, usually p99 or p99.9:

```text
p99 latency increase vs baseline (%)
```

Each point is one compression configuration:

```text
baseline
compression eligibility >= 256B
compression eligibility >= 512B
compression eligibility >= 1KB
compression eligibility >= 4KB
compression + hot-value skip
compression + different dictionary size
compression + different compression level
...
```

Example shape:

```text
p99 latency penalty
^
|                         bad
|                    x
|              x
|        x
|   x
| x
|_______________________________> memory saved
          good frontier
```

The best configs form a **Pareto frontier**: more memory saved with the least latency cost.

This is the most important graph because users do not really care about “compression ratio” alone. They care about:

> “How much latency do I pay for how much RAM I save?”

I would make one version for **p50**, **p95**, **p99**, and **p99.9**, or use separate small panels.

---

## 2. Latency percentile delta chart

For each config, show how every percentile changes compared to baseline.

**X-axis:** latency percentile

```text
p50, p90, p95, p99, p99.9
```

**Y-axis:** latency increase/decrease vs baseline

```text
Latency delta vs baseline, ms
```

or

```text
Latency delta vs baseline, %
```

Each line is one compression config.

Example:

```text
Latency delta vs baseline
^
|                         config C
|                    config B
|             config A
|____p50___p90___p95___p99___p99.9____>
```

This graph answers:

> “Does compression only affect the tail, or does it shift the whole latency distribution?”

For Valkey, this is especially important because compression may look cheap at p50 but painful at p99/p999 if decompression, worker backlog, dictionary lookup, or skipped-hot-value behavior creates tail spikes.

I would probably prefer **absolute latency delta in microseconds/ms** over percentage for this graph, because percentages can exaggerate small baselines.

Example:

```text
p99 baseline = 0.8 ms
p99 compressed = 1.2 ms
delta = +0.4 ms
```

That is more meaningful than saying “+50%”.

---

## 3. Memory breakdown bar chart

For every configuration, show memory as stacked bars.

**X-axis:** compression config
**Y-axis:** memory used

Stacked components:

```text
compressed value payload
uncompressed value payload
metadata overhead
dictionary memory
fragmentation / allocator overhead
other Redis/Valkey overhead
```

Example:

```text
Memory used
^
| baseline       ██████████████████████
| config A       ████████████▒▒
| config B       █████████▒▒▒
| config C       ███████▒▒▒▒▒
|
+--------------------------------------> configs
```

This is useful because “saved memory” can hide overhead. A user may ask:

> “Why did I only save 28% when the compression ratio says 45%?”

The answer may be metadata, allocator fragmentation, small values being ineligible, dictionary overhead, or hot values being left uncompressed.

---

## 4. Command-specific latency impact

Since the workload has a command distribution, do not only show aggregate latency. Show the command families separately.

For example:

```text
GET
SET
MGET
HGET
HSET
ZADD
ZRANGE
...
```

For each command, show p50/p95/p99 delta vs baseline.

A good format is a heatmap:

```text
             p50     p95     p99    p99.9
GET          +2µs    +8µs    +40µs   +120µs
SET          +5µs    +20µs   +80µs   +300µs
MGET         +10µs   +60µs   +200µs  +900µs
```

Color intensity would represent severity.

This graph answers:

> “Which commands are paying the compression cost?”

For inline compression, I would expect the biggest effects around commands that read/write compressed values, especially multi-key or large-value commands.

---

## 5. Throughput/headroom graph

Even if the workload TPS is fixed, users care whether the server is closer to saturation.

Show:

```text
CPU utilization
compression worker utilization
main-thread utilization
ops/sec achieved
queue depth / compression backlog
```

For each config.

A simple graph:

**X-axis:** config
**Y-axis:** utilization %

Bars:

```text
main thread CPU
compression worker CPU
total process CPU
```

This tells the user:

> “The latency is okay now, but do I still have capacity headroom?”

This is important because two configs may have the same p99 latency at 100k TPS, but one uses 40% CPU and the other uses 85% CPU. The second one is riskier.

---

## 6. Config comparison table

After the graphs, include one compact summary table.

Example:

| Config          | Memory saved | p50 delta | p95 delta | p99 delta | p99.9 delta | CPU delta | Recommended? |
| --------------- | -----------: | --------: | --------: | --------: | ----------: | --------: | ------------ |
| Baseline        |           0% |         0 |         0 |         0 |           0 |         0 | reference    |
| Compress >=512B |          31% |      +3µs |     +18µs |     +80µs |      +240µs |      +12% | yes          |
| Compress >=1KB  |          24% |      +1µs |      +9µs |     +35µs |      +110µs |       +6% | safer        |
| Compress >=256B |          38% |      +8µs |     +45µs |    +220µs |      +900µs |      +28% | risky        |

This table is probably what users will screenshot.

---

# Recommended dashboard layout

I would structure the benchmark report like this:

## Section 1: Workload summary

Because the workload is constant per simulation, show it once:

```text
TPS: 100k
Read/write mix: 80/20
Command distribution: GET 70%, SET 20%, MGET 10%
Value-size distribution: p50 512B, p95 8KB, p99 64KB
Key distribution: Zipfian theta=0.99
Dataset size: 100M keys
Compression candidates: 58% of keys, 82% of bytes
```

This is important because otherwise the graphs are impossible to interpret.

---

## Section 2: Main tradeoff

Show:

```text
Memory saved % vs p99 latency delta
Memory saved % vs p99.9 latency delta
```

This should be the first real graph.

---

## Section 3: Latency distribution impact

Show percentile deltas:

```text
p50 / p90 / p95 / p99 / p99.9
```

Either as line charts or grouped bars.

---

## Section 4: Memory explanation

Show:

```text
Total memory by config
Memory breakdown by payload/metadata/dictionary/fragmentation
Compression eligibility coverage
```

---

## Section 5: Operational risk

Show:

```text
main-thread CPU
compression-worker CPU
compression queue depth
evictions
timeouts
rejected/skipped compressions
hot-value skip rate
```

---

# The graph I would absolutely include

The single best graph is:

```text
X-axis: memory saved vs baseline %
Y-axis: p99.9 latency increase vs baseline, in ms
Point label: compression config
Point size: CPU utilization or compression worker utilization
```

That gives you a very dense and useful chart:

```text
               p99.9 latency cost
                     ^
                     |
            risky    |        config D
                     |
                     |   config C
                     |
                     | config B
                     |
          good       |      config A
                     |
                     +---------------------------->
                         memory saved %
```

Then the ideal config is the point farthest to the **bottom-right**:

```text
high memory savings, low tail-latency cost
```

---

# Important: use delta from baseline, not raw latency

For this benchmark, almost everything should be shown as:

```text
compressed config - baseline
```

or

```text
compressed config / baseline
```

Because the workload is static. The user wants to know what changes when enabling compression.

So instead of:

```text
p99 latency = 1.34 ms
```

prefer:

```text
p99 latency = +0.21 ms vs baseline
```

or:

```text
p99 latency = +18% vs baseline
```

Best is to show both:

```text
+0.21 ms / +18%
```

---

# Metrics I would collect per run

Minimum useful set:

```text
total memory used
used_memory_dataset
allocator fragmentation ratio
compression metadata memory
dictionary memory
compressed bytes
uncompressed bytes
compression ratio
eligible keys %
eligible bytes %
actually compressed keys %
actually compressed bytes %
hot-skip rate
```

Latency:

```text
overall p50/p90/p95/p99/p99.9
per-command p50/p95/p99/p99.9
read vs write latency
timeout/error rate
```

CPU/headroom:

```text
main-thread CPU
compression-worker CPU
total CPU
compression queue depth
compression/decompression ops/sec
average compression time
average decompression time
```

Correctness/stability:

```text
evictions
OOM/rejected writes
latency spikes
worker backlog
dictionary rebuilds
dictionary misses
```

---

# My preferred final report format

For each workload, generate one report page like this:

```text
Workload: 80/20 GET/SET, 100k TPS, Zipfian theta=0.99, value p50=512B p99=64KB

1. Recommended configs
   - Best memory saving under +10% p99 latency
   - Best latency-safe config
   - Maximum memory-saving config

2. Tradeoff graph
   - Memory saved vs p99/p99.9 latency delta

3. Latency delta by percentile
   - p50/p95/p99/p99.9 vs baseline

4. Memory breakdown
   - total memory and compressed/uncompressed/overhead split

5. Command-level heatmap
   - GET/SET/MGET/etc. latency deltas

6. Operational headroom
   - CPU, worker utilization, queue depth
```

---

The key design principle:

**Do not present compression as “faster/slower.” Present it as a cost curve: “for this workload, every extra X% memory saving costs Y µs at p99/p99.9 and Z% CPU.”**

