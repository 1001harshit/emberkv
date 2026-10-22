# Benchmarks: emberkv vs Redis, Kvrocks and Dragonfly

Produced by [`bench/compare.py`](bench/compare.py) through the
[Benchmark workflow](.github/workflows/bench.yml), so anyone can rerun it from
the Actions tab. Run on 2026-10-23:

- **Machine:** GitHub-hosted runner, 4 vCPU x86_64, Ubuntu 24.04. The servers and
  the load generator shared the machine.
- **Load:** `redis-benchmark` with 50 clients, 64-byte values and 100k random keys.
- **Versions:** emberkv at `f53bdf5`, Redis 7.0.15 (Ubuntu package),
  `apache/kvrocks:latest`, `dragonflydb/dragonfly:latest`, all with default settings
  except the persistence options listed.

| Server | What it is |
|---|---|
| emberkv | this project: LSM tree on disk, Go |
| Redis | in-memory, C, single-threaded; tested with its default RDB snapshots and with the append-only file (AOF) |
| [Kvrocks](https://kvrocks.apache.org) | Redis protocol on RocksDB, C++. **The closest design to emberkv.** |
| [Dragonfly](https://www.dragonflydb.io) | in-memory, multi-threaded, C++ |

## Summary

- **Crash safety:** like Redis with AOF and Kvrocks, emberkv lost **zero** acknowledged writes
  when killed with `kill -9`. Redis in its default configuration lost **all 30,350**.
- **Memory:** emberkv used the least memory, about **3× less than Redis** for the same data,
  because the data lives on disk. It also used the least disk.
- **vs Kvrocks** (the fair comparison): emberkv is **1.2–1.8× faster** at pipeline 1. Pipelined,
  it is up to **4.4× faster for `SET`**, **1.9× for `GET`** and **1.6× for `MSET`**. It roughly
  ties on `HSET`, and Kvrocks is ahead on pipelined `SADD`.
- **vs Redis:** Redis is still faster, as an in-memory C server should be: about 1.5× at
  pipeline 1, and 1.3–7× pipelined.
- **Compatibility:** 146 of 147 commands returned byte-for-byte the same reply as Redis 7.0.
  The one difference is `SELECT 1`, because emberkv has a single database. All 10 `redis-cli`
  checks passed (`--scan`, `--pipe`, stdin mode, `--latency`), and the comparison itself
  was driven through `redis-py`.

## Speed fixes: before vs after

Four changes, measured as the old build against the new build in the **same run on the same
machine**. The baseline is `015c9b5`, from before any of them:

1. **[Striped per-key locks](lsm/locks.go).** Read-modify-write commands on different keys no
   longer wait for each other. Only the step that appends to the log buffer stays serial.
2. **[Block cache](internal/cache/cache.go).** A 64 MiB sharded LRU of verified SSTable blocks,
   so hot reads skip the system call, the allocation and the checksum.
3. **One durability wait per pipeline.** Profiling showed the WAL `write(2)` was the next
   bottleneck: each connection waited for the disk after every command. Like Redis, the server
   now runs every command it has read, waits once for all of them to be durable, and only then
   replies.
4. **Plain batches for blind writes.** `MSET`, `SET` and `FLUSHALL` write a batch under their
   key locks (`lsm.WriteKeys`) instead of going through a transaction, which they don't need.
   This fixed most of the `MSET` slowdown that change 1 introduced.

| fsync | pipeline | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|---|
| always | 16 | 78.0k → **262k (+235%)** | 327k → **605k (+85%)** | 72.1k → **142k (+98%)** | 60.1k → **110k (+83%)** | 63.0k → **111k (+76%)** | 37.8k → **43.2k (+14%)** |
| everysec | 16 | 246k → 237k (−4%) | 337k → **597k (+77%)** | 94.6k → **133k (+41%)** | 73.9k → **103k (+40%)** | 77.5k → **105k (+36%)** | 43.3k → 45.0k (+4%) |
| always | 1 | 44.6k → 44.4k (−1%) | 53.3k → 55.0k (+3%) | 44.8k → 45.4k (+1%) | 39.3k → 41.8k (+6%) | 41.4k → 42.9k (+4%) | 27.4k → **30.1k (+10%)** |
| everysec | 1 | 53.5k → 53.5k (0%) | 53.9k → 54.7k (+2%) | 52.3k → 52.3k (0%) | 49.2k → 51.3k (+4%) | 50.6k → 51.7k (+2%) | 37.3k → 32.9k (**−12%**) |

How to read it:

- **Pipelined writes with `fsync always`** gained the most, because each disk write and fsync
  now covers a whole pipeline from every client. With full durability, the gap to Redis with
  `AOF always` on pipelined `SET` narrowed from 4.2× (78k vs 330k) to 1.3× (262k vs 330k).
- **Pipelined `GET`** gained 77–85% from the block cache.
- **Pipeline 1 is mostly unchanged**, as expected: there, each request waits on a network round
  trip, not on locks or disk.
- **`MSET` is now faster than before in three of four configurations.** The exception is `fsync
  everysec` at pipeline 1, still about 10% slower, and reproduced locally (52.1k → 47.2k/s).
  - **Ruled out lock-stripe collisions:** raising the stripe count from 256 to 4096 barely
    changed it (47.9k/s).
  - **What a CPU profile shows instead:** about 8% more CPU per `MSET`, split between the Go
    scheduler waking goroutines and WAL commits. That suggests slightly smaller group commits.
  - **Status:** the root cause is not pinned down yet.

On an Apple M2 Pro (10 cores, `emberbench`, pipeline 16) the gains were larger:
`fsync always` `INCR` went from 14.7k to 188k, and `GET` from 618k to 1.55M.

## Throughput, pipeline 1 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 53,476 | 54,735 | 52,315 | 51,308 | 51,666 | 32,862 |
| **emberkv (fsync always)** | 44,375 | 54,990 | 45,382 | 41,815 | 42,863 | 30,084 |
| Redis (default: RDB snapshots) | 74,655 | 75,047 | 75,758 | 75,901 | 71,250 | 72,807 |
| Redis (AOF everysec) | 77,851 | 74,019 | 79,083 | 76,805 | 75,643 | 55,021 |
| Redis (AOF always) | 46,232 | 72,727 | 51,203 | 48,274 | 52,604 | 33,184 |
| Kvrocks | 30,441 | 43,860 | 30,741 | 29,499 | 32,026 | 18,399 |
| Dragonfly | 40,169 | 40,323 | 40,380 | 41,339 | 41,563 | 32,895 |

## Throughput, pipeline 16 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 237,417 | 597,372 | 133,333 | 103,316 | 105,152 | 44,996 |
| **emberkv (fsync always)** | 261,712 | 604,961 | 142,389 | 109,757 | 110,926 | 43,180 |
| Redis (default: RDB snapshots) | 769,823 | 909,091 | 946,074 | 826,446 | 954,198 | 209,556 |
| Redis (AOF everysec) | 452,080 | 907,441 | 599,161 | 476,417 | 933,707 | 129,467 |
| Redis (AOF always) | 329,598 | 956,023 | 429,369 | 356,888 | 714,796 | 111,957 |
| Kvrocks | 59,691 | 318,878 | 58,197 | 109,505 | 126,008 | 28,355 |
| Dragonfly | 435,161 | 471,032 | 476,417 | 455,996 | 504,032 | 129,016 |

## Latency, pipeline 1 (p50 / p99 ms)

| Server | SET | GET | HSET |
|---|---|---|---|
| **emberkv (fsync everysec)** | 0.50 / 1.17 | 0.49 / 0.97 | 0.53 / 2.02 |
| **emberkv (fsync always)** | 0.83 / 2.00 | 0.49 / 0.96 | 0.91 / 2.33 |
| Redis (default: RDB snapshots) | 0.34 / 0.54 | 0.34 / 0.52 | 0.34 / 0.52 |
| Redis (AOF everysec) | 0.34 / 0.62 | 0.34 / 0.54 | 0.34 / 0.53 |
| Redis (AOF always) | 0.95 / 1.85 | 0.34 / 0.54 | 0.92 / 1.57 |
| Kvrocks | 1.38 / 4.00 | 0.86 / 2.67 | 1.40 / 5.12 |
| Dragonfly | 0.87 / 2.49 | 0.85 / 2.54 | 0.85 / 2.46 |

The speed fixes also cut emberkv's tail latency: p99 for `GET` fell from 1.22 to 0.97 ms, and for
`HSET` from 3.29 to 2.02 ms.

## Memory and disk

Each server loaded the same data: 1,000,000 random `SET`s with 100-byte values, which gives
about 632k distinct keys. Memory is the process RSS (container memory for Docker servers).

| Server | Keys | Memory | Disk |
|---|---|---|---|
| **emberkv (fsync everysec)** | 631,580 | **50 MiB** | **94 MiB** |
| Redis (AOF everysec) | 631,961 | 147 MiB | 115 MiB |
| Kvrocks | 632,692 | 254 MiB | 219 MiB |
| Dragonfly | 631,677 | 140 MiB | n/a (no persistence) |

emberkv's memory is bounded by the memtable, the block cache (64 MiB at most), and each SSTable's
sparse index and Bloom filter, not by the size of the data. Redis must hold everything in RAM.
Kvrocks's and Dragonfly's figures varied by up to 50% between runs; emberkv's and Redis's stayed
within 10%.

## Crash safety

One client writes as fast as it can for 3 seconds. Then the server is killed with `kill -9`
and restarted, and every acknowledged key is read back.

| Server | Acknowledged writes | Lost after restart |
|---|---|---|
| **emberkv (fsync always)** | 8,443 | **0** |
| **emberkv (fsync everysec)** | 25,620 | **0** |
| Redis (default: RDB snapshots) | 30,350 | **30,350 (all)** |
| Redis (AOF everysec) | 28,255 | 0 |
| Redis (AOF always) | 8,851 | 0 |
| Kvrocks | 14,727 | 0 |

Because replies to a pipeline are held until the whole batch is durable, this was also checked
with 8 pipelining clients (16 commands per batch). Across about 1.6 million acknowledged
pipelined writes, none were lost.

A process kill doesn't simulate power loss, because data already handed to the OS survives
it. Under power loss, `fsync everysec` (emberkv and Redis alike) can lose up to about a second
of writes, while `fsync always` loses nothing it acknowledged.

## Where emberkv is still slower, and why

- **Pipelined read-modify-write (`INCR`, `HSET`, `SADD`: about 105–140k/s vs Redis's 360k–950k).**
  Each one still reads through the LSM layers before writing, and every write takes the
  memtable's single exclusive lock. A concurrent (lock-free) skip list is the next step.
- **Pipelined `GET` (about 600k/s vs Redis's 910k/s).** The remaining cost is checking the
  memtable and Bloom filters, decoding blocks, and Go's per-connection overhead.
- **`MSET` at pipeline 1 with `fsync everysec`:** see above.
- **One database.** `SELECT` accepts only `0`, as in Redis Cluster.

## Caveats

- **Shared machine:** a 4 vCPU shared VM with the client on the same host, so absolute numbers
  are noisy. The before/after table is the most reliable part, because both builds ran in the
  same job.
- **Dragonfly is disadvantaged:** it is built to scale across many cores. On 4 vCPUs that it
  shares with the client, this test does not show it at its best.
- **Redis version:** 7.0.15 from Ubuntu's package, not the latest Redis.
