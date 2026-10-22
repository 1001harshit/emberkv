# Benchmarks: emberkv vs Redis, Kvrocks and Dragonfly

Produced by [`bench/compare.py`](bench/compare.py) through the
[Benchmark workflow](.github/workflows/bench.yml), so anyone can rerun it from
the Actions tab. Run on 2026-10-23:

- **Machine:** GitHub-hosted runner, 4 vCPU x86_64, Ubuntu 24.04. The servers and
  the load generator shared the machine.
- **Load:** `redis-benchmark` with 50 clients, 64-byte values and 100k random keys.
- **Versions:** emberkv at `7de7042`, Redis 7.0.15 (Ubuntu package),
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
  when killed with `kill -9`. Redis in its default configuration lost **all 34,157**.
- **Memory:** emberkv used the least memory, about **3× less than Redis** for the same data,
  because the data lives on disk. It also used the least disk.
- **vs Kvrocks** (the fair comparison): emberkv is **1.2–1.7× faster** at pipeline 1. Pipelined,
  it is up to **4× faster for `SET`** and **1.7× for `GET`**. It roughly ties on `HSET`, and
  Kvrocks is ahead on pipelined `SADD`.
- **vs Redis:** Redis is still faster, as an in-memory C server should be: about 1.5× at
  pipeline 1, and 1.6–8× pipelined.
- **Compatibility:** 146 of 147 commands returned byte-for-byte the same reply as Redis 7.0.
  The one difference is `SELECT 1`, because emberkv has a single database. All 10 `redis-cli`
  checks passed (`--scan`, `--pipe`, stdin mode, `--latency`), and the comparison itself
  was driven through `redis-py`.

## Speed fixes: before vs after

Three changes, measured old build against new build in the **same run on the same machine**
(baseline `015c9b5`):

1. **[Striped per-key locks](lsm/locks.go).** Read-modify-write commands on different keys no
   longer wait for each other. Only the step that appends to the log buffer stays serial.
2. **[Block cache](internal/cache/cache.go).** A 64 MiB sharded LRU of verified SSTable blocks,
   so hot reads skip the system call, the allocation and the checksum.
3. **One durability wait per pipeline.** Profiling showed the WAL `write(2)` was the next
   bottleneck: each connection waited for the disk after every command. Like Redis, the server
   now runs every command it has read, waits once for all of them to be durable, and only then
   replies.

| fsync | pipeline | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|---|
| always | 16 | 84.8k → **237k (+180%)** | 324k → **572k (+76%)** | 73.1k → **136k (+86%)** | 60.2k → **110k (+83%)** | 62.8k → **111k (+76%)** | 37.2k → 37.0k (−1%) |
| everysec | 16 | 229k → 221k (−3%) | 328k → **568k (+73%)** | 92.4k → **126k (+37%)** | 71.9k → **102k (+42%)** | 75.3k → **103k (+37%)** | 41.9k → 38.3k (**−9%**) |
| always | 1 | 46.1k → 46.1k (0%) | 54.2k → 55.5k (+2%) | 46.5k → 45.9k (−1%) | 39.9k → 42.8k (+7%) | 41.9k → 43.6k (+4%) | 27.6k → 27.8k (+1%) |
| everysec | 1 | 54.2k → 53.6k (−1%) | 54.1k → 55.0k (+2%) | 54.2k → 53.1k (−2%) | 50.5k → 52.2k (+3%) | 51.5k → 52.8k (+3%) | 35.8k → 30.3k (**−15%**) |

How to read it:

- **Pipelined writes with `fsync always`** gained the most, because each disk write and fsync
  now covers a whole pipeline from every client. With full durability, the gap to Redis with
  `AOF always` on pipelined `SET` narrowed from 4.1× (85k vs 345k) to 1.5× (237k vs 345k),
  and emberkv is now 4× faster than Kvrocks there.
- **Pipelined `GET`** gained 73–76% from the block cache.
- **Pipeline 1 is unchanged**, as expected: there, each request waits on a network round trip,
  not on locks or disk.
- **`MSET` regressed 9–15% with `everysec`.** It now takes ten key locks and builds a small
  transaction map instead of writing a plain batch. A lock-only batch path would fix it;
  that is not done yet.

On an Apple M2 Pro (10 cores, `emberbench`, pipeline 16) the gains were larger:
`fsync always` `INCR` 14.7k → 188k, and `GET` 618k → 1.55M.

## Throughput, pipeline 1 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 53,591 | 55,006 | 53,064 | 52,219 | 52,840 | 30,321 |
| **emberkv (fsync always)** | 46,051 | 55,525 | 45,914 | 42,790 | 43,620 | 27,828 |
| Redis (default: RDB snapshots) | 78,927 | 77,760 | 78,493 | 79,460 | 78,401 | 77,340 |
| Redis (AOF everysec) | 81,800 | 78,802 | 82,102 | 83,056 | 80,386 | 55,695 |
| Redis (AOF always) | 50,050 | 77,851 | 54,127 | 49,322 | 55,741 | 32,669 |
| Kvrocks | 31,240 | 44,484 | 31,211 | 30,012 | 33,107 | 18,300 |
| Dragonfly | 40,659 | 41,059 | 41,195 | 41,911 | 42,105 | 33,630 |

## Throughput, pipeline 16 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 221,043 | 568,182 | 126,454 | 102,229 | 103,466 | 38,266 |
| **emberkv (fsync always)** | 237,417 | 571,755 | 135,962 | 109,938 | 110,668 | 37,032 |
| Redis (default: RDB snapshots) | 729,927 | 927,644 | 1,004,016 | 825,764 | 982,318 | 197,161 |
| Redis (AOF everysec) | 447,027 | 910,747 | 653,595 | 424,088 | 932,836 | 123,274 |
| Redis (AOF always) | 344,947 | 818,331 | 433,276 | 361,533 | 684,932 | 107,910 |
| Kvrocks | 59,816 | 327,761 | 60,339 | 106,519 | 125,739 | 28,873 |
| Dragonfly | 435,920 | 473,261 | 476,644 | 456,830 | 514,668 | 126,534 |

## Latency, pipeline 1 (p50 / p99 ms)

| Server | SET | GET | HSET |
|---|---|---|---|
| **emberkv (fsync everysec)** | 0.49 / 1.26 | 0.49 / 0.97 | 0.52 / 2.16 |
| **emberkv (fsync always)** | 0.81 / 1.95 | 0.49 / 0.96 | 0.90 / 2.24 |
| Redis (default: RDB snapshots) | 0.32 / 0.51 | 0.33 / 0.51 | 0.32 / 0.43 |
| Redis (AOF everysec) | 0.31 / 0.52 | 0.33 / 0.42 | 0.31 / 0.41 |
| Redis (AOF always) | 0.90 / 1.68 | 0.33 / 0.50 | 0.91 / 1.55 |
| Kvrocks | 1.34 / 4.03 | 0.85 / 2.69 | 1.30 / 5.58 |
| Dragonfly | 0.84 / 2.60 | 0.83 / 2.53 | 0.82 / 2.54 |

The fixes also cut emberkv's tail latency: p99 for `GET` fell from 1.54 to 0.97 ms, and for
`HSET` from 3.38 to 2.16 ms.

## Memory and disk

Each server loaded the same data: 1,000,000 random `SET`s with 100-byte values, which gives
about 632k distinct keys. Memory is the process RSS (container memory for Docker servers).

| Server | Keys | Memory | Disk |
|---|---|---|---|
| **emberkv (fsync everysec)** | 632,351 | **46 MiB** | **94 MiB** |
| Redis (AOF everysec) | 632,082 | 144 MiB | 116 MiB |
| Kvrocks | 632,060 | 169 MiB | 218 MiB |
| Dragonfly | 632,249 | 96 MiB | n/a (no persistence) |

emberkv's memory is bounded by the memtable, the block cache (64 MiB at most), and each SSTable's
sparse index and Bloom filter, not by the size of the data. Redis must hold everything in RAM.

## Crash safety

One client writes as fast as it can for 3 seconds. Then the server is killed with `kill -9`
and restarted, and every acknowledged key is read back.

| Server | Acknowledged writes | Lost after restart |
|---|---|---|
| **emberkv (fsync always)** | 9,187 | **0** |
| **emberkv (fsync everysec)** | 28,477 | **0** |
| Redis (default: RDB snapshots) | 34,157 | **34,157 (all)** |
| Redis (AOF everysec) | 33,281 | 0 |
| Redis (AOF always) | 9,597 | 0 |
| Kvrocks | 16,150 | 0 |

Because replies to a pipeline are now held until the whole batch is durable, this was also
checked with 8 pipelining clients (16 commands per batch). Across about 1.6 million acknowledged
pipelined writes, none were lost.

A process kill doesn't simulate power loss, because data already handed to the OS survives
it. Under power loss, `fsync everysec` (emberkv and Redis alike) can lose up to about a second
of writes, while `fsync always` loses nothing it acknowledged.

## Where emberkv is still slower, and why

- **Pipelined read-modify-write (`INCR`, `HSET`, `SADD`: about 100–135k/s vs Redis's 400k–1M).**
  Each one still reads through the LSM layers before writing, and every write takes the
  memtable's single exclusive lock. A concurrent (lock-free) skip list is the next step.
- **Pipelined `GET` (about 570k/s vs Redis's 900k/s).** The remaining cost is checking the
  memtable and Bloom filters, decoding blocks, and Go's per-connection overhead.
- **`MSET`:** see the regression above.
- **One database.** `SELECT` accepts only `0`, as in Redis Cluster.

## Caveats

- **Shared machine:** a 4 vCPU shared VM with the client on the same host, so absolute numbers
  are noisy. The before/after table is the most reliable part, because both builds ran in the
  same job.
- **Dragonfly is disadvantaged:** it is built to scale across many cores. On 4 vCPUs that it
  shares with the client, this test does not show it at its best.
- **Redis version:** 7.0.15 from Ubuntu's package, not the latest Redis.
