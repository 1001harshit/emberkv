# Benchmarks: emberkv vs Redis, Kvrocks and Dragonfly

Produced by [`bench/compare.py`](bench/compare.py) through the
[Benchmark workflow](.github/workflows/bench.yml), so anyone can rerun it from
the Actions tab. Run on 2026-10-23:

- **Machine:** GitHub-hosted runner, 4 vCPU x86_64, Ubuntu 24.04. The servers and
  the load generator shared the machine.
- **Load:** `redis-benchmark` with 50 clients, 64-byte values and 100k random keys.
- **Versions:** emberkv at `d161fa7`, Redis 7.0.15 (Ubuntu package), `apache/kvrocks:latest`,
  `dragonflydb/dragonfly:latest`, all with default settings except the persistence options listed.

| Server | What it is |
|---|---|
| emberkv | this project: LSM tree on disk, Go |
| Redis | in-memory, C, single-threaded; tested with its default RDB snapshots and with the append-only file (AOF) |
| [Kvrocks](https://kvrocks.apache.org) | Redis protocol on RocksDB, C++. **The closest design to emberkv.** |
| [Dragonfly](https://www.dragonflydb.io) | in-memory, multi-threaded, C++ |

## Summary

- **Crash safety:** like Redis with AOF and Kvrocks, emberkv lost **zero** acknowledged writes
  when killed with `kill -9`. Redis in its default configuration lost **all 34,481**.
- **Memory:** emberkv used the least memory, about **3× less than Redis** for the same data,
  because the data lives on disk. It also used the least disk.
- **vs Kvrocks** (the fair comparison): emberkv is **1.5–2× faster** at pipeline 1 for every
  command, and up to **3.8× faster** for pipelined `SET`. Kvrocks is faster for pipelined
  `HSET` and `SADD`.
- **vs Redis:** Redis is faster, about 1.5× at pipeline 1 and 3–12× when pipelined, as expected
  for a mature in-memory C server. With equal durability (fsync on every write), single
  writes are about even: `SET` 43.9k/s for emberkv vs 42.9k/s for Redis.
- **Compatibility:** 146 of 147 commands returned byte-for-byte the same reply as Redis 7.0.
  The one difference is `SELECT 1`, because emberkv has a single database. All 10 `redis-cli`
  checks passed (`--scan`, `--pipe`, stdin mode, `--latency`), and the comparison itself
  was driven through `redis-py`.

## Throughput, pipeline 1 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 53,079 | 52,924 | 52,383 | 47,778 | 49,652 | 35,032 |
| **emberkv (fsync always)** | 43,927 | 53,433 | 44,504 | 38,700 | 40,858 | 26,378 |
| Redis (default: RDB snapshots) | 77,489 | 75,472 | 76,307 | 79,872 | 75,901 | 78,771 |
| Redis (AOF everysec) | 84,926 | 81,566 | 84,818 | 85,143 | 83,717 | 53,519 |
| Redis (AOF always) | 42,872 | 74,710 | 48,900 | 43,697 | 51,800 | 31,221 |
| Kvrocks | 29,735 | 43,754 | 29,878 | 29,193 | 32,441 | 17,926 |
| Dragonfly | 40,502 | 40,535 | 40,833 | 41,110 | 41,736 | 32,410 |

## Throughput, pipeline 16 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 222,767 | 312,305 | 87,481 | 70,190 | 73,099 | 40,422 |
| **emberkv (fsync always)** | 79,942 | 305,157 | 67,069 | 57,136 | 58,194 | 35,947 |
| Redis (default: RDB snapshots) | 838,223 | 992,064 | 1,009,082 | 851,064 | 1,052,632 | 202,675 |
| Redis (AOF everysec) | 487,805 | 956,023 | 660,502 | 466,636 | 981,354 | 122,579 |
| Redis (AOF always) | 325,098 | 841,751 | 394,322 | 331,126 | 698,812 | 95,274 |
| Kvrocks | 58,862 | 318,573 | 59,130 | 102,512 | 125,109 | 28,441 |
| Dragonfly | 425,532 | 459,982 | 462,749 | 426,439 | 488,520 | 122,805 |

## Latency, pipeline 1 (p50 / p99 ms)

| Server | SET | GET | HSET |
|---|---|---|---|
| **emberkv (fsync everysec)** | 0.50 / 1.22 | 0.50 / 1.32 | 0.55 / 3.54 |
| **emberkv (fsync always)** | 0.84 / 3.20 | 0.50 / 1.42 | 0.96 / 4.08 |
| Redis (default: RDB snapshots) | 0.33 / 0.52 | 0.34 / 0.53 | 0.32 / 0.47 |
| Redis (AOF everysec) | 0.30 / 0.47 | 0.31 / 0.47 | 0.30 / 0.43 |
| Redis (AOF always) | 1.02 / 3.08 | 0.34 / 0.55 | 1.01 / 2.81 |
| Kvrocks | 1.44 / 4.11 | 0.86 / 2.74 | 1.41 / 5.23 |
| Dragonfly | 0.84 / 2.56 | 0.82 / 2.62 | 0.84 / 2.58 |

## Memory and disk

Each server loaded the same data: 1,000,000 random `SET`s with 100-byte values, which gives
about 632k distinct keys. Memory is the process RSS (container memory for Docker servers).

| Server | Keys | Memory | Disk |
|---|---|---|---|
| **emberkv (fsync everysec)** | 632,202 | **46 MiB** | **94 MiB** |
| Redis (AOF everysec) | 632,342 | 144 MiB | 115 MiB |
| Kvrocks | 632,174 | 168 MiB | 218 MiB |
| Dragonfly | 631,541 | 96 MiB | n/a (no persistence) |

emberkv's memory is bounded by the memtable, plus each SSTable's sparse index and Bloom filter,
not by the size of the data. Redis must hold everything in RAM.

## Crash safety

One client writes as fast as it can for 3 seconds. Then the server is killed with `kill -9`
and restarted, and every acknowledged key is read back.

| Server | Acknowledged writes | Lost after restart |
|---|---|---|
| **emberkv (fsync always)** | 8,546 | **0** |
| **emberkv (fsync everysec)** | 29,582 | **0** |
| Redis (default: RDB snapshots) | 34,481 | **34,481 (all)** |
| Redis (AOF everysec) | 34,087 | 0 |
| Redis (AOF always) | 9,183 | 0 |
| Kvrocks | 15,993 | 0 |

A process kill doesn't simulate power loss, because data already handed to the OS survives
it. Under power loss, `fsync everysec` (emberkv and Redis alike) can lose up to about a second
of writes, while `fsync always` loses nothing it acknowledged.

## Where emberkv is slower, and why

- **Pipelined read-modify-write (`INCR`, `HSET`, `SADD`: about 70–90k/s).** Each one takes the
  global write lock and then reads through the LSM layers (memtable, frozen memtables,
  SSTables) before writing, so they run one at a time. Plain `SET` is a blind write and
  reaches about 223k/s. Next steps: striped per-key locks and a memtable-first fast path.
- **Pipelined `GET` (about 312k/s vs Redis's 1M/s).** Redis serves from a hash table in RAM
  with one thread and no locks. emberkv checks the memtable, Bloom filters and SSTable blocks,
  and has no block cache yet; adding one is on the roadmap.
- **One database.** `SELECT` accepts only `0`, as in Redis Cluster.

## Caveats

- **Shared machine:** a 4 vCPU shared VM with the client on the same host, so absolute numbers are
  noisy. Compare the rows with each other rather than trusting any single figure.
- **Dragonfly is disadvantaged:** it is built to scale across many cores. On 4 vCPUs that it shares
  with the client, this test does not show it at its best.
- **Redis version:** 7.0.15 from Ubuntu's package, not the latest Redis.
