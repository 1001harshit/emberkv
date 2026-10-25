# Changelog

## v0.3.0: lists, sorted sets, transactions and Pub/Sub

emberkv now covers the Redis data types and features most applications use: lists, sorted sets,
`MULTI`/`EXEC` transactions, Pub/Sub, and password and TLS security. Time travel works on all of
them: `ASOF -5m LRANGE queue 0 -1` shows a list as it was five minutes ago, and `ROLLBACK`
restores it.

### New

- **Lists:** `LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`/`RPOP` with count, `LLEN`, `LRANGE`,
  `LINDEX`, `LSET`, `LTRIM`, `LREM`, `LINSERT`, `LMOVE`, `RPOPLPUSH`.
- **Sorted sets:** `ZADD` (`NX`/`XX`/`GT`/`LT`/`CH`/`INCR`), `ZINCRBY`, `ZREM`, `ZSCORE`,
  `ZMSCORE`, `ZCARD`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZRANGE` (`BYSCORE`/`REV`/`LIMIT`/`WITHSCORES`),
  `ZREVRANGE`, `ZRANGEBYSCORE`, `ZREVRANGEBYSCORE`, `ZPOPMIN`, `ZPOPMAX`, `ZREMRANGEBYSCORE`,
  `ZREMRANGEBYRANK`.
- **Transactions:** `MULTI`, `EXEC`, `DISCARD`, `WATCH`, `UNWATCH`. No client sees a half-applied
  transaction.
- **Pub/Sub:** `SUBSCRIBE`, `PSUBSCRIBE`, `UNSUBSCRIBE`, `PUNSUBSCRIBE`, `PUBLISH`, `PUBSUB`. A
  subscriber too slow to keep up is disconnected rather than slowing publishers down.
- **Security:**
  - Passwords: `AUTH`, set with `-requirepass` or `$EMBERKV_PASSWORD`.
  - TLS: `-tls-cert` and `-tls-key`.
  - emberkv now warns at startup if it listens beyond localhost without a password.

### Compatibility

211 of 212 commands in the differential test reply byte-for-byte like Redis 7.0. The one
difference is still `SELECT 1`: emberkv has a single database.

### Performance

From one run of the [Benchmark workflow](.github/workflows/bench.yml) on a 4 vCPU GitHub runner,
`redis-benchmark` with 50 clients, every server in the same job (requests/s):

| | LPUSH p1 | LPOP p1 | LPOP p16 | RPOP p16 | ZPOPMIN p16 |
|---|---|---|---|---|---|
| **emberkv v0.3.0** (history 1 h) | 84.0k | 84.4k | 81.7k | 77.5k | 173k |
| Redis 7.0 (AOF everysec) | 126k | 115k | 866k | 816k | 1.52M |
| Kvrocks | 56.0k | 54.1k | 71.7k | 74.1k | did not finish |
| Dragonfly | 76.5k | 80.1k | 789k | 778k | 880k |

- **Kvrocks**, the closest design, is behind emberkv on every list test. Its sorted-set test did
  not finish within 5 minutes, in two runs.
- **Redis and Dragonfly**, both in memory, are far ahead when pipelined.
- **Weak spots:** pipelined `LRANGE_100` and `ZADD` on one hot key (30k/s and 40k/s), and
  `ZPOPMAX`, which reads the whole set.
- **Full tables:** [BENCHMARKS.md](BENCHMARKS.md#lists-and-sorted-sets).

### Fixed

- **Emptying a large list or sorted set took quadratic time**, because each pop stepped over
  everything earlier pops had deleted. It is linear now, with a test that checks it.
- **Pops are 2–3× faster:** they look elements up by key instead of scanning for them.

### Upgrading

The on-disk format is unchanged: v0.3.0 opens data directories written by v0.2.0.

## v0.2.0: time travel and branches

emberkv now remembers history. Read any key as it was minutes ago, undo an accidental
`FLUSHALL`, and branch the whole database in milliseconds, all through the Redis protocol.

### New: time travel and branches

| Command | What it does |
|---|---|
| `ASOF <time> <read command> [args…]` | Run any read command (`GET`, `HGETALL`, `SMEMBERS`, `KEYS`, `SCAN`, `TTL`…) against the data as it was at `<time>` |
| `HISTORY <key> [COUNT n]` | A key's past versions, newest first, with timestamps |
| `ROLLBACK <time>` | Make the whole database what it was at `<time>`, TTLs included. It can itself be undone |
| `BRANCH CREATE <name> [ASOF <time>]` | An instant, independent copy of the current branch, now or as it was then |
| `BRANCH USE` / `CURRENT` / `LIST` / `DROP` | Switch a connection between branches and manage them |

`<time>` is `-10m`-style relative, unix milliseconds, or RFC 3339.

- **How far back:** history is kept for the `-history` window, one hour by default. `-history 0`
  turns it off.
- **Branch cost:** a branch shares the parent's immutable files through hard links. Branching a
  189k-key, 37 MB database took 94 ms and 4 KB of disk.

### Also new since v0.1.0

- **Hashes and sets** (`HSET`, `HGETALL`, `HINCRBY`, `SADD`, `SMEMBERS`, `SINTER`… 22 commands), with
  Redis's `WRONGTYPE` errors, using the Apache Kvrocks storage design.
- **Group commit:** concurrent writes share one log write and one fsync. Each client pipeline is made
  durable with a single wait, as Redis does with its AOF.
- **Striped per-key locks:** read-modify-write commands on different keys run in parallel.
- **Block cache:** a 64 MiB sharded LRU of SSTable blocks (`-block-cache-mb`).
- **Exact Redis error text** for unknown commands. 146 of 147 commands in a differential test reply
  byte-for-byte like Redis 7.0.
- **Releases:** Homebrew, a checksum-verified curl installer, and multi-arch Docker images.

### How emberkv differs from the alternatives

| | emberkv | Redis / Valkey | Kvrocks / Pika | Dragonfly |
|---|---|---|---|---|
| Where data lives | disk (LSM tree) | RAM | disk (RocksDB) | RAM |
| Read the past (`ASOF`, `HISTORY`) | **yes** | no | no | no |
| Undo changes (`ROLLBACK`) | **yes** | no | no | no |
| Instant branches | **yes** | no | no | no |
| Memory for 632k keys (measured) | **55–62 MiB** | 146 MiB | 268 MiB | 139 MiB |
| Written in | Go, no dependencies | C | C++ | C++ |
| Clustering, replication, lists, sorted sets | not yet | yes | yes | yes |

### Performance

From one run of the [Benchmark workflow](.github/workflows/bench.yml): every server on the same
4 vCPU GitHub runner, `redis-benchmark` with 50 clients and 64-byte values. Numbers move between
runs, so compare rows within this run.

**Against the alternatives** (requests/s):

| | SET p1 | GET p1 | SET p16 | GET p16 | INCR p16 |
|---|---|---|---|---|---|
| **emberkv v0.2.0** (history 1 h) | 61.7k | 69.4k | 158k | 529k | 115k |
| **emberkv v0.2.0** (`-history 0`) | 59.5k | 69.4k | 213k | 585k | 132k |
| Redis 7.0 (AOF everysec) | 89.2k | 87.4k | 513k | 831k | 717k |
| Kvrocks | 44.4k | 59.9k | 83.4k | 401k | 95.5k |
| Dragonfly | 50.9k | 51.4k | 574k | 601k | 601k |

- **Redis** is faster, as an in-memory C server should be.
- **Kvrocks**, the closest design, is slower than emberkv for these commands. It is ahead on pipelined
  `HSET` and `SADD`.

**Against v0.1.0** (same run):

| fsync | pipeline | SET | GET | INCR | MSET |
|---|---|---|---|---|---|
| always | 16 | 53.2k → **114k (+115%)** | 395k → 501k (+27%) | 58.0k → 76.3k (+32%) | 15.0k → 16.5k (+10%) |
| everysec | 16 | 274k → 158k (**−42%**) | 399k → 529k (+33%) | 109k → 115k (+5%) | 29.4k → 19.3k (**−35%**) |
| everysec | 1 | 66.3k → 61.7k (−7%) | 67.0k → 69.4k (+4%) | 66.6k → 64.8k (−3%) | 39.9k → 20.0k (**−50%**) |

(v0.1.0 had no hashes or sets, so those columns aren't comparable.)

- **What got faster:** durable writes and reads, thanks to group commit, the block cache and
  per-key locks.
- **What got slower:** with the default hour of history, high-rate blind writes (`SET`, `MSET`).
  Keeping versions costs disk and compaction work.
  - **With `-history 0`,** pipelined `SET` is 22% below v0.1.0 instead of 42%, while `GET` is 47% and
    `INCR` 21% above it.
  - **Recommendation:** if raw write throughput matters more than time travel, run with `-history 0`.
- **Crash safety:** unchanged at zero acknowledged writes lost on `kill -9`. Redis's default
  configuration lost all 53,369.
- **Full tables:** [BENCHMARKS.md](BENCHMARKS.md).

### Breaking change: on-disk format 2

Records now carry a version, so **data directories written by v0.1.0 can't be opened**. emberkv
refuses them with a clear error instead of misreading them, which could otherwise drop writes.

v0.1.0 only stored strings. To carry data over:
1. Run v0.1.0 on its old directory on another port.
2. Copy each key across (`SCAN`, then `GET` and `PTTL` on the old server, and `SET … PX` on the new
   one).

### Known limitations

- **`ROLLBACK`:** pauses writes while it runs and isn't atomic. After a crash part-way, run it again
  to finish.
- **`BRANCH DROP`:** breaks other connections still using that branch.
- **One database:** `SELECT` accepts only `0`; branches give you named, separate datasets instead.

## v0.1.0

First release: a persistent, Redis-compatible key-value store on an LSM tree, with a write-ahead log
with Redis-style fsync policies, Bloom-filtered SSTables, background compaction, TTLs, and string
commands.
