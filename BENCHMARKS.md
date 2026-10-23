# Benchmarks: emberkv vs Redis, Kvrocks and Dragonfly

Produced by [`bench/compare.py`](bench/compare.py) through the
[Benchmark workflow](.github/workflows/bench.yml), so anyone can rerun it from
the Actions tab. Run on 2026-10-23:

- **Machine:** GitHub-hosted runner, 4 vCPU x86_64, Ubuntu 24.04. The servers and
  the load generator shared the machine.
- **Load:** `redis-benchmark` with 50 clients, 64-byte values and 100k random keys.
- **Versions:** emberkv at `7b48a71`, Redis 7.0.15 (Ubuntu package),
  `apache/kvrocks:latest`, `dragonflydb/dragonfly:latest`, all with default settings
  except the persistence options listed.
- **emberkv's history:** it keeps 1 hour of history by default (for `ASOF`, `HISTORY`, `ROLLBACK`
  and branches). It was also run with `-history 0` to show what that costs.

| Server | What it is |
|---|---|
| emberkv | this project: LSM tree on disk, Go, with versioned history |
| Redis | in-memory, C, single-threaded; tested with its default RDB snapshots and with the append-only file (AOF) |
| [Kvrocks](https://kvrocks.apache.org) | Redis protocol on RocksDB, C++. **The closest design to emberkv.** |
| [Dragonfly](https://www.dragonflydb.io) | in-memory, multi-threaded, C++ |

## Summary

- **Crash safety:** like Redis with AOF and Kvrocks, emberkv lost **zero** acknowledged writes
  when killed with `kill -9`. Redis in its default configuration lost **all 41,212**.
- **Memory:** emberkv used the least memory, about **2.4× less than Redis** for the same data,
  because the data lives on disk.
- **vs Kvrocks** (the fair comparison): emberkv is **1.2–1.6× faster** at pipeline 1. Pipelined,
  it is **3.4× faster for `SET`**, 1.4× for `GET` and `INCR`, and 1.1× for `MSET`. Kvrocks is
  ahead on pipelined `HSET` and `SADD`.
- **vs Redis:** Redis is faster, as an in-memory C server should be: 1.2–1.3× at pipeline 1, and
  1.6–12× pipelined.
- **Time travel has a cost:** with the default 1 hour of history, pipelined throughput is 13–29%
  lower than before history existed. With history off, 7–21% lower (see below).
- **Against other databases that can read the past** (etcd, immudb, Dolt): emberkv was the fastest
  at writes, current reads, past reads and history, and used the least disk. Dolt created
  branches faster. See [the comparison](#against-other-databases-that-can-read-the-past).
- **Lists and sorted sets:** at pipeline 1, emberkv pushes faster than Kvrocks and Dragonfly and
  pops a little slower. Pipelined pops are its weakest result: about 3× slower than Kvrocks.
  Kvrocks's sorted-set tests did not finish. See [lists and sorted sets](#lists-and-sorted-sets).
- **Compatibility:** 146 of 147 commands returned byte-for-byte the same reply as Redis 7.0.
  A later run that added lists, sorted sets, transactions and Pub/Sub matched on 211 of 212.
  The one difference is `SELECT 1`, because emberkv has a single database. All 10 `redis-cli`
  checks passed (`--scan`, `--pipe`, stdin mode, `--latency`), and the comparison itself
  was driven through `redis-py`.

## The cost of time travel

The same run also benchmarked the build from just before time travel (`b843449`), so the
cost is measured on the same machine. Pipeline 16:

| emberkv | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| before time travel | 261k | 641k | 139k | 105k | 106k | 45.5k |
| history on (default, 1 h) | 227k (−13%) | 490k (−24%) | 104k (−25%) | 79k (−25%) | 75k (−29%) | 33.7k (−26%) |
| history off (`-history 0`) | 223k (−15%) | 522k (−19%) | 122k (−12%) | 92k (−13%) | 99k (−7%) | 35.9k (−21%) |

At pipeline 1 (one request per round trip), all three are within a few percent of each other:
there, each request mostly waits on the network.

Where the cost comes from:

- **The versioned engine itself (history off):** every record carries a version, lookups seek
  by key *and* version, and flushes and compactions run each record through the retention
  rule. With history off, the memtable replaces old versions in place
  ([`NewLatestOnly`](internal/memtable/memtable.go)). That cut the `INCR` loss from 22% to 12%
  locally. A CPU profile shows no single hot spot left; the rest is spread thinly.
- **Keeping history (history on):** old versions inside the window are stored and carried
  through every compaction, which costs disk and the CPU to rewrite them.
  - **Disk grows with how fast keys are overwritten, times the window.** This run's loading
    phase gave 127 MiB with history vs 102 MiB without.
  - **A worst case:** locally, 4 million overwrites of the same 100k keys within the hour grew
    the data directory from 25 MB to about 1 GB.
  - **For write-heavy cache workloads that don't need time travel,** run with `-history 0` or
    a shorter window such as `-history 10m`.

## Speed fixes (measured at `f53bdf5`, before time travel)

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

## Against other databases that can read the past

The comparisons above are with Redis-style servers. This one is with databases that can also read
the past, which is what emberkv is for. [`bench/timetravel`](bench/timetravel) runs one *checked*
workload against each, through each database's own client:

1. 10,000 keys are written 10 times each (100,000 writes), with a checkpoint after round 5.
2. Every key's latest value is read, and must be round 10.
3. Every key is read as of the checkpoint, and must be round 5.
4. The full history of 1,000 keys is fetched.
5. A branch is created, where the database supports it.

All four fsync every write (emberkv with `-fsync always`, the others by default) and get 24
concurrent clients, since immudb rejects more than about 30. The run used the
[Time-travel benchmark workflow](.github/workflows/timetravel-bench.yml) on one 4 vCPU GitHub
runner on 2026-10-23. **Every database answered every check correctly** (0 errors, 0 wrong values).

| | emberkv | [etcd](https://etcd.io) 3.5.17 | [immudb](https://immudb.io) 1.11.0 | [Dolt](https://www.dolthub.com) 2.6.0 |
|---|---|---|---|---|
| Writes/s | **30.6k** (p50 0.81 ms) | 15.6k (1.74 ms) | 1.9k (12.7 ms) | 1.0k (23.4 ms) |
| Read latest/s | **66.0k** (0.34 ms) | 19.5k (1.14 ms) | 15.3k (1.46 ms) | 5.3k (4.22 ms) |
| Read as of the checkpoint/s | **54.8k** (0.40 ms) | 19.8k (1.12 ms) | 13.4k (1.64 ms) | 4.6k (4.64 ms) |
| Full history of a key/s | **41.1k** (0.54 ms) | 1.9k (12.4 ms)¹ | 11.4k (1.92 ms) | 2.1k (9.06 ms)² |
| Versions per key kept | 10 (every write) | 10 (every write) | 10 (every write) | 2 (commits only)² |
| Create a branch | 8.5 ms | not supported | not supported | **1.8 ms** |
| Disk after the run | **3.4 MiB** | 138 MiB³ | 60 MiB | 1,484 MiB⁴ |

**Notes:**

1. etcd has no history call. Its history was walked one `Get` per version, going back through
   `ModRevision - 1`.
2. Dolt keeps history only at Dolt commits (`CALL DOLT_COMMIT`), not at every write. This run made
   two of them, at the checkpoint and at the end.
3. Part of etcd's size is space its storage engine allocates ahead of use.
4. Measured before running `dolt gc`.

What this shows, and what it doesn't:

- **emberkv is the fastest at every per-key operation here:** 2× etcd on writes and about 3× on
  reads, past or present, and well ahead of immudb and Dolt. It also used the least disk.
- **Dolt creates branches faster:** a Dolt branch is a pointer to a commit. An emberkv branch
  first flushes the memtable to disk and then hard-links the files.
- **They are not the same kind of database, so the numbers measure different jobs:**
  - **etcd** replicates every write through Raft consensus, even on a single node, so it can run
    as a fault-tolerant cluster. emberkv can't.
  - **immudb** makes its history tamper-proof with cryptographic proofs. This benchmark used its
    plain (unverified) reads and writes, but it still pays for maintaining the proof structures.
  - **Dolt** is a full SQL database with Git-style commits, diffs and merges, and every operation
    here went through SQL.

  emberkv is the lightest of the four. That is why it's fast, and also why it doesn't offer those
  guarantees.
- **Reading the past works differently in each:**
  - **emberkv:** any point in time inside its `-history` window.
  - **etcd:** any revision until it is compacted.
  - **immudb:** any revision of a key, kept forever.
  - **Dolt:** any commit.

## Lists and sorted sets

Measured in a later run of the same workflow, at `5633b76`. Every server ran again in that job,
so compare rows within these two tables, not with the tables above. Each test pushes onto, or
pops from, a single key, which is how `redis-benchmark` works.

**Pipeline 1** (requests/s):

| Server | LPUSH | LPOP | RPUSH | RPOP | LRANGE_100 | ZADD | ZPOPMIN |
|---|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 70,472 | 47,438 | 74,627 | 46,318 | 47,551 | 48,112 | 36,397 |
| **emberkv (fsync always)** | 45,096 | 31,928 | 51,962 | 34,578 | 48,555 | 21,418 | 26,874 |
| **emberkv (everysec, history off)** | 78,247 | 57,904 | 77,071 | 58,360 | 49,044 | 68,918 | 64,082 |
| Redis (default: RDB snapshots) | 90,334 | 96,108 | 94,832 | 96,993 | 74,156 | 97,752 | 91,575 |
| Redis (AOF everysec) | 97,371 | 96,712 | 94,922 | 94,384 | 76,046 | 99,751 | 94,206 |
| Redis (AOF always) | 63,735 | 67,981 | 63,151 | 66,957 | 76,394 | 53,022 | 79,334 |
| Kvrocks | 59,809 | 59,312 | 61,013 | 58,599 | 44,643 | - | - |
| Dragonfly | 60,624 | 61,444 | 60,680 | 61,331 | 44,573 | 61,958 | 62,933 |

**Pipeline 16** (requests/s):

| Server | LPUSH | LPOP | RPUSH | RPOP | LRANGE_100 | ZADD | ZPOPMIN |
|---|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 103,724 | 33,831 | 121,359 | 39,970 | 48,431 | 47,601 | 167,029 |
| **emberkv (fsync always)** | 117,467 | 24,511 | 128,833 | 23,933 | 45,608 | 40,074 | 142,086 |
| **emberkv (everysec, history off)** | 195,046 | 61,132 | 168,748 | 67,870 | 82,720 | 53,905 | 320,308 |
| Redis (default: RDB snapshots) | 1,287,001 | 1,146,789 | 1,360,544 | 1,236,094 | 182,083 | 535,906 | 1,362,398 |
| Redis (AOF everysec) | 794,913 | 874,891 | 797,448 | 923,361 | 181,653 | 479,386 | 1,326,260 |
| Redis (AOF always) | 530,786 | 604,230 | 521,648 | 611,621 | 180,018 | 374,813 | 1,215,067 |
| Kvrocks | 145,012 | 113,559 | 157,060 | 118,483 | 65,651 | - | - |
| Dragonfly | 446,030 | 799,360 | 479,157 | 836,120 | 107,886 | 504,796 | 851,789 |

- **Kvrocks's `ZADD`/`ZPOPMIN` row is empty** because that test did not finish within the
  harness's 5-minute limit, at either pipeline depth. emberkv had the same problem until
  `7b3cbbd`: each pop scanned past the records of every member popped before it, so emptying a
  set took quadratic time. The likely cause in Kvrocks is the same, RocksDB tombstones, but this
  run doesn't prove it.
- **Pipelined pops are emberkv's weakest result.** Each pop scans for the element at the end of
  the list, and every pop on one key waits for that key's lock, so pipelining helps little.
  History makes it worse: the hot key's elements keep their old versions for an hour.
- **Same run, strings:** most string results matched earlier runs, but `SET` at pipeline 1 with
  `fsync everysec` fell to 46k from 79k for v0.2.0. That was the first test against a freshly
  started server, the same place as the outlier above. It needs a rerun before it can be called
  a regression.

## Throughput, pipeline 1 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 53,191 | 54,289 | 51,401 | 50,340 | 50,213 | 30,335 |
| **emberkv (fsync always)** | 36,597 | 54,274 | 44,873 | 40,750 | 40,626 | 25,723 |
| **emberkv (everysec, history off)** | 33,283* | 54,215 | 51,948 | 51,020 | 51,216 | 30,053 |
| Redis (default: RDB snapshots) | 63,211 | 64,082 | 64,998 | 66,778 | 66,467 | 65,768 |
| Redis (AOF everysec) | 68,329 | 64,641 | 68,166 | 68,564 | 65,338 | 51,613 |
| Redis (AOF always) | 51,125 | 63,351 | 56,322 | 51,177 | 58,005 | 33,518 |
| Kvrocks | 35,651 | 46,436 | 35,676 | 33,956 | 36,357 | 18,589 |
| Dragonfly | 39,968 | 40,024 | 40,064 | 40,917 | 41,135 | 32,685 |

\* The first test against a freshly started server. It is out of line with the other columns
and with every earlier run, so treat it as noise.

## Throughput, pipeline 16 (requests/s)

| Server | SET | GET | INCR | HSET | SADD | MSET |
|---|---|---|---|---|---|---|
| **emberkv (fsync everysec)** | 226,655 | 489,716 | 104,178 | 78,703 | 74,890 | 33,698 |
| **emberkv (fsync always)** | 213,265 | 441,501 | 107,886 | 69,925 | 76,190 | 32,912 |
| **emberkv (everysec, history off)** | 222,568 | 521,648 | 121,921 | 91,895 | 98,619 | 35,868 |
| Redis (default: RDB snapshots) | 712,251 | 836,120 | 840,336 | 749,625 | 928,505 | 188,857 |
| Redis (AOF everysec) | 455,581 | 780,031 | 616,523 | 484,262 | 894,454 | 108,897 |
| Redis (AOF always) | 339,213 | 815,661 | 443,066 | 362,845 | 704,722 | 102,249 |
| Kvrocks | 66,587 | 340,020 | 76,423 | 116,387 | 132,644 | 29,631 |
| Dragonfly | 452,489 | 470,588 | 481,696 | 463,392 | 517,866 | 139,412 |

## Latency, pipeline 1 (p50 / p99 ms)

| Server | SET | GET | HSET |
|---|---|---|---|
| **emberkv (fsync everysec)** | 0.49 / 1.25 | 0.49 / 1.05 | 0.51 / 2.12 |
| **emberkv (fsync always)** | 0.80 / 2.94 | 0.49 / 1.05 | 0.90 / 2.40 |
| **emberkv (everysec, history off)** | 0.50 / 1.66 | 0.49 / 1.06 | 0.50 / 2.06 |
| Redis (default: RDB snapshots) | 0.39 / 0.54 | 0.39 / 0.53 | 0.38 / 0.50 |
| Redis (AOF everysec) | 0.38 / 0.49 | 0.39 / 0.54 | 0.38 / 0.49 |
| Redis (AOF always) | 0.84 / 1.38 | 0.39 / 0.54 | 0.84 / 1.44 |
| Kvrocks | 1.12 / 3.46 | 0.80 / 2.83 | 1.16 / 5.03 |
| Dragonfly | 0.86 / 2.61 | 0.85 / 2.62 | 0.83 / 2.54 |

## Memory and disk

Each server loaded the same data: 1,000,000 random `SET`s with 100-byte values, which gives
about 632k distinct keys. Memory is the process RSS (container memory for Docker servers).

| Server | Keys | Memory | Disk |
|---|---|---|---|
| **emberkv (fsync everysec)** | 631,900 | **58 MiB** | 127 MiB |
| **emberkv (everysec, history off)** | 632,248 | **55 MiB** | **102 MiB** |
| Redis (AOF everysec) | 633,106 | 139 MiB | 114 MiB |
| Kvrocks | 632,482 | 268 MiB | 220 MiB |
| Dragonfly | 632,096 | 139 MiB | n/a (no persistence) |

emberkv's memory is bounded by the memtable, the block cache (64 MiB at most), and each SSTable's
sparse index and Bloom filter, not by the size of the data. Redis must hold everything in RAM.
Kvrocks's and Dragonfly's figures varied by up to 50% between runs; emberkv's and Redis's stayed
within about 10%.

## Crash safety

One client writes as fast as it can for 3 seconds. Then the server is killed with `kill -9`
and restarted, and every acknowledged key is read back.

| Server | Acknowledged writes | Lost after restart |
|---|---|---|
| **emberkv (fsync always)** | 10,246 | **0** |
| **emberkv (fsync everysec)** | 35,186 | **0** |
| Redis (default: RDB snapshots) | 41,212 | **41,212 (all)** |
| Redis (AOF everysec) | 39,720 | 0 |
| Redis (AOF always) | 11,438 | 0 |
| Kvrocks | 18,282 | 0 |

Because replies to a pipeline are held until the whole batch is durable, this was also checked
with 8 pipelining clients (16 commands per batch). Across about 1.6 million acknowledged
pipelined writes, none were lost.

A process kill doesn't simulate power loss, because data already handed to the OS survives
it. Under power loss, `fsync everysec` (emberkv and Redis alike) can lose up to about a second
of writes, while `fsync always` loses nothing it acknowledged.

## Where emberkv is still slower, and why

- **Pipelined read-modify-write (`INCR`, `HSET`, `SADD`: about 75–120k/s vs Redis's 440k–930k).**
  Each one reads through the LSM layers before writing, and every write takes the memtable's
  single exclusive lock. A concurrent (lock-free) skip list is the next step.
- **Pipelined `GET` (about 490–520k/s vs Redis's 780–840k/s).** The remaining cost is checking
  the memtable and Bloom filters, decoding blocks (now with versions), and Go's per-connection
  overhead.
- **Pipelined list pops (about 24–40k/s vs Kvrocks's 114k).** Each pop scans for its element and
  holds the key's lock while it does, as described [above](#lists-and-sorted-sets).
- **Keeping history**, as measured above.
- **One database.** `SELECT` accepts only `0`, as in Redis Cluster; named branches cover the
  "separate dataset" use.

## Caveats

- **Shared machine:** a 4 vCPU shared VM with the client on the same host, so absolute numbers
  are noisy, and runs on different days differ. The before/after tables are the most reliable
  part, because each compares builds within one job.
- **Dragonfly is disadvantaged:** it is built to scale across many cores. On 4 vCPUs that it
  shares with the client, this test does not show it at its best.
- **Redis version:** 7.0.15 from Ubuntu's package, not the latest Redis.
