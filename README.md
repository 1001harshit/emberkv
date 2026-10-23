# emberkv

[![CI](https://github.com/1001harshit/emberkv/actions/workflows/ci.yml/badge.svg)](https://github.com/1001harshit/emberkv/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/1001harshit/emberkv.svg)](https://pkg.go.dev/github.com/1001harshit/emberkv/lsm)

**A Redis-compatible database with time travel. Read any key as it was 10 minutes ago, undo an accidental `FLUSHALL`, and branch your whole dataset in milliseconds.**

emberkv speaks the Redis protocol, so `redis-cli` and every Redis client library work with it. Unlike Redis, it keeps your data on disk, survives `kill -9`, and **remembers history**:

```
127.0.0.1:6380> ASOF -10m HGETALL user:42        # any read command, as of 10 minutes ago
127.0.0.1:6380> HISTORY price                    # every past value of a key, with timestamps
127.0.0.1:6380> ROLLBACK -5m                     # undo everything since then, FLUSHALL included
127.0.0.1:6380> BRANCH CREATE experiment         # an instant, independent copy of the database
127.0.0.1:6380> BRANCH USE experiment            # ...and play with it; main is untouched
```

None of Redis, Valkey, Kvrocks, Pika or Tendis can do this. emberkv can because it is built from scratch on a log-structured merge tree that keeps versions instead of overwriting them, and whose files never change once written. It has no dependencies outside the Go standard library.

```
$ emberkv -dir ./data
emberkv listening on 127.0.0.1:6380 (data dir "./data", fsync everysec, history 1h0m0s)

$ redis-cli -p 6380
127.0.0.1:6380> SET user:1 "ada" EX 3600
OK
127.0.0.1:6380> INCR visits
(integer) 1
127.0.0.1:6380> HSET user:2 name "grace" lang "cobol"
(integer) 2
127.0.0.1:6380> SADD tags go db
(integer) 2
127.0.0.1:6380> SCAN 0 MATCH user:* COUNT 100
1) "0"
2) 1) "user:1"
```

## Time travel and branches

Every write is stored as a new **version** with a timestamp, and versions are kept for the `-history` window (one hour by default). That's what makes these possible:

| Command | What it does |
|---|---|
| `ASOF <time> <read command> [args…]` | Runs any read command (`GET`, `MGET`, `HGETALL`, `SMEMBERS`, `KEYS`, `SCAN`, `TTL`, `DBSIZE`…) against the data as it was at `<time>`. Writes are refused. |
| `HISTORY <key> [COUNT n]` | The key's past versions, newest first, as `[unix_ms, type, value]` (for hashes and sets the value is the size; `nil` means deleted). |
| `ROLLBACK <time>` | Makes the whole database what it was at `<time>`, TTLs included, and replies with how many keys changed. The rollback is itself recorded in history, so it can be undone. |
| `BRANCH CREATE <name> [ASOF <time>]` | A separate copy of the current branch, as it is now or as it was then. Built from hard links to the same immutable files, so it takes milliseconds and almost no disk: branching a 189k-key, 37 MB database took 94 ms and 4 KB. |
| `BRANCH USE <name>` / `CURRENT` / `LIST` / `DROP <name>` | Switch this connection between branches (like `SELECT`, with names), and manage them. `main` is the database the server started with. |

`<time>` is a relative duration (`-10m`, `-1h30m`), unix milliseconds, or an RFC 3339 timestamp (`2026-10-23T01:00:00Z`).

```
127.0.0.1:6380> SET price 100
127.0.0.1:6380> SET price 250
127.0.0.1:6380> DEL price
127.0.0.1:6380> GET price
(nil)
127.0.0.1:6380> ASOF -3s GET price
"100"
127.0.0.1:6380> HISTORY price
1) 1) (integer) 1792702209414
   2) "none"
   3) (nil)
2) 1) (integer) 1792702207390
   2) "string"
   3) "250"
3) 1) (integer) 1792702205366
   2) "string"
   3) "100"
```

**What it's for:** recovering from mistakes (a bad deploy, a wrong `DEL`, a `FLUSHALL` in the wrong terminal); debugging ("what did this session hold when the error happened?"); auditing changes; and trying risky migrations or load tests on a branch of real data without touching production.

**The cost:** measured on the same machine, the versioned engine and one hour of history cost **13–29% of pipelined throughput**, and more disk for the old versions kept inside the window. Memory isn't affected. Turning history off (`-history 0`) still leaves a 7–21% cost from the versioned engine itself. Disk use grows with how often keys are overwritten times the window length, so for write-heavy cache workloads use `-history 0` or a shorter window such as `-history 10m`. Details are in [BENCHMARKS.md](BENCHMARKS.md#the-cost-of-time-travel).

## Features

- **Time travel and branches** (above).
- **Redis protocol (RESP2)** with pipelining and inline commands. 146 of 147 commands in a differential test returned byte-for-byte the same reply as Redis 7.0. Tested with `redis-cli` (including `--scan`, `--pipe` and `--latency`), `redis-py` and `go-redis`.
- **Durable writes with group commit.** A CRC-checked write-ahead log with Redis-style fsync policies (`always`, `everysec`, `no`). Concurrent writers share one `write` and one `fsync`. Torn writes from a crash are detected and discarded on recovery.
- **LSM storage engine.** A skip-list memtable flushes to sorted SSTables with 4 KiB checksummed blocks, a sparse index and a per-table Bloom filter (about 1% false positives). A sharded LRU **block cache** keeps hot blocks in memory.
- **Parallel writes.** Striped per-key locks let read-modify-write commands on different keys run concurrently. Each client pipeline is made durable with one wait, as in Redis.
- **Background compaction.** Uses RocksDB-style "universal" compaction with size-ratio and space-amplification triggers. It reclaims overwritten, deleted and expired data.
- **Strings, hashes, sets, lists and sorted sets** on one LSM keyspace, using the Kvrocks design: a metadata record per key plus one record per field, member or element. Deleting or expiring a collection of any size is a single write, and a compaction filter later reclaims the orphaned records.
- **Transactions:** `MULTI`/`EXEC`/`DISCARD` with optimistic locking through `WATCH`. No other client sees or interleaves with a half-applied transaction.
- **Pub/Sub:** `SUBSCRIBE`, `PSUBSCRIBE` (glob patterns) and `PUBLISH`, with slow subscribers disconnected instead of blocking publishers.
- **Security:** `AUTH` with `-requirepass`, and TLS with `-tls-cert`/`-tls-key`.
- **TTLs.** `EX` / `PX` / `EXAT` / `PXAT` / `KEEPTTL`, `EXPIRE`, `TTL` and `PERSIST`. Expiry is stored with each record and survives restarts.
- **Atomic commands.** `MSET` is all-or-nothing even across a crash. `INCR`, `SET NX`, `HSET`, `HINCRBY`, `SADD`, `LMOVE`, `ZADD` and the rest are atomic.
- **Embeddable.** The `lsm` package is a standalone Go key-value store, and `keyspace` adds Redis types on top. Neither needs the server.

## Install

**Homebrew** (macOS and Linux), the same way you'd install Redis:

```bash
brew install 1001harshit/tap/emberkv
brew services start emberkv        # runs in the background and starts at login
```

**One-line installer** (macOS and Linux). It picks the right build for your machine and verifies its SHA-256 checksum:

```bash
curl -fsSL https://raw.githubusercontent.com/1001harshit/emberkv/main/install.sh | sh
```

**Docker** (amd64 and arm64):

```bash
docker run -d --name emberkv -p 6380:6380 -v emberkv-data:/data ghcr.io/1001harshit/emberkv
```

**Windows, or by hand:** download the archive for your platform from the [releases page](https://github.com/1001harshit/emberkv/releases/latest).

**From source:** `go install github.com/1001harshit/emberkv/cmd/emberkv@latest`

Then connect with any Redis client:

```bash
redis-cli -p 6380
```

### Running it

```bash
emberkv -dir ~/.emberkv          # foreground, data in ~/.emberkv
emberkv -version
```

From a clone of this repo:

```bash
make run          # build and start on 127.0.0.1:6380
make test         # unit + integration tests with the race detector
make bench        # load test a running server
```

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `127.0.0.1:6380` | listen address (there is no auth, so keep it on localhost or a private network) |
| `-dir` | `data` | data directory |
| `-fsync` | `everysec` | `always`: fsync every write, so nothing acknowledged is ever lost. `everysec`: lose at most about 1s of writes. `no`: leave it to the OS |
| `-memtable-mb` | `4` | memtable size before it is flushed to an SSTable |
| `-compaction-trigger` | `4` | number of SSTables before background compaction starts |
| `-block-cache-mb` | `64` | memory for caching hot SSTable blocks (`0` disables) |
| `-history` | `1h` | how far back `ASOF`, `HISTORY`, `ROLLBACK` and `BRANCH … ASOF` can reach (`0` keeps no history) |
| `-requirepass` | `$EMBERKV_PASSWORD` | clients must `AUTH` with this password first. Prefer the environment variable, which keeps the password out of `ps` |
| `-tls-cert`, `-tls-key` | | serve TLS only, with this certificate and key (PEM) |

emberkv warns at startup if it listens beyond localhost without a password. Anyone who can reach an unprotected port can read and change everything.

### As a Go library

```go
db, err := lsm.Open("./data", lsm.Options{})
if err != nil { log.Fatal(err) }
defer db.Close()

db.Put([]byte("hello"), []byte("world"))
db.PutTTL([]byte("session"), token, 30*time.Minute)
v, err := db.Get([]byte("hello")) // err == lsm.ErrNotFound if absent

db.Write(new(lsm.Batch).Set(k1, v1, 0).Delete(k2)) // atomic

db.Scan([]byte("user:"), func(k []byte, it lsm.Item) bool {
    fmt.Printf("%s = %s\n", k, it.Value)
    return bytes.HasPrefix(k, []byte("user:"))
})
```

## Architecture

```
               SET k v                                    GET k
                  │                                         │
                  ▼                                         ▼
  ┌───────────────────────────┐         ┌─────────────────────────────────────┐
  │ 1. append to WAL (+fsync) │         │ memtable → frozen memtables →       │
  │ 2. insert into memtable   │         │ SSTables newest→oldest; the first   │
  └─────────────┬─────────────┘         │ hit wins. Bloom filters skip most   │
                │ memtable ≥ 4 MiB      │ tables without touching disk.       │
                ▼                       └─────────────────────────────────────┘
  ┌───────────────────────────┐
  │ freeze memtable, new WAL  │  ── flush goroutine ──▶  000042.sst (newest)
  └───────────────────────────┘                           000037.sst
                                                          000021.sst (oldest)
                                                             │
                              compaction goroutine merges ◀──┘
                              runs of adjacent tables
```

**Write path.** Every write, or batch of writes, becomes a single WAL record (`crc32c | length | entries`) and is then inserted into the skip list. When the memtable reaches its size limit it is frozen. A new memtable and WAL take over, and a background goroutine writes the frozen one out as an SSTable. Once the new table is listed in the `MANIFEST`, which is replaced atomically via write-temp-then-rename, the old WAL is deleted. If flushing falls behind, writers stall instead of letting memory grow without limit.

**Concurrency.** A writer locks only its own key: one of 256 [striped locks](lsm/locks.go), picked by hashing the key. So `INCR a` and `INCR b` do their reads and logic in parallel, while two `INCR a` still run one after the other. Blind writes like `SET` take the key lock too, so they can't slip between another command's read and write. The only globally serial step is a short critical section that appends the record to the in-memory log buffer and the memtable, which keeps the log in execution order.

**Group commit.** After that step a writer releases its locks and waits for durability. The first waiter becomes the *leader*: it writes everything buffered so far with a single `write(2)` (plus a single `fsync` under `-fsync always`) and wakes the others. Under load, dozens of writes share one syscall. This is the leader/follower scheme RocksDB uses, combined with "early lock release". A concurrent reader can see a write a moment before it is durable, as in Redis, but no client is acknowledged until its write is durable. The log also keeps writes in order, so a crash can only lose a suffix of unacknowledged writes. With 50 clients and `-fsync always`, this took `SET` from 365 to about 11,000 ops/s. The server goes one step further, as Redis does with its AOF. It runs every command a client has pipelined, waits *once* for all of them to be durable, and only then sends the replies. One log write therefore covers whole pipelines from many clients, which more than doubled pipelined writes under `-fsync always`.

**Versions (MVCC).** Every write gets a *version*: a hybrid timestamp with unix milliseconds in the high bits and a counter in the low 20 bits. Versions are handed out under the append lock, so they order writes exactly even if the system clock jumps backwards, and the last one is saved in the `MANIFEST`. All writes of one batch share a version, so a reader at any point in time sees a batch whole or not at all. The memtable and SSTables keep every version of a key, ordered by key and then newest version first. A read at time *T* seeks to the newest version at or before *T*; a read "now" is simply a read at the newest version. Flushes and compactions keep every version newer than the `-history` window, plus the one each key had when the window starts (that's what a read at the window's edge sees), and drop the rest.

**Time travel on top.** `ASOF` runs a command through a read-only view of the keyspace at *T*, so hashes and sets come back as they were then, field by field. The compaction filter that removes a deleted hash's fields keeps them while any metadata version in the window still points to them. `ROLLBACK` walks the present and the past side by side and writes back every record that differs, as new versions. `BRANCH CREATE` flushes, then hard-links the parent's SSTables into a new directory. A branch of the past adds a per-table *version cutoff* to the new `MANIFEST` instead of rewriting data, and each side's compactions only ever remove their own links.

**Read path.** Point lookups check the memtable, then frozen memtables, then each SSTable from newest to oldest, and stop at the first one holding a version of the key at or before the read point. A tombstone or an expired entry means "not found". Each SSTable keeps its index and Bloom filter in memory, so a lookup costs at most one 4 KiB block read per table that might contain the key, and none at all if the block is in the [block cache](internal/cache/cache.go). The cache is a 16-shard LRU keyed by (table, block); tables are immutable, so cached blocks never go stale. Scans and compactions read through the cache without filling it, so one big scan can't evict the hot set. Scans run a k-way heap merge over all sources and keep only the newest version of each key.

**Compaction.** Tables form one sequence from newest to oldest. The compactor only merges *contiguous* runs, so the merged table can take the run's place without breaking that order. It picks a run by these rules ([`compaction.go`](lsm/compaction.go)):
1. **Space amplification:** if the newer tables together are at least 2× the oldest, merge everything.
2. **Size ratio:** otherwise merge the first run of similarly sized tables, which keeps write amplification logarithmic.
3. **Table count:** otherwise merge the newest tables until the count is back under the trigger.

Tombstones and expired entries can only be dropped when the run includes the oldest table. Before that, they still have to hide older versions further down. `COMPACT` (an emberkv extension) forces a full merge.

**Data types (`keyspace/`).** Redis types are mapped onto the flat LSM keyspace the way [Apache Kvrocks](https://kvrocks.apache.org/community/data-structure-on-rocksdb) does it:

```
'm' | key                                        → type | string value                 (TTL on this record)
'm' | key                                        → type | version | size [| head | tail]  (collections; lists add head/tail)
'f' | len(key) | key | version | field           → field value (hash), "" (set)
'f' | len(key) | key | version | index(8)        → list element
'f' | len(key) | key | version | member          → score (sorted set)
'z' | len(key) | key | version | score(8) | member → ""    (sorted set, ordered by score)
```

A list's elements sit at positions `[head, tail)`, which start in the middle of the 64-bit range so the list can grow at either end: `LPUSH`, `RPOP` and friends are one write per element, and `LINDEX` is one lookup. `LREM` and `LINSERT` rewrite the list under a new version instead of shifting positions. A sorted set stores each member twice: member → score for `ZSCORE`, and a `'z'` record whose key sorts by score for `ZRANGE`, `ZRANGEBYSCORE` and `ZRANK`. Scores are encoded so that byte order equals numeric order, negatives included.

Every operation first reads the key's metadata to get its type and current *version*, then reads or writes the member records under that version. All of it runs inside an `lsm.Txn`, so it is applied as one atomic WAL batch. Deleting, overwriting or expiring a hash with a million fields is therefore one metadata write: the old fields keep a version that is never read again. A [compaction filter](keyspace/keyspace.go) hook in the engine drops those orphaned records when compaction rewrites them. Versions come from a monotonic clock and are never reused, so a record that is stale once stays stale forever, which is what makes dropping it safe even in a non-bottom compaction. HGETALL, SMEMBERS and HKEYS are prefix scans over `'f' | key | version`. A plain `SET` is a blind write, with no read first, because it overwrites any type.

**Crash recovery.** On startup, files missing from the `MANIFEST` are removed as leftovers of an interrupted flush or compaction. Every WAL is replayed oldest first, stopping at the first torn or corrupt record. The result is flushed to a table before new writes are accepted.

### On-disk format

```
000017.wal     [crc32c(4) | len(4) | count | entry…] …
000018.sst     [data block + crc]… [index block + crc] [bloom filter] [footer: offsets | count | magic "EMBERKV2"]
MANIFEST       {"format": 2, "next_file": 19, "last_version": …, "tables": [18, 12, 5], "cutoffs": {…}}
entry          kind(1) | version(uvarint) | expire_at_ms(uvarint) | key_len(uvarint) | key | val_len(uvarint) | value
branches/      one directory per branch, each a complete database
```

Format 2 added versions. A data directory written by v0.1.0 (format 1) is refused with a clear error instead of being misread.

## Supported commands

| Group | Commands |
|---|---|
| Strings | `GET` `SET [NX\|XX] [GET] [EX\|PX\|EXAT\|PXAT\|KEEPTTL]` `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `MGET` `MSET` `APPEND` `STRLEN` |
| Counters | `INCR` `DECR` `INCRBY` `DECRBY` |
| Keys | `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` `SCAN [MATCH] [COUNT] [TYPE]` `DBSIZE` `FLUSHDB` `FLUSHALL` |
| Lists | `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP [count]` `RPOP [count]` `LLEN` `LRANGE` `LINDEX` `LSET` `LTRIM` `LREM` `LINSERT` `LMOVE` `RPOPLPUSH` |
| Sorted sets | `ZADD [NX\|XX] [GT\|LT] [CH] [INCR]` `ZINCRBY` `ZREM` `ZSCORE` `ZMSCORE` `ZCARD` `ZCOUNT` `ZRANK` `ZREVRANK` `ZRANGE [BYSCORE] [REV] [LIMIT] [WITHSCORES]` `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZPOPMIN` `ZPOPMAX` `ZREMRANGEBYSCORE` `ZREMRANGEBYRANK` |
| Transactions | `MULTI` `EXEC` `DISCARD` `WATCH` `UNWATCH` |
| Pub/Sub | `SUBSCRIBE` `PSUBSCRIBE` `UNSUBSCRIBE` `PUNSUBSCRIBE` `PUBLISH` `PUBSUB CHANNELS\|NUMSUB\|NUMPAT` |
| Hashes | `HSET` `HMSET` `HSETNX` `HGET` `HMGET` `HGETALL` `HKEYS` `HVALS` `HDEL` `HLEN` `HEXISTS` `HSTRLEN` `HINCRBY` |
| Sets | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SINTER` `SUNION` `SDIFF` |
| Expiry | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Server | `AUTH` `PING` `ECHO` `SELECT 0` `INFO [section]` `TIME` `QUIT` `COMMAND` `CLIENT` `CONFIG GET` |
| Time travel | `ASOF <time> <command…>` `HISTORY <key> [COUNT n]` `ROLLBACK <time>` |
| Branches | `BRANCH CREATE <name> [ASOF <time>]` `BRANCH USE <name>` `BRANCH CURRENT` `BRANCH LIST` `BRANCH DROP <name>` |
| emberkv | `COMPACT`: flush and fully compact, reclaiming space outside the history window |

Strings, hashes, sets, lists and sorted sets are supported, with Redis's `WRONGTYPE` errors.

Not supported yet:
- **Commands:** blocking list commands (`BLPOP`…), `ZRANGE … BYLEX`, set-operation `STORE` variants.
- **Types:** streams, bitmaps, HyperLogLog, geo.
- **Scripting:** Lua.
- **Databases:** there is a single database; `SELECT` accepts only `0`, as in Redis Cluster.

## Testing

```bash
go test -race ./...
```

- **Model-based test**: 30,000 random puts, deletes, batches and reads, with reopens along the way, checked against a plain Go map. It uses a 4 KiB memtable so that flushes and compactions happen constantly.
- **Crash test**: copies the data directory while the database is still running (what the disk looks like after a power cut) and checks that the copy recovers every acknowledged write. I also checked by hand that `kill -9` in the middle of a write stream loses nothing with `-fsync always`.
- **Corruption tests**: torn WAL tails, flipped bits in WAL records and SSTable blocks, and bad footers.
- **Keyspace tests**: hash and set semantics, `WRONGTYPE` rules, and that recreating a deleted hash never shows its old fields. They also check that compaction physically removes the fields of deleted, overwritten and expired hashes (an exact on-disk record count), and that concurrent `HINCRBY`/`SADD` never lose updates.
- **Server integration tests**: drive every command over real TCP, including pipelining, inline commands, protocol errors, `SCAN` completeness and concurrent clients.

## Benchmarks

### Compared with Redis, Kvrocks and Dragonfly

From the [Benchmark workflow](.github/workflows/bench.yml), on one 4 vCPU GitHub runner with `redis-benchmark`. Full tables, method and caveats are in **[BENCHMARKS.md](BENCHMARKS.md)**.

| | emberkv | Redis (AOF everysec) | Kvrocks | Dragonfly |
|---|---|---|---|---|
| `SET` req/s, pipeline 1 | 53k | 68k | 36k | 40k |
| `GET` req/s, pipeline 1 | 54k | 65k | 46k | 40k |
| `SET` req/s, pipeline 16 | 227k | 456k | 67k | 452k |
| `GET` req/s, pipeline 16 | 490k | 780k | 340k | 471k |
| Memory for 632k keys | **58 MiB** | 139 MiB | 268 MiB | 139 MiB |
| Acked writes lost on `kill -9` | **0** | 0 (default config: all) | 0 | n/a |
| Time travel and branches | **yes** | no | no | no |

emberkv numbers are with the default one hour of history. Redis is faster, as an in-memory C server should be. emberkv beats Kvrocks, the closest on-disk design, on most commands, uses about 2.4× less memory than Redis, and is the only one of the four that can read the past. The measured price of that is in [BENCHMARKS.md](BENCHMARKS.md#the-cost-of-time-travel).

### Compared with other databases that can read the past

Same checked workload for all four: 100k writes, then latest reads, reads as of a checkpoint and full key histories. Every write was fsynced and every answer was verified. Details and caveats are in [BENCHMARKS.md](BENCHMARKS.md#against-other-databases-that-can-read-the-past).

| | emberkv | etcd | immudb | Dolt |
|---|---|---|---|---|
| Writes/s | **30.6k** | 15.6k | 1.9k | 1.0k |
| Read as of the past/s | **54.8k** | 19.8k | 13.4k | 4.6k |
| Full key history/s | **41.1k** | 1.9k | 11.4k | 2.1k |
| Create a branch | 8.5 ms | — | — | **1.8 ms** |
| Disk | **3.4 MiB** | 138 MiB | 60 MiB | 1,484 MiB |

These are different kinds of database. etcd adds Raft consensus, immudb adds tamper-proof cryptographic history, and Dolt is a full SQL database with Git-style merges. emberkv is the lightest of the four.

### On an Apple M2 Pro

`emberbench` is a small `redis-benchmark`-style load generator, run with 50 clients, 64-byte values and 100k keys, on an Apple M2 Pro:

| Test (`-fsync everysec`) | Pipeline 1 | Pipeline 16 |
|---|---|---|
| `SET` | ~120k ops/s (p50 0.36 ms) | ~125k ops/s |
| `GET` | ~141k ops/s (p50 0.33 ms) | ~695k ops/s |
| `INCR` | ~105k ops/s | |
| `HSET` | ~75k ops/s | |
| `HGET` | ~136k ops/s | |
| `SADD` | ~80k ops/s | |
| 80/20 read/write | ~130k ops/s | |

Effect of group commit on writes (pipeline 1):

| | Before | After |
|---|---|---|
| `SET`, `-fsync everysec` | 65k ops/s, p50 0.98 ms | **100k ops/s**, p50 0.41 ms |
| `SET`, `-fsync always` | 365 ops/s, p50 148 ms | **11k ops/s**, p50 4.3 ms (30×) |

## Roadmap

- [x] Group commit: batch concurrent writers into one WAL write and fsync
- [x] Block cache for hot SSTable blocks
- [x] Striped per-key locks so pipelined `INCR`/`HSET`/`SADD` don't serialize on one write lock
- [ ] Lock-free skip-list memtable (the next write bottleneck)
- [x] Lock-only batch path for blind writes (`MSET`, `SET`, `FLUSHALL`)
- [ ] Find the remaining ~10% `MSET` gap at pipeline 1 with `fsync everysec`
- [x] Hashes and sets, encoded as composite keys over the same LSM (Kvrocks-style)
- [x] Versioned history (MVCC) with `ASOF`, `HISTORY`, `ROLLBACK` and instant `BRANCH`es
- [ ] Merge a branch back into its parent
- [ ] `HISTORY` for individual hash fields and set members
- [x] Lists and sorted sets
- [x] `MULTI`/`EXEC`/`WATCH` transactions and Pub/Sub
- [ ] Snapshots and iterators that don't hold the read lock for the whole scan
- [ ] Key-prefix compression inside blocks
- [ ] Replication via WAL shipping
- [x] `AUTH` and TLS
- [ ] Blocking list commands (`BLPOP`, `BRPOP`, `BLMOVE`)
- [ ] Streams
- [x] Release builds for Linux, macOS and Windows, a Homebrew tap, a curl installer and a Docker image

## License

[MIT](LICENSE)
