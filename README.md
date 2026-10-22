# emberkv

[![CI](https://github.com/1001harshit/emberkv/actions/workflows/ci.yml/badge.svg)](https://github.com/1001harshit/emberkv/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/1001harshit/emberkv.svg)](https://pkg.go.dev/github.com/1001harshit/emberkv/lsm)

**A persistent, Redis-compatible key-value database built from scratch in Go on a log-structured merge tree.**

Point `redis-cli` or any Redis client library at it. Unlike Redis, your data lives on disk and survives `kill -9`. Every write goes to a checksummed write-ahead log before it is acknowledged, then moves through an in-memory skip list into immutable, Bloom-filtered SSTables that are compacted in the background.

No dependencies outside the Go standard library.

```
$ emberkv -dir ./data
emberkv listening on 127.0.0.1:6380 (data dir "./data", fsync everysec)

$ redis-cli -p 6380
127.0.0.1:6380> SET user:1 "ada" EX 3600
OK
127.0.0.1:6380> INCR visits
(integer) 1
127.0.0.1:6380> SCAN 0 MATCH user:* COUNT 100
1) "0"
2) 1) "user:1"
```

## Features

- **Redis protocol (RESP2)** with pipelining and inline commands. Verified with [`go-redis`](https://github.com/redis/go-redis), including pipelines, `SCAN` iterators and TTLs. Other RESP2 clients such as `redis-cli` and `redis-py` should work too, since they speak the same protocol, but they haven't been tested yet.
- **Durable writes with group commit.** A CRC-checked write-ahead log with Redis-style fsync policies (`always`, `everysec`, `no`). Concurrent writers share one `write` and one `fsync`. Torn writes from a crash are detected and discarded on recovery.
- **LSM storage engine.** A skip-list memtable flushes to sorted SSTables with 4 KiB checksummed blocks, a sparse index and a per-table Bloom filter (about 1% false positives).
- **Background compaction.** Uses RocksDB-style "universal" compaction with size-ratio and space-amplification triggers. It reclaims overwritten, deleted and expired data.
- **TTLs.** `EX` / `PX` / `EXAT` / `PXAT` / `KEEPTTL`, `EXPIRE`, `TTL` and `PERSIST`. Expiry is stored with each record and survives restarts.
- **Atomic batches and read-modify-write.** `MSET` is all-or-nothing even across a crash. `INCR`, `SET NX`, `APPEND` and the rest are atomic.
- **Embeddable.** The `lsm` package is a standalone Go key-value store you can use without the server.

## Quick start

```bash
go install github.com/1001harshit/emberkv/cmd/emberkv@latest
emberkv -dir ./data
```

Or from a clone:

```bash
make run          # build and start on 127.0.0.1:6380
make test         # unit + integration tests with the race detector
make bench        # load test a running server
```

With Docker:

```bash
docker build -t emberkv .
docker run -p 6380:6380 -v emberkv-data:/data emberkv
```

| Flag | Default | Meaning |
|---|---|---|
| `-addr` | `127.0.0.1:6380` | listen address (there is no auth, so keep it on localhost or a private network) |
| `-dir` | `data` | data directory |
| `-fsync` | `everysec` | `always`: fsync every write, so nothing acknowledged is ever lost. `everysec`: lose at most about 1s of writes. `no`: leave it to the OS |
| `-memtable-mb` | `4` | memtable size before it is flushed to an SSTable |
| `-compaction-trigger` | `4` | number of SSTables before background compaction starts |

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

**Group commit.** Writers hold the write lock only long enough to encode their record into an in-memory log buffer and apply it to the memtable. Then they release the lock and wait for durability. The first waiter becomes the *leader*: it writes everything buffered so far with a single `write(2)` (plus a single `fsync` under `-fsync always`) and wakes the others. Under load, dozens of writes share one syscall. This is the leader/follower scheme RocksDB uses, combined with "early lock release". A concurrent reader can see a write a moment before it is durable, as in Redis, but no client is acknowledged until its write is durable. The log also keeps writes in order, so a crash can only lose a suffix of unacknowledged writes. With 50 clients and `-fsync always`, this took `SET` from 365 to about 11,000 ops/s.

**Read path.** Point lookups check the memtable, then frozen memtables, then each SSTable from newest to oldest, and stop at the first version found. A tombstone or an expired entry means "not found". Each SSTable keeps its index and Bloom filter in memory, so a lookup costs at most one 4 KiB block read per table that might contain the key. Scans run a k-way heap merge over all sources and keep only the newest version of each key.

**Compaction.** Tables form one sequence from newest to oldest. The compactor only merges *contiguous* runs, so the merged table can take the run's place without breaking that order. It picks a run by these rules ([`compaction.go`](lsm/compaction.go)):
1. **Space amplification:** if the newer tables together are at least 2× the oldest, merge everything.
2. **Size ratio:** otherwise merge the first run of similarly sized tables, which keeps write amplification logarithmic.
3. **Table count:** otherwise merge the newest tables until the count is back under the trigger.

Tombstones and expired entries can only be dropped when the run includes the oldest table. Before that, they still have to hide older versions further down. `COMPACT` (an emberkv extension) forces a full merge.

**Crash recovery.** On startup, files missing from the `MANIFEST` are removed as leftovers of an interrupted flush or compaction. Every WAL is replayed oldest first, stopping at the first torn or corrupt record. The result is flushed to a table before new writes are accepted.

### On-disk format

```
000017.wal     [crc32c(4) | len(4) | count | entry…] …
000018.sst     [data block + crc]… [index block + crc] [bloom filter] [footer: offsets | count | magic]
MANIFEST       {"next_file": 19, "tables": [18, 12, 5]}
entry          kind(1) | expire_at_ms(uvarint) | key_len(uvarint) | key | val_len(uvarint) | value
```

## Supported commands

| Group | Commands |
|---|---|
| Strings | `GET` `SET [NX\|XX] [GET] [EX\|PX\|EXAT\|PXAT\|KEEPTTL]` `SETNX` `SETEX` `PSETEX` `GETSET` `GETDEL` `MGET` `MSET` `APPEND` `STRLEN` |
| Counters | `INCR` `DECR` `INCRBY` `DECRBY` |
| Keys | `DEL` `UNLINK` `EXISTS` `TYPE` `KEYS` `SCAN [MATCH] [COUNT] [TYPE]` `DBSIZE` `FLUSHDB` `FLUSHALL` |
| Expiry | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` `TTL` `PTTL` `PERSIST` |
| Server | `PING` `ECHO` `SELECT 0` `INFO [section]` `TIME` `QUIT` `COMMAND` `CLIENT` `CONFIG GET` |
| emberkv | `COMPACT`: flush and fully compact, reclaiming space |

Only the string type is supported. Lists, hashes, sets and the rest are on the roadmap.

## Testing

```bash
go test -race ./...
```

- **Model-based test**: 30,000 random puts, deletes, batches and reads, with reopens along the way, checked against a plain Go map. It uses a 4 KiB memtable so that flushes and compactions happen constantly.
- **Crash test**: copies the data directory while the database is still running (what the disk looks like after a power cut) and checks that the copy recovers every acknowledged write. I also checked by hand that `kill -9` in the middle of a write stream loses nothing with `-fsync always`.
- **Corruption tests**: torn WAL tails, flipped bits in WAL records and SSTable blocks, and bad footers.
- **Server integration tests**: drive every command over real TCP, including pipelining, inline commands, protocol errors, `SCAN` completeness and concurrent clients.

## Benchmarks

`emberbench` is a small `redis-benchmark`-style load generator. It was run with 50 clients, 64-byte values and 100k keys, on an Apple M2 Pro:

| Test (`-fsync everysec`) | Pipeline 1 | Pipeline 16 |
|---|---|---|
| `SET` | ~100k ops/s (p50 0.41 ms) | ~125k ops/s |
| `GET` | ~141k ops/s (p50 0.33 ms) | ~695k ops/s |
| `INCR` | ~105k ops/s | |
| 80/20 read/write | ~118k ops/s | |

Effect of group commit on writes (pipeline 1):

| | Before | After |
|---|---|---|
| `SET`, `-fsync everysec` | 65k ops/s, p50 0.98 ms | **100k ops/s**, p50 0.41 ms |
| `SET`, `-fsync always` | 365 ops/s, p50 148 ms | **11k ops/s**, p50 4.3 ms (30×) |

## Roadmap

- [x] Group commit: batch concurrent writers into one WAL write and fsync
- [ ] Block cache for hot SSTable blocks
- [ ] Hashes, lists and sets, encoded as composite keys over the same LSM
- [ ] Snapshots and iterators that don't hold the read lock for the whole scan
- [ ] Key-prefix compression inside blocks
- [ ] Replication via WAL shipping
- [ ] `AUTH` and TLS

## License

[MIT](LICENSE)
