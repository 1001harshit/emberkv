// Package lsm is an embeddable key-value store built on a log-structured
// merge tree.
//
// Writes go to a write-ahead log and an in-memory memtable. A full memtable is
// frozen and flushed by a background goroutine into an immutable SSTable.
// Another goroutine merges runs of SSTables (see compaction.go), dropping
// shadowed versions, and dropping tombstones and expired keys when the run
// includes the oldest table.
package lsm

import (
	"errors"
	"fmt"
	"hash/maphash"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/1001harshit/emberkv/internal/base"
	"github.com/1001harshit/emberkv/internal/cache"
	"github.com/1001harshit/emberkv/internal/memtable"
	"github.com/1001harshit/emberkv/internal/sstable"
	"github.com/1001harshit/emberkv/internal/wal"
)

var (
	// ErrNotFound is returned when a key does not exist or has expired.
	ErrNotFound = errors.New("lsm: key not found")
	// ErrClosed is returned by operations on a closed DB.
	ErrClosed = errors.New("lsm: database closed")
)

// Options configures a DB. Zero values select the defaults.
type Options struct {
	// MemtableSize is the approximate size in bytes at which the memtable is
	// frozen and flushed. Default 4 MiB.
	MemtableSize int
	// SyncMode controls WAL fsync behaviour. Default wal.SyncEverySec.
	SyncMode wal.SyncMode
	// CompactionTrigger is how many SSTables may exist before background
	// compaction starts merging them. Default 4.
	CompactionTrigger int
	// BloomBitsPerKey sizes each table's Bloom filter. Default 10 (~1% false positives).
	BloomBitsPerKey int
	// BlockCacheSize is the memory, in bytes, for caching recently read
	// SSTable blocks. Default 64 MiB; negative disables the cache.
	BlockCacheSize int64
	// MaxImmutable is how many frozen memtables may await flushing before
	// writes stall. Default 4.
	MaxImmutable int
	// Logger receives background errors and events. Default discards.
	Logger *log.Logger
	// Now returns the current time; used for TTLs. Default time.Now.
	Now func() time.Time
	// CompactionFilter, if set, is called for each value entry rewritten by a
	// compaction; returning true drops the entry. It lets a layer above
	// garbage-collect records that became unreachable, such as the fields of
	// a deleted hash. It must only drop keys that can never become reachable
	// again, because an older copy of a dropped key may still exist in older
	// tables. It may read from the DB.
	CompactionFilter func(key []byte, item Item) bool
}

func (o Options) withDefaults() Options {
	if o.MemtableSize <= 0 {
		o.MemtableSize = 4 << 20
	}
	if o.CompactionTrigger < 2 {
		o.CompactionTrigger = 4
	}
	if o.BloomBitsPerKey <= 0 {
		o.BloomBitsPerKey = 10
	}
	if o.BlockCacheSize == 0 {
		o.BlockCacheSize = 64 << 20
	}
	if o.MaxImmutable <= 0 {
		o.MaxImmutable = 4
	}
	if o.Logger == nil {
		o.Logger = log.New(io.Discard, "", 0)
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	return o
}

// Item is a stored value and its expiry.
type Item struct {
	Value    []byte
	ExpireAt int64 // unix milliseconds; 0 means no expiry
}

type table struct {
	id uint64
	r  *sstable.Reader
}

type immutable struct {
	mem     *memtable.Memtable
	walPath string
}

// DB is a persistent, concurrency-safe key-value store.
type DB struct {
	dir  string
	opts Options
	lock *os.File

	// locks serialises writers that touch the same keys; see locks.go.
	locks keyLocks
	// appendMu serialises the short step that appends to the WAL buffer and
	// the memtable, so the log records writes in the order they happen.
	appendMu sync.Mutex
	// compactMu ensures only one compaction runs at a time.
	compactMu sync.Mutex

	// mu guards the fields below. Lock order: key locks, appendMu, compactMu, mu.
	mu      sync.RWMutex
	stall   *sync.Cond
	mem     *memtable.Memtable
	wal     *wal.Writer
	walPath string
	imm     []*immutable // newest first
	tables  []*table     // newest first
	bgErr   error
	closed  bool

	nextFile atomic.Uint64
	cache    *cache.Cache // nil when disabled

	flushCh   chan struct{}
	compactCh chan struct{}
	closing   chan struct{}
	wg        sync.WaitGroup

	flushes     atomic.Uint64
	compactions atomic.Uint64
}

// Open opens or creates a database in dir, replaying any write-ahead logs left
// by a previous run.
func Open(dir string, opts Options) (*DB, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockDir(dir)
	if err != nil {
		return nil, err
	}
	db := &DB{
		dir:       dir,
		opts:      opts.withDefaults(),
		lock:      lock,
		flushCh:   make(chan struct{}, 1),
		compactCh: make(chan struct{}, 1),
		closing:   make(chan struct{}),
	}
	db.stall = sync.NewCond(&db.mu)
	db.locks.seed = maphash.MakeSeed()
	if db.opts.BlockCacheSize > 0 {
		db.cache = cache.New(db.opts.BlockCacheSize)
	}
	if err := db.recover(); err != nil {
		for _, t := range db.tables {
			t.r.Close()
		}
		unlockDir(lock)
		return nil, err
	}
	db.wg.Add(2)
	go db.flushLoop()
	go db.compactLoop()
	signal(db.compactCh)
	return db, nil
}

func (db *DB) tablePath(id uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.sst", id))
}

func (db *DB) walFilePath(id uint64) string {
	return filepath.Join(db.dir, fmt.Sprintf("%06d.wal", id))
}

func (db *DB) newFileID() uint64 { return db.nextFile.Add(1) - 1 }

func (db *DB) recover() error {
	m, err := readManifest(db.dir)
	if err != nil {
		return err
	}
	live := make(map[uint64]bool)
	for _, id := range m.Tables {
		r, err := sstable.Open(db.tablePath(id))
		if err != nil {
			return err
		}
		r.SetCache(id, db.cache)
		db.tables = append(db.tables, &table{id: id, r: r})
		live[id] = true
	}

	ents, err := os.ReadDir(db.dir)
	if err != nil {
		return err
	}
	next := m.NextFile
	var wals []uint64
	for _, de := range ents {
		name := de.Name()
		ext := filepath.Ext(name)
		id, err := strconv.ParseUint(strings.TrimSuffix(name, ext), 10, 64)
		if err != nil {
			continue
		}
		next = max(next, id+1)
		switch ext {
		case ".wal":
			wals = append(wals, id)
		case ".sst":
			if !live[id] {
				// Output of a flush or compaction that never made it into the manifest.
				os.Remove(filepath.Join(db.dir, name))
			}
		}
	}
	db.nextFile.Store(next)

	// Replay logs oldest first; together they hold every write not yet in a table.
	sort.Slice(wals, func(i, j int) bool { return wals[i] < wals[j] })
	mem := memtable.New()
	for _, id := range wals {
		_, err := wal.Replay(db.walFilePath(id), func(entries []base.Entry) error {
			for _, e := range entries {
				mem.Put(e)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}
	if mem.Len() > 0 {
		t, err := db.writeTable(db.newFileID(), mem.Iterator(nil), false, nil)
		if err != nil {
			return err
		}
		db.tables = append([]*table{t}, db.tables...)
		db.opts.Logger.Printf("recovered %d keys from write-ahead log", mem.Len())
	}
	if err := db.saveManifestLocked(); err != nil {
		return err
	}
	for _, id := range wals {
		os.Remove(db.walFilePath(id))
	}

	db.mem = memtable.New()
	db.walPath = db.walFilePath(db.newFileID())
	db.wal, err = wal.Create(db.walPath, db.opts.SyncMode)
	return err
}

func (db *DB) saveManifestLocked() error {
	m := manifest{NextFile: db.nextFile.Load(), Tables: []uint64{}}
	for _, t := range db.tables {
		m.Tables = append(m.Tables, t.id)
	}
	return writeManifest(db.dir, m)
}

// writeTable writes the entries from it to a new table. When bottom is true,
// tombstones and expired entries are dropped; entries for which drop returns
// true are dropped too. It returns nil if no entries remain.
func (db *DB) writeTable(id uint64, it base.Iterator, bottom bool, drop func(*base.Entry) bool) (*table, error) {
	path := db.tablePath(id)
	w, err := sstable.Create(path, db.opts.BloomBitsPerKey)
	if err != nil {
		return nil, err
	}
	now := db.nowMs()
	for it.Next() {
		e := it.Entry()
		if bottom && !e.Live(now) {
			continue
		}
		if drop != nil && e.Kind == base.KindSet && drop(e) {
			continue
		}
		if err := w.Add(e); err != nil {
			w.Abort()
			return nil, err
		}
	}
	if err := it.Err(); err != nil {
		w.Abort()
		return nil, err
	}
	if w.Count() == 0 {
		w.Abort()
		return nil, nil
	}
	if err := w.Finish(); err != nil {
		return nil, err
	}
	r, err := sstable.Open(path)
	if err != nil {
		return nil, err
	}
	r.SetCache(id, db.cache)
	return &table{id: id, r: r}, nil
}

func signal(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

func (db *DB) nowMs() int64 { return db.opts.Now().UnixMilli() }

// Now returns the current time according to the DB's clock, which is what
// expiry times are compared against.
func (db *DB) Now() time.Time { return db.opts.Now() }

// ---- writes ----

// Batch is a group of writes applied atomically.
type Batch struct {
	entries []base.Entry
}

// Set adds a write of key=value with an optional expiry (unix ms, 0 for none).
func (b *Batch) Set(key, value []byte, expireAt int64) *Batch {
	b.entries = append(b.entries, base.Entry{Key: key, Value: value, Kind: base.KindSet, ExpireAt: expireAt})
	return b
}

// Delete adds a deletion of key.
func (b *Batch) Delete(key []byte) *Batch {
	b.entries = append(b.entries, base.Entry{Key: key, Kind: base.KindDelete})
	return b
}

// Len returns the number of writes in the batch.
func (b *Batch) Len() int { return len(b.entries) }

// Put stores value under key with no expiry.
func (db *DB) Put(key, value []byte) error {
	return db.Write(new(Batch).Set(key, value, 0))
}

// PutTTL stores value under key, expiring after ttl.
func (db *DB) PutTTL(key, value []byte, ttl time.Duration) error {
	return db.Write(new(Batch).Set(key, value, db.opts.Now().Add(ttl).UnixMilli()))
}

// Delete removes key. Deleting a missing key is not an error.
func (db *DB) Delete(key []byte) error {
	return db.Write(new(Batch).Delete(key))
}

// Write applies every write in b atomically: after a crash, either all or
// none of them are recovered. It takes the key locks of every key in b.
func (db *DB) Write(b *Batch) error {
	keys := make([][]byte, len(b.entries))
	for i := range b.entries {
		keys[i] = b.entries[i].Key
	}
	unlock := db.locks.lock(keys)
	c, err := db.apply(b.entries)
	unlock()
	if err != nil {
		return err
	}
	return c.Wait()
}

// Update atomically reads key and replaces it with fn's result. fn receives
// nil if the key is absent or expired. If write is false nothing changes;
// otherwise next is stored, or the key is deleted when next is nil.
func (db *DB) Update(key []byte, fn func(cur *Item) (next *Item, write bool, err error)) error {
	c, err := db.update(key, fn)
	if err != nil {
		return err
	}
	return c.Wait()
}

func (db *DB) update(key []byte, fn func(cur *Item) (*Item, bool, error)) (Commit, error) {
	defer db.locks.lock([][]byte{key})()

	var cur *Item
	item, err := db.GetItem(key)
	switch {
	case err == nil:
		cur = &item
	case !errors.Is(err, ErrNotFound):
		return Commit{}, err
	}
	next, write, err := fn(cur)
	if err != nil || !write {
		return Commit{}, err
	}
	e := base.Entry{Key: key, Kind: base.KindDelete}
	if next != nil {
		e = base.Entry{Key: key, Value: next.Value, Kind: base.KindSet, ExpireAt: next.ExpireAt}
	} else if cur == nil {
		return Commit{}, nil // deleting a key that is already gone
	}
	return db.apply([]base.Entry{e})
}

// Txn is a read-modify-write transaction over any number of keys. It sees
// its own pending writes, and its writes are applied as one atomic batch.
type Txn struct {
	db      *DB
	entries []base.Entry
	index   map[string]int
}

// Get returns the item for key as the transaction currently sees it.
func (tx *Txn) Get(key []byte) (Item, error) {
	if i, ok := tx.index[string(key)]; ok {
		e := &tx.entries[i]
		if !e.Live(tx.db.nowMs()) {
			return Item{}, ErrNotFound
		}
		return Item{Value: e.Value, ExpireAt: e.ExpireAt}, nil
	}
	return tx.db.GetItem(key)
}

func (tx *Txn) put(e base.Entry) {
	if i, ok := tx.index[string(e.Key)]; ok {
		tx.entries[i] = e
		return
	}
	if tx.index == nil {
		tx.index = make(map[string]int)
	}
	tx.index[string(e.Key)] = len(tx.entries)
	tx.entries = append(tx.entries, e)
}

// Set stages a write of key=value with an optional expiry (unix ms, 0 for none).
func (tx *Txn) Set(key, value []byte, expireAt int64) {
	tx.put(base.Entry{Key: key, Value: value, Kind: base.KindSet, ExpireAt: expireAt})
}

// Delete stages a deletion of key.
func (tx *Txn) Delete(key []byte) {
	tx.put(base.Entry{Key: key, Kind: base.KindDelete})
}

// Txn runs fn with exclusive access to every key and atomically applies the
// writes it stages, unless fn returns an error. Prefer TxnKeys, which only
// excludes writers of the same keys.
func (db *DB) Txn(fn func(tx *Txn) error) error {
	return db.txn(db.locks.lockAll(), fn)
}

// TxnKeys is Txn holding only the key locks for lockKeys: transactions with
// disjoint lock keys run their reads and logic in parallel. Every write that
// fn stages, and every key whose value fn's decision depends on, must be
// covered by lockKeys, in the sense that all other writers of that data lock
// the same keys. A layer above may use its own lock keys, e.g. a user key
// that stands for several internal records.
func (db *DB) TxnKeys(lockKeys [][]byte, fn func(tx *Txn) error) error {
	return db.txn(db.locks.lock(lockKeys), fn)
}

// TxnKeysNoWait is TxnKeys without waiting for durability: the writes are
// applied and visible when it returns, and the caller must call Wait on the
// result before telling anyone they are durable. A server can apply a whole
// pipeline of commands and wait once, so they share one log write.
func (db *DB) TxnKeysNoWait(lockKeys [][]byte, fn func(tx *Txn) error) (Commit, error) {
	return db.txnNoWait(db.locks.lock(lockKeys), fn)
}

func (db *DB) txn(unlock func(), fn func(tx *Txn) error) error {
	c, err := db.txnNoWait(unlock, fn)
	if err != nil {
		return err
	}
	return c.Wait()
}

func (db *DB) txnNoWait(unlock func(), fn func(tx *Txn) error) (Commit, error) {
	defer unlock()
	tx := &Txn{db: db}
	if err := fn(tx); err != nil {
		return Commit{}, err
	}
	return db.apply(tx.entries)
}

// Commit identifies a write that has been applied to the memtable but may
// not yet be durable. The zero Commit is already durable.
type Commit struct {
	w   *wal.Writer
	lsn uint64
}

// Wait blocks until the write is durable according to the sync mode.
func (c Commit) Wait() error {
	if c.w == nil {
		return nil
	}
	return c.w.Commit(c.lsn)
}

// apply appends entries to the WAL buffer and the memtable. The caller holds
// the key locks of the entries and must call wait on the result after
// releasing them.
//
// Only this step is serialised across all writers (by appendMu), and it does
// no I/O. Reads and logic of a transaction run before it, in parallel with
// writers of other keys.
//
// This is group commit with early lock release: the next writer can apply
// its changes while this one waits for I/O, and one leader writes and
// fsyncs the log for everyone waiting. A concurrent reader may therefore see
// a write slightly before it is durable (as in Redis), but a client is only
// acknowledged once its write is durable, and the log preserves order, so a
// crash can only lose a suffix of unacknowledged writes.
func (db *DB) apply(entries []base.Entry) (Commit, error) {
	if len(entries) == 0 {
		return Commit{}, nil
	}
	db.appendMu.Lock()
	defer db.appendMu.Unlock()
	db.mu.RLock()
	closed, bgErr := db.closed, db.bgErr
	db.mu.RUnlock()
	if closed {
		return Commit{}, ErrClosed
	}
	if bgErr != nil {
		return Commit{}, bgErr
	}
	w := db.wal
	lsn, err := w.Append(entries...)
	if err != nil {
		return Commit{}, err
	}
	for _, e := range entries {
		db.mem.Put(e)
	}
	if db.mem.Size() >= db.opts.MemtableSize {
		// rotate closes w, which writes and fsyncs everything buffered in it.
		if err := db.rotate(); err != nil {
			return Commit{}, err
		}
	}
	return Commit{w: w, lsn: lsn}, nil
}

// rotate freezes the current memtable and starts a new memtable and log.
// The caller holds appendMu.
func (db *DB) rotate() error {
	path := db.walFilePath(db.newFileID())
	w, err := wal.Create(path, db.opts.SyncMode)
	if err != nil {
		return err
	}

	db.mu.Lock()
	for len(db.imm) >= db.opts.MaxImmutable && db.bgErr == nil {
		db.stall.Wait() // flushing has fallen behind; apply backpressure
	}
	if db.bgErr != nil {
		err := db.bgErr
		db.mu.Unlock()
		w.Close()
		os.Remove(path)
		return err
	}
	old := db.wal
	db.imm = append([]*immutable{{mem: db.mem, walPath: db.walPath}}, db.imm...)
	db.mem = memtable.New()
	db.wal, db.walPath = w, path
	db.mu.Unlock()

	if err := old.Close(); err != nil {
		db.opts.Logger.Printf("closing write-ahead log: %v", err)
	}
	signal(db.flushCh)
	return nil
}

// Flush writes the memtable to an SSTable and waits until every frozen
// memtable has been flushed.
func (db *DB) Flush() error {
	db.appendMu.Lock()
	defer db.appendMu.Unlock()
	db.mu.RLock()
	closed := db.closed
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if db.mem.Len() > 0 {
		if err := db.rotate(); err != nil {
			return err
		}
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	for len(db.imm) > 0 && db.bgErr == nil {
		db.stall.Wait()
	}
	return db.bgErr
}

func (db *DB) setBgErr(err error) {
	db.opts.Logger.Printf("background error: %v", err)
	db.mu.Lock()
	if db.bgErr == nil {
		db.bgErr = err
	}
	db.stall.Broadcast()
	db.mu.Unlock()
}

// ---- background work ----

func (db *DB) flushLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.closing:
			return
		case <-db.flushCh:
		}
		for {
			db.mu.RLock()
			var im *immutable
			if n := len(db.imm); n > 0 {
				im = db.imm[n-1]
			}
			db.mu.RUnlock()
			if im == nil {
				break
			}
			if err := db.flush(im); err != nil {
				db.setBgErr(err)
				break
			}
		}
	}
}

// flush writes the oldest frozen memtable to a new, newest table.
func (db *DB) flush(im *immutable) error {
	t, err := db.writeTable(db.newFileID(), im.mem.Iterator(nil), false, nil)
	if err != nil {
		return err
	}
	db.mu.Lock()
	if t != nil {
		db.tables = append([]*table{t}, db.tables...)
	}
	db.imm = db.imm[:len(db.imm)-1]
	err = db.saveManifestLocked()
	db.stall.Broadcast()
	db.mu.Unlock()
	if err != nil {
		return err // keep the log; recovery will replay it
	}
	os.Remove(im.walPath)
	db.flushes.Add(1)
	signal(db.compactCh)
	return nil
}

// ---- reads ----

func (db *DB) lookup(key []byte) (base.Entry, bool, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return base.Entry{}, false, ErrClosed
	}
	if e, ok := db.mem.Get(key); ok {
		return e, true, nil
	}
	for _, im := range db.imm {
		if e, ok := im.mem.Get(key); ok {
			return e, true, nil
		}
	}
	for _, t := range db.tables {
		e, ok, err := t.r.Get(key)
		if err != nil || ok {
			return e, ok, err
		}
	}
	return base.Entry{}, false, nil
}

// Get returns the value stored for key, or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	item, err := db.GetItem(key)
	return item.Value, err
}

// GetItem returns the value and expiry stored for key, or ErrNotFound.
func (db *DB) GetItem(key []byte) (Item, error) {
	e, ok, err := db.lookup(key)
	if err != nil {
		return Item{}, err
	}
	if !ok || !e.Live(db.nowMs()) {
		return Item{}, ErrNotFound
	}
	return Item{Value: e.Value, ExpireAt: e.ExpireAt}, nil
}

// Scan calls fn for each live key >= start (nil for all) in ascending order,
// until fn returns false. fn must not call methods on db.
func (db *DB) Scan(start []byte, fn func(key []byte, item Item) bool) error {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return ErrClosed
	}
	iters := []base.Iterator{db.mem.Iterator(start)}
	for _, im := range db.imm {
		iters = append(iters, im.mem.Iterator(start))
	}
	for _, t := range db.tables {
		iters = append(iters, t.r.Iterator(start))
	}
	now := db.nowMs()
	m := newMergeIterator(iters)
	for m.Next() {
		e := m.Entry()
		if !e.Live(now) {
			continue
		}
		if !fn(e.Key, Item{Value: e.Value, ExpireAt: e.ExpireAt}) {
			break
		}
	}
	return m.Err()
}

// Count returns the number of live keys. It scans the whole database.
func (db *DB) Count() (int, error) {
	n := 0
	err := db.Scan(nil, func([]byte, Item) bool { n++; return true })
	return n, err
}

// TableInfo describes one SSTable.
type TableInfo struct {
	ID    uint64
	Keys  uint64
	Bytes int64
}

// Stats is a snapshot of the store's internal state.
type Stats struct {
	MemtableBytes int
	MemtableKeys  int
	Immutable     int
	Tables        []TableInfo
	Flushes       uint64
	Compactions   uint64
	CacheHits     uint64
	CacheMisses   uint64
	CacheBytes    int64
}

// Stats returns a snapshot of the store's internal state.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	s := Stats{
		MemtableBytes: db.mem.Size(),
		MemtableKeys:  db.mem.Len(),
		Immutable:     len(db.imm),
		Flushes:       db.flushes.Load(),
		Compactions:   db.compactions.Load(),
	}
	if db.cache != nil {
		s.CacheHits, s.CacheMisses, s.CacheBytes = db.cache.Stats()
	}
	for _, t := range db.tables {
		s.Tables = append(s.Tables, TableInfo{ID: t.id, Keys: t.r.Count(), Bytes: t.r.Size()})
	}
	return s
}

// Close stops background work and releases the database. Unflushed writes are
// safe in the write-ahead log and are recovered by the next Open.
func (db *DB) Close() error {
	// Waiting for appendMu lets any in-flight apply finish; later ones see closed.
	db.appendMu.Lock()
	defer db.appendMu.Unlock()

	db.mu.Lock()
	if db.closed {
		db.mu.Unlock()
		return nil
	}
	db.closed = true
	db.mu.Unlock()

	close(db.closing)
	db.wg.Wait()
	// Wait for a manual Compact that may still be running.
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	err := db.wal.Close()
	db.mu.Lock()
	for _, t := range db.tables {
		t.r.Close()
	}
	db.mu.Unlock()
	if uerr := unlockDir(db.lock); err == nil {
		err = uerr
	}
	return err
}
