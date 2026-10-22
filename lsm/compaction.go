package lsm

import (
	"os"
	"slices"

	"github.com/1001harshit/emberkv/internal/base"
)

// Compaction follows the "universal" strategy used by RocksDB: tables form a
// single newest-to-oldest sequence, and compaction merges a contiguous run of
// them into one table at the same position. Merging only contiguous runs
// keeps the sequence ordered by age, so a newer version always shadows an
// older one.
const (
	// sizeRatio lets a table join a run when it is at most (1+sizeRatio)
	// times the combined size of the newer tables already in the run.
	sizeRatio = 0.1
	// maxSpaceAmp triggers a full compaction once the newer tables together
	// are this many times the size of the oldest (largest) one.
	maxSpaceAmp = 2.0
)

// pickCompaction chooses the run of tables [start, end) to merge, given table
// sizes newest first. It returns start = -1 when no compaction is needed.
func pickCompaction(sizes []int64, trigger int) (start, end int) {
	n := len(sizes)
	if n < trigger || n < 2 {
		return -1, -1
	}

	// 1. Space amplification: too much data sits above the oldest table,
	//    likely overwrites and deletes of it. Rewrite everything.
	var newer int64
	for _, s := range sizes[:n-1] {
		newer += s
	}
	if float64(newer) >= maxSpaceAmp*float64(sizes[n-1]) {
		return 0, n
	}

	// 2. Size ratio: merge the first run of similarly sized tables, which
	//    keeps write amplification logarithmic.
	for start := 0; start < n-1; start++ {
		acc, end := sizes[start], start+1
		for end < n && float64(sizes[end]) <= float64(acc)*(1+sizeRatio) {
			acc += sizes[end]
			end++
		}
		if end-start >= 2 {
			return start, end
		}
	}

	// 3. Table count: merge the newest tables to get back under the trigger.
	return 0, n - trigger + 2
}

func (db *DB) compactLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.closing:
			return
		case <-db.compactCh:
		}
		for {
			did, err := db.compactOnce()
			if err != nil {
				db.setBgErr(err)
				break
			}
			if !did {
				break
			}
			select {
			case <-db.closing:
				return
			default:
			}
		}
	}
}

// compactOnce runs one compaction if one is due and reports whether it did.
func (db *DB) compactOnce() (bool, error) {
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.RLock()
	tables := slices.Clone(db.tables)
	db.mu.RUnlock()

	sizes := make([]int64, len(tables))
	for i, t := range tables {
		sizes[i] = t.r.Size()
	}
	start, end := pickCompaction(sizes, db.opts.CompactionTrigger)
	if start < 0 {
		return false, nil
	}
	return true, db.compactRun(tables[start:end], end == len(tables))
}

// Compact flushes the memtable and merges every table into one, discarding
// overwritten, deleted and expired data.
func (db *DB) Compact() error {
	if err := db.Flush(); err != nil {
		return err
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.RLock()
	tables, closed := slices.Clone(db.tables), db.closed
	db.mu.RUnlock()
	if closed {
		return ErrClosed
	}
	if len(tables) == 0 {
		return nil
	}
	return db.compactRun(tables, true)
}

// compactRun merges a contiguous run of tables (newest first) into one table
// that takes the run's place. bottom says the run includes the oldest table,
// so tombstones and expired entries can be dropped. Caller holds compactMu.
func (db *DB) compactRun(inputs []*table, bottom bool) error {
	iters := make([]base.Iterator, len(inputs))
	for i, t := range inputs {
		iters[i] = t.iterator(nil)
	}
	var drop func(*base.Entry) bool
	if f := db.opts.CompactionFilter; f != nil {
		drop = func(e *base.Entry) bool {
			return f(e.Key, Item{Value: e.Value, ExpireAt: e.ExpireAt})
		}
	}
	out, err := db.writeTable(db.newFileID(), newMergeIterator(iters), bottom, drop)
	if err != nil {
		return err
	}

	db.mu.Lock()
	// Flushes may have added newer tables since the run was chosen, but only
	// at the front, so the run is still contiguous.
	i := slices.Index(db.tables, inputs[0])
	tables := make([]*table, 0, len(db.tables)-len(inputs)+1)
	tables = append(tables, db.tables[:i]...)
	if out != nil {
		tables = append(tables, out)
	}
	tables = append(tables, db.tables[i+len(inputs):]...)
	db.tables = tables
	err = db.saveManifestLocked()
	db.mu.Unlock()
	if err != nil {
		return err
	}

	// Readers hold mu.RLock for the whole of each read, so once the swap above
	// is done no reader can still be using the input tables.
	var inBytes int64
	for _, t := range inputs {
		inBytes += t.r.Size()
		t.r.Close()
		os.Remove(db.tablePath(t.id))
	}
	db.compactions.Add(1)
	if out != nil {
		db.opts.Logger.Printf("compacted %d tables (%d bytes) into %06d.sst (%d keys, %d bytes)",
			len(inputs), inBytes, out.id, out.r.Count(), out.r.Size())
	}
	return nil
}
