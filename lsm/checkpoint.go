package lsm

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/1001harshit/emberkv/internal/base"
	"github.com/1001harshit/emberkv/internal/fileutil"
)

// Checkpoint creates, in the new directory dir, a separate database holding
// this one's data, as it is now or, if at is non-zero, as it was at time at.
// The copy is cheap: SSTables never change once written, so they are shared
// through hard links rather than copied, and a past state is selected with a
// per-table cutoff instead of rewriting any data. The two databases are then
// independent; each one's compactions remove only its own links.
//
// dir should be on the same filesystem; otherwise tables are copied.
func (db *DB) Checkpoint(dir string, at time.Time) error {
	if !at.IsZero() && at.Before(db.OldestReadable()) {
		return fmt.Errorf("lsm: checkpoint time %v is older than the history window", at)
	}
	// Move everything into tables, then hold off compactions so no table
	// disappears while it is being linked.
	if err := db.Flush(); err != nil {
		return err
	}
	db.compactMu.Lock()
	defer db.compactMu.Unlock()

	db.mu.RLock()
	tables := append([]*table(nil), db.tables...)
	m := manifest{Format: formatVersion, NextFile: db.nextFile.Load(), LastVersion: db.lastVersion.Load(), Tables: []uint64{}}
	db.mu.RUnlock()

	if err := os.Mkdir(dir, 0o755); err != nil {
		return err
	}
	ok := false
	defer func() {
		if !ok {
			os.RemoveAll(dir)
		}
	}()

	var cutoff uint64
	if !at.IsZero() {
		cutoff = base.VersionAt(at.UnixMilli())
	}
	for _, t := range tables {
		name := filepath.Base(db.tablePath(t.id))
		if err := linkOrCopy(db.tablePath(t.id), filepath.Join(dir, name)); err != nil {
			return err
		}
		m.Tables = append(m.Tables, t.id)
		c := t.cutoff
		if cutoff != 0 && (c == 0 || cutoff < c) {
			c = cutoff
		}
		if c != 0 {
			if m.Cutoffs == nil {
				m.Cutoffs = make(map[uint64]uint64)
			}
			m.Cutoffs[t.id] = c
		}
	}
	if err := writeManifest(dir, m); err != nil {
		return err
	}
	ok = true
	return nil
}

func linkOrCopy(src, dst string) error {
	if err := os.Link(src, dst); err == nil {
		return nil
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	if err := out.Sync(); err != nil {
		out.Close()
		return err
	}
	if err := out.Close(); err != nil {
		return err
	}
	return fileutil.SyncDir(filepath.Dir(dst))
}
