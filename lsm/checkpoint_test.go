//go:build unix

package lsm

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestCheckpointIsIndependentAndShared(t *testing.T) {
	clock := newFakeClock()
	root := t.TempDir()
	opts := historyOpts(clock, time.Hour)
	db := mustOpen(t, filepath.Join(root, "main"), opts)
	defer db.Close()
	for i := 0; i < 2000; i++ {
		db.Put([]byte(fmt.Sprintf("k%04d", i)), []byte("main"))
		clock.Advance(time.Millisecond)
	}

	branchDir := filepath.Join(root, "branch")
	if err := db.Checkpoint(branchDir, time.Time{}); err != nil {
		t.Fatal(err)
	}
	br := mustOpen(t, branchDir, opts)
	defer br.Close()

	// Tables are hard links, not copies.
	ents, _ := os.ReadDir(branchDir)
	linked := 0
	for _, e := range ents {
		if filepath.Ext(e.Name()) == ".sst" {
			fi, _ := os.Stat(filepath.Join(branchDir, e.Name()))
			if fi.Sys().(*syscall.Stat_t).Nlink >= 2 {
				linked++
			}
		}
	}
	if linked == 0 {
		t.Fatal("no SSTable in the branch is a hard link")
	}

	br.Put([]byte("k0001"), []byte("branch"))
	br.Delete([]byte("k0002"))
	db.Put([]byte("k0003"), []byte("main-changed"))
	mustGet(t, br, "k0001", "branch")
	mustMissing(t, br, "k0002")
	mustGet(t, br, "k0003", "main")
	mustGet(t, db, "k0001", "main")
	mustGet(t, db, "k0002", "main")

	// The parent compacting (and unlinking) its tables must not hurt the branch.
	if err := db.Compact(); err != nil {
		t.Fatal(err)
	}
	if n, _ := br.Count(); n != 1999 {
		t.Fatalf("branch Count = %d after parent compaction", n)
	}
	mustGet(t, br, "k1999", "main")
}

func TestCheckpointOfThePast(t *testing.T) {
	clock := newFakeClock()
	root := t.TempDir()
	opts := historyOpts(clock, time.Hour)
	db := mustOpen(t, filepath.Join(root, "main"), opts)
	defer db.Close()

	db.Put([]byte("a"), []byte("1"))
	db.Put([]byte("b"), []byte("1"))
	clock.Advance(time.Second)
	past := clock.Now()
	clock.Advance(time.Second)
	db.Put([]byte("a"), []byte("2"))
	db.Delete([]byte("b"))
	db.Put([]byte("c"), []byte("new"))
	clock.Advance(time.Second)

	branchDir := filepath.Join(root, "past")
	if err := db.Checkpoint(branchDir, past); err != nil {
		t.Fatal(err)
	}
	br := mustOpen(t, branchDir, opts)
	mustGet(t, br, "a", "1")
	mustGet(t, br, "b", "1")
	mustMissing(t, br, "c")

	// New writes in the branch are newer than the cutoff and visible.
	clock.Advance(time.Second)
	br.Put([]byte("c"), []byte("branch"))
	mustGet(t, br, "c", "branch")

	// The cutoff survives the branch's own compaction and a reopen.
	if err := br.Compact(); err != nil {
		t.Fatal(err)
	}
	br.Close()
	br = mustOpen(t, branchDir, opts)
	defer br.Close()
	mustGet(t, br, "a", "1")
	mustGet(t, br, "b", "1")
	mustGet(t, br, "c", "branch")
	mustGet(t, db, "a", "2")

	if err := db.Checkpoint(filepath.Join(root, "too-old"), past.Add(-2*time.Hour)); err == nil {
		t.Fatal("checkpoint older than the history window succeeded")
	}
}
