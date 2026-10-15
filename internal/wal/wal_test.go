package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/1001harshit/emberkv/internal/base"
)

func writeLog(t *testing.T, path string, batches int) {
	t.Helper()
	w, err := Create(path, SyncAlways)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < batches; i++ {
		err := w.Append(
			base.Entry{Key: []byte(fmt.Sprintf("k%d", i)), Value: []byte("v"), Kind: base.KindSet, ExpireAt: int64(i)},
			base.Entry{Key: []byte(fmt.Sprintf("d%d", i)), Kind: base.KindDelete},
		)
		if err != nil {
			t.Fatal(err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
}

func replayAll(t *testing.T, path string) [][]base.Entry {
	t.Helper()
	var got [][]base.Entry
	if _, err := Replay(path, func(es []base.Entry) error {
		got = append(got, es)
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	return got
}

func TestRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	writeLog(t, path, 100)
	got := replayAll(t, path)
	if len(got) != 100 {
		t.Fatalf("replayed %d batches, want 100", len(got))
	}
	e := got[42][0]
	if string(e.Key) != "k42" || string(e.Value) != "v" || e.ExpireAt != 42 || e.Kind != base.KindSet {
		t.Fatalf("batch 42 decoded wrong: %+v", e)
	}
	if got[42][1].Kind != base.KindDelete {
		t.Fatal("tombstone decoded wrong")
	}
}

func TestTornTailIsIgnored(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	writeLog(t, path, 10)
	data, _ := os.ReadFile(path)
	// Simulate a crash part-way through writing the final record.
	if err := os.WriteFile(path, data[:len(data)-3], 0o644); err != nil {
		t.Fatal(err)
	}
	if got := replayAll(t, path); len(got) != 9 {
		t.Fatalf("replayed %d batches, want 9", len(got))
	}
}

func TestCorruptRecordStopsReplay(t *testing.T) {
	path := filepath.Join(t.TempDir(), "000001.wal")
	writeLog(t, path, 10)
	data, _ := os.ReadFile(path)
	data[len(data)/2] ^= 0xff
	os.WriteFile(path, data, 0o644)
	got := replayAll(t, path)
	if len(got) >= 10 || len(got) == 0 {
		t.Fatalf("replayed %d batches; expected a prefix before the corruption", len(got))
	}
}

func TestParseSyncMode(t *testing.T) {
	for in, want := range map[string]SyncMode{"always": SyncAlways, "everysec": SyncEverySec, "no": SyncNever} {
		if got, err := ParseSyncMode(in); err != nil || got != want {
			t.Errorf("ParseSyncMode(%q) = %v, %v", in, got, err)
		}
	}
	if _, err := ParseSyncMode("sometimes"); err == nil {
		t.Error("expected error for unknown mode")
	}
}
