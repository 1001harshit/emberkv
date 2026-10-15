// Package wal implements the write-ahead log that makes memtable contents
// durable before they are flushed to an SSTable.
//
// The log is a sequence of records, each holding one atomic batch:
//
//	crc32c(4) | length(4) | count(uvarint) | entry...
//
// The CRC covers the length field and the payload. Replay stops at the first
// torn or corrupt record, which is what a crash mid-write leaves behind.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/1001harshit/emberkv/internal/base"
	"github.com/1001harshit/emberkv/internal/fileutil"
)

// SyncMode controls when the log is fsynced, mirroring Redis's appendfsync.
type SyncMode int

const (
	// SyncEverySec fsyncs at most once per second; a crash loses up to ~1s of writes.
	SyncEverySec SyncMode = iota
	// SyncAlways fsyncs after every write; nothing acknowledged is ever lost.
	SyncAlways
	// SyncNever leaves fsync to the OS.
	SyncNever
)

func (m SyncMode) String() string {
	switch m {
	case SyncAlways:
		return "always"
	case SyncNever:
		return "no"
	default:
		return "everysec"
	}
}

// ParseSyncMode parses "always", "everysec" or "no".
func ParseSyncMode(s string) (SyncMode, error) {
	switch strings.ToLower(s) {
	case "always":
		return SyncAlways, nil
	case "everysec", "":
		return SyncEverySec, nil
	case "no", "never":
		return SyncNever, nil
	}
	return 0, fmt.Errorf("wal: unknown sync mode %q (want always, everysec or no)", s)
}

const headerSize = 8

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// Writer appends batches to a log file.
type Writer struct {
	mu    sync.Mutex
	f     *os.File
	mode  SyncMode
	buf   []byte
	dirty bool
	err   error

	stop chan struct{}
	done chan struct{}
}

// Create creates a new, empty log file at path.
func Create(path string, mode SyncMode) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	if err := fileutil.SyncDir(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, err
	}
	w := &Writer{f: f, mode: mode}
	if mode == SyncEverySec {
		w.stop = make(chan struct{})
		w.done = make(chan struct{})
		go w.syncLoop()
	}
	return w, nil
}

func (w *Writer) syncLoop() {
	defer close(w.done)
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-t.C:
			w.mu.Lock()
			if w.dirty && w.err == nil {
				if err := w.f.Sync(); err != nil {
					w.err = err
				}
				w.dirty = false
			}
			w.mu.Unlock()
		}
	}
}

// Append writes entries as a single atomic record.
func (w *Writer) Append(entries ...base.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return w.err
	}

	buf := append(w.buf[:0], make([]byte, headerSize)...)
	buf = binary.AppendUvarint(buf, uint64(len(entries)))
	for i := range entries {
		buf = base.AppendEntry(buf, &entries[i])
	}
	binary.LittleEndian.PutUint32(buf[4:8], uint32(len(buf)-headerSize))
	binary.LittleEndian.PutUint32(buf[0:4], crc32.Checksum(buf[4:], crcTable))
	w.buf = buf

	if _, err := w.f.Write(buf); err != nil {
		w.err = err
		return err
	}
	if w.mode == SyncAlways {
		if err := w.f.Sync(); err != nil {
			w.err = err
			return err
		}
	} else {
		w.dirty = true
	}
	return nil
}

// Close syncs and closes the log.
func (w *Writer) Close() error {
	if w.stop != nil {
		close(w.stop)
		<-w.done
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	err := w.err
	if serr := w.f.Sync(); err == nil {
		err = serr
	}
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	return err
}

// Replay calls fn with each intact batch in the log at path, in order. It
// returns the byte offset just past the last intact record.
func Replay(path string, fn func([]base.Entry) error) (int64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, err
	}
	off := 0
	for off+headerSize <= len(data) {
		sum := binary.LittleEndian.Uint32(data[off:])
		n := int(binary.LittleEndian.Uint32(data[off+4:]))
		end := off + headerSize + n
		if n < 0 || end > len(data) {
			break // torn write at the tail
		}
		if crc32.Checksum(data[off+4:end], crcTable) != sum {
			break
		}
		entries, err := decodeBatch(data[off+headerSize : end])
		if err != nil {
			break
		}
		if err := fn(entries); err != nil {
			return int64(off), err
		}
		off = end
	}
	return int64(off), nil
}

func decodeBatch(p []byte) ([]base.Entry, error) {
	count, n := binary.Uvarint(p)
	if n <= 0 || count > uint64(len(p)) {
		return nil, base.ErrCorrupt
	}
	entries := make([]base.Entry, 0, count)
	for i := uint64(0); i < count; i++ {
		e, m, err := base.DecodeEntry(p[n:])
		if err != nil {
			return nil, err
		}
		entries = append(entries, e)
		n += m
	}
	if n != len(p) {
		return nil, base.ErrCorrupt
	}
	return entries, nil
}
