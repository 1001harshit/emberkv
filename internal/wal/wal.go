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

// Writer appends batches to a log file with group commit.
//
// Append only encodes a record into an in-memory buffer and returns its log
// sequence number (LSN), the byte offset just past the record. Commit then
// waits until the record has reached the OS (and, under SyncAlways, the
// disk). The first committer to arrive becomes the leader: it writes out
// everything buffered so far with one write(2), and one fsync if required,
// while later committers wait. So N concurrent writers cost one syscall
// round instead of N.
type Writer struct {
	mu   sync.Mutex
	cond *sync.Cond
	f    *os.File
	mode SyncMode

	buf      []byte // appended but not yet written
	spare    []byte // recycled buffer, swapped with buf by the leader
	appended uint64 // LSN of the end of buf
	written  uint64 // bytes handed to the OS
	synced   uint64 // bytes known to be on disk
	leading  bool   // a leader is writing outside the lock
	dirty    bool   // written but not yet fsynced (SyncEverySec)
	err      error

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
	w.cond = sync.NewCond(&w.mu)
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
			if !w.dirty || w.err != nil {
				w.mu.Unlock()
				continue
			}
			w.dirty = false
			w.mu.Unlock()
			// fsync without holding the lock so appends and writes continue.
			if err := w.f.Sync(); err != nil {
				w.mu.Lock()
				w.err = err
				w.cond.Broadcast()
				w.mu.Unlock()
			}
		}
	}
}

// Append encodes entries as a single atomic record into the in-memory buffer
// and returns the record's LSN. It performs no I/O; call Commit to make the
// record durable.
func (w *Writer) Append(entries ...base.Entry) (uint64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.err != nil {
		return 0, w.err
	}
	start := len(w.buf)
	w.buf = append(w.buf, make([]byte, headerSize)...)
	w.buf = binary.AppendUvarint(w.buf, uint64(len(entries)))
	for i := range entries {
		w.buf = base.AppendEntry(w.buf, &entries[i])
	}
	rec := w.buf[start:]
	binary.LittleEndian.PutUint32(rec[4:8], uint32(len(rec)-headerSize))
	binary.LittleEndian.PutUint32(rec[0:4], crc32.Checksum(rec[4:], crcTable))
	w.appended += uint64(len(rec))
	return w.appended, nil
}

// durableLocked returns the LSN up to which Commit may return.
func (w *Writer) durableLocked() uint64 {
	if w.mode == SyncAlways {
		return w.synced
	}
	return w.written
}

// Commit blocks until every record up to lsn is written to the OS and, in
// SyncAlways mode, fsynced. Concurrent callers share writes and fsyncs.
func (w *Writer) Commit(lsn uint64) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		if w.durableLocked() >= lsn {
			return nil
		}
		if w.err != nil {
			return w.err
		}
		if w.leading {
			w.cond.Wait()
			continue
		}

		// Become the leader for everything appended so far.
		w.leading = true
		data, target := w.buf, w.appended
		w.buf, w.spare = w.spare[:0], nil
		w.mu.Unlock()

		_, err := w.f.Write(data)
		if err == nil && w.mode == SyncAlways {
			err = w.f.Sync()
		}

		w.mu.Lock()
		w.leading = false
		w.spare = data[:0]
		if err != nil {
			w.err = err
		} else {
			w.written = target
			if w.mode == SyncAlways {
				w.synced = target
			} else {
				w.dirty = true
			}
		}
		w.cond.Broadcast()
	}
}

// Close writes out any buffered records, fsyncs and closes the log.
func (w *Writer) Close() error {
	if w.stop != nil {
		close(w.stop)
		<-w.done
	}
	w.mu.Lock()
	lsn := w.appended
	w.mu.Unlock()
	err := w.Commit(lsn)

	w.mu.Lock()
	defer w.mu.Unlock()
	if serr := w.f.Sync(); err == nil {
		err = serr
	}
	w.synced = w.written
	if cerr := w.f.Close(); err == nil {
		err = cerr
	}
	if err != nil && w.err == nil {
		w.err = err
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
