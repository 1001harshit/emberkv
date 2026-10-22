// Package sstable implements immutable, sorted on-disk tables.
//
// File layout:
//
//	[data block]...[data block][index block][bloom filter][footer]
//
// A data block is a run of encoded entries (~4 KiB) followed by a crc32c.
// Entries are sorted by key ascending, then version descending, and a table
// may hold several versions of a key. The index block lists each data
// block's first key and version, offset and length, followed by a crc32c.
// The Bloom filter covers user keys. The 48-byte footer locates the index and
// filter:
//
//	indexOff(8) | indexLen(8) | bloomOff(8) | bloomLen(8) | count(8) | magic(8)
package sstable

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"sort"

	"github.com/1001harshit/emberkv/internal/base"
	"github.com/1001harshit/emberkv/internal/bloom"
	"github.com/1001harshit/emberkv/internal/cache"
)

const (
	targetBlockSize        = 4096
	footerSize             = 48
	magic           uint64 = 0x32564b5245424d45 // "EMBERKV2" little-endian; v2 added versions
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

type indexEntry struct {
	firstKey     []byte
	firstVersion uint64
	offset       uint64
	length       uint64
}

// Writer builds an SSTable from entries added in strictly increasing
// (key ascending, version descending) order.
type Writer struct {
	f          *os.File
	bw         *bufio.Writer
	path       string
	bitsPerKey int

	offset      uint64
	block       []byte
	blockFirst  []byte
	blockFirstV uint64
	index       []indexEntry
	hashes      []uint64
	lastKey     []byte
	lastVersion uint64
	count       uint64
	err         error
}

// Create starts a new table at path.
func Create(path string, bitsPerKey int) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{f: f, bw: bufio.NewWriterSize(f, 64<<10), path: path, bitsPerKey: bitsPerKey}, nil
}

// Count returns the number of entries added so far.
func (w *Writer) Count() uint64 { return w.count }

// Add appends an entry. (key, version) pairs must be strictly increasing in
// entry order.
func (w *Writer) Add(e *base.Entry) error {
	if w.err != nil {
		return w.err
	}
	newKey := w.count == 0 || !bytes.Equal(e.Key, w.lastKey)
	if w.count > 0 && base.Compare(e.Key, e.Version, w.lastKey, w.lastVersion) <= 0 {
		w.err = fmt.Errorf("sstable: key %q version %d added out of order", e.Key, e.Version)
		return w.err
	}
	if len(w.block) == 0 {
		w.blockFirst, w.blockFirstV = bytes.Clone(e.Key), e.Version
	}
	w.block = base.AppendEntry(w.block, e)
	if newKey {
		w.hashes = append(w.hashes, bloom.Hash(e.Key))
	}
	w.lastKey, w.lastVersion = append(w.lastKey[:0], e.Key...), e.Version
	w.count++
	if len(w.block) >= targetBlockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *Writer) write(p []byte) {
	if w.err != nil {
		return
	}
	_, w.err = w.bw.Write(p)
	w.offset += uint64(len(p))
}

func (w *Writer) flushBlock() error {
	if len(w.block) == 0 {
		return w.err
	}
	w.block = binary.LittleEndian.AppendUint32(w.block, crc32.Checksum(w.block, crcTable))
	w.index = append(w.index, indexEntry{firstKey: w.blockFirst, firstVersion: w.blockFirstV,
		offset: w.offset, length: uint64(len(w.block))})
	w.write(w.block)
	w.block = w.block[:0]
	return w.err
}

// Finish writes the index, filter and footer, then fsyncs and closes the file.
func (w *Writer) Finish() error {
	w.flushBlock()

	indexOff := w.offset
	var idx []byte
	idx = binary.AppendUvarint(idx, uint64(len(w.index)))
	for _, ie := range w.index {
		idx = binary.AppendUvarint(idx, uint64(len(ie.firstKey)))
		idx = append(idx, ie.firstKey...)
		idx = binary.AppendUvarint(idx, ie.firstVersion)
		idx = binary.AppendUvarint(idx, ie.offset)
		idx = binary.AppendUvarint(idx, ie.length)
	}
	idx = binary.LittleEndian.AppendUint32(idx, crc32.Checksum(idx, crcTable))
	w.write(idx)

	bloomOff := w.offset
	filter := bloom.Build(w.hashes, w.bitsPerKey).Marshal()
	w.write(filter)

	var footer [footerSize]byte
	binary.LittleEndian.PutUint64(footer[0:], indexOff)
	binary.LittleEndian.PutUint64(footer[8:], uint64(len(idx)))
	binary.LittleEndian.PutUint64(footer[16:], bloomOff)
	binary.LittleEndian.PutUint64(footer[24:], uint64(len(filter)))
	binary.LittleEndian.PutUint64(footer[32:], w.count)
	binary.LittleEndian.PutUint64(footer[40:], magic)
	w.write(footer[:])

	if w.err == nil {
		w.err = w.bw.Flush()
	}
	if w.err == nil {
		w.err = w.f.Sync()
	}
	if cerr := w.f.Close(); w.err == nil {
		w.err = cerr
	}
	if w.err != nil {
		os.Remove(w.path)
	}
	return w.err
}

// Abort discards a table that is being written.
func (w *Writer) Abort() {
	w.f.Close()
	os.Remove(w.path)
}

// Reader serves lookups and scans from a finished table. It is safe for
// concurrent use.
type Reader struct {
	f      *os.File
	id     uint64
	cache  *cache.Cache
	index  []indexEntry
	filter *bloom.Filter
	count  uint64
	size   int64
}

// Open opens the table at path, loading its index and filter into memory.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	r, err := open(f)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable %s: %w", path, err)
	}
	return r, nil
}

func open(f *os.File) (*Reader, error) {
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	if size < footerSize {
		return nil, base.ErrCorrupt
	}
	var footer [footerSize]byte
	if _, err := f.ReadAt(footer[:], size-footerSize); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint64(footer[40:]) != magic {
		return nil, base.ErrCorrupt
	}
	indexOff := binary.LittleEndian.Uint64(footer[0:])
	indexLen := binary.LittleEndian.Uint64(footer[8:])
	bloomOff := binary.LittleEndian.Uint64(footer[16:])
	bloomLen := binary.LittleEndian.Uint64(footer[24:])
	limit := uint64(size - footerSize)
	if indexLen < 4 || indexOff+indexLen > limit || bloomOff+bloomLen > limit {
		return nil, base.ErrCorrupt
	}

	idx := make([]byte, indexLen)
	if _, err := f.ReadAt(idx, int64(indexOff)); err != nil {
		return nil, err
	}
	body := idx[:len(idx)-4]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(idx[len(idx)-4:]) {
		return nil, base.ErrCorrupt
	}
	index, err := decodeIndex(body)
	if err != nil {
		return nil, err
	}

	fb := make([]byte, bloomLen)
	if _, err := f.ReadAt(fb, int64(bloomOff)); err != nil {
		return nil, err
	}
	filter, err := bloom.Unmarshal(fb)
	if err != nil {
		return nil, err
	}
	return &Reader{
		f:      f,
		index:  index,
		filter: filter,
		count:  binary.LittleEndian.Uint64(footer[32:]),
		size:   size,
	}, nil
}

func decodeIndex(b []byte) ([]indexEntry, error) {
	n, m := binary.Uvarint(b)
	if m <= 0 || n > uint64(len(b)) {
		return nil, base.ErrCorrupt
	}
	off := m
	next := func() (uint64, bool) {
		v, m := binary.Uvarint(b[off:])
		if m <= 0 {
			return 0, false
		}
		off += m
		return v, true
	}
	index := make([]indexEntry, 0, n)
	for i := uint64(0); i < n; i++ {
		kl, ok := next()
		if !ok || kl > uint64(len(b)-off) {
			return nil, base.ErrCorrupt
		}
		key := b[off : off+int(kl)]
		off += int(kl)
		ver, ok0 := next()
		bo, ok1 := next()
		bl, ok2 := next()
		if !ok0 || !ok1 || !ok2 || bl < 4 {
			return nil, base.ErrCorrupt
		}
		index = append(index, indexEntry{firstKey: key, firstVersion: ver, offset: bo, length: bl})
	}
	return index, nil
}

// SetCache makes the reader keep recently read blocks in c, under id, which
// must be unique per table among the readers sharing c. Call it before the
// reader is shared.
func (r *Reader) SetCache(id uint64, c *cache.Cache) {
	r.id, r.cache = id, c
}

// Count returns the number of entries in the table.
func (r *Reader) Count() uint64 { return r.count }

// Size returns the file size in bytes.
func (r *Reader) Size() int64 { return r.size }

// Close closes the underlying file.
func (r *Reader) Close() error { return r.f.Close() }

// findBlock returns the index of the last block whose first entry is at or
// before (key, version) in entry order, or -1 if every block starts after it.
func (r *Reader) findBlock(key []byte, version uint64) int {
	i := sort.Search(len(r.index), func(i int) bool {
		return base.Compare(r.index[i].firstKey, r.index[i].firstVersion, key, version) > 0
	})
	return i - 1
}

// readBlock returns the verified contents of data block i, from the cache
// when possible. On a miss the block is read from the file and, if fill is
// true, cached; scans pass false so a large scan does not evict the hot
// blocks that point lookups depend on. Blocks are never modified after this,
// so entries decoded from them stay valid.
func (r *Reader) readBlock(i int, fill bool) ([]byte, error) {
	if r.cache != nil {
		if b, ok := r.cache.Get(r.id, i); ok {
			return b, nil
		}
	}
	b, err := r.readBlockFromFile(i)
	if err == nil && fill && r.cache != nil {
		r.cache.Put(r.id, i, b)
	}
	return b, err
}

func (r *Reader) readBlockFromFile(i int) ([]byte, error) {
	ie := r.index[i]
	buf := make([]byte, ie.length)
	if _, err := r.f.ReadAt(buf, int64(ie.offset)); err != nil {
		return nil, err
	}
	body := buf[:len(buf)-4]
	if crc32.Checksum(body, crcTable) != binary.LittleEndian.Uint32(buf[len(buf)-4:]) {
		return nil, base.ErrCorrupt
	}
	return body, nil
}

// Get returns the newest entry for key with a version <= asOf, including
// tombstones.
func (r *Reader) Get(key []byte, asOf uint64) (base.Entry, bool, error) {
	if !r.filter.MayContain(key) {
		return base.Entry{}, false, nil
	}
	it := r.seek(key, asOf, true)
	if !it.Next() {
		return base.Entry{}, false, it.Err()
	}
	if e := it.Entry(); bytes.Equal(e.Key, key) {
		return *e, true, nil
	}
	return base.Entry{}, false, nil
}

// Iterator returns an iterator over every version of every key >= start (nil
// means all), in entry order.
func (r *Reader) Iterator(start []byte) base.Iterator {
	if start == nil {
		return &iterator{r: r, bi: -1}
	}
	return r.seek(start, base.MaxVersion, false)
}

// seek returns an iterator positioned before the first entry at or after
// (key, version). fill says whether blocks it reads go into the cache.
func (r *Reader) seek(key []byte, version uint64, fill bool) *iterator {
	bi := max(r.findBlock(key, version), 0)
	return &iterator{r: r, bi: bi - 1, seekKey: key, seekVersion: version, seeking: true, fill: fill}
}

type iterator struct {
	r           *Reader
	bi          int
	block       []byte
	off         int
	seeking     bool
	seekKey     []byte
	seekVersion uint64
	fill        bool
	cur         base.Entry
	err         error
}

func (it *iterator) Next() bool {
	for it.err == nil {
		if it.off >= len(it.block) {
			it.bi++
			if it.bi >= len(it.r.index) {
				return false
			}
			it.block, it.err = it.r.readBlock(it.bi, it.fill)
			it.off = 0
			continue
		}
		e, n, err := base.DecodeEntry(it.block[it.off:])
		if err != nil {
			it.err = err
			return false
		}
		it.off += n
		if it.seeking {
			if base.Compare(e.Key, e.Version, it.seekKey, it.seekVersion) < 0 {
				continue
			}
			it.seeking = false
		}
		it.cur = e
		return true
	}
	return false
}

func (it *iterator) Entry() *base.Entry { return &it.cur }
func (it *iterator) Err() error         { return it.err }
