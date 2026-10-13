// Package bloom implements the Bloom filter stored in each SSTable, letting
// point lookups skip tables that cannot contain a key.
package bloom

import (
	"errors"
	"hash/fnv"
)

// Filter is an immutable Bloom filter using double hashing.
type Filter struct {
	bits []byte
	k    uint8
}

// Hash returns the 64-bit hash of key used to populate and probe filters.
func Hash(key []byte) uint64 {
	h := fnv.New64a()
	h.Write(key)
	// FNV alone mixes the high bits poorly; finish with the murmur3 mixer.
	x := h.Sum64()
	x ^= x >> 33
	x *= 0xff51afd7ed558ccd
	x ^= x >> 33
	x *= 0xc4ceb9fe1a85ec53
	x ^= x >> 33
	return x
}

// Build creates a filter for the given key hashes with roughly bitsPerKey
// bits per key. 10 bits per key gives about a 1% false-positive rate.
func Build(hashes []uint64, bitsPerKey int) *Filter {
	if bitsPerKey < 1 {
		bitsPerKey = 1
	}
	k := int(float64(bitsPerKey) * 0.69) // ln(2) * m/n is optimal
	k = max(1, min(k, 30))

	nbits := max(len(hashes)*bitsPerKey, 64)
	f := &Filter{bits: make([]byte, (nbits+7)/8), k: uint8(k)}
	for _, h := range hashes {
		f.add(h)
	}
	return f
}

func (f *Filter) probes(h uint64, fn func(pos uint32) bool) bool {
	n := uint32(len(f.bits) * 8)
	h1, h2 := uint32(h), uint32(h>>32)|1
	for i := uint32(0); i < uint32(f.k); i++ {
		if !fn((h1 + i*h2) % n) {
			return false
		}
	}
	return true
}

func (f *Filter) add(h uint64) {
	f.probes(h, func(pos uint32) bool {
		f.bits[pos/8] |= 1 << (pos % 8)
		return true
	})
}

// MayContain reports whether key might be in the set. False means definitely not.
func (f *Filter) MayContain(key []byte) bool {
	return f.probes(Hash(key), func(pos uint32) bool {
		return f.bits[pos/8]&(1<<(pos%8)) != 0
	})
}

// Marshal encodes the filter as its bit array followed by one byte for k.
func (f *Filter) Marshal() []byte {
	out := make([]byte, 0, len(f.bits)+1)
	out = append(out, f.bits...)
	return append(out, f.k)
}

// Unmarshal decodes a filter produced by Marshal.
func Unmarshal(b []byte) (*Filter, error) {
	if len(b) < 2 {
		return nil, errors.New("bloom: filter too short")
	}
	k := b[len(b)-1]
	if k == 0 || k > 30 {
		return nil, errors.New("bloom: invalid hash count")
	}
	return &Filter{bits: b[:len(b)-1], k: k}, nil
}
