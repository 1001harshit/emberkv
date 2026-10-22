package lsm

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/1001harshit/emberkv/internal/fileutil"
)

const manifestName = "MANIFEST"

// formatVersion identifies the on-disk format. 1 was emberkv v0.1.0; 2 added
// a version to every record (WAL and SSTables).
const formatVersion = 2

// manifest is the durable list of live SSTables. It is rewritten atomically
// after every flush and compaction; files not listed in it are garbage.
type manifest struct {
	Format      int      `json:"format"`
	NextFile    uint64   `json:"next_file"`
	LastVersion uint64   `json:"last_version"` // newest version assigned to a write
	Tables      []uint64 `json:"tables"`       // table ids, newest first
}

func readManifest(dir string) (manifest, error) {
	var m manifest
	data, err := os.ReadFile(filepath.Join(dir, manifestName))
	if errors.Is(err, fs.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return m, err
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return m, fmt.Errorf("lsm: corrupt manifest: %w", err)
	}
	if m.Format == 0 {
		m.Format = 1 // written before the field existed
	}
	if m.Format != formatVersion {
		// Refuse rather than misread: an old write-ahead log would otherwise
		// look torn, and its writes would be silently dropped.
		return m, fmt.Errorf("lsm: %s uses on-disk format %d, but this build reads format %d; "+
			"export the data with the version that wrote it and import it here", dir, m.Format, formatVersion)
	}
	return m, nil
}

func writeManifest(dir string, m manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return fileutil.WriteFileAtomic(filepath.Join(dir, manifestName), data)
}
