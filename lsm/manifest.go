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

// manifest is the durable list of live SSTables. It is rewritten atomically
// after every flush and compaction; files not listed in it are garbage.
type manifest struct {
	NextFile uint64   `json:"next_file"`
	Tables   []uint64 `json:"tables"` // table ids, newest first
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
	return m, nil
}

func writeManifest(dir string, m manifest) error {
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	return fileutil.WriteFileAtomic(filepath.Join(dir, manifestName), data)
}
