//go:build !unix

package lsm

import (
	"os"
	"path/filepath"
)

// lockDir creates the LOCK file but does not enforce exclusivity on this platform.
func lockDir(dir string) (*os.File, error) {
	return os.OpenFile(filepath.Join(dir, "LOCK"), os.O_CREATE|os.O_RDWR, 0o644)
}

func unlockDir(f *os.File) error { return f.Close() }
