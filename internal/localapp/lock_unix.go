//go:build unix

package localapp

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// AcquireLock takes an exclusive lock on the data directory. Two harness
// processes on one directory would interleave appends to the same event logs
// and database, so the second one is refused. The lock is released when the
// process exits, however it exits.
func AcquireLock(dataDir string) (release func(), err error) {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(filepath.Join(dataDir, ".lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another harness is already using %s (stop it first)", dataDir)
	}
	return func() { _ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN); f.Close() }, nil
}
