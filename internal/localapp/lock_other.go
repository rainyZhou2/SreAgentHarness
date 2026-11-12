//go:build !unix

package localapp

import "os"

// AcquireLock is a no-op where flock is unavailable.
func AcquireLock(dataDir string) (func(), error) {
	return func() {}, os.MkdirAll(dataDir, 0o700)
}
