package localapp

import (
	"archive/tar"
	"compress/gzip"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go-sre-harness/internal/localstore"
)

// Files that never go into a backup: credentials and scratch space.
var secretFiles = map[string]bool{"token": true, "token.json": true, "local.env": true, ".lock": true}

const backupFormat = 1

// BackupManifest describes a backup archive.
type BackupManifest struct {
	Format        int       `json:"format"`
	SchemaVersion int       `json:"schema_version"`
	CreatedAt     time.Time `json:"created_at"`
	Files         int       `json:"files"`
	Bytes         int64     `json:"bytes"`
}

// Backup writes a gzip-compressed tar of the data directory to out and
// returns what it holds. It is safe while the harness runs: the database is
// copied first as a consistent snapshot (VACUUM INTO), then the append-only
// event logs, so every event the snapshot indexes is in the archive (a few
// later events may be too; the logs are the source of truth). Tokens and
// local.env are left out: a backup must not carry credentials.
func Backup(dataDir, out string) (BackupManifest, error) {
	var m BackupManifest
	dbPath := filepath.Join(dataDir, "harness.db")
	if _, err := os.Stat(dbPath); err != nil {
		return m, fmt.Errorf("no harness database in %s", dataDir)
	}
	if _, err := os.Stat(out); err == nil {
		return m, fmt.Errorf("%s already exists", out)
	}
	tmpDir, err := os.MkdirTemp(filepath.Dir(out), ".backup-*")
	if err != nil {
		return m, err
	}
	defer os.RemoveAll(tmpDir)
	snap := filepath.Join(tmpDir, "harness.db")
	if err := snapshotDB(dbPath, snap); err != nil {
		return m, fmt.Errorf("snapshot database: %w", err)
	}

	f, err := os.OpenFile(out, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return m, err
	}
	ok := false
	defer func() {
		f.Close()
		if !ok {
			os.Remove(out)
		}
	}()
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)

	add := func(name, src string) error {
		st, err := os.Stat(src)
		if err != nil {
			return err
		}
		in, err := os.Open(src)
		if err != nil {
			return err
		}
		defer in.Close()
		// Append-only logs can grow while we copy: take exactly the size
		// seen now.
		size := st.Size()
		if err := tw.WriteHeader(&tar.Header{Name: name, Mode: 0o600, Size: size, ModTime: st.ModTime(), Typeflag: tar.TypeReg}); err != nil {
			return err
		}
		n, err := io.CopyN(tw, in, size)
		m.Files++
		m.Bytes += n
		if errors.Is(err, io.EOF) { // shrank meanwhile: pad is impossible, fail
			return fmt.Errorf("%s changed while being copied", src)
		}
		return err
	}
	m = BackupManifest{Format: backupFormat, SchemaVersion: localstore.SchemaVersion, CreatedAt: time.Now().UTC()}
	if err := add("harness.db", snap); err != nil {
		return m, err
	}
	if cfg := filepath.Join(dataDir, "harness.local.json"); fileExists(cfg) {
		if err := add("harness.local.json", cfg); err != nil {
			return m, err
		}
	}
	for _, sub := range []string{"events", "artifacts"} {
		root := filepath.Join(dataDir, sub)
		err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				if errors.Is(err, fs.ErrNotExist) {
					return nil
				}
				return err
			}
			if d.IsDir() || !d.Type().IsRegular() || strings.HasPrefix(d.Name(), ".tmp") {
				return nil
			}
			rel, _ := filepath.Rel(dataDir, p)
			return add(filepath.ToSlash(rel), p)
		})
		if err != nil {
			return m, err
		}
	}
	mb, _ := json.Marshal(m)
	if err := tw.WriteHeader(&tar.Header{Name: "manifest.json", Mode: 0o600, Size: int64(len(mb)), ModTime: m.CreatedAt, Typeflag: tar.TypeReg}); err != nil {
		return m, err
	}
	if _, err := tw.Write(mb); err != nil {
		return m, err
	}
	if err := tw.Close(); err != nil {
		return m, err
	}
	if err := gz.Close(); err != nil {
		return m, err
	}
	ok = true
	return m, f.Close()
}

func snapshotDB(src, dst string) error {
	db, err := sql.Open("sqlite", src+"?_pragma=busy_timeout(5000)")
	if err != nil {
		return err
	}
	defer db.Close()
	_, err = db.Exec(`VACUUM INTO ?`, dst)
	return err
}

func fileExists(p string) bool { _, err := os.Stat(p); return err == nil }

const maxRestoreBytes = 64 << 30

// Restore replaces the data directory with the contents of a backup. The
// harness must not be running (the data directory lock is taken). The old
// directory is never deleted: it is renamed to <dir>.before-restore-<time>,
// and its credentials (token files, local.env) are carried over so existing
// clients keep working. It returns where the old data went ("" if there was none).
func Restore(dataDir, archive string) (previous string, m BackupManifest, err error) {
	in, err := os.Open(archive)
	if err != nil {
		return "", m, err
	}
	defer in.Close()
	created := !fileExists(dataDir) // the lock would create it
	release, err := AcquireLock(dataDir)
	if err != nil {
		return "", m, err
	}
	defer release()

	stamp := time.Now().UTC().Format("20060102T150405Z")
	tmp := dataDir + ".restore-" + stamp
	if err := os.MkdirAll(tmp, 0o700); err != nil {
		return "", m, err
	}
	committed := false
	defer func() {
		if !committed {
			os.RemoveAll(tmp)
			if created {
				os.RemoveAll(dataDir) // only the lock file is in it
			}
		}
	}()
	gz, err := gzip.NewReader(in)
	if err != nil {
		return "", m, fmt.Errorf("not a harness backup: %w", err)
	}
	tr := tar.NewReader(gz)
	var total int64
	for {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return "", m, fmt.Errorf("read backup: %w", err)
		}
		if h.Typeflag != tar.TypeReg {
			continue
		}
		name := filepath.Clean(filepath.FromSlash(h.Name))
		if filepath.IsAbs(name) || name == "." || strings.HasPrefix(name, "..") || secretFiles[name] {
			return "", m, fmt.Errorf("backup holds an unsafe path %q", h.Name)
		}
		if total += h.Size; total > maxRestoreBytes {
			return "", m, errors.New("backup is implausibly large")
		}
		dst := filepath.Join(tmp, name)
		if err := os.MkdirAll(filepath.Dir(dst), 0o700); err != nil {
			return "", m, err
		}
		out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return "", m, err
		}
		if _, err := io.Copy(out, tr); err != nil {
			out.Close()
			return "", m, err
		}
		if err := out.Close(); err != nil {
			return "", m, err
		}
	}
	mb, err := os.ReadFile(filepath.Join(tmp, "manifest.json"))
	if err != nil {
		return "", m, errors.New("not a harness backup: manifest.json is missing")
	}
	if err := json.Unmarshal(mb, &m); err != nil || m.Format != backupFormat {
		return "", m, fmt.Errorf("unsupported backup format %d", m.Format)
	}
	if m.SchemaVersion > localstore.SchemaVersion {
		return "", m, fmt.Errorf("backup was made by a newer harness (schema v%d, this build reads v%d)", m.SchemaVersion, localstore.SchemaVersion)
	}
	if !fileExists(filepath.Join(tmp, "harness.db")) {
		return "", m, errors.New("backup has no database")
	}
	_ = os.Remove(filepath.Join(tmp, "manifest.json"))

	if created || !fileExists(filepath.Join(dataDir, "harness.db")) {
		// Nothing to keep (at most the lock file): replace it outright.
		if err := os.RemoveAll(dataDir); err != nil {
			return "", m, err
		}
	} else {
		previous = dataDir + ".before-restore-" + stamp
		// Credentials stay with the installation, not the backup.
		for n := range secretFiles {
			if n == ".lock" {
				continue
			}
			if b, err := os.ReadFile(filepath.Join(dataDir, n)); err == nil {
				if err := os.WriteFile(filepath.Join(tmp, n), b, 0o600); err != nil {
					return "", m, err
				}
			}
		}
		if err := os.Rename(dataDir, previous); err != nil {
			return "", m, err
		}
	}
	if err := os.Rename(tmp, dataDir); err != nil {
		if previous != "" {
			_ = os.Rename(previous, dataDir) // put the old data back
		}
		return "", m, err
	}
	committed = true
	return previous, m, nil
}

// Replay-only artifacts: full prompts and raw model responses. They dominate
// disk use and are not needed to read a run's result, steps or evidence.
var prunableKinds = []string{"model-request", "model-response", "prompt-manifest"}

// PruneReport is what Prune removed (or would remove).
type PruneReport struct {
	Files  int   `json:"files"`
	Bytes  int64 `json:"bytes"`
	DryRun bool  `json:"dry_run"`
}

// Prune deletes replay-only artifacts older than olderThan (by modification
// time) and folds the database write-ahead log. Run results, steps, tool
// observations, memories and event logs are kept. The harness must not be
// running. With dryRun nothing is deleted.
func Prune(dataDir string, olderThan time.Duration, dryRun bool) (PruneReport, error) {
	r := PruneReport{DryRun: dryRun}
	if olderThan <= 0 {
		return r, errors.New("older-than must be positive")
	}
	release, err := AcquireLock(dataDir)
	if err != nil {
		return r, err
	}
	defer release()
	cutoff := time.Now().Add(-olderThan)
	for _, kind := range prunableKinds {
		matches, _ := filepath.Glob(filepath.Join(dataDir, "artifacts", "*", kind))
		for _, root := range matches {
			err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
				if err != nil || d.IsDir() || !d.Type().IsRegular() {
					return nil
				}
				st, err := d.Info()
				if err != nil || st.ModTime().After(cutoff) {
					return nil
				}
				r.Files++
				r.Bytes += st.Size()
				if !dryRun {
					return os.Remove(p)
				}
				return nil
			})
			if err != nil {
				return r, err
			}
		}
	}
	if !dryRun {
		if db, err := sql.Open("sqlite", filepath.Join(dataDir, "harness.db")+"?_pragma=busy_timeout(5000)"); err == nil {
			_, _ = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`)
			db.Close()
		}
	}
	return r, nil
}

// DirUsage is the disk use of the data directory by area.
type DirUsage struct {
	Total  int64            `json:"total"`
	ByArea map[string]int64 `json:"by_area"`
}

// Usage sums file sizes under dataDir, grouped by its top-level entry.
func Usage(dataDir string) (DirUsage, error) {
	u := DirUsage{ByArea: map[string]int64{}}
	err := filepath.WalkDir(dataDir, func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || d.Name() == ".lock" {
			return nil
		}
		st, err := d.Info()
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(dataDir, p)
		area := strings.SplitN(filepath.ToSlash(rel), "/", 2)[0]
		if strings.HasPrefix(area, "harness.db") {
			area = "database"
		}
		u.ByArea[area] += st.Size()
		u.Total += st.Size()
		return nil
	})
	return u, err
}

// FormatBytes renders n in binary units.
func FormatBytes(n int64) string {
	const k = 1024
	if n < k {
		return fmt.Sprintf("%d B", n)
	}
	units := "KMGT"
	f, i := float64(n)/k, 0
	for f >= k && i < len(units)-1 {
		f /= k
		i++
	}
	return fmt.Sprintf("%.1f %ciB", f, units[i])
}

// areas returns the keys of m in descending value order (for reports).
func areas(m map[string]int64) []string {
	ks := make([]string, 0, len(m))
	for k := range m {
		ks = append(ks, k)
	}
	sort.Slice(ks, func(i, j int) bool { return m[ks[i]] > m[ks[j]] })
	return ks
}
