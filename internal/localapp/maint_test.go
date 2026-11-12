package localapp

import (
	"archive/tar"
	"compress/gzip"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"go-sre-harness/internal/localstore"
)

func seedDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "data")
	st, err := localstore.Open(filepath.Join(dir, "harness.db"))
	if err != nil {
		t.Fatal(err)
	}
	st.Close()
	if _, _, err := EnsureToken(dir); err != nil {
		t.Fatal(err)
	}
	put := func(rel, body string, age time.Duration) {
		p := filepath.Join(dir, rel)
		_ = os.MkdirAll(filepath.Dir(p), 0o700)
		if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		ts := time.Now().Add(-age)
		_ = os.Chtimes(p, ts, ts)
	}
	put("events/local/run1.jsonl", `{"seq":1}`+"\n", time.Hour)
	put("artifacts/local/result/a", "answer", 100*24*time.Hour)
	put("artifacts/local/model-request/old", "prompt-old", 100*24*time.Hour)
	put("artifacts/local/model-request/new", "prompt-new", time.Hour)
	put("artifacts/local/prompt-manifest/old", "m", 100*24*time.Hour)
	put("harness.local.json", `{}`, time.Hour)
	return dir
}

func TestBackupExcludesSecretsAndRestores(t *testing.T) {
	dir := seedDir(t)
	out := filepath.Join(t.TempDir(), "b.tar.gz")
	m, err := Backup(dir, out)
	if err != nil || m.Files < 5 {
		t.Fatalf("backup: %+v %v", m, err)
	}
	// the archive holds no credentials
	f, _ := os.Open(out)
	gz, _ := gzip.NewReader(f)
	tr := tar.NewReader(gz)
	for {
		h, err := tr.Next()
		if err != nil {
			break
		}
		if secretFiles[h.Name] {
			t.Errorf("backup contains %s", h.Name)
		}
	}
	f.Close()
	if _, err := Backup(dir, out); err == nil {
		t.Fatal("must not overwrite an existing backup")
	}

	// Wipe an artifact, restore, and check it is back and the token survived.
	_ = os.Remove(filepath.Join(dir, "artifacts/local/result/a"))
	_, tokBefore := readTok(t, dir)
	prev, _, err := Restore(dir, out)
	if err != nil || prev == "" {
		t.Fatalf("restore: %q %v", prev, err)
	}
	if b, err := os.ReadFile(filepath.Join(dir, "artifacts/local/result/a")); err != nil || string(b) != "answer" {
		t.Fatalf("artifact not restored: %q %v", b, err)
	}
	if _, tokAfter := readTok(t, dir); tokAfter != tokBefore {
		t.Fatal("restore must keep the installation's token")
	}
	if !fileExists(filepath.Join(prev, "harness.db")) {
		t.Fatal("the previous data must be kept, not deleted")
	}
	st, err := localstore.Open(filepath.Join(dir, "harness.db"))
	if err != nil {
		t.Fatalf("restored database must open: %v", err)
	}
	st.Close()
}

func readTok(t *testing.T, dir string) (string, string) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "token"))
	if err != nil {
		t.Fatal(err)
	}
	return dir, string(b)
}

func TestRestoreRejectsUnsafeArchives(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "data")
	bad := filepath.Join(t.TempDir(), "bad.tar.gz")
	f, _ := os.Create(bad)
	gz := gzip.NewWriter(f)
	tw := tar.NewWriter(gz)
	_ = tw.WriteHeader(&tar.Header{Name: "../escape", Mode: 0o600, Size: 1, Typeflag: tar.TypeReg})
	_, _ = tw.Write([]byte("x"))
	tw.Close()
	gz.Close()
	f.Close()
	if _, _, err := Restore(dir, bad); err == nil {
		t.Fatal("path traversal must be rejected")
	}
	notArchive := filepath.Join(t.TempDir(), "x")
	_ = os.WriteFile(notArchive, []byte("hello"), 0o600)
	if _, _, err := Restore(dir, notArchive); err == nil {
		t.Fatal("garbage must be rejected")
	}
	if fileExists(dir) {
		t.Fatal("a failed restore must not create or touch the data dir")
	}
}

func TestPruneOnlyOldReplayArtifacts(t *testing.T) {
	dir := seedDir(t)
	r, err := Prune(dir, 30*24*time.Hour, true)
	if err != nil || r.Files != 2 || !r.DryRun {
		t.Fatalf("dry run: %+v %v", r, err)
	}
	if !fileExists(filepath.Join(dir, "artifacts/local/model-request/old")) {
		t.Fatal("dry run must delete nothing")
	}
	r, err = Prune(dir, 30*24*time.Hour, false)
	if err != nil || r.Files != 2 {
		t.Fatalf("prune: %+v %v", r, err)
	}
	for rel, want := range map[string]bool{
		"artifacts/local/model-request/old":   false,
		"artifacts/local/prompt-manifest/old": false,
		"artifacts/local/model-request/new":   true, // recent
		"artifacts/local/result/a":            true, // results are never pruned
		"events/local/run1.jsonl":             true, // event logs are never pruned
	} {
		if fileExists(filepath.Join(dir, rel)) != want {
			t.Errorf("%s exists=%v want %v", rel, !want, want)
		}
	}
	if _, err := Prune(dir, 0, true); err == nil {
		t.Fatal("zero duration must be rejected")
	}
}

func TestLockRefusesSecondHolder(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no flock")
	}
	dir := t.TempDir()
	rel, err := AcquireLock(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := AcquireLock(dir); err == nil {
		t.Fatal("a second holder must be refused")
	}
	if _, err := Prune(dir, time.Hour, true); err == nil {
		t.Fatal("maintenance must refuse while the harness runs")
	}
	rel()
	rel2, err := AcquireLock(dir)
	if err != nil {
		t.Fatalf("lock must be free after release: %v", err)
	}
	rel2()
}
