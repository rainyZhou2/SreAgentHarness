// Package artifact stores large or sensitive payloads (inputs, decisions, tool
// outputs) outside the database. The database only keeps refs and hashes
// (§5.1). Objects are content-addressed, so re-uploading after a crash is
// idempotent and an orphan left by a failed DB commit is harmless (GC later).
package artifact

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

var ErrNotFound = errors.New("artifact not found")

type Store interface {
	// Put durably stores data and returns an immutable ref plus its sha256 hash.
	Put(ctx context.Context, tenantID, kind string, data []byte) (ref, hash string, err error)
	// Get returns the bytes for a ref, refusing refs of another tenant.
	Get(ctx context.Context, tenantID, ref string) ([]byte, error)
}

func Hash(data []byte) string {
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func objectKey(tenantID, kind string, data []byte) (string, string, error) {
	if tenantID == "" || strings.ContainsAny(tenantID, "/\\.") || kind == "" || strings.ContainsAny(kind, "/\\.") {
		return "", "", fmt.Errorf("artifact: invalid tenant or kind")
	}
	h := Hash(data)
	return tenantID + "/" + kind + "/" + strings.TrimPrefix(h, "sha256:"), h, nil
}

func parseRef(scheme, tenantID, ref string) (string, error) {
	key, ok := strings.CutPrefix(ref, scheme+"://")
	if !ok || !strings.HasPrefix(key, tenantID+"/") || strings.Contains(key, "..") {
		return "", ErrNotFound
	}
	return key, nil
}

// FS is a filesystem-backed store suitable for single-node deployments and
// development; production uses S3/MinIO behind the same interface.
type FS struct{ Root string }

func (s *FS) Put(_ context.Context, tenantID, kind string, data []byte) (string, string, error) {
	key, h, err := objectKey(tenantID, kind, data)
	if err != nil {
		return "", "", err
	}
	path := filepath.Join(s.Root, filepath.FromSlash(key))
	if _, err := os.Stat(path); err == nil {
		return "fs://" + key, h, nil
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return "", "", err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), ".tmp-*")
	if err != nil {
		return "", "", err
	}
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		os.Remove(tmp.Name())
		return "", "", err
	}
	if err := tmp.Sync(); err != nil { // durable before we promise anything to the DB
		tmp.Close()
		os.Remove(tmp.Name())
		return "", "", err
	}
	if err := tmp.Close(); err != nil {
		os.Remove(tmp.Name())
		return "", "", err
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		os.Remove(tmp.Name())
		return "", "", err
	}
	return "fs://" + key, h, nil
}

func (s *FS) Get(_ context.Context, tenantID, ref string) ([]byte, error) {
	key, err := parseRef("fs", tenantID, ref)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(filepath.Join(s.Root, filepath.FromSlash(key)))
	if errors.Is(err, os.ErrNotExist) {
		return nil, ErrNotFound
	}
	return data, err
}

// Memory is an in-process store for tests. FailPuts makes Put fail, to verify
// that a step is never reported successful without its artifact (§22.1).
type Memory struct {
	mu       sync.Mutex
	objects  map[string][]byte
	FailPuts bool
}

func NewMemory() *Memory { return &Memory{objects: map[string][]byte{}} }

func (m *Memory) Put(_ context.Context, tenantID, kind string, data []byte) (string, string, error) {
	key, h, err := objectKey(tenantID, kind, data)
	if err != nil {
		return "", "", err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailPuts {
		return "", "", errors.New("artifact store unavailable")
	}
	m.objects[key] = append([]byte(nil), data...)
	return "mem://" + key, h, nil
}

func (m *Memory) SetFailPuts(v bool) {
	m.mu.Lock()
	m.FailPuts = v
	m.mu.Unlock()
}

func (m *Memory) Get(_ context.Context, tenantID, ref string) ([]byte, error) {
	key, err := parseRef("mem", tenantID, ref)
	if err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, ErrNotFound
	}
	return append([]byte(nil), data...), nil
}

// Deleter is implemented by stores that support retention deletes.
type Deleter interface {
	Delete(ctx context.Context, tenantID, ref string) error
}

func (s *FS) Delete(_ context.Context, tenantID, ref string) error {
	key, err := parseRef("fs", tenantID, ref)
	if err != nil {
		return err
	}
	err = os.Remove(filepath.Join(s.Root, filepath.FromSlash(key)))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (m *Memory) Delete(_ context.Context, tenantID, ref string) error {
	key, err := parseRef("mem", tenantID, ref)
	if err != nil {
		return err
	}
	m.mu.Lock()
	delete(m.objects, key)
	m.mu.Unlock()
	return nil
}
