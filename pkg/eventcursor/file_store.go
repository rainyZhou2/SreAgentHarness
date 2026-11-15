package eventcursor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const FormatVersion = 1

type Cursor struct {
	FormatVersion int       `json:"format_version"`
	KeyID         string    `json:"key_id"`
	Sequence      int64     `json:"sequence"`
	EventID       string    `json:"event_id,omitempty"`
	UpdatedAt     time.Time `json:"updated_at"`
}

type Store interface {
	Load(ctx context.Context, key Key) (Cursor, bool, error)
	Advance(ctx context.Context, key Key, sequence int64, eventID string) error
}

type FileStore struct {
	dir   string
	locks sync.Map
}

func NewFileStore(dir string) (*FileStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("%w: cursor directory", ErrInvalid)
	}
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create event cursor directory: %w", err)
	}
	return &FileStore{dir: dir}, nil
}

func (s *FileStore) lock(keyID string) *sync.Mutex {
	lock, _ := s.locks.LoadOrStore(keyID, &sync.Mutex{})
	return lock.(*sync.Mutex)
}

func (s *FileStore) path(keyID string) string { return filepath.Join(s.dir, keyID+".json") }

func (s *FileStore) Load(ctx context.Context, key Key) (Cursor, bool, error) {
	normalized, err := key.normalized()
	if err != nil {
		return Cursor{}, false, err
	}
	if err := ctx.Err(); err != nil {
		return Cursor{}, false, err
	}
	keyID := normalized.ID()
	lock := s.lock(keyID)
	lock.Lock()
	defer lock.Unlock()
	return s.load(keyID)
}

func (s *FileStore) load(keyID string) (Cursor, bool, error) {
	data, err := os.ReadFile(s.path(keyID))
	if errors.Is(err, fs.ErrNotExist) {
		return Cursor{}, false, nil
	}
	if err != nil {
		return Cursor{}, false, fmt.Errorf("read event cursor: %w", err)
	}
	var cursor Cursor
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&cursor); err != nil {
		return Cursor{}, false, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if err := ensureJSONEnd(decoder); err != nil {
		return Cursor{}, false, fmt.Errorf("%w: %v", ErrCorrupt, err)
	}
	if cursor.FormatVersion != FormatVersion || cursor.KeyID != keyID || cursor.Sequence <= 0 || cursor.UpdatedAt.IsZero() {
		return Cursor{}, false, fmt.Errorf("%w: incompatible cursor record", ErrCorrupt)
	}
	return cursor, true, nil
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("multiple JSON values")
		}
		return err
	}
	return nil
}

func (s *FileStore) Advance(ctx context.Context, key Key, sequence int64, eventID string) error {
	normalized, err := key.normalized()
	if err != nil {
		return err
	}
	if sequence <= 0 || len(eventID) > 512 {
		return fmt.Errorf("%w: cursor position", ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	keyID := normalized.ID()
	lock := s.lock(keyID)
	lock.Lock()
	defer lock.Unlock()
	current, ok, err := s.load(keyID)
	if err != nil {
		return err
	}
	if ok {
		switch {
		case sequence < current.Sequence:
			return ErrRegression
		case sequence == current.Sequence && eventID != current.EventID:
			return ErrConflict
		case sequence == current.Sequence:
			return nil
		}
	}
	next := Cursor{FormatVersion: FormatVersion, KeyID: keyID, Sequence: sequence, EventID: eventID, UpdatedAt: time.Now().UTC()}
	data, err := json.Marshal(next)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	file, err := os.CreateTemp(s.dir, ".cursor-*")
	if err != nil {
		return fmt.Errorf("create event cursor transaction: %w", err)
	}
	tmp := file.Name()
	defer os.Remove(tmp)
	if err := file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write event cursor transaction: %w", err)
	}
	if err := os.Rename(tmp, s.path(keyID)); err != nil {
		return fmt.Errorf("commit event cursor: %w", err)
	}
	return nil
}
