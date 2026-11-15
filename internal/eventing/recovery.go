package eventing

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode/utf8"
)

var (
	ErrSourceFenced         = errors.New("source checkpoint lease fenced")
	ErrInvalidCheckpoint    = errors.New("invalid source checkpoint")
	ErrInvalidReconcilePage = errors.New("invalid source reconcile page")
)

type SourceCheckpoint struct {
	TenantID     string
	Source       string
	Cursor       string
	Owner        string
	FencingToken int64
	LeaseUntil   *time.Time
	NextRetryAt  *time.Time
	LastError    string
	UpdatedAt    time.Time
}

type SourceReconcileRequest struct {
	TenantID string
	Source   string
	Cursor   string
	Limit    int
}

type SourceReconcilePage struct {
	Events        []Envelope
	NextCursor    string
	HighWatermark bool
}

type SourceReconciler interface {
	Reconcile(ctx context.Context, req SourceReconcileRequest) (SourceReconcilePage, error)
}

func ValidateSourceCheckpoint(cursor string) error {
	if len(cursor) > 4096 || !utf8.ValidString(cursor) || strings.ContainsRune(cursor, '\x00') {
		return ErrInvalidCheckpoint
	}
	return nil
}
