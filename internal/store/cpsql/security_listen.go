package cpsql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand"
	"time"

	"github.com/jackc/pgx/v5"
)

const securityNotificationChannel = "harness_security_revocation"

type SecurityNotification struct {
	TenantID string `json:"tenant_id"`
	Scope    string `json:"scope"`
	ScopeID  string `json:"scope_id"`
	Version  int64  `json:"version"`
}

type SecurityListener struct {
	Config     *pgx.ConnConfig
	Invalidate func(SecurityNotification)
	Log        *slog.Logger
	MinBackoff time.Duration
	MaxBackoff time.Duration
}

func (l *SecurityListener) Run(ctx context.Context) error {
	if l.Config == nil || l.Invalidate == nil {
		return errors.New("security listener config and invalidation callback are required")
	}
	minBackoff, maxBackoff := l.MinBackoff, l.MaxBackoff
	if minBackoff <= 0 {
		minBackoff = 250 * time.Millisecond
	}
	if maxBackoff < minBackoff {
		maxBackoff = 5 * time.Second
	}
	backoff := minBackoff
	random := rand.New(rand.NewSource(time.Now().UnixNano()))
	for {
		err := l.listen(ctx)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil && l.Log != nil {
			l.Log.Warn("security revocation listener disconnected", "err", err)
		}
		delay := backoff/2 + time.Duration(random.Int63n(int64(backoff)/2+1))
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil
		case <-timer.C:
		}
		backoff *= 2
		if backoff > maxBackoff {
			backoff = maxBackoff
		}
	}
}

func (l *SecurityListener) listen(ctx context.Context) error {
	conn, err := pgx.ConnectConfig(ctx, l.Config.Copy())
	if err != nil {
		return err
	}
	defer conn.Close(context.WithoutCancel(ctx))
	if _, err := conn.Exec(ctx, "LISTEN "+securityNotificationChannel); err != nil {
		return err
	}
	for {
		notification, err := conn.WaitForNotification(ctx)
		if err != nil {
			return err
		}
		var event SecurityNotification
		if err := json.Unmarshal([]byte(notification.Payload), &event); err != nil {
			if l.Log != nil {
				l.Log.Warn("ignored malformed security revocation notification")
			}
			continue
		}
		if event.TenantID == "" || event.ScopeID == "" || event.Version <= 0 || !validSecurityScope(event.Scope) {
			continue
		}
		l.Invalidate(event)
	}
}

func validSecurityScope(scope string) bool {
	switch scope {
	case "tenant", "subject", "work", "run":
		return true
	default:
		return false
	}
}

func securityNotificationPayload(event SecurityNotification) (string, error) {
	if event.TenantID == "" || event.ScopeID == "" || event.Version <= 0 || !validSecurityScope(event.Scope) {
		return "", fmt.Errorf("invalid security notification")
	}
	encoded, err := json.Marshal(event)
	return string(encoded), err
}
