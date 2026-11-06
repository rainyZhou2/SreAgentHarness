// Package eventsinks registers the built-in event_sink plugins.
package eventsinks

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventsink"
	"go-sre-harness/internal/plugin"
)

// LogConfig configures the log sink.
type LogConfig struct {
	eventsink.Options
}

// WebhookConfig configures the webhook sink.
type WebhookConfig struct {
	eventsink.Options
	URL string `json:"url"`
	// Format: "json" (default) posts the event as JSON; "slack" posts a
	// Slack/Mattermost-compatible {"text": ...} message.
	Format  string            `json:"format,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	// Secret signs the body: X-Harness-Signature: sha256=<hex HMAC-SHA256>.
	Secret  string          `json:"secret,omitempty"`
	Timeout plugin.Duration `json:"timeout,omitempty"` // default 10s
}

func init() {
	plugin.Register(plugin.EventSink, "log", "writes matching events to the process log",
		func(_ context.Context, h *plugin.Host, c plugin.Config) (eventsink.Sink, error) {
			var cfg LogConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			if err := cfg.Validate(); err != nil {
				return nil, err
			}
			return &logSink{opts: cfg.Options, log: h.Log}, nil
		})
	plugin.Register(plugin.EventSink, "webhook", "HTTP POST per event (JSON or Slack format, optional HMAC signature)",
		func(_ context.Context, _ *plugin.Host, c plugin.Config) (eventsink.Sink, error) {
			var cfg WebhookConfig
			if err := c.Decode(&cfg); err != nil {
				return nil, err
			}
			return NewWebhook(cfg)
		})
}

type logSink struct {
	opts eventsink.Options
	log  *slog.Logger
}

func (s *logSink) SinkOptions() eventsink.Options { return s.opts }

func (s *logSink) Deliver(_ context.Context, e domain.Event) error {
	s.log.Info("run event", "event_type", e.Type, "tenant_id", e.TenantID, "run_id", e.RunID,
		"event_seq", e.Seq, "event_id", e.ID, "payload", json.RawMessage(e.Payload))
	return nil
}

// Webhook posts events over HTTP.
type Webhook struct {
	cfg    WebhookConfig
	client *http.Client
}

// NewWebhook validates the config.
func NewWebhook(cfg WebhookConfig) (*Webhook, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	u, err := url.Parse(cfg.URL)
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return nil, fmt.Errorf("url must be an absolute http(s) URL")
	}
	switch cfg.Format {
	case "", "json", "slack":
	default:
		return nil, fmt.Errorf("format must be json or slack, got %q", cfg.Format)
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = plugin.Duration(10 * time.Second)
	}
	return &Webhook{cfg: cfg, client: &http.Client{Timeout: cfg.Timeout.D()}}, nil
}

func (w *Webhook) SinkOptions() eventsink.Options { return w.cfg.Options }

func (w *Webhook) Deliver(ctx context.Context, e domain.Event) error {
	var body []byte
	var err error
	if w.cfg.Format == "slack" {
		body, err = json.Marshal(map[string]string{"text": SlackText(e)})
	} else {
		body, err = json.Marshal(e)
	}
	if err != nil {
		return eventsink.Permanent(err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.cfg.URL, bytes.NewReader(body))
	if err != nil {
		return eventsink.Permanent(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "go-sre-harness-event-sink")
	req.Header.Set("X-Harness-Event", e.Type)
	req.Header.Set("X-Harness-Event-Id", e.ID) // de-duplication key: delivery is at-least-once
	for k, v := range w.cfg.Headers {
		req.Header.Set(k, v)
	}
	if w.cfg.Secret != "" {
		m := hmac.New(sha256.New, []byte(w.cfg.Secret))
		m.Write(body)
		req.Header.Set("X-Harness-Signature", "sha256="+hex.EncodeToString(m.Sum(nil)))
	}
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 64<<10))
	switch {
	case resp.StatusCode >= 200 && resp.StatusCode < 300:
		return nil
	case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode >= 500:
		return fmt.Errorf("webhook returned %s", resp.Status)
	default:
		return eventsink.Permanent(fmt.Errorf("webhook returned %s", resp.Status))
	}
}

// SlackText renders a short human-readable line for chat tools.
func SlackText(e domain.Event) string {
	var p map[string]any
	_ = json.Unmarshal(e.Payload, &p)
	var parts []string
	for _, k := range []string{"tool", "target", "risk_class", "approval_request_id", "reason", "status", "error", "decision"} {
		if v, ok := p[k]; ok && v != nil && fmt.Sprint(v) != "" {
			parts = append(parts, fmt.Sprintf("%s=%v", k, v))
		}
	}
	icon := ""
	switch {
	case strings.HasPrefix(e.Type, "approval.requested"):
		icon = ":raising_hand: "
	case strings.HasSuffix(e.Type, ".failed"):
		icon = ":x: "
	case strings.HasSuffix(e.Type, ".completed"):
		icon = ":white_check_mark: "
	}
	text := fmt.Sprintf("%s*%s* tenant `%s` run `%s`", icon, e.Type, e.TenantID, e.RunID)
	if len(parts) > 0 {
		text += " — " + strings.Join(parts, ", ")
	}
	return text
}
