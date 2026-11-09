package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"go-sre-harness/desktop/harness"
)

// Works: the continuous-Work views (timeline, action cards, outcome cards).
// Every call goes to the v2 control plane API (/v1/works, /v1/actions).

// ListWorks lists Works, newest first. status is a comma list (empty: all).
func (a *App) ListWorks(status, kind string, limit int) (map[string]any, error) {
	if limit <= 0 || limit > 200 {
		limit = 50
	}
	q := url.Values{"limit": {strconv.Itoa(limit)}}
	if status = strings.TrimSpace(status); status != "" {
		q.Set("status", status)
	}
	if kind = strings.TrimSpace(kind); kind != "" {
		q.Set("kind", kind)
	}
	return a.get("/v1/works?" + q.Encode())
}

// WorkView gathers everything the Work page shows in one call. Only the
// Work itself is required; optional parts that fail (for example the
// scorecard when the server has no evaluator) are reported in "errors".
func (a *App) WorkView(id string) (map[string]any, error) {
	if strings.TrimSpace(id) == "" {
		return nil, errors.New(a.tr("work id is required", "缺少 Work ID"))
	}
	base := "/v1/works/" + url.PathEscape(id)
	w, err := a.get(base)
	if err != nil {
		return nil, err
	}
	out := map[string]any{"work": w}
	errs := map[string]string{}
	for key, path := range map[string]string{
		"events":     base + "/events?limit=500",
		"actions":    base + "/actions",
		"outcomes":   base + "/outcomes",
		"hypotheses": base + "/hypotheses",
		"evidence":   base + "/evidence",
		"runs":       base + "/runs",
		"watches":    base + "/watches",
		"evaluation": base + "/evaluation",
	} {
		v, err := a.get(path)
		if err != nil {
			errs[key] = err.Error()
			continue
		}
		out[key] = v["items"]
	}
	if len(errs) > 0 {
		out["errors"] = errs
	}
	return out, nil
}

// WorkCommand sends a Work command (pause, resume, cancel, replan, resolve,
// confirm, reopen, wake). expectedVersion guards against stale screens.
func (a *App) WorkCommand(id, typ string, payload map[string]any, expectedVersion int64, reason string) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	body := map[string]any{"type": typ}
	if payload != nil {
		body["payload"] = payload
	}
	if reason != "" {
		body["reason"] = reason
	}
	h := http.Header{}
	if expectedVersion > 0 {
		h.Set("If-Match", strconv.Quote(strconv.FormatInt(expectedVersion, 10)))
	}
	var out map[string]any
	if err := c.DoWith(a.ctx, http.MethodPost, "/v1/works/"+url.PathEscape(id)+"/commands", h, body, &out); err != nil {
		a.logEvent("warn", "work command %s on %s failed: %v", typ, id, err)
		return nil, err
	}
	return out, nil
}

// DecideAction approves or rejects a controlled action. The intent hash
// binds the decision to the plan the user saw.
func (a *App) DecideAction(actionID string, approve bool, intentHash, comment string) (map[string]any, error) {
	decision := "approve"
	if !approve {
		decision = "reject"
	}
	return a.post("/v1/actions/"+url.PathEscape(actionID)+"/approve",
		map[string]any{"decision": decision, "intent_hash": intentHash, "comment": comment})
}

// ReconcileAction asks the server to inspect an action with an unknown
// effect (read-only; it never re-executes).
func (a *App) ReconcileAction(actionID string) (map[string]any, error) {
	return a.post("/v1/actions/"+url.PathEscape(actionID)+"/reconcile", map[string]any{})
}

// ResolveAction records a human's finding for an unresolved effect.
func (a *App) ResolveAction(actionID string, applied bool, comment string) (map[string]any, error) {
	if strings.TrimSpace(comment) == "" {
		return nil, errors.New(a.tr("say what you checked", "请说明你检查了什么"))
	}
	return a.post("/v1/actions/"+url.PathEscape(actionID)+"/resolve", map[string]any{"applied": applied, "comment": comment})
}

// WorkPostmortem returns the ledger postmortem of a Work as Markdown.
func (a *App) WorkPostmortem(id string) (string, error) {
	c, err := a.c()
	if err != nil {
		return "", err
	}
	data, _, _, err := c.Download(a.ctx, "/v1/works/"+url.PathEscape(id)+"/postmortem?format=markdown", 1<<20)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// CreatePostmortem opens (or returns) the postmortem Work of a closed Work.
func (a *App) CreatePostmortem(id string) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	h := http.Header{"Idempotency-Key": {"postmortem:" + id}}
	var out map[string]any
	if err := c.DoWith(a.ctx, http.MethodPost, "/v1/works/"+url.PathEscape(id)+"/postmortem", h, nil, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *App) post(path string, body any) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	if err := c.Do(a.ctx, http.MethodPost, path, body, &out); err != nil {
		var apiErr *harness.APIError
		if errors.As(err, &apiErr) && apiErr.Status == http.StatusConflict {
			return nil, fmt.Errorf("%s: %w", a.tr("the action changed; reload", "操作已变化，请刷新"), err)
		}
		return nil, err
	}
	return out, nil
}
