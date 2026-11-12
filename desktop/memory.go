package main

import (
	"errors"
	"net/http"
	"net/url"
	"strings"
)

// Personal memory: the preferences the agent keeps about the signed-in user
// (language, format, defaults). The server shows and changes only the
// caller's own, so these calls need no user argument.

// MyMemories lists the user's personal memories.
func (a *App) MyMemories() (map[string]any, error) { return a.get("/v1/me/memories") }

// SetMyMemory creates or revises one personal memory. The server rejects
// text that reads like instructions to the agent and redacts secrets; the
// response says how many values were redacted.
func (a *App) SetMyMemory(key, value string) (map[string]any, error) {
	key, value = strings.TrimSpace(key), strings.TrimSpace(value)
	if key == "" || value == "" {
		return nil, errors.New(a.tr("a name and a value are required", "需要填写名称和内容"))
	}
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPut, "/v1/me/memories/"+url.PathEscape(key), map[string]string{"value": value}, &out)
}

// DeleteMyMemory removes (archives) one personal memory.
func (a *App) DeleteMyMemory(key string) error {
	c, err := a.c()
	if err != nil {
		return err
	}
	return c.Do(a.ctx, http.MethodDelete, "/v1/me/memories/"+url.PathEscape(strings.TrimSpace(key)), nil, nil)
}

// ---- learned (long-term) memory: review what the agent proposes ----

func memoryList(a *App, status string) ([]map[string]any, error) {
	out, err := a.get("/v1/memories?limit=200&status=" + url.QueryEscape(status))
	if err != nil {
		return nil, err
	}
	var res []map[string]any
	raw, _ := out["memories"].([]any)
	for _, r := range raw {
		if m, ok := r.(map[string]any); ok && m["scope"] != "user" {
			res = append(res, m)
		}
	}
	return res, nil
}

// LearnedMemories returns the long-term memories the agent proposed and
// that await review (each with the ACTIVE memory it would replace, so the
// reviewer sees what changes), and the active ones in use. Only ACTIVE
// memories reach later runs.
func (a *App) LearnedMemories() (map[string]any, error) {
	var pending []map[string]any
	for _, st := range []string{"CANDIDATE", "VALIDATING"} {
		l, err := memoryList(a, st)
		if err != nil {
			return nil, err
		}
		pending = append(pending, l...)
	}
	active, err := memoryList(a, "ACTIVE")
	if err != nil {
		return nil, err
	}
	for _, p := range pending {
		subject, _ := p["subject"].(string)
		if subject == "" {
			continue
		}
		for _, o := range active {
			if o["id"] != p["id"] && o["subject"] == subject && o["scope"] == p["scope"] && o["scope_id"] == p["scope_id"] {
				p["replaces"] = o
				break
			}
		}
	}
	if pending == nil {
		pending = []map[string]any{}
	}
	if active == nil {
		active = []map[string]any{}
	}
	stale, err := memoryList(a, "STALE")
	if err != nil {
		return nil, err
	}
	if stale == nil {
		stale = []map[string]any{}
	}
	// Clean-up suggestions come from the memory curator; a server without it
	// (404) simply has none.
	suggestions := []any{}
	if out, err := a.get("/v1/memory/curation?status=PENDING&limit=100"); err == nil {
		if l, ok := out["proposals"].([]any); ok && l != nil {
			suggestions = l
		}
	}
	return map[string]any{"pending": pending, "active": active, "stale": stale, "suggestions": suggestions}, nil
}

// RunCuration asks the server to look for memories worth setting aside
// (duplicates, facts superseded by newer ones, lessons later verified runs
// refuted, memories nobody retrieves). It only creates suggestions.
func (a *App) RunCuration() (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPost, "/v1/memory/curation/run", map[string]string{}, &out)
}

// DecideCuration applies (sets the memories aside, restorably) or dismisses
// a suggestion.
func (a *App) DecideCuration(id, decision, reason string) error {
	if id == "" || (decision != "apply" && decision != "dismiss") {
		return errors.New("invalid decision")
	}
	c, err := a.c()
	if err != nil {
		return err
	}
	return c.Do(a.ctx, http.MethodPost, "/v1/memory/curation/"+url.PathEscape(id)+"/decide",
		map[string]string{"decision": decision, "reason": reason}, nil)
}

// ReviewMemory approves or rejects a proposal, or archives an active memory.
// decision is "approve", "reject" or "archive"; expectedStatus is the status
// the reviewer saw (the server refuses if it changed meanwhile).
func (a *App) ReviewMemory(id, expectedStatus, decision, reason string) error {
	to := map[string]string{"approve": "ACTIVE", "reject": "REJECTED", "archive": "ARCHIVED"}[decision]
	if to == "" || id == "" || expectedStatus == "" {
		return errors.New("invalid review request")
	}
	c, err := a.c()
	if err != nil {
		return err
	}
	return c.Do(a.ctx, http.MethodPost, "/v1/memories/"+url.PathEscape(id)+"/transition",
		map[string]string{"expected_status": expectedStatus, "to": to, "reason": reason}, nil)
}

// ReviseMemory writes a new version of a memory's title and content (the
// status is unchanged); used to correct a proposal before approving it.
func (a *App) ReviseMemory(id string, expectedVersion int64, title, content, reason string) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	body := map[string]any{"expected_version": expectedVersion, "title": title, "content": content, "reason": reason}
	return out, c.Do(a.ctx, http.MethodPatch, "/v1/memories/"+url.PathEscape(id), body, &out)
}
