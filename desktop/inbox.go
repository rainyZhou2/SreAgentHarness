package main

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/wailsapp/wails/v2/pkg/runtime"
)

// ---------- chats: search, rename, archive, delete, export ----------

// SearchSessions lists chats by status ("active" or "archived") whose title
// contains q (empty = all).
func (a *App) SearchSessions(status, q string, limit int) (map[string]any, error) {
	if limit <= 0 || limit > 200 {
		limit = 100
	}
	v := url.Values{"limit": {fmt.Sprint(limit)}}
	if status != "" {
		v.Set("status", status)
	}
	if q = strings.TrimSpace(q); q != "" {
		v.Set("q", q)
	}
	return a.get("/v1/sessions?" + v.Encode())
}

func (a *App) patchSession(id string, body map[string]any) (map[string]any, error) {
	c, err := a.c()
	if err != nil {
		return nil, err
	}
	var out map[string]any
	return out, c.Do(a.ctx, http.MethodPatch, "/v1/sessions/"+url.PathEscape(id), body, &out)
}

// RenameSession sets a chat's title.
func (a *App) RenameSession(id, title string) (map[string]any, error) {
	return a.patchSession(id, map[string]any{"title": title})
}

// ArchiveSession moves a chat to (or back from) the archive.
func (a *App) ArchiveSession(id string, archived bool) (map[string]any, error) {
	st := "ACTIVE"
	if archived {
		st = "ARCHIVED"
	}
	return a.patchSession(id, map[string]any{"status": st})
}

// DeleteSession hides a chat for good (the server keeps its runs for audit).
func (a *App) DeleteSession(id string) error {
	_, err := a.patchSession(id, map[string]any{"status": "DELETED"})
	return err
}

// chatMarkdown renders a chat as Markdown: each question, the answer and
// the tool calls made for it.
func (a *App) chatMarkdown(sessionID string) (string, string, error) {
	tr, err := a.Transcript(sessionID)
	if err != nil {
		return "", "", err
	}
	title := "Chat"
	if v, ok := tr["view"].(map[string]any); ok {
		if s, ok := v["session"].(map[string]any); ok {
			if t, _ := s["title"].(string); t != "" {
				title = t
			}
		}
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# %s\n\n_Exported from SRE Harness on %s_\n\n", title, time.Now().Format("2006-01-02 15:04"))
	for _, t := range tr["turns"].([]TurnView) {
		fmt.Fprintf(&b, "## You\n\n%s\n\n", strings.TrimSpace(t.Text))
		if t.RunID == "" {
			fmt.Fprintf(&b, "_(%s)_\n\n", strings.ToLower(t.Status))
			continue
		}
		view, err := a.RunView(t.RunID)
		if err != nil {
			fmt.Fprintf(&b, "_(could not load the reply: %v)_\n\n", err)
			continue
		}
		b.WriteString("## Agent\n\n")
		if steps, ok := view["steps"].(map[string]any); ok {
			for _, x := range asList(steps["steps"]) {
				st, _ := x.(map[string]any)
				if st["kind"] != "tool" {
					continue
				}
				in, _ := st["input"].(map[string]any)
				name, _ := in["name"].(string)
				fmt.Fprintf(&b, "- tool `%s` %s → %v\n", name, compactJSON(in["args"]), st["status"])
			}
		}
		answer, status := "", ""
		if run, ok := view["run"].(map[string]any); ok {
			status, _ = run["display_status"].(string)
			if res, ok := run["result"].(map[string]any); ok {
				answer, _ = res["answer"].(string)
			}
		}
		if answer != "" {
			fmt.Fprintf(&b, "\n%s\n\n", strings.TrimSpace(answer))
		} else {
			fmt.Fprintf(&b, "\n_(%s)_\n\n", strings.ToLower(status))
		}
	}
	return b.String(), title, nil
}

func asList(v any) []any { l, _ := v.([]any); return l }

func compactJSON(v any) string {
	if v == nil {
		return ""
	}
	s := fmt.Sprint(v)
	if m, ok := v.(map[string]any); ok {
		var parts []string
		for k, x := range m {
			parts = append(parts, fmt.Sprintf("%s=%v", k, x))
		}
		s = strings.Join(parts, " ")
	}
	if len(s) > 120 {
		s = s[:120] + "…"
	}
	return s
}

var unsafeName = regexp.MustCompile(`[^\p{L}\p{N}\-_ ]+`)

// ExportChat saves a chat as Markdown and returns the file path. The
// desktop app asks where to save; browser preview writes to ~/Downloads.
func (a *App) ExportChat(sessionID string) (string, error) {
	md, title, err := a.chatMarkdown(sessionID)
	if err != nil {
		return "", err
	}
	name := strings.TrimSpace(unsafeName.ReplaceAllString(title, " "))
	if name == "" {
		name = "chat"
	}
	if len(name) > 60 {
		name = name[:60]
	}
	name += ".md"
	home, _ := os.UserHomeDir()
	path := filepath.Join(home, "Downloads", name)
	if a.native {
		path, err = runtime.SaveFileDialog(a.ctx, runtime.SaveDialogOptions{DefaultFilename: name, Title: "Export chat",
			DefaultDirectory: filepath.Join(home, "Downloads"),
			Filters:          []runtime.FileFilter{{DisplayName: "Markdown", Pattern: "*.md"}}})
		if err != nil {
			return "", err
		}
		if path == "" {
			return "", nil // cancelled
		}
	}
	if err := os.WriteFile(path, []byte(md), 0o644); err != nil {
		return "", err
	}
	return path, nil
}

// ---------- approvals inbox and notifications ----------

// PendingApprovals lists the tenant's approvals waiting for a decision.
func (a *App) PendingApprovals() (map[string]any, error) { return a.get("/v1/approvals?limit=100") }

// ApproveFromInbox approves or rejects an approval without opening its
// chat: it reads the run version and sends the command.
func (a *App) ApproveFromInbox(runID, approvalID, actionHash string, approve bool, reason string) (map[string]any, error) {
	if runID == "" || approvalID == "" {
		return nil, errors.New("run and approval are required")
	}
	run, err := a.get("/v1/runs/" + url.PathEscape(runID))
	if err != nil {
		return nil, err
	}
	var ver int64
	if r, ok := run["run"].(map[string]any); ok {
		if v, ok := r["version"].(float64); ok {
			ver = int64(v)
		}
	}
	typ := "approve_tool"
	if !approve {
		typ = "reject_tool"
	}
	return a.Command(runID, CommandRequest{Type: typ, ExpectedVersion: ver, Reason: reason,
		Payload: map[string]any{"approval_request_id": approvalID, "action_hash": actionHash}})
}

// watchApprovals polls the inbox, emits "approvals:update" with the list
// and posts a system notification for each approval it has not seen.
func (a *App) watchApprovals() {
	seen := map[string]bool{}
	first := true
	if a.native {
		if err := runtime.InitializeNotifications(a.ctx); err == nil {
			_, _ = runtime.RequestNotificationAuthorization(a.ctx)
			runtime.OnNotificationResponse(a.ctx, func(r runtime.NotificationResult) {
				if r.Error != nil {
					return
				}
				runtime.WindowShow(a.ctx)
				a.emit("approvals:open", r.Response.UserInfo)
			})
		}
	}
	t := time.NewTicker(8 * time.Second)
	defer t.Stop()
	for {
		if out, err := a.PendingApprovals(); err == nil {
			list := asList(out["approvals"])
			a.emit("approvals:update", list)
			now := map[string]bool{}
			for _, x := range list {
				ap, _ := x.(map[string]any)
				id, _ := ap["id"].(string)
				now[id] = true
				if seen[id] || first {
					continue
				}
				a.notifyApproval(ap)
			}
			seen, first = now, false
		}
		select {
		case <-a.ctx.Done():
			return
		case <-t.C:
		case <-a.inboxKick:
		}
	}
}

func (a *App) notifyApproval(ap map[string]any) {
	if !a.native {
		return
	}
	str := func(k string) string { s, _ := ap[k].(string); return s }
	body := fmt.Sprintf("%s %s on %s", str("risk_class"), str("tool_name"), str("target"))
	if t := str("session_title"); t != "" {
		body += " · " + t
	}
	_ = runtime.SendNotification(a.ctx, runtime.NotificationOptions{ID: str("id"), Title: "Approval needed", Body: body,
		Data: map[string]interface{}{"run_id": str("run_id"), "session_id": str("session_id"), "approval_id": str("id")}})
}

// RefreshApprovals re-polls the inbox now (after a decision).
func (a *App) RefreshApprovals() {
	select {
	case a.inboxKick <- struct{}{}:
	default:
	}
}
