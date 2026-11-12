package cli

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
)

// Chat is an interactive session on a harness server. Input and output are
// injected, so the loop is tested without a terminal.
type Chat struct {
	C        *Client
	In       io.Reader
	Out      io.Writer
	Session  string // resume this session (empty: create one)
	Title    string
	AutoYes  bool // approve without asking (never for R3, which the server denies anyway)
	lines    chan string
	pending  []approval
	streamed bool // text was streamed since the last newline
}

type approval struct {
	ID         string `json:"id"`
	ActionHash string `json:"action_hash"`
	Risk       string `json:"risk_class"`
	Tool       string `json:"tool_name"`
	Target     string `json:"target"`
	Status     string `json:"status"`
	RunID      string `json:"run_id"`
	Session    string `json:"session_title"`
}

const helpText = `commands:
  /new [title]     start a new session
  /sessions        list recent sessions
  /resume <id>     continue a session
  /approvals       list pending approvals
  /approve <n>     approve pending approval n   (/reject <n> to reject)
  /help, /quit
anything else is sent to the agent.
`

// Run reads lines until EOF or /quit.
func (ch *Chat) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch.lines = make(chan string)
	go func() {
		defer close(ch.lines)
		sc := bufio.NewScanner(ch.In)
		sc.Buffer(make([]byte, 64<<10), 1<<20)
		for sc.Scan() {
			select {
			case ch.lines <- sc.Text():
			case <-ctx.Done():
				return
			}
		}
	}()
	var who struct {
		Tenant, Subject string
	}
	if err := ch.C.Do(ctx, "GET", "/v1/whoami", nil, &who); err != nil {
		return fmt.Errorf("cannot reach the harness at %s: %w", ch.C.BaseURL, err)
	}
	fmt.Fprintf(ch.Out, "connected to %s as %s (/help for commands)\n", ch.C.BaseURL, who.Subject)
	if ch.Session == "" {
		if err := ch.newSession(ctx, ch.Title); err != nil {
			return err
		}
	} else {
		fmt.Fprintf(ch.Out, "session %s\n", ch.Session)
	}
	for {
		fmt.Fprint(ch.Out, "\n> ")
		line, ok := <-ch.lines
		if !ok {
			fmt.Fprintln(ch.Out)
			return nil
		}
		line = strings.TrimSpace(line)
		switch {
		case line == "":
		case line == "/quit" || line == "/exit":
			return nil
		case strings.HasPrefix(line, "/"):
			if err := ch.command(ctx, line); err != nil {
				fmt.Fprintln(ch.Out, "error:", err)
			}
		default:
			if err := ch.turn(ctx, line); err != nil {
				if errors.Is(err, context.Canceled) {
					return err
				}
				fmt.Fprintln(ch.Out, "error:", err)
			}
		}
	}
}

func (ch *Chat) newSession(ctx context.Context, title string) error {
	if title == "" {
		title = "cli chat"
	}
	var out struct {
		Session struct {
			ID string `json:"id"`
		} `json:"session"`
	}
	if err := ch.C.Do(ctx, "POST", "/v1/sessions", map[string]any{"title": title}, &out); err != nil {
		return err
	}
	ch.Session = out.Session.ID
	fmt.Fprintf(ch.Out, "session %s\n", ch.Session)
	return nil
}

func (ch *Chat) command(ctx context.Context, line string) error {
	f := strings.Fields(line)
	switch f[0] {
	case "/help":
		fmt.Fprint(ch.Out, helpText)
	case "/new":
		return ch.newSession(ctx, strings.TrimSpace(strings.TrimPrefix(line, "/new")))
	case "/resume":
		if len(f) != 2 {
			return errors.New("usage: /resume <session id>")
		}
		var s struct {
			Session struct {
				ID, Title string
			} `json:"session"`
			ID    string `json:"id"`
			Title string `json:"title"`
		}
		if err := ch.C.Do(ctx, "GET", "/v1/sessions/"+url.PathEscape(f[1]), nil, &s); err != nil {
			return err
		}
		ch.Session = f[1]
		fmt.Fprintf(ch.Out, "session %s\n", ch.Session)
	case "/sessions":
		var out struct {
			Sessions []struct {
				ID, Title, Status string
			} `json:"sessions"`
		}
		if err := ch.C.Do(ctx, "GET", "/v1/sessions?limit=15", nil, &out); err != nil {
			return err
		}
		for _, s := range out.Sessions {
			fmt.Fprintf(ch.Out, "  %s  %-8s %s\n", s.ID, s.Status, s.Title)
		}
		if len(out.Sessions) == 0 {
			fmt.Fprintln(ch.Out, "  (none)")
		}
	case "/approvals":
		return ch.listApprovals(ctx)
	case "/approve", "/reject":
		if len(f) != 2 {
			return fmt.Errorf("usage: %s <n> (see /approvals)", f[0])
		}
		n, err := strconv.Atoi(f[1])
		if err != nil || n < 1 || n > len(ch.pending) {
			return errors.New("no such approval: run /approvals first")
		}
		return ch.decide(ctx, ch.pending[n-1], f[0] == "/approve", "")
	default:
		return fmt.Errorf("unknown command %s (try /help)", f[0])
	}
	return nil
}

func (ch *Chat) listApprovals(ctx context.Context) error {
	var out struct {
		Approvals []approval `json:"approvals"`
	}
	if err := ch.C.Do(ctx, "GET", "/v1/approvals", nil, &out); err != nil {
		return err
	}
	ch.pending = ch.pending[:0]
	for _, a := range out.Approvals {
		if a.Status == "PENDING" {
			ch.pending = append(ch.pending, a)
		}
	}
	if len(ch.pending) == 0 {
		fmt.Fprintln(ch.Out, "  no pending approvals")
	}
	for i, a := range ch.pending {
		fmt.Fprintf(ch.Out, "  %d. %s %s on %s  (%s)\n", i+1, a.Risk, a.Tool, a.Target, a.Session)
	}
	return nil
}

// turn sends one message and follows its run to the end.
func (ch *Chat) turn(ctx context.Context, text string) error {
	var out struct {
		Turn struct {
			RunID string `json:"run_id"`
		} `json:"turn"`
	}
	body := map[string]any{"text": text, "client_request_id": newKey()}
	if err := ch.C.Do(ctx, "POST", "/v1/sessions/"+url.PathEscape(ch.Session)+"/turns", body, &out); err != nil {
		return err
	}
	runID := out.Turn.RunID
	if runID == "" {
		return errors.New("the server did not start a run")
	}
	// Input closing while a decision is pending stops the wait; the run
	// stays WAITING_APPROVAL and can be decided later (/approvals).
	fctx, stop := context.WithCancel(ctx)
	defer stop()
	detached := false
	err := ch.C.Follow(fctx, runID, func(e Event) {
		if err := ch.onEvent(fctx, runID, e); errors.Is(err, errInputClosed) {
			detached = true
			stop()
		}
	}, ch.onDelta)
	ch.endLine()
	if detached {
		fmt.Fprintf(ch.Out, "  input closed: run %s is still waiting for approval\n", runID)
		return nil
	}
	if err != nil {
		return err
	}
	return ch.printResult(ctx, runID)
}

func (ch *Chat) endLine() {
	if ch.streamed {
		fmt.Fprintln(ch.Out)
		ch.streamed = false
	}
}

func (ch *Chat) onDelta(d Delta) {
	switch d.Event.Type {
	case "text_delta":
		if d.Event.Text != "" {
			fmt.Fprint(ch.Out, d.Event.Text)
			ch.streamed = true
		}
	case "tool_call_complete":
		ch.endLine()
		fmt.Fprintf(ch.Out, "  → %s\n", d.Event.ToolName)
	}
}

var errInputClosed = errors.New("input closed")

func (ch *Chat) onEvent(ctx context.Context, runID string, e Event) error {
	switch e.Type {
	case "approval.requested":
		ch.endLine()
		if err := ch.askApprovals(ctx, runID); err != nil {
			if errors.Is(err, errInputClosed) {
				return err
			}
			if ctx.Err() == nil {
				fmt.Fprintln(ch.Out, "  approval:", err)
			}
		}
	case "plan.generated", "plan.fallback", "run.waiting_input", "run.failed", "run.cancelled", "run.paused":
		ch.endLine()
		fmt.Fprintf(ch.Out, "  · %s\n", e.Type)
	}
	return nil
}

// askApprovals shows the run's pending approvals and applies the user's decision.
func (ch *Chat) askApprovals(ctx context.Context, runID string) error {
	var out struct {
		Approvals []approval `json:"approvals"`
	}
	if err := ch.C.Do(ctx, "GET", "/v1/runs/"+url.PathEscape(runID)+"/approvals", nil, &out); err != nil {
		return err
	}
	for _, a := range out.Approvals {
		if a.Status != "PENDING" {
			continue
		}
		a.RunID = runID
		fmt.Fprintf(ch.Out, "\n  approval needed: %s %s\n    target: %s\n    hash:   %s\n", a.Risk, a.Tool, a.Target, shortHash(a.ActionHash))
		if ch.AutoYes && a.Risk != "R3" {
			fmt.Fprintln(ch.Out, "    (auto-approved by --yes)")
			if err := ch.decide(ctx, a, true, "approved with --yes"); err != nil {
				return err
			}
			continue
		}
		fmt.Fprint(ch.Out, "    approve? [y/N/reason to reject] ")
		ans, ok := <-ch.lines
		if !ok {
			return errInputClosed
		}
		ans = strings.TrimSpace(ans)
		approve := strings.EqualFold(ans, "y") || strings.EqualFold(ans, "yes")
		reason := ""
		if !approve && !strings.EqualFold(ans, "n") && !strings.EqualFold(ans, "no") {
			reason = ans
		}
		if err := ch.decide(ctx, a, approve, reason); err != nil {
			return err
		}
	}
	return nil
}

func (ch *Chat) decide(ctx context.Context, a approval, approve bool, reason string) error {
	var run struct {
		Run struct {
			Version int64 `json:"version"`
		} `json:"run"`
	}
	if err := ch.C.Do(ctx, "GET", "/v1/runs/"+url.PathEscape(a.RunID), nil, &run); err != nil {
		return err
	}
	typ := "approve_tool"
	if !approve {
		typ = "reject_tool"
	}
	body := map[string]any{"type": typ, "expected_version": run.Run.Version,
		"payload": map[string]any{"approval_request_id": a.ID, "action_hash": a.ActionHash}}
	if reason != "" {
		body["reason"] = reason
	}
	if err := ch.C.Do(ctx, "POST", "/v1/runs/"+url.PathEscape(a.RunID)+"/commands", body, nil); err != nil {
		return err
	}
	verb := "rejected"
	if approve {
		verb = "approved"
	}
	fmt.Fprintf(ch.Out, "    %s\n", verb)
	return nil
}

func (ch *Chat) printResult(ctx context.Context, runID string) error {
	var run struct {
		Display string `json:"display_status"`
		Result  struct {
			Answer     string `json:"answer"`
			Hypotheses []struct {
				Statement  string  `json:"statement"`
				Status     string  `json:"status"`
				Confidence float64 `json:"confidence_estimate"`
			} `json:"hypotheses"`
		} `json:"result"`
		Error json.RawMessage `json:"error"`
	}
	if err := ch.C.Do(ctx, "GET", "/v1/runs/"+url.PathEscape(runID), nil, &run); err != nil {
		return err
	}
	if run.Result.Answer != "" {
		fmt.Fprintf(ch.Out, "\n%s\n", run.Result.Answer)
		for _, h := range run.Result.Hypotheses {
			fmt.Fprintf(ch.Out, "  [%s] %s (%.0f%%)\n", h.Status, h.Statement, h.Confidence*100)
		}
	}
	fmt.Fprintf(ch.Out, "  run %s: %s\n", runID, run.Display)
	return nil
}

func shortHash(h string) string {
	if len(h) > 23 {
		return h[:23] + "…"
	}
	return h
}
