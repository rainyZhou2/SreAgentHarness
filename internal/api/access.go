package api

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"strings"

	"go-sre-harness/internal/auth"
	"go-sre-harness/internal/domain"
)

// Who may see what.
//
// Roles say what a caller may DO; this says whose data they may SEE. A user's
// chats and the runs behind them are theirs: other members of the tenant, even
// operators, do not see them. The rules, in one place:
//
//   - A session, and the runs started from it, belong to the user who created
//     it. The owner and tenant admins can read and act on them.
//   - Runs that no user started (triggers, schedules, Works) are shared
//     operational work: everyone in the tenant sees them.
//   - An approver can open a run that has an approval request, and a session
//     that has a pending approval, so they can judge what they are asked to
//     approve. That access is read-only for sessions. It is not extended to
//     other runs of the same user.
//
// Anything a caller may not see answers 404, the same as a missing one, so
// ids cannot be probed.

// userScopedKey makes a create-idempotency key private to its caller. The
// stores keep one key space per tenant, so with the bare header value a second
// user who sent the same key (or guessed another's) would be handed the first
// user's run or session, "created: false". Replays by the same user still
// match.
func userScopedKey(actor, key string) string {
	sum := sha256.Sum256([]byte(actor + "\x00" + key))
	return "u:" + hex.EncodeToString(sum[:20])
}

func isAdmin(p auth.Principal) bool { return p.Has(auth.Admin) }

// ownsRun: the run is shared operational work or the caller started it.
func ownsRun(p auth.Principal, r domain.Run) bool {
	if domain.SharedInitiator(r.InitiatorType) {
		return true
	}
	return r.InitiatorID != nil && *r.InitiatorID == p.Subject
}

// grant says how a caller may use a run.
type grant int

const (
	grantNone     grant = iota
	grantFull           // owner, admin, or a shared run: read and act within their role
	grantApprover       // an approver on a run with an approval request: read, and decide approvals
)

func (s *Server) runGrant(ctx context.Context, p auth.Principal, runID string) (grant, error) {
	if isAdmin(p) {
		return grantFull, nil
	}
	run, err := s.Store.GetRun(ctx, p.Tenant, runID)
	if errors.Is(err, domain.ErrNotFound) {
		return grantFull, nil // the handler answers 404 itself
	}
	if err != nil {
		return grantNone, err
	}
	if ownsRun(p, run) {
		return grantFull, nil
	}
	if p.Has(auth.Approver) {
		apps, err := s.Store.ListApprovals(ctx, p.Tenant, runID)
		if err != nil {
			return grantNone, err
		}
		if len(apps) > 0 {
			return grantApprover, nil
		}
	}
	return grantNone, nil
}

// RunAccessible reports whether the caller may read a run (the owner, an
// admin, shared runs, and an approver for a run with an approval request).
func (s *Server) RunAccessible(ctx context.Context, p auth.Principal, runID string) (bool, error) {
	g, err := s.runGrant(ctx, p, runID)
	return g != grantNone, err
}

// RunWritable reports whether the caller may use a run as their own: copy it
// into a dataset, act on it. The approver exception does not extend to it.
func (s *Server) RunWritable(ctx context.Context, p auth.Principal, runID string) (bool, error) {
	g, err := s.runGrant(ctx, p, runID)
	return g == grantFull, err
}

type approverOnlyKey struct{}

// approverOnly marks a request that was let in as an approver, not as the
// run's owner: it may decide approvals and nothing else.
func approverOnly(ctx context.Context) bool {
	v, _ := ctx.Value(approverOnlyKey{}).(bool)
	return v
}

// approvalCommand: the commands an approver may issue on someone else's run.
func approvalCommand(t domain.CommandType) bool {
	switch t {
	case domain.CmdApproveTool, domain.CmdRejectTool, domain.CmdResolveUnknown, domain.CmdOverrideResult:
		return true
	}
	return false
}

func (s *Server) sessionAccessible(ctx context.Context, p auth.Principal, sessionID, method string) (bool, error) {
	if isAdmin(p) {
		return true, nil
	}
	sess, err := s.Store.GetSession(ctx, p.Tenant, sessionID)
	if errors.Is(err, domain.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if sess.CreatedBy == p.Subject {
		return true, nil
	}
	if p.Has(auth.Approver) && (method == http.MethodGet || method == http.MethodHead) {
		pending, err := s.Store.ListPendingApprovals(ctx, p.Tenant, 200)
		if err != nil {
			return false, err
		}
		for _, a := range pending {
			if a.SessionID != nil && *a.SessionID == sessionID {
				return true, nil
			}
		}
	}
	return false, nil
}

// scopeMiddleware applies the rules to every route that names a run or a
// session, whichever handler serves it, so a new route under those prefixes
// cannot forget the check.
func (s *Server) scopeMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p, ok := auth.FromContext(r.Context())
		if !ok {
			next.ServeHTTP(w, r)
			return
		}
		kind, id := scopedID(r.URL.Path)
		if id == "" {
			next.ServeHTTP(w, r)
			return
		}
		if kind == "session" {
			allowed, err := s.sessionAccessible(r.Context(), p, id, r.Method)
			switch {
			case err != nil:
				s.storeErr(w, err)
			case !allowed:
				writeErr(w, http.StatusNotFound, "not_found", "run or resource not found")
			default:
				next.ServeHTTP(w, r)
			}
			return
		}
		g, err := s.runGrant(r.Context(), p, id)
		switch {
		case err != nil:
			s.storeErr(w, err)
		case g == grantNone:
			writeErr(w, http.StatusNotFound, "not_found", "run or resource not found")
		case g == grantApprover:
			// Let in to judge an approval: read, and post the decision
			// (the command type is checked where it is parsed). Nothing else.
			read := r.Method == http.MethodGet || r.Method == http.MethodHead
			decide := r.Method == http.MethodPost && strings.HasSuffix(strings.TrimRight(r.URL.Path, "/"), "/commands")
			if !read && !decide {
				writeErr(w, http.StatusForbidden, "approver_scope",
					"approvers may read this run and decide its approvals; other changes belong to its owner")
				return
			}
			next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), approverOnlyKey{}, true)))
		default:
			next.ServeHTTP(w, r)
		}
	})
}

// scopedID extracts the run or session id a path addresses ("" if none).
func scopedID(path string) (kind, id string) {
	for prefix, k := range map[string]string{"/v1/runs/": "run", "/v1/reasoning/runs/": "run", "/v1/sessions/": "session"} {
		if rest, ok := strings.CutPrefix(path, prefix); ok {
			seg, _, _ := strings.Cut(rest, "/")
			return k, seg
		}
	}
	return "", ""
}

// visibleGraphNode hides what would expose another user's work. Run, step and
// thought nodes carry who started their run: they are shown when that run is
// shared operational work or the caller's own, and hidden when the owner is
// someone else or unknown (a step or thought label is the agent's reasoning
// text). A user's personal-memory titles, and the shard node that groups
// them, are shown only to that user. Admins see everything.
func visibleGraphNode(p auth.Principal, n domain.GraphNode) bool {
	if isAdmin(p) {
		return true
	}
	switch n.Kind {
	case "run", "step", "thought":
		typ, _ := n.Attrs["initiator_type"].(string)
		who, _ := n.Attrs["initiator_id"].(string)
		if typ == "" {
			return false // unknown owner: fail closed
		}
		return domain.SharedInitiator(typ) || (who != "" && who == p.Subject)
	case "memory":
		if sc, _ := n.Attrs["scope"].(string); sc == "user" {
			owner, _ := n.Attrs["scope_id"].(string)
			return owner == p.Subject
		}
	case "shard":
		if owner, ok := strings.CutPrefix(n.Ref, "user:"); ok {
			return owner == p.Subject
		}
	}
	return true
}
