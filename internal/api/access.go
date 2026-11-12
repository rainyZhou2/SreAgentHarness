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

// RunAccessible reports whether the caller may see a run. A run that does not
// exist is reported as accessible so that the handler answers 404 itself.
func (s *Server) RunAccessible(ctx context.Context, p auth.Principal, runID string) (bool, error) {
	if isAdmin(p) {
		return true, nil
	}
	run, err := s.Store.GetRun(ctx, p.Tenant, runID)
	if errors.Is(err, domain.ErrNotFound) {
		return true, nil
	}
	if err != nil {
		return false, err
	}
	if ownsRun(p, run) {
		return true, nil
	}
	if p.Has(auth.Approver) {
		apps, err := s.Store.ListApprovals(ctx, p.Tenant, runID)
		if err != nil {
			return false, err
		}
		return len(apps) > 0, nil
	}
	return false, nil
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
		var allowed bool
		var err error
		if kind == "session" {
			allowed, err = s.sessionAccessible(r.Context(), p, id, r.Method)
		} else {
			allowed, err = s.RunAccessible(r.Context(), p, id)
		}
		switch {
		case err != nil:
			s.storeErr(w, err)
		case !allowed:
			writeErr(w, http.StatusNotFound, "not_found", "run or resource not found")
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

// visibleGraphNode hides what would expose another user's work: a user's
// personal memory titles, and the steps and thoughts of runs (their labels
// are the agent's reasoning text and carry no owner). Admins see everything.
func visibleGraphNode(p auth.Principal, n domain.GraphNode) bool {
	if isAdmin(p) {
		return true
	}
	switch n.Kind {
	case "thought", "step":
		return false
	case "memory":
		if sc, _ := n.Attrs["scope"].(string); sc == "user" {
			owner, _ := n.Attrs["scope_id"].(string)
			return owner == p.Subject
		}
	}
	return true
}
