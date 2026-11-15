package api_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"go-sre-harness/internal/api"
	"go-sre-harness/internal/artifact"
	"go-sre-harness/internal/localstore"
	"go-sre-harness/internal/memory"
)

type memoryAuthority struct {
	decisionVerdict string
	outcomeVerdict  string
}

func (a memoryAuthority) MemoryDecision(context.Context, string, string) (memory.DecisionEvidence, error) {
	return memory.DecisionEvidence{Verdict: a.decisionVerdict, WorkID: "w", Generation: 1, EvidenceRefs: []string{"evidence:1"}}, nil
}
func (a memoryAuthority) MemoryOutcome(context.Context, string, string) (memory.OutcomeEvidence, error) {
	return memory.OutcomeEvidence{Verdict: a.outcomeVerdict, WorkID: "w", Generation: 1, EvidenceRefs: []string{"evidence:1"}}, nil
}

func TestMemoryVerificationAndApprovalRoutes(t *testing.T) {
	st, err := localstore.Open(filepath.Join(t.TempDir(), "h.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	server := &api.Server{Store: st, Artifacts: artifact.NewMemory()}
	governance := &memory.GovernanceService{Store: st, Authority: memoryAuthority{decisionVerdict: "CONFIRMED", outcomeVerdict: "SATISFIED"}}
	server.Extend(&api.MemoryAPI{Store: st, Writer: memory.Writer{Embedder: memory.HashingEmbedder{Dims: 16}, Artifacts: artifact.NewMemory()},
		Server: server, Governance: governance})
	handler := server.Handler()
	do := func(user, roles, method, path, body string) (int, map[string]any) {
		req := httptest.NewRequest(method, path, strings.NewReader(body))
		req.Header.Set("X-Tenant-ID", "t")
		req.Header.Set("X-Actor-ID", user)
		req.Header.Set("X-Roles", roles)
		req.Header.Set("Idempotency-Key", "request-1")
		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		out := map[string]any{}
		_ = json.Unmarshal(rec.Body.Bytes(), &out)
		return rec.Code, out
	}
	code, out := do("alice", "operator", "POST", "/api/memories", `{"scope":"service","scope_id":"api","memory_type":"semantic","subject":"limits","title":"limits","content":"max 10","evidence_refs":["evidence:1"]}`)
	if code != http.StatusCreated {
		t.Fatalf("create: %d %v", code, out)
	}
	id := out["memory"].(map[string]any)["id"].(string)
	verifyBody := `{"expected_version":1,"decision_ref":"decision:w:key","outcome_ref":"outcome:w:o"}`
	if code, _ := do("alice", "operator", "POST", "/api/memories/"+id+"/verify", verifyBody); code != http.StatusForbidden {
		t.Fatalf("operator verified memory: %d", code)
	}
	if code, _ := do("bob", "approver", "POST", "/api/memories/"+id+"/approve", `{"expected_version":1}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("approval skipped verification: %d", code)
	}
	if code, _ := do("bob", "approver", "POST", "/api/memories/"+id+"/transition", `{"expected_status":"CANDIDATE","to":"VERIFIED"}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("generic transition published memory: %d", code)
	}
	if code, out = do("verifier", "service", "POST", "/api/memories/"+id+"/verify", verifyBody); code != http.StatusOK {
		t.Fatalf("verify: %d %v", code, out)
	}
	if code, out = do("bob", "approver", "POST", "/api/memories/"+id+"/approve", `{"expected_version":1}`); code != http.StatusOK || out["status"] != memory.Approved {
		t.Fatalf("approve: %d %v", code, out)
	}
}
