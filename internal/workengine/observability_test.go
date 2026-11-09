package workengine

import (
	"context"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"go-sre-harness/internal/obs"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/work"
)

// The shipped alert rules and dashboard reference only metrics the harness
// exports (RFC §21.3): a renamed metric must not silently blind an alert.
func TestDashboardsReferenceExportedMetrics(t *testing.T) {
	s, err := cpsql.OpenSQLite(filepath.Join(t.TempDir(), "cp.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// One open Work so the scrape-time Work gauge has a sample.
	tpl := work.Builtin()[0]
	if _, _, err := s.CreateWork(context.Background(), work.NewWork{TenantID: "t", Kind: work.KindIncident, Title: "x",
		Goal: work.GoalSpec{Intent: "mitigate", SubjectRefs: []string{"service://prod/x"}}, Template: tpl,
		IdempotencyKey: "k", Now: time.Now()}); err != nil {
		t.Fatal(err)
	}
	m := obs.NewMetrics()
	om := orchestration.NewMetrics(m.Reg, s)
	om.GuardDenied("scope", "x") // counters export once touched
	var b strings.Builder
	m.Reg.Write(context.Background(), &b)
	exported := map[string]bool{}
	for _, line := range strings.Split(b.String(), "\n") {
		if strings.HasPrefix(line, "# TYPE ") {
			exported[strings.Fields(line)[2]] = true
		}
	}
	ref := regexp.MustCompile(`harness_[a-z0-9_]+`)
	for _, f := range []string{"../../deploy/observability/harness-alerts.yaml", "../../deploy/observability/harness-dashboard.json"} {
		body, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		for _, name := range ref.FindAllString(string(body), -1) {
			base := name
			for _, suf := range []string{"_bucket", "_sum", "_count"} {
				base = strings.TrimSuffix(base, suf)
			}
			if !exported[base] && !strings.HasPrefix(base, "harness_v2") {
				t.Errorf("%s references %s, which is not exported", filepath.Base(f), name)
			}
		}
	}
}
