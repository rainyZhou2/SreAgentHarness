package app

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/governance"
	"go-sre-harness/internal/impact"
	"go-sre-harness/internal/observability"
	"go-sre-harness/internal/orchestration"
	"go-sre-harness/internal/plugin"
	"go-sre-harness/internal/policy"
	"go-sre-harness/internal/store/cpsql"
	"go-sre-harness/internal/workengine"
)

const governUsage = `usage:
  harness validate-config [-config harness.yaml] [-tier local|staging|production] [-format text|json]
  harness doctor [-checks db,migrations,artifact,policy,sources,events,action] [-format text|json]
  harness release-gate -baseline baseline.json -candidate candidate.json

validate-config reads the configuration and the environment, checks them
and evaluates the deployment gate for the tier. It connects to nothing and
changes nothing; it exits 1 when the deployment would be refused or would
run degraded. doctor checks a running deployment's dependencies
(HARNESS_DATABASE_URL) and exits 1 when a check fails. release-gate
compares two attestations (GET /v1/governance/attestation, or the JSON
output of validate-config) and exits 1 on a regression. No command prints
a token, a secret or an endpoint URL.`

// buildSHA is the VCS revision the binary was built from.
func buildSHA() string {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, s := range info.Settings {
			if s.Key == "vcs.revision" {
				return s.Value
			}
		}
	}
	return "unknown"
}

// deploymentOf derives the gate's view of the process from the
// configuration. The config digest covers the work section and the names
// of the enabled plugins — never plugin settings, which may hold secrets.
func deploymentOf(cfg File, e Env, auditSink bool) workengine.Deployment {
	flags := workengine.FlagsFromEnv(e)
	d := workengine.Deployment{Tier: flags.Tier, BuildSHA: buildSHA(), StoreKind: "postgres", AuditSink: auditSink}
	names := map[string][]string{}
	for kind, specs := range cfg.Plugins {
		for _, s := range specs.Enabled() {
			names[kind] = append(names[kind], s.Name)
		}
		sort.Strings(names[kind])
	}
	if a := names["artifact_store"]; len(a) > 0 {
		d.ArtifactStore = a[0]
	}
	if p := names["policy"]; len(p) > 0 {
		d.PolicyVersion, d.PolicyCustom = p[0], p[0] != "default"
	}
	d.AuthMode = "token"
	for _, a := range names["authenticator"] {
		switch a {
		case "dev":
			d.AuthMode = "dev" // one development authenticator makes the whole chain untrusted
		case "oidc":
			if d.AuthMode != "dev" {
				d.AuthMode = "oidc"
			}
		}
	}
	if len(names["authenticator"]) == 0 {
		d.AuthMode = ""
	}
	d.ConfigDigest = governance.Digest(struct {
		Work    workengine.Config
		Plugins map[string][]string
	}{cfg.Work, names})
	return d
}

func capabilityNames(cfg File) []string {
	var out []string
	for _, s := range cfg.Plugins["work_extension"].Enabled() {
		out = append(out, s.Name)
	}
	sort.Strings(out)
	return out
}

// checkConfig validates the parts of the configuration the work engine
// would refuse at start, without building anything that connects out.
func checkConfig(cfg File) []governance.Check {
	var out []governance.Check
	add := func(id string, err error, fix string) {
		c := governance.Check{ID: id, Status: governance.Pass}
		if err != nil {
			c.Status, c.Detail, c.Fix, c.Blocks = governance.Fail, err.Error(), fix, "start"
		}
		out = append(out, c)
	}
	_, err := orchestration.NewCatalog(cfg.Work.Templates)
	add("config.templates", err, "fix the work.templates section")
	_, err = eventing.NewRouter(cfg.Work.CorrelationRules)
	add("config.correlation_rules", err, "fix the work.correlation_rules section")
	_, err = orchestration.ValidateQueueLimits(cfg.Work.Queues)
	add("config.queues", err, "fix the work.queues section")
	var oerr error
	if o := cfg.Work.Observability; o != nil {
		seen := map[string]bool{}
		for _, src := range o.Sources {
			s := src
			if s.State == "" {
				s.State = observability.StateActive
			}
			switch {
			case s.TenantID == "":
				oerr = errors.Join(oerr, fmt.Errorf("source %s: tenant_id is required", s.ID))
			case seen[s.TenantID+"/"+s.ID]:
				oerr = errors.Join(oerr, fmt.Errorf("source %s is declared twice for tenant %s", s.ID, s.TenantID))
			}
			seen[s.TenantID+"/"+s.ID] = true
			if err := s.Normalize(); err != nil {
				oerr = errors.Join(oerr, fmt.Errorf("source %s: %w", s.ID, err))
			}
			if _, ok := o.Endpoints[s.EndpointRef]; !ok && s.Backend != "ledger" {
				oerr = errors.Join(oerr, fmt.Errorf("source %s: endpoint_ref %q is not declared", s.ID, s.EndpointRef))
			}
			if _, ok := o.Credentials[s.CredentialID]; !ok && s.CredentialID != "" {
				oerr = errors.Join(oerr, fmt.Errorf("source %s: credential_id %q is not declared", s.ID, s.CredentialID))
			}
		}
		for name, c := range o.Credentials {
			if c.TokenEnv == "" && c.TokenFile == "" {
				oerr = errors.Join(oerr, fmt.Errorf("credential %s names no token_env or token_file", name))
			}
		}
		for _, r := range o.Required {
			if !seen[r] {
				oerr = errors.Join(oerr, fmt.Errorf("required source %q is not declared (want <tenant>/<source id>)", r))
			}
		}
	}
	add("config.observability", oerr, "fix the work.observability section")
	var perr error
	for kind, specs := range cfg.Plugins {
		known := map[string]bool{}
		for _, p := range plugin.Registered() {
			if p.Kind == kind {
				known[p.Name] = true
			}
		}
		for _, s := range specs.Enabled() {
			if !known[s.Name] {
				perr = errors.Join(perr, fmt.Errorf("plugin %s/%s is not compiled into this binary", kind, s.Name))
			}
		}
	}
	add("config.plugins", perr, "run: harness plugins")
	return out
}

func printChecks(w io.Writer, checks []governance.Check) (failed int) {
	for _, c := range checks {
		mark := map[string]string{governance.Pass: "ok  ", governance.Fail: "FAIL", governance.Warn: "warn", governance.Skip: "skip"}[c.Status]
		fmt.Fprintf(w, "%s  %-32s %s\n", mark, c.ID, c.Detail)
		if c.Fix != "" && c.Status != governance.Pass {
			fmt.Fprintf(w, "      fix: %s\n", c.Fix)
		}
		if c.Status == governance.Fail {
			failed++
		}
	}
	return failed
}

// Govern runs validate-config, doctor and release-gate; it returns the
// process exit code.
func Govern(ctx context.Context, cmd string, args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet(cmd, flag.ContinueOnError)
	fs.SetOutput(stderr)
	config := fs.String("config", "", "configuration file (default: HARNESS_CONFIG)")
	tier := fs.String("tier", "", "deployment tier to validate for (default: HARNESS_DEPLOYMENT_TIER)")
	format := fs.String("format", "text", "text or json")
	checks := fs.String("checks", "db,migrations,artifact,policy,sources,events,action", "doctor: checks to run")
	baseline := fs.String("baseline", "", "release-gate: baseline attestation JSON")
	candidate := fs.String("candidate", "", "release-gate: candidate attestation JSON")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	e := Env(func(k string) string {
		if k == "HARNESS_DEPLOYMENT_TIER" && *tier != "" {
			return *tier
		}
		return getenv(k)
	})
	emit := func(v any) {
		b, _ := json.MarshalIndent(v, "", "  ")
		fmt.Fprintln(stdout, string(b))
	}
	if cmd == "release-gate" {
		read := func(path string) (governance.Attestation, error) {
			var a governance.Attestation
			b, err := os.ReadFile(path)
			if err != nil {
				return a, err
			}
			// Accept the bare attestation or validate-config's JSON output.
			var wrapped struct {
				Attestation *governance.Attestation `json:"attestation"`
			}
			if json.Unmarshal(b, &wrapped) == nil && wrapped.Attestation != nil {
				return *wrapped.Attestation, nil
			}
			return a, json.Unmarshal(b, &a)
		}
		if *baseline == "" || *candidate == "" {
			fmt.Fprintln(stderr, governUsage)
			return 2
		}
		base, err := read(*baseline)
		if err != nil {
			fmt.Fprintln(stderr, "release-gate: baseline:", err)
			return 2
		}
		cand, err := read(*candidate)
		if err != nil {
			fmt.Fprintln(stderr, "release-gate: candidate:", err)
			return 2
		}
		diff := governance.ReleaseGate(base, cand)
		if *format == "json" {
			emit(diff)
		} else {
			for _, r := range diff.Regressions {
				fmt.Fprintln(stdout, "REGRESSION ", r)
			}
			for _, n := range diff.Notes {
				fmt.Fprintln(stdout, "note       ", n)
			}
			fmt.Fprintf(stdout, "release gate: %s (%s %s → %s %s)\n", map[bool]string{true: "passed", false: "FAILED"}[diff.OK],
				base.Tier, base.Stage, cand.Tier, cand.Stage)
		}
		if !diff.OK {
			return 1
		}
		return 0
	}
	if _, err := governance.ParseTier(e("HARNESS_DEPLOYMENT_TIER")); err != nil {
		fmt.Fprintln(stderr, cmd+":", err)
		return 2
	}
	cfg, err := FromEnv(e)
	if err == nil {
		path := *config
		if path == "" {
			path = e("HARNESS_CONFIG")
		}
		cfg, err = LoadFile(path, cfg)
	}
	if err != nil {
		fmt.Fprintln(stderr, cmd+": configuration:", err)
		return 1
	}
	flags := workengine.FlagsFromEnv(e)
	switch cmd {
	case "validate-config":
		conf := checkConfig(cfg)
		d := deploymentOf(cfg, e, true)
		var sources []governance.SourceStatus
		if o := cfg.Work.Observability; o != nil {
			required := map[string]bool{}
			for _, r := range o.Required {
				required[r] = true
			}
			for _, s := range o.Sources {
				state := s.State
				if state == "" {
					state = observability.StateActive
				}
				// Not probed: validation connects to nothing.
				sources = append(sources, governance.SourceStatus{ID: s.TenantID + "/" + s.ID, Kind: string(s.Kind), State: state,
					Required: required[s.TenantID+"/"+s.ID]})
			}
		}
		// Actions come from capabilities that are only built at start; the
		// allow-list is checked for shape here and against the registry by
		// the running deployment's attestation.
		in := workengine.GateInput(d, flags, workengine.ExpectedSchema(), capabilityNames(cfg), nil, nil, sources, time.Now().UTC())
		for _, name := range flags.ActionAllowlist {
			in.Actions = append(in.Actions, governance.ActionInfo{Name: name, Risk: "R1", IdempotencyMode: "QUERY_BEFORE_RETRY", HasVersionFact: true})
		}
		for _, name := range flags.Preauthorized {
			in.Actions = append(in.Actions, governance.ActionInfo{Name: name, Risk: "R1", IdempotencyMode: "QUERY_BEFORE_RETRY", HasVersionFact: true})
		}
		att := governance.Evaluate(in)
		bad := 0
		for _, c := range conf {
			if c.Status == governance.Fail {
				bad++
			}
		}
		if *format == "json" {
			emit(map[string]any{"config": conf, "attestation": att})
		} else {
			fmt.Fprintf(stdout, "tier %s — requested stage %s, allowed stage %s, mode %s\n\n", att.Tier, att.RequestedStage, att.Stage, att.Mode)
			printChecks(stdout, conf)
			printChecks(stdout, att.Checks)
		}
		if bad > 0 || att.Mode != governance.ModeNormal {
			return 1
		}
		return 0
	case "doctor":
		return doctor(ctx, cfg, e, flags, strings.Split(*checks, ","), *format, emit, stdout, stderr)
	}
	fmt.Fprintln(stderr, governUsage)
	return 2
}

// doctor checks the dependencies of a deployment. Every check is
// read-only except the artifact round trip, which writes one small object
// under the tenant "harness-doctor".
func doctor(ctx context.Context, cfg File, e Env, flags workengine.Flags, which []string, format string, emit func(any),
	stdout, stderr io.Writer) int {
	var out []governance.Check
	add := func(id string, err error, detail, fix string) {
		c := governance.Check{ID: id, Status: governance.Pass, Detail: detail}
		if err != nil {
			c.Status, c.Detail, c.Fix = governance.Fail, err.Error(), fix
		}
		out = append(out, c)
	}
	want := map[string]bool{}
	for _, w := range which {
		want[strings.TrimSpace(w)] = true
	}
	var pool *pgxpool.Pool
	var store *cpsql.Store
	if url := e("HARNESS_DATABASE_URL"); url == "" {
		add("db", errors.New("HARNESS_DATABASE_URL is not set"), "", "set HARNESS_DATABASE_URL")
	} else {
		cctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		p, err := pgxpool.New(cctx, url)
		if err == nil {
			err = p.Ping(cctx)
		}
		cancel()
		if err != nil {
			// The driver's error may quote the connection string.
			add("db", errors.New("the database is not reachable with the configured URL"), "", "check HARNESS_DATABASE_URL and network access")
		} else {
			pool, store = p, cpsql.OpenPostgres(p)
			defer p.Close()
			if want["db"] {
				add("db", nil, "connected", "")
			}
		}
	}
	if want["migrations"] && pool != nil {
		var v string
		err := pool.QueryRow(ctx, `SELECT coalesce(max(version), '') FROM schema_migrations`).Scan(&v)
		switch {
		case err != nil:
			add("migrations", errors.New("the schema_migrations table is not readable"), "", "run: harness migrate")
		case v != workengine.ExpectedSchema():
			add("migrations", fmt.Errorf("database schema is %q, this build needs %q", v, workengine.ExpectedSchema()), "", "run: harness migrate")
		default:
			add("migrations", nil, "schema "+v, "")
		}
	}
	if want["artifact"] {
		h := plugin.NewHost("doctor", slog.Default())
		st, err := plugin.BuildOne(ctx, plugin.ArtifactStore, h, cfg.Plugins["artifact_store"])
		if err == nil && st != nil {
			body := []byte("harness doctor " + time.Now().UTC().Format(time.RFC3339Nano))
			var ref string
			if ref, _, err = st.Put(ctx, "harness-doctor", "probe", body); err == nil {
				var got []byte
				if got, err = st.Get(ctx, "harness-doctor", ref); err == nil && string(got) != string(body) {
					err = errors.New("the object read back differs from the one written")
				}
			}
		}
		if err != nil {
			err = errors.New("artifact store round trip failed: " + redact(err))
		}
		add("artifact", err, "write and read back succeeded", "check the artifact_store plugin configuration and its permissions")
	}
	if want["policy"] {
		d := deploymentOf(cfg, e, true)
		var err error
		if flags.Tier == governance.TierProduction && flags.ActionV2 && !d.PolicyCustom {
			err = errors.New("production actions are enabled under the built-in default policy")
		}
		add("policy", err, "policy "+d.PolicyVersion+" ("+string(policy.Default().Environment)+" defaults)", "configure a policy plugin")
	}
	if want["sources"] && store != nil {
		o := cfg.Work.Observability
		if o == nil || len(o.Sources) == 0 {
			out = append(out, governance.Check{ID: "sources", Status: governance.Skip, Detail: "no observability sources are configured"})
		} else {
			b := &observability.Broker{Catalog: store, Audit: store, Endpoints: o.Endpoints, Credentials: o.Credentials,
				Clock: store.Clock, Local: map[string]observability.Factory{"ledger": impact.Ledger(store)}}
			for _, s := range o.Sources {
				pctx, cancel := context.WithTimeout(ctx, 20*time.Second)
				hl, err := b.Probe(pctx, s.TenantID, s.ID)
				cancel()
				id := "sources." + s.TenantID + "/" + s.ID
				switch {
				case errors.Is(err, observability.ErrNotFound):
					add(id, errors.New("the source is not registered yet (it is registered when the engine starts)"), "", "start the harness once, then rerun")
				case err != nil:
					add(id, errors.New("probe failed"), "", "check the source catalog")
				case !hl.Authorized:
					add(id, errors.New("the backend refused the credential"), "", "check credential "+s.CredentialID+" and its permissions")
				case !hl.OK:
					add(id, errors.New("unreachable: "+hl.Error), "", "check endpoint "+s.EndpointRef+" and network policy")
				default:
					add(id, nil, fmt.Sprintf("healthy (%d ms)", hl.LatencyMS), "")
				}
			}
		}
	}
	if (want["events"] || want["action"]) && store != nil {
		bl, err := store.Backlog(ctx, time.Now().UTC())
		if err != nil {
			add("backlog", errors.New("the control-plane backlog is not readable"), "", "run: harness migrate")
		} else {
			if want["events"] {
				var err error
				if n := bl.Inbox["DEAD"].Count; n > 0 {
					err = fmt.Errorf("%d event(s) are dead-lettered", n)
				} else if it := bl.Inbox["FAILED"]; it.Count > 0 && it.Oldest > 10*time.Minute {
					err = fmt.Errorf("%d event(s) keep failing (oldest %s)", it.Count, it.Oldest.Round(time.Second))
				} else if bl.OverdueWatches.Count > 0 {
					err = fmt.Errorf("%d watch(es) are overdue: the reconciler is not running", bl.OverdueWatches.Count)
				}
				add("events", err, fmt.Sprintf("inbox new=%d failed=%d", bl.Inbox["NEW"].Count, bl.Inbox["FAILED"].Count),
					"inspect GET /v1/events/{id}; check the scheduler role is running")
			}
			if want["action"] {
				var err error
				switch {
				case bl.UnapprovedDispatches > 0:
					err = fmt.Errorf("%d action(s) were dispatched without an approval on record", bl.UnapprovedDispatches)
				case bl.Actions["EFFECT_UNKNOWN"].Count+bl.Actions["MANUAL_RECOVERY"].Count > 0:
					err = fmt.Errorf("%d action(s) have an unknown effect and wait for a human (oldest %s)",
						bl.Actions["EFFECT_UNKNOWN"].Count+bl.Actions["MANUAL_RECOVERY"].Count,
						max(bl.Actions["EFFECT_UNKNOWN"].Oldest, bl.Actions["MANUAL_RECOVERY"].Oldest).Round(time.Second))
				}
				add("action", err, fmt.Sprintf("waiting approval=%d", bl.Actions["WAITING_APPROVAL"].Count),
					"reconcile or resolve them: POST /v1/actions/{id}/reconcile | /resolve")
			}
		}
	}
	failed := 0
	if format == "json" {
		for _, c := range out {
			if c.Status == governance.Fail {
				failed++
			}
		}
		emit(map[string]any{"checks": out, "failed": failed})
	} else {
		failed = printChecks(stdout, out)
		fmt.Fprintf(stdout, "\n%d check(s), %d failed\n", len(out), failed)
	}
	_ = stderr
	if failed > 0 {
		return 1
	}
	return 0
}

// redact keeps an error's words and drops anything that looks like a URL
// or a path with credentials.
func redact(err error) string {
	fields := strings.Fields(err.Error())
	for i, f := range fields {
		if strings.Contains(f, "://") || strings.Contains(f, "@") {
			fields[i] = "<redacted>"
		}
	}
	s := strings.Join(fields, " ")
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}
