package sretools

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/tools"
)

// KubernetesConfig talks to the API server directly (no client-go) with a
// service-account token. Namespaces must be allowlisted. DryRun defaults to
// true: the restart is validated server-side (dryRun=All) and nothing changes
// until an operator sets dry_run=false.
type KubernetesConfig struct {
	APIServer  string        `json:"api_server"`
	TokenFile  string        `json:"token_file,omitempty"`
	CAFile     string        `json:"ca_file,omitempty"`
	Namespaces []string      `json:"namespaces"`
	DryRun     *bool         `json:"dry_run,omitempty"`
	Timeout    time.Duration `json:"-"`
	HTTP       *http.Client  `json:"-"`
	Token      string        `json:"-"` // tests
}

const (
	restartedAtAnno = "kubectl.kubernetes.io/restartedAt"
	restartKeyAnno  = "sre-harness.io/restart-key"
	revisionAnno    = "deployment.kubernetes.io/revision"
)

var dns1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9]*[a-z0-9])?$`)

type Kubernetes struct {
	cfg    KubernetesConfig
	dryRun bool
	ns     map[string]bool
}

// InClusterConfig fills defaults from the pod service account.
func InClusterConfig(c KubernetesConfig) KubernetesConfig {
	if c.APIServer == "" {
		if h, p := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT"); h != "" {
			c.APIServer = "https://" + h + ":" + p
		}
	}
	if c.TokenFile == "" {
		c.TokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
	}
	if c.CAFile == "" {
		c.CAFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
	}
	return c
}

func NewKubernetes(cfg KubernetesConfig) (*Kubernetes, error) {
	if cfg.APIServer == "" {
		return nil, errors.New("kubernetes: api_server is required")
	}
	if len(cfg.Namespaces) == 0 {
		return nil, errors.New("kubernetes: namespaces allowlist is required")
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 20 * time.Second
	}
	if cfg.HTTP == nil {
		tc := &tls.Config{MinVersion: tls.VersionTLS12}
		if cfg.CAFile != "" {
			pem, err := os.ReadFile(cfg.CAFile)
			if err != nil {
				return nil, fmt.Errorf("kubernetes: ca_file: %w", err)
			}
			pool := x509.NewCertPool()
			pool.AppendCertsFromPEM(pem)
			tc.RootCAs = pool
		}
		cfg.HTTP = &http.Client{Timeout: cfg.Timeout, Transport: &http.Transport{TLSClientConfig: tc}}
	}
	k := &Kubernetes{cfg: cfg, dryRun: cfg.DryRun == nil || *cfg.DryRun, ns: map[string]bool{}}
	for _, n := range cfg.Namespaces {
		k.ns[n] = true
	}
	return k, nil
}

func (k *Kubernetes) DryRun() bool { return k.dryRun }

func (k *Kubernetes) Tools() []tools.Tool {
	mode := "LIVE: changes the cluster"
	if k.dryRun {
		mode = "DRY-RUN: validated by the API server, no change is made"
	}
	return []tools.Tool{
		&tools.Func{Desc: tools.Descriptor{Name: "k8s.deployment_status", Version: "1", Risk: domain.RiskR0,
			TargetArg: "namespace,deployment", Trust: "T2", Source: "native",
			Description: "Read a Deployment's replicas, conditions and current revision.", InputSchema: deploySchema},
			Fn: k.status},
		&tools.Func{Desc: tools.Descriptor{Name: "k8s.rollout_restart", Version: "1", Risk: domain.RiskR2,
			TargetArg: "namespace,deployment", Trust: "T2", Source: "native",
			Description: "Rolling restart of a Deployment (like kubectl rollout restart). Requires approval. Mode: " + mode,
			InputSchema: deploySchema},
			Fn: k.restart},
	}
}

var deploySchema = json.RawMessage(`{"type":"object","properties":{` +
	`"namespace":{"type":"string"},"deployment":{"type":"string"},` +
	`"reason":{"type":"string","description":"Why the restart is needed"}},` +
	`"required":["namespace","deployment"],"additionalProperties":false}`)

type deployArgs struct {
	Namespace  string `json:"namespace"`
	Deployment string `json:"deployment"`
	Reason     string `json:"reason"`
}

func (k *Kubernetes) parse(args json.RawMessage) (deployArgs, error) {
	var a deployArgs
	if err := json.Unmarshal(args, &a); err != nil {
		return a, fmt.Errorf("%w: invalid arguments: %v", domain.ErrNotDispatched, err)
	}
	if !dns1123.MatchString(a.Namespace) || !dns1123.MatchString(a.Deployment) || len(a.Deployment) > 253 {
		return a, fmt.Errorf("%w: namespace and deployment must be DNS-1123 names", domain.ErrNotDispatched)
	}
	if !k.ns[a.Namespace] {
		return a, fmt.Errorf("%w: namespace %q is not in the allowlist", domain.ErrNotDispatched, a.Namespace)
	}
	return a, nil
}

func (k *Kubernetes) token() (string, error) {
	if k.cfg.Token != "" {
		return k.cfg.Token, nil
	}
	if k.cfg.TokenFile == "" {
		return "", nil
	}
	b, err := os.ReadFile(k.cfg.TokenFile) // re-read: projected tokens rotate
	return strings.TrimSpace(string(b)), err
}

func (k *Kubernetes) do(ctx context.Context, method, path string, q url.Values, body []byte, ctype string) (int, []byte, error) {
	u := strings.TrimRight(k.cfg.APIServer, "/") + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(ctx, method, u, bytes.NewReader(body))
	if err != nil {
		return 0, nil, err
	}
	tok, err := k.token()
	if err != nil {
		return 0, nil, err
	}
	if tok != "" {
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	req.Header.Set("Accept", "application/json")
	if ctype != "" {
		req.Header.Set("Content-Type", ctype)
	}
	resp, err := k.cfg.HTTP.Do(req)
	if err != nil {
		return 0, nil, err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	return resp.StatusCode, b, err
}

type deployment struct {
	Metadata struct {
		Name        string            `json:"name"`
		Namespace   string            `json:"namespace"`
		Generation  int64             `json:"generation"`
		Annotations map[string]string `json:"annotations"`
	} `json:"metadata"`
	Spec struct {
		Replicas *int64 `json:"replicas"`
		Template struct {
			Metadata struct {
				Annotations map[string]string `json:"annotations"`
			} `json:"metadata"`
		} `json:"template"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		Replicas           int64 `json:"replicas"`
		UpdatedReplicas    int64 `json:"updatedReplicas"`
		ReadyReplicas      int64 `json:"readyReplicas"`
		AvailableReplicas  int64 `json:"availableReplicas"`
		Conditions         []struct {
			Type    string `json:"type"`
			Status  string `json:"status"`
			Reason  string `json:"reason"`
			Message string `json:"message"`
		} `json:"conditions"`
	} `json:"status"`
}

func (k *Kubernetes) get(ctx context.Context, a deployArgs) (deployment, error) {
	var d deployment
	code, body, err := k.do(ctx, http.MethodGet, deployPath(a), nil, nil, "")
	if err != nil {
		return d, domain.Retryable(fmt.Errorf("kubernetes: %w", err), 2*time.Second)
	}
	switch {
	case code == http.StatusNotFound:
		return d, fmt.Errorf("%w: deployment %s/%s not found", domain.ErrNotDispatched, a.Namespace, a.Deployment)
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		return d, fmt.Errorf("%w: kubernetes denied access (HTTP %d)", domain.ErrNotDispatched, code)
	case code >= 500 || code == 429:
		return d, domain.Retryable(fmt.Errorf("kubernetes: HTTP %d", code), 5*time.Second)
	case code != http.StatusOK:
		return d, fmt.Errorf("%w: kubernetes: HTTP %d: %s", domain.ErrNotDispatched, code, clip(body))
	}
	err = json.Unmarshal(body, &d)
	return d, err
}

func deployPath(a deployArgs) string {
	return "/apis/apps/v1/namespaces/" + a.Namespace + "/deployments/" + a.Deployment
}

func summary(d deployment) map[string]any {
	conds := []string{}
	for _, c := range d.Status.Conditions {
		conds = append(conds, fmt.Sprintf("%s=%s (%s)", c.Type, c.Status, c.Reason))
	}
	var desired int64 = 1
	if d.Spec.Replicas != nil {
		desired = *d.Spec.Replicas
	}
	return map[string]any{"namespace": d.Metadata.Namespace, "deployment": d.Metadata.Name, "desired_replicas": desired,
		"ready_replicas": d.Status.ReadyReplicas, "updated_replicas": d.Status.UpdatedReplicas,
		"available_replicas": d.Status.AvailableReplicas, "generation": d.Metadata.Generation,
		"observed_generation": d.Status.ObservedGeneration, "revision": d.Metadata.Annotations[revisionAnno],
		"conditions": conds, "last_restarted_at": d.Spec.Template.Metadata.Annotations[restartedAtAnno]}
}

func (k *Kubernetes) status(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	a, err := k.parse(args)
	if err != nil {
		return nil, err
	}
	d, err := k.get(ctx, a)
	if err != nil {
		return nil, err
	}
	return json.Marshal(summary(d))
}

// restart patches the pod template annotation, exactly like kubectl. The
// idempotency key is written into the template, so a retried attempt finds
// its own earlier restart and does not restart again.
func (k *Kubernetes) restart(ctx context.Context, key string, args json.RawMessage) (json.RawMessage, error) {
	// Without a key the "already applied" check below would match any
	// deployment that has no restart annotation yet and skip the restart.
	if key == "" {
		return nil, fmt.Errorf("%w: k8s.rollout_restart requires an idempotency key", domain.ErrNotDispatched)
	}
	a, err := k.parse(args)
	if err != nil {
		return nil, err
	}
	before, err := k.get(ctx, a) // pre-dispatch read: errors here are safe
	if err != nil {
		if errors.Is(err, domain.ErrNotDispatched) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	revision := before.Metadata.Annotations[revisionAnno]
	rollback := map[string]any{
		"description": "A restart does not change the pod spec; if new pods fail, roll back to the previous revision.",
		"command":     fmt.Sprintf("kubectl rollout undo deployment/%s -n %s --to-revision=%s", a.Deployment, a.Namespace, revision),
		"verify":      fmt.Sprintf("kubectl rollout status deployment/%s -n %s", a.Deployment, a.Namespace),
	}
	if !k.dryRun && before.Spec.Template.Metadata.Annotations[restartKeyAnno] == key {
		return json.Marshal(map[string]any{"result": "already applied by an earlier attempt", "dry_run": false,
			"before": summary(before), "rollback_plan": rollback, "idempotency_key": key})
	}
	now := time.Now().UTC().Format(time.RFC3339)
	patch, _ := json.Marshal(map[string]any{"spec": map[string]any{"template": map[string]any{"metadata": map[string]any{
		"annotations": map[string]string{restartedAtAnno: now, restartKeyAnno: key}}}}})
	q := url.Values{"fieldManager": {"sre-harness"}}
	if k.dryRun {
		q.Set("dryRun", "All")
	}
	code, body, err := k.do(ctx, http.MethodPatch, deployPath(a), q, patch, "application/strategic-merge-patch+json")
	if err != nil {
		if k.dryRun {
			return nil, domain.Retryable(err, 2*time.Second) // a dry run changes nothing
		}
		return nil, fmt.Errorf("%w: kubernetes patch: %v", domain.ErrUnknownEffect, err)
	}
	switch {
	case code == http.StatusOK:
	case code == 400 || code == 401 || code == 403 || code == 404 || code == 409 || code == 422:
		// The API server rejected the request: nothing was applied.
		return nil, fmt.Errorf("%w: kubernetes rejected the restart (HTTP %d): %s", domain.ErrNotDispatched, code, clip(body))
	default:
		if k.dryRun {
			return nil, domain.Retryable(fmt.Errorf("kubernetes: HTTP %d", code), 5*time.Second)
		}
		return nil, fmt.Errorf("%w: kubernetes patch HTTP %d", domain.ErrUnknownEffect, code)
	}
	var after deployment
	_ = json.Unmarshal(body, &after)
	result := "rolling restart started"
	if k.dryRun {
		result = "dry run: the API server accepted the restart; nothing was changed"
	}
	return json.Marshal(map[string]any{"result": result, "dry_run": k.dryRun, "restarted_at": now, "reason": a.Reason,
		"before": summary(before), "after_generation": after.Metadata.Generation, "rollback_plan": rollback,
		"idempotency_key": key, "operation_id": fmt.Sprintf("%s/%s@gen%d", a.Namespace, a.Deployment, after.Metadata.Generation)})
}

func clip(b []byte) string {
	if len(b) > 512 {
		return string(b[:512]) + "…"
	}
	return string(b)
}
