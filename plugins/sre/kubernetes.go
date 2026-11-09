package sre

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"go-sre-harness/internal/action"
	"go-sre-harness/internal/capability"
	"go-sre-harness/internal/cpkit"
	"go-sre-harness/internal/domain"
	"go-sre-harness/internal/eventing"
	"go-sre-harness/internal/tools"
	"go-sre-harness/internal/world"
)

// KubernetesConfig configures the Kubernetes capability. It talks to the
// API server directly with a service-account token; namespaces must be
// allowlisted, and every write supports server-side dry-run (default ON:
// nothing changes until the operator sets dry_run=false).
type KubernetesConfig struct {
	capability.HTTPConfig
	InCluster  bool     `json:"in_cluster,omitempty"`
	Namespaces []string `json:"namespaces"`
	DryRun     *bool    `json:"dry_run,omitempty"`
	// ServiceLabel names the pod/deployment label that identifies the
	// service (default app.kubernetes.io/name, then app).
	ServiceLabel string              `json:"service_label,omitempty"`
	CollectEvery capability.Duration `json:"collect_every,omitempty"`
	// MaxReplicas bounds deployment.scale (default 50); MaxScaleFactor the
	// change per action (default 2: at most double or halve).
	MaxReplicas    int     `json:"max_replicas,omitempty"`
	MaxScaleFactor float64 `json:"max_scale_factor,omitempty"`
	// MaxPods bounds the pods collected per namespace (default 2000).
	MaxPods int `json:"max_pods,omitempty"`
}

const (
	effectKeyAnno = "sre-harness.io/effect-key"
	revisionAnno  = "deployment.kubernetes.io/revision"
)

var dns1123 = regexp.MustCompile(`^[a-z0-9]([-a-z0-9.]*[a-z0-9])?$`)

// kube is the API client shared by the Kubernetes and Traffic capabilities.
type kube struct {
	c      *capability.Client
	ns     map[string]bool
	dryRun bool
}

func newKube(cfg KubernetesConfig) (*kube, error) {
	if cfg.InCluster {
		if cfg.URL == "" {
			if h, p := os.Getenv("KUBERNETES_SERVICE_HOST"), os.Getenv("KUBERNETES_SERVICE_PORT"); h != "" {
				cfg.URL = "https://" + h + ":" + p
			}
		}
		if cfg.TokenFile == "" && cfg.Token == "" {
			cfg.TokenFile = "/var/run/secrets/kubernetes.io/serviceaccount/token"
		}
		if cfg.CAFile == "" && cfg.HTTP == nil {
			cfg.CAFile = "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt"
		}
	}
	if len(cfg.Namespaces) == 0 {
		return nil, fmt.Errorf("kubernetes: namespaces allowlist is required")
	}
	c, err := capability.NewClient(cfg.HTTPConfig)
	if err != nil {
		return nil, fmt.Errorf("kubernetes: %w", err)
	}
	k := &kube{c: c, ns: map[string]bool{}, dryRun: cfg.DryRun == nil || *cfg.DryRun}
	for _, n := range cfg.Namespaces {
		if !dns1123.MatchString(n) {
			return nil, fmt.Errorf("kubernetes: namespace %q is not a DNS-1123 name", n)
		}
		k.ns[n] = true
	}
	return k, nil
}

func (k *kube) namespaces() []string {
	out := make([]string, 0, len(k.ns))
	for n := range k.ns {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// nsName splits "<scheme>://<ns>/<name>" and checks the allowlist.
func (k *kube) nsName(ref, scheme string) (string, string, error) {
	rest, ok := strings.CutPrefix(ref, scheme+"://")
	if !ok {
		return "", "", fmt.Errorf("target %q is not a %s:// ref", ref, scheme)
	}
	ns, name, ok := strings.Cut(rest, "/")
	if !ok || !dns1123.MatchString(ns) || !dns1123.MatchString(name) || strings.Contains(name, "/") {
		return "", "", fmt.Errorf("target %q must be %s://<namespace>/<name>", ref, scheme)
	}
	if !k.ns[ns] {
		return "", "", fmt.Errorf("namespace %q is not in the allowlist", ns)
	}
	return ns, name, nil
}

func (k *kube) writeQuery() url.Values {
	q := url.Values{"fieldManager": {"sre-harness"}}
	if k.dryRun {
		q.Set("dryRun", "All")
	}
	return q
}

func (k *kube) patch(ctx context.Context, path, ctype string, body any, out any) error {
	return k.c.Do(ctx, capability.Request{Method: http.MethodPatch, Path: path, Query: k.writeQuery(), Body: body,
		ContentType: ctype}, out)
}

// ---- API objects (the fields we read) ----

type objMeta struct {
	Name              string            `json:"name"`
	Namespace         string            `json:"namespace"`
	UID               string            `json:"uid"`
	Generation        int64             `json:"generation"`
	ResourceVersion   string            `json:"resourceVersion"`
	Labels            map[string]string `json:"labels"`
	Annotations       map[string]string `json:"annotations"`
	CreationTimestamp time.Time         `json:"creationTimestamp"`
	OwnerReferences   []struct {
		Kind       string `json:"kind"`
		Name       string `json:"name"`
		Controller bool   `json:"controller"`
	} `json:"ownerReferences"`
}

type container struct {
	Name  string `json:"name"`
	Image string `json:"image"`
}

type podTemplate struct {
	Metadata objMeta `json:"metadata"`
	Spec     struct {
		Containers []container `json:"containers"`
	} `json:"spec"`
}

type k8sDeployment struct {
	Metadata objMeta `json:"metadata"`
	Spec     struct {
		Replicas *int32      `json:"replicas"`
		Paused   bool        `json:"paused"`
		Template podTemplate `json:"template"`
	} `json:"spec"`
	Status struct {
		ObservedGeneration int64 `json:"observedGeneration"`
		Replicas           int32 `json:"replicas"`
		ReadyReplicas      int32 `json:"readyReplicas"`
		AvailableReplicas  int32 `json:"availableReplicas"`
		UpdatedReplicas    int32 `json:"updatedReplicas"`
		Conditions         []struct {
			Type, Status, Reason, Message string
		} `json:"conditions"`
	} `json:"status"`
}

func (d k8sDeployment) replicas() int32 {
	if d.Spec.Replicas == nil {
		return 1
	}
	return *d.Spec.Replicas
}

func (d k8sDeployment) image(name string) (string, string) {
	for _, c := range d.Spec.Template.Spec.Containers {
		if name == "" || c.Name == name {
			return c.Name, c.Image
		}
	}
	return "", ""
}

type k8sReplicaSet struct {
	Metadata objMeta `json:"metadata"`
	Spec     struct {
		Template podTemplate `json:"template"`
	} `json:"spec"`
}

type k8sPod struct {
	Metadata objMeta `json:"metadata"`
	Spec     struct {
		NodeName   string      `json:"nodeName"`
		Containers []container `json:"containers"`
	} `json:"spec"`
	Status struct {
		Phase             string `json:"phase"`
		Reason            string `json:"reason"`
		ContainerStatuses []struct {
			Name         string `json:"name"`
			Ready        bool   `json:"ready"`
			RestartCount int32  `json:"restartCount"`
			State        struct {
				Waiting *struct{ Reason string } `json:"waiting"`
			} `json:"state"`
		} `json:"containerStatuses"`
	} `json:"status"`
}

func (p k8sPod) ready() (bool, int32, string) {
	ready, restarts, waiting := len(p.Status.ContainerStatuses) > 0, int32(0), ""
	for _, c := range p.Status.ContainerStatuses {
		ready = ready && c.Ready
		restarts += c.RestartCount
		if c.State.Waiting != nil && waiting == "" {
			waiting = c.State.Waiting.Reason
		}
	}
	return ready, restarts, waiting
}

func (p k8sPod) owner() (string, string) {
	for _, o := range p.Metadata.OwnerReferences {
		if o.Controller {
			return o.Kind, o.Name
		}
	}
	return "", ""
}

type k8sNode struct {
	Metadata objMeta `json:"metadata"`
	Spec     struct {
		Unschedulable bool `json:"unschedulable"`
	} `json:"spec"`
	Status struct {
		Conditions []struct {
			Type               string    `json:"type"`
			Status             string    `json:"status"`
			Reason             string    `json:"reason"`
			LastTransitionTime time.Time `json:"lastTransitionTime"`
		} `json:"conditions"`
		NodeInfo struct {
			KubeletVersion string `json:"kubeletVersion"`
		} `json:"nodeInfo"`
	} `json:"status"`
}

func (n k8sNode) readyCondition() (string, time.Time) {
	for _, c := range n.Status.Conditions {
		if c.Type == "Ready" {
			return c.Status, c.LastTransitionTime
		}
	}
	return "Unknown", time.Time{}
}

type list[T any] struct {
	Items    []T `json:"items"`
	Metadata struct {
		Continue string `json:"continue"`
	} `json:"metadata"`
}

// listAll pages through a list endpoint up to max items.
func listAll[T any](ctx context.Context, k *kube, path string, q url.Values, max int) ([]T, error) {
	var out []T
	if q == nil {
		q = url.Values{}
	}
	q.Set("limit", "500")
	for {
		var l list[T]
		if err := k.c.Get(ctx, path, q, &l); err != nil {
			return nil, err
		}
		out = append(out, l.Items...)
		if l.Metadata.Continue == "" || len(out) >= max {
			break
		}
		q.Set("continue", l.Metadata.Continue)
	}
	if len(out) > max {
		out = out[:max]
	}
	return out, nil
}

func (k *kube) deployment(ctx context.Context, ns, name string) (k8sDeployment, error) {
	var d k8sDeployment
	err := k.c.Get(ctx, "/apis/apps/v1/namespaces/"+ns+"/deployments/"+name, nil, &d)
	return d, err
}

// ---- capability ----

type kubernetes struct {
	capability.Base
	cfg KubernetesConfig
	k   *kube
}

func newKubernetes(cfg KubernetesConfig) (*kubernetes, error) {
	k, err := newKube(cfg)
	if err != nil {
		return nil, err
	}
	if cfg.CollectEvery == 0 {
		cfg.CollectEvery = capability.Duration(30 * time.Second)
	}
	if cfg.MaxReplicas <= 0 {
		cfg.MaxReplicas = 50
	}
	if cfg.MaxScaleFactor <= 1 {
		cfg.MaxScaleFactor = 2
	}
	if cfg.MaxPods <= 0 {
		cfg.MaxPods = 2000
	}
	return &kubernetes{cfg: cfg, k: k}, nil
}

func (c *kubernetes) ID() string      { return "kubernetes" }
func (c *kubernetes) Version() string { return "1" }

func (c *kubernetes) RegisterWorld(r capability.WorldRegistry) error {
	if c.cfg.CollectEvery <= 0 {
		return nil
	}
	return r.AddCollector("topology", c.cfg.CollectEvery.D(), capability.CollectorFunc(c.collect))
}

func (c *kubernetes) RegisterEvents(r capability.EventRegistry) error {
	if err := r.AddSource("kubernetes", capability.NormalizerFunc(c.normalize)); err != nil {
		return err
	}
	// Warnings about a resource attach to the open Work on that resource;
	// they never open one on their own (an alert does).
	return r.AddRule(eventing.Rule{Name: "kubernetes-warnings", Group: "service-health", Match: eventing.Predicate{Sources: []string{"kubernetes"},
		EventTypes: []string{"k8s.warning.*"}}, Key: "subject", Action: "attach"})
}

func (c *kubernetes) RegisterTools(r capability.ToolRegistry) error {
	for _, t := range []tools.Tool{
		&tools.Func{Desc: tools.Descriptor{Name: "k8s.describe", Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
			TargetArg: "kind,namespace,name", Description: "Summarize a Deployment, Pod or Node (spec, status, conditions).",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"kind":{"enum":["deployment","pod","node"]},` +
				`"namespace":{"type":"string"},"name":{"type":"string"}},"required":["kind","name"],"additionalProperties":false}`)},
			Fn: c.describe},
		&tools.Func{Desc: tools.Descriptor{Name: "k8s.events", Version: "1", Risk: domain.RiskR0, Trust: "T2", Source: "native",
			TargetArg: "namespace,name", Description: "Recent Kubernetes events of an object (newest first, at most 50).",
			InputSchema: json.RawMessage(`{"type":"object","properties":{"namespace":{"type":"string"},"name":{"type":"string"}},` +
				`"required":["namespace","name"],"additionalProperties":false}`)},
			Fn: c.events},
	} {
		if err := r.AddTool(t); err != nil {
			return err
		}
	}
	return nil
}

func (c *kubernetes) RegisterActions(r capability.ActionRegistry) error {
	for _, e := range []action.Executor{&k8sRollback{c}, &k8sScale{c}, &nodeCordon{c}, &nodeDrain{c}} {
		if err := r.AddAction(e); err != nil {
			return err
		}
	}
	return nil
}

func (c *kubernetes) serviceOf(labels map[string]string, ns string) string {
	keys := []string{"app.kubernetes.io/name", "app"}
	if c.cfg.ServiceLabel != "" {
		keys = []string{c.cfg.ServiceLabel}
	}
	for _, k := range keys {
		if v := labels[k]; v != "" && dns1123.MatchString(v) {
			return "service://" + ns + "/" + v
		}
	}
	return ""
}

func fact(subj, pred, val string, trust string) world.Fact {
	return world.Fact{SubjectRef: subj, Predicate: pred, Value: cpkit.Truncate(val, 2000), TrustTier: trust}
}

// collect maps deployments, pods and nodes into facts and the topology
// service -> deployment -> pod -> node.
func (c *kubernetes) collect(ctx context.Context, now time.Time) (capability.Observation, error) {
	var o capability.Observation
	t2 := world.TrustFirstParty
	nodes, err := listAll[k8sNode](ctx, c.k, "/api/v1/nodes", nil, 5000)
	if err != nil {
		return o, fmt.Errorf("list nodes: %w", err)
	}
	for _, n := range nodes {
		ref := "node://" + n.Metadata.Name
		ready, since := n.readyCondition()
		o.Facts = append(o.Facts, fact(ref, "ready", strings.ToLower(ready), t2),
			fact(ref, "unschedulable", strconv.FormatBool(n.Spec.Unschedulable), t2),
			fact(ref, "kubelet_version", n.Status.NodeInfo.KubeletVersion, t2))
		if ready != "True" && !since.IsZero() {
			o.Events = append(o.Events, eventing.Envelope{ID: fmt.Sprintf("k8s:node-not-ready:%s:%d", n.Metadata.Name, since.Unix()),
				Source: "kubernetes", Type: "k8s.warning.node_not_ready", SubjectRef: ref,
				DedupKey: "k8s:node-not-ready:" + n.Metadata.Name, OccurredAt: since, ObservedAt: now, Confidence: 1,
				Labels: map[string]string{"node": n.Metadata.Name, "ready": ready}})
		}
	}
	for _, ns := range c.k.namespaces() {
		deps, err := listAll[k8sDeployment](ctx, c.k, "/apis/apps/v1/namespaces/"+ns+"/deployments", nil, 2000)
		if err != nil {
			return o, fmt.Errorf("list deployments in %s: %w", ns, err)
		}
		rsOwner := map[string]string{} // replicaset -> deployment
		rss, err := listAll[k8sReplicaSet](ctx, c.k, "/apis/apps/v1/namespaces/"+ns+"/replicasets", nil, 10000)
		if err != nil {
			return o, fmt.Errorf("list replicasets in %s: %w", ns, err)
		}
		for _, rs := range rss {
			for _, ow := range rs.Metadata.OwnerReferences {
				if ow.Controller && ow.Kind == "Deployment" {
					rsOwner[rs.Metadata.Name] = ow.Name
				}
			}
		}
		for _, d := range deps {
			ref := "deployment://" + ns + "/" + d.Metadata.Name
			_, img := d.image("")
			available := "false"
			for _, cnd := range d.Status.Conditions {
				if cnd.Type == "Available" && cnd.Status == "True" {
					available = "true"
				}
			}
			o.Facts = append(o.Facts, fact(ref, "image", img, t2),
				fact(ref, "generation", strconv.FormatInt(d.Metadata.Generation, 10), t2),
				fact(ref, "observed_generation", strconv.FormatInt(d.Status.ObservedGeneration, 10), t2),
				fact(ref, "replicas", strconv.Itoa(int(d.replicas())), t2),
				fact(ref, "ready_replicas", strconv.Itoa(int(d.Status.ReadyReplicas)), t2),
				fact(ref, "available", available, t2),
				fact(ref, "revision", d.Metadata.Annotations[revisionAnno], t2),
				fact(ref, "paused", strconv.FormatBool(d.Spec.Paused), t2))
			if svc := c.serviceOf(d.Spec.Template.Metadata.Labels, ns); svc != "" {
				o.Relations = append(o.Relations, world.Relation{FromRef: svc, Type: "deployed_as", ToRef: ref})
				o.Facts = append(o.Facts, fact(svc, "deployment", ref, t2))
			}
		}
		pods, err := listAll[k8sPod](ctx, c.k, "/api/v1/namespaces/"+ns+"/pods", nil, c.cfg.MaxPods)
		if err != nil {
			return o, fmt.Errorf("list pods in %s: %w", ns, err)
		}
		for _, p := range pods {
			ref := "pod://" + ns + "/" + p.Metadata.Name
			ready, restarts, waiting := p.ready()
			phase := p.Status.Phase
			if waiting != "" {
				phase += "/" + waiting
			}
			o.Facts = append(o.Facts, fact(ref, "phase", phase, t2), fact(ref, "ready", strconv.FormatBool(ready), t2),
				fact(ref, "restarts", strconv.Itoa(int(restarts)), t2))
			if len(p.Spec.Containers) > 0 {
				o.Facts = append(o.Facts, fact(ref, "image", p.Spec.Containers[0].Image, t2))
			}
			if kind, name := p.owner(); kind == "ReplicaSet" && rsOwner[name] != "" {
				o.Relations = append(o.Relations, world.Relation{FromRef: "deployment://" + ns + "/" + rsOwner[name],
					Type: "owns", ToRef: ref})
			}
			if p.Spec.NodeName != "" {
				o.Relations = append(o.Relations, world.Relation{FromRef: ref, Type: "runs_on", ToRef: "node://" + p.Spec.NodeName})
			}
		}
	}
	return o, nil
}

// ---- events webhook ----

type k8sEvent struct {
	Metadata       objMeta `json:"metadata"`
	InvolvedObject struct {
		Kind, Namespace, Name, UID string
	} `json:"involvedObject"`
	Reason         string    `json:"reason"`
	Message        string    `json:"message"`
	Type           string    `json:"type"`
	Count          int       `json:"count"`
	FirstTimestamp time.Time `json:"firstTimestamp"`
	LastTimestamp  time.Time `json:"lastTimestamp"`
	EventTime      time.Time `json:"eventTime"`
}

func objectRef(kind, ns, name string) string {
	switch kind {
	case "Deployment":
		return "deployment://" + ns + "/" + name
	case "Pod":
		return "pod://" + ns + "/" + name
	case "Node":
		return "node://" + name
	case "ReplicaSet":
		return "replicaset://" + ns + "/" + name
	case "StatefulSet":
		return "statefulset://" + ns + "/" + name
	}
	return ""
}

// normalize accepts a core/v1 Event or EventList (event exporters). Events
// of namespaces outside the allowlist are dropped.
func (c *kubernetes) normalize(_ context.Context, tenantID string, _ http.Header, body []byte, now time.Time) ([]eventing.Envelope, error) {
	var evs []k8sEvent
	var probe struct {
		Kind  string     `json:"kind"`
		Items []k8sEvent `json:"items"`
	}
	if err := json.Unmarshal(body, &probe); err != nil {
		return nil, fmt.Errorf("kubernetes: payload: %w", err)
	}
	if probe.Kind == "EventList" || probe.Items != nil {
		evs = probe.Items
	} else {
		var e k8sEvent
		if err := json.Unmarshal(body, &e); err != nil {
			return nil, fmt.Errorf("kubernetes: event: %w", err)
		}
		evs = []k8sEvent{e}
	}
	var out []eventing.Envelope
	for _, e := range evs {
		io := e.InvolvedObject
		if io.Kind != "Node" && !c.k.ns[io.Namespace] {
			continue
		}
		subj := objectRef(io.Kind, io.Namespace, io.Name)
		if subj == "" || e.Reason == "" {
			continue
		}
		at := e.LastTimestamp
		if at.IsZero() {
			at = e.EventTime
		}
		if at.IsZero() || at.After(now.Add(5*time.Minute)) {
			at = now
		}
		sev := "normal"
		if e.Type == "Warning" {
			sev = "warning"
		}
		reason := strings.ToLower(predicateSafe(e.Reason))
		payload, _ := json.Marshal(map[string]any{"message": cpkit.Truncate(e.Message, 1000), "count": e.Count})
		out = append(out, eventing.Envelope{ID: fmt.Sprintf("k8s:%s:%d", e.Metadata.UID, max(e.Count, 1)),
			TenantID: tenantID, Source: "kubernetes", Type: "k8s." + sev + "." + reason, SubjectRef: subj,
			DedupKey: "k8s:" + subj + ":" + reason, OccurredAt: at, ObservedAt: now, Confidence: 1,
			Labels: map[string]string{"reason": cpkit.Truncate(e.Reason, 128), "kind": io.Kind}, Payload: payload})
	}
	return out, nil
}

// ---- read tools ----

func (c *kubernetes) describe(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Kind, Namespace, Name string }
	if err := json.Unmarshal(args, &a); err != nil {
		return nil, fmt.Errorf("%w: %v", domain.ErrNotDispatched, err)
	}
	if !dns1123.MatchString(a.Name) {
		return nil, fmt.Errorf("%w: invalid name", domain.ErrNotDispatched)
	}
	if a.Kind != "node" && !c.k.ns[a.Namespace] {
		return nil, fmt.Errorf("%w: namespace %q is not in the allowlist", domain.ErrNotDispatched, a.Namespace)
	}
	switch a.Kind {
	case "deployment":
		d, err := c.k.deployment(ctx, a.Namespace, a.Name)
		if err != nil {
			return nil, err
		}
		_, img := d.image("")
		return json.Marshal(map[string]any{"name": d.Metadata.Name, "namespace": d.Metadata.Namespace,
			"generation": d.Metadata.Generation, "observed_generation": d.Status.ObservedGeneration, "image": img,
			"replicas": d.replicas(), "ready": d.Status.ReadyReplicas, "available": d.Status.AvailableReplicas,
			"updated": d.Status.UpdatedReplicas, "paused": d.Spec.Paused, "revision": d.Metadata.Annotations[revisionAnno],
			"conditions": d.Status.Conditions})
	case "pod":
		var p k8sPod
		if err := c.k.c.Get(ctx, "/api/v1/namespaces/"+a.Namespace+"/pods/"+a.Name, nil, &p); err != nil {
			return nil, err
		}
		ready, restarts, waiting := p.ready()
		return json.Marshal(map[string]any{"name": p.Metadata.Name, "phase": p.Status.Phase, "reason": p.Status.Reason,
			"ready": ready, "restarts": restarts, "waiting": waiting, "node": p.Spec.NodeName, "containers": p.Spec.Containers})
	case "node":
		var n k8sNode
		if err := c.k.c.Get(ctx, "/api/v1/nodes/"+a.Name, nil, &n); err != nil {
			return nil, err
		}
		return json.Marshal(map[string]any{"name": n.Metadata.Name, "unschedulable": n.Spec.Unschedulable,
			"conditions": n.Status.Conditions, "kubelet": n.Status.NodeInfo.KubeletVersion})
	}
	return nil, fmt.Errorf("%w: kind must be deployment, pod or node", domain.ErrNotDispatched)
}

func (c *kubernetes) events(ctx context.Context, _ string, args json.RawMessage) (json.RawMessage, error) {
	var a struct{ Namespace, Name string }
	if err := json.Unmarshal(args, &a); err != nil || !c.k.ns[a.Namespace] || !dns1123.MatchString(a.Name) {
		return nil, fmt.Errorf("%w: an allowlisted namespace and a valid name are required", domain.ErrNotDispatched)
	}
	evs, err := listAll[k8sEvent](ctx, c.k, "/api/v1/namespaces/"+a.Namespace+"/events",
		url.Values{"fieldSelector": {"involvedObject.name=" + a.Name}}, 500)
	if err != nil {
		return nil, err
	}
	sort.Slice(evs, func(i, j int) bool { return evs[i].LastTimestamp.After(evs[j].LastTimestamp) })
	type item struct {
		Type, Reason, Message string
		Count                 int
		Last                  time.Time
	}
	out := []item{}
	for i, e := range evs {
		if i >= 50 {
			break
		}
		out = append(out, item{e.Type, e.Reason, cpkit.Truncate(e.Message, 300), e.Count, e.LastTimestamp})
	}
	return json.Marshal(map[string]any{"events": out})
}

// ---- actions ----

func parseArgs(p action.Plan, v any) error {
	if len(p.Args) == 0 {
		return nil
	}
	if err := json.Unmarshal(p.Args, v); err != nil {
		return capability.NotApplied("args: %v", err)
	}
	return nil
}

func refused(reasons ...string) action.PreflightResult {
	return action.PreflightResult{Allowed: false, Reasons: reasons}
}

// pinned checks the deployment generation against the plan.
func pinned(p action.Plan, d k8sDeployment) error {
	if p.TargetVersion != "" && p.TargetVersion != strconv.FormatInt(d.Metadata.Generation, 10) {
		return capability.NotApplied("deployment changed: generation %d, plan pinned %s", d.Metadata.Generation, p.TargetVersion)
	}
	return nil
}

func annotated(m objMeta, key string) bool { return key != "" && m.Annotations[effectKeyAnno] == key }

type k8sRollback struct{ c *kubernetes }

func (r *k8sRollback) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "deployment.rollback", Version: "1", ResourceKinds: []string{"deployment"}, Risk: domain.RiskR2,
		Description:     "Roll a Deployment back to the image of its previous revision (or to_image).",
		InputSchema:     json.RawMessage(`{"type":"object","properties":{"to_image":{"type":"string"},"container":{"type":"string"}},"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsDryRun: true, SupportsRollback: true,
		RequiredFacts: []string{"image", "generation"}, VersionFact: "generation", BlastRadius: "single_deployment",
		Timeout: time.Minute}
}

type rollbackArgs struct {
	ToImage   string `json:"to_image"`
	Container string `json:"container"`
}

// previousImage finds the image of the newest older revision.
func (r *k8sRollback) previousImage(ctx context.Context, ns string, d k8sDeployment, container string) (string, error) {
	cur, _ := strconv.Atoi(d.Metadata.Annotations[revisionAnno])
	rss, err := listAll[k8sReplicaSet](ctx, r.c.k, "/apis/apps/v1/namespaces/"+ns+"/replicasets", nil, 10000)
	if err != nil {
		return "", err
	}
	best, bestRev := "", 0
	for _, rs := range rss {
		owned := false
		for _, o := range rs.Metadata.OwnerReferences {
			owned = owned || (o.Controller && o.Kind == "Deployment" && o.Name == d.Metadata.Name)
		}
		rev, _ := strconv.Atoi(rs.Metadata.Annotations[revisionAnno])
		if !owned || rev >= cur || rev <= bestRev {
			continue
		}
		for _, c := range rs.Spec.Template.Spec.Containers {
			if container == "" || c.Name == container {
				best, bestRev = c.Image, rev
				break
			}
		}
	}
	if best == "" {
		return "", fmt.Errorf("no previous revision of %s/%s", ns, d.Metadata.Name)
	}
	return best, nil
}

func (r *k8sRollback) resolve(ctx context.Context, p action.Plan) (string, string, k8sDeployment, string, string, error) {
	ns, name, err := r.c.k.nsName(p.TargetRef, "deployment")
	if err != nil {
		return "", "", k8sDeployment{}, "", "", capability.NotApplied("%v", err)
	}
	var a rollbackArgs
	if err := parseArgs(p, &a); err != nil {
		return "", "", k8sDeployment{}, "", "", err
	}
	d, err := r.c.k.deployment(ctx, ns, name)
	if err != nil {
		return "", "", k8sDeployment{}, "", "", err
	}
	cname, cur := d.image(a.Container)
	if cname == "" {
		return "", "", d, "", "", capability.NotApplied("container %q not found", a.Container)
	}
	to := a.ToImage
	if to == "" {
		if to, err = r.previousImage(ctx, ns, d, cname); err != nil {
			return "", "", d, "", "", capability.NotApplied("%v", err)
		}
	}
	if to == cur {
		return ns, name, d, cname, to, capability.NotApplied("deployment already runs %s", to)
	}
	return ns, name, d, cname, to, nil
}

func (r *k8sRollback) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	_, _, d, _, to, err := r.resolve(ctx, p)
	if err != nil {
		return refused(err.Error()), nil
	}
	if d.Spec.Paused {
		return refused("deployment rollout is paused"), nil
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{p.TargetRef + "#revision=" +
		d.Metadata.Annotations[revisionAnno] + "&to=" + to}}, nil
}

func (r *k8sRollback) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	ns, name, d, cname, to, err := r.resolve(ctx, p)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	if err := pinned(p, d); err != nil {
		return nil, err
	}
	_, from := d.image(cname)
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]string{effectKeyAnno: key}},
		"spec": map[string]any{"template": map[string]any{"spec": map[string]any{"containers": []map[string]string{{"name": cname, "image": to}}}}}}
	var out k8sDeployment
	if err := r.c.k.patch(ctx, "/apis/apps/v1/namespaces/"+ns+"/deployments/"+name,
		"application/strategic-merge-patch+json", patch, &out); err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"from_image": from, "to_image": to, "container": cname,
		"generation": out.Metadata.Generation, "dry_run": r.c.k.dryRun})
}

func (r *k8sRollback) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	ns, name, err := r.c.k.nsName(p.TargetRef, "deployment")
	if err != nil {
		return action.InspectResult{}, err
	}
	d, err := r.c.k.deployment(ctx, ns, name)
	if err != nil {
		return action.InspectResult{}, err
	}
	applied := annotated(d.Metadata, p.EffectKey)
	det, _ := json.Marshal(map[string]any{"generation": d.Metadata.Generation, "effect_key_present": applied})
	return action.InspectResult{Applied: &applied, Detail: det}, nil
}

type k8sScale struct{ c *kubernetes }

func (s *k8sScale) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "deployment.scale", Version: "1", ResourceKinds: []string{"deployment"}, Risk: domain.RiskR1,
		Description:     "Set a Deployment's replicas (bounded: never to 0, at most max_scale_factor per step).",
		InputSchema:     json.RawMessage(`{"type":"object","properties":{"replicas":{"type":"integer","minimum":1}},"required":["replicas"],"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsDryRun: true, SupportsRollback: true, Compensation: "deployment.scale",
		RequiredFacts: []string{"replicas"}, VersionFact: "generation", BlastRadius: "single_deployment", Timeout: 30 * time.Second}
}

func (s *k8sScale) check(ctx context.Context, p action.Plan) (string, string, k8sDeployment, int, error) {
	ns, name, err := s.c.k.nsName(p.TargetRef, "deployment")
	if err != nil {
		return "", "", k8sDeployment{}, 0, capability.NotApplied("%v", err)
	}
	var a struct {
		Replicas *int `json:"replicas"`
	}
	if err := parseArgs(p, &a); err != nil {
		return "", "", k8sDeployment{}, 0, err
	}
	if a.Replicas == nil || *a.Replicas < 1 || *a.Replicas > s.c.cfg.MaxReplicas {
		return "", "", k8sDeployment{}, 0, capability.NotApplied("replicas must be within [1, %d]", s.c.cfg.MaxReplicas)
	}
	d, err := s.c.k.deployment(ctx, ns, name)
	if err != nil {
		return "", "", d, 0, err
	}
	cur, want := float64(max(d.replicas(), 1)), float64(*a.Replicas)
	if want > cur*s.c.cfg.MaxScaleFactor || want < cur/s.c.cfg.MaxScaleFactor {
		return "", "", d, 0, capability.NotApplied("scaling %d -> %d exceeds the factor %.1f per step", int(cur), *a.Replicas, s.c.cfg.MaxScaleFactor)
	}
	return ns, name, d, *a.Replicas, nil
}

func (s *k8sScale) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	if _, _, _, _, err := s.check(ctx, p); err != nil {
		return refused(err.Error()), nil
	}
	return action.PreflightResult{Allowed: true}, nil
}

func (s *k8sScale) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	ns, name, d, n, err := s.check(ctx, p)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	if err := pinned(p, d); err != nil {
		return nil, err
	}
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]string{effectKeyAnno: key}},
		"spec": map[string]any{"replicas": n}}
	if err := s.c.k.patch(ctx, "/apis/apps/v1/namespaces/"+ns+"/deployments/"+name, "application/merge-patch+json", patch, nil); err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"from": d.replicas(), "to": n, "dry_run": s.c.k.dryRun})
}

func (s *k8sScale) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	ns, name, err := s.c.k.nsName(p.TargetRef, "deployment")
	if err != nil {
		return action.InspectResult{}, err
	}
	d, err := s.c.k.deployment(ctx, ns, name)
	if err != nil {
		return action.InspectResult{}, err
	}
	applied := annotated(d.Metadata, p.EffectKey)
	return action.InspectResult{Applied: &applied}, nil
}

func nodeName(ref string) (string, error) {
	name, ok := strings.CutPrefix(ref, "node://")
	if !ok || !dns1123.MatchString(name) {
		return "", fmt.Errorf("target %q must be node://<name>", ref)
	}
	return name, nil
}

type nodeCordon struct{ c *kubernetes }

func (n *nodeCordon) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "node.cordon", Version: "1", ResourceKinds: []string{"node"}, Risk: domain.RiskR2,
		Description:     "Mark a node unschedulable (new pods avoid it; running pods stay).",
		InputSchema:     json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsDryRun: true, SupportsRollback: true,
		RequiredFacts: []string{"ready"}, BlastRadius: "single_node", Timeout: 30 * time.Second}
}

func (n *nodeCordon) node(ctx context.Context, ref string) (string, k8sNode, error) {
	name, err := nodeName(ref)
	if err != nil {
		return "", k8sNode{}, capability.NotApplied("%v", err)
	}
	var node k8sNode
	err = n.c.k.c.Get(ctx, "/api/v1/nodes/"+name, nil, &node)
	return name, node, err
}

func (n *nodeCordon) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	_, node, err := n.node(ctx, p.TargetRef)
	if err != nil {
		return refused(err.Error()), nil
	}
	if node.Spec.Unschedulable {
		return refused("node is already cordoned"), nil
	}
	return action.PreflightResult{Allowed: true}, nil
}

func (n *nodeCordon) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	name, _, err := n.node(ctx, p.TargetRef)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	patch := map[string]any{"metadata": map[string]any{"annotations": map[string]string{effectKeyAnno: key}},
		"spec": map[string]any{"unschedulable": true}}
	if err := n.c.k.patch(ctx, "/api/v1/nodes/"+name, "application/merge-patch+json", patch, nil); err != nil {
		return nil, capability.ActionError(err)
	}
	return json.Marshal(map[string]any{"node": name, "unschedulable": true, "dry_run": n.c.k.dryRun})
}

func (n *nodeCordon) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	_, node, err := n.node(ctx, p.TargetRef)
	if err != nil {
		return action.InspectResult{}, err
	}
	applied := node.Spec.Unschedulable && annotated(node.Metadata, p.EffectKey)
	return action.InspectResult{Applied: &applied}, nil
}

type nodeDrain struct{ c *kubernetes }

func (n *nodeDrain) Descriptor() action.Descriptor {
	return action.Descriptor{Name: "node.drain", Version: "1", ResourceKinds: []string{"node"}, Risk: domain.RiskR2,
		Description:     "Evict the pods of a cordoned node through the Eviction API (PodDisruptionBudgets apply; DaemonSet and mirror pods stay).",
		InputSchema:     json.RawMessage(`{"type":"object","properties":{},"additionalProperties":false}`),
		IdempotencyMode: action.IdemQueryBeforeRetry, SupportsDryRun: true,
		RequiredFacts: []string{"unschedulable"}, BlastRadius: "single_node", Timeout: 5 * time.Minute}
}

func (n *nodeDrain) evictable(ctx context.Context, node string) ([]k8sPod, error) {
	pods, err := listAll[k8sPod](ctx, n.c.k, "/api/v1/pods", url.Values{"fieldSelector": {"spec.nodeName=" + node}}, 1000)
	if err != nil {
		return nil, err
	}
	var out []k8sPod
	for _, p := range pods {
		kind, _ := p.owner()
		if kind == "DaemonSet" || p.Metadata.Annotations["kubernetes.io/config.mirror"] != "" ||
			p.Status.Phase == "Succeeded" || p.Status.Phase == "Failed" {
			continue
		}
		out = append(out, p)
	}
	return out, nil
}

func (n *nodeDrain) Preflight(ctx context.Context, p action.Plan) (action.PreflightResult, error) {
	_, node, err := (&nodeCordon{n.c}).node(ctx, p.TargetRef)
	if err != nil {
		return refused(err.Error()), nil
	}
	if !node.Spec.Unschedulable {
		return refused("cordon the node first (node.cordon)"), nil
	}
	pods, err := n.evictable(ctx, node.Metadata.Name)
	if err != nil {
		return refused(err.Error()), nil
	}
	for _, pod := range pods {
		if !n.c.k.ns[pod.Metadata.Namespace] {
			return refused(fmt.Sprintf("pod %s/%s is in a namespace outside the allowlist", pod.Metadata.Namespace, pod.Metadata.Name)), nil
		}
	}
	return action.PreflightResult{Allowed: true, EvidenceRefs: []string{fmt.Sprintf("%s#pods=%d", p.TargetRef, len(pods))}}, nil
}

// Execute evicts every evictable pod. A partial drain (PDB refusal or an
// error midway) is returned as an error that is not ErrNotApplied: some
// pods moved, so the result is unknown and goes to a human.
func (n *nodeDrain) Execute(ctx context.Context, p action.Plan, key string) (json.RawMessage, error) {
	name, node, err := (&nodeCordon{n.c}).node(ctx, p.TargetRef)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	if !node.Spec.Unschedulable {
		return nil, capability.NotApplied("node %s is not cordoned", name)
	}
	pods, err := n.evictable(ctx, name)
	if err != nil {
		return nil, capability.BeforeWrite(err)
	}
	evicted, blocked := 0, []string{}
	for _, pod := range pods {
		if !n.c.k.ns[pod.Metadata.Namespace] {
			return nil, capability.NotApplied("pod %s/%s is outside the namespace allowlist", pod.Metadata.Namespace, pod.Metadata.Name)
		}
	}
	for _, pod := range pods {
		body := map[string]any{"apiVersion": "policy/v1", "kind": "Eviction",
			"metadata": map[string]string{"name": pod.Metadata.Name, "namespace": pod.Metadata.Namespace}}
		if n.c.k.dryRun {
			body["deleteOptions"] = map[string]any{"dryRun": []string{"All"}}
		}
		err := n.c.k.c.Do(ctx, capability.Request{Method: http.MethodPost,
			Path: "/api/v1/namespaces/" + pod.Metadata.Namespace + "/pods/" + pod.Metadata.Name + "/eviction", Body: body}, nil)
		switch {
		case err == nil, capability.IsStatus(err, http.StatusNotFound):
			evicted++
		case capability.IsStatus(err, http.StatusTooManyRequests):
			blocked = append(blocked, pod.Metadata.Namespace+"/"+pod.Metadata.Name)
		default:
			if evicted == 0 {
				return nil, capability.ActionError(err)
			}
			return nil, fmt.Errorf("drain stopped after %d evictions: %w", evicted, err)
		}
	}
	if len(blocked) > 0 {
		if evicted == 0 {
			return nil, capability.NotApplied("every eviction was refused by a PodDisruptionBudget: %s", strings.Join(blocked, ", "))
		}
		return nil, fmt.Errorf("partial drain: %d evicted, %d blocked by PodDisruptionBudgets (%s)", evicted, len(blocked),
			strings.Join(blocked, ", "))
	}
	return json.Marshal(map[string]any{"node": name, "evicted": evicted, "dry_run": n.c.k.dryRun})
}

func (n *nodeDrain) Inspect(ctx context.Context, p action.Plan) (action.InspectResult, error) {
	name, err := nodeName(p.TargetRef)
	if err != nil {
		return action.InspectResult{}, err
	}
	pods, err := n.evictable(ctx, name)
	if err != nil {
		return action.InspectResult{}, err
	}
	applied := len(pods) == 0
	det, _ := json.Marshal(map[string]any{"remaining_pods": len(pods)})
	return action.InspectResult{Applied: &applied, Detail: det}, nil
}
