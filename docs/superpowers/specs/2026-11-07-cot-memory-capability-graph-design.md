# CoT trace, sharded memory and capability graph

Status: approved
Date: 2026-11-07

## Context

The harness records a decision artifact per reason step and pins retrieved
memories to steps, but three capabilities remain disconnected:

- The decision trace is a flat `reason_summary` plus hypotheses. There is no
  explicit plan, observation, reflection or verification chain to render.
- Long-term memory is filtered by `scope/scope_id`, but there is no reusable
  partition key and no way to ask which memories belong to a capability.
- Tools, skills, plugins and MCP servers carry descriptors, yet the runtime
  does not record which capability a step used or how capabilities relate to
  services and to each other.

This document defines a typed provenance model and a read-only graph
projection that connect the decision trace, memory and capabilities without
changing the facts owned by runs, steps and memory rows.

## Goals

- Persist an explicit, auditable chain per reason/tool step: plan, decision,
  hypotheses, observation, verification, reflection and conclusion.
- Partition memory by composable shard keys and use shards in retrieval.
- Record capability usage and declared relationships (ownership, service,
  alternatives, composition, ordering).
- Expose the relationships in the desktop trace, a graph page, and the
  plugin/customize surfaces.
- Keep PostgreSQL as the source of truth, with tenant isolation and RLS on
  every new table.

## Non-goals

- Storing hidden chain-of-thought or raw provider reasoning tokens.
- Introducing an external graph database.
- Replacing full-text/vector retrieval with graph traversal.
- Changing the step engine's lease/fence semantics.
- Making graph writes asynchronous at the expense of step consistency.

## Architecture

Typed provenance tables hold hot-path facts. A `graph_edges` view unions those
tables into a uniform edge set for the UI and bounded multi-hop queries. The
view is derived, never authoritative.

```
agent_steps ──< run_thoughts
     │               │
     ├──< step_capability_refs ──> capability_nodes
     ├──< step_memory_hits ──────> agent_memories ──< memory_shards
     └──< runs ──────────────────> incidents / services / runs
capability_relations: capability <-> capability, service, mcp server
graph_edges (VIEW): all of the above as (src, relation, dst)
```

## Stable references

- `tool:<name>@<version>`
- `skill:<tenant>/<name>@<hash>`
- `mcp-server:<tenant>/<name>@<version>`
- `plugin:<kind>/<name>`
- `service:<id>`
- `incident:<id>`
- `run:<id>`
- `step:<id>`
- `memory:<id>@<version>`

References are plain strings. Domain tables remain the source of labels,
descriptions, risk and trust. No graph row owns a copy of mutable metadata.

## Data model

All tables carry `tenant_id`, are covered by RLS, and are indexed by tenant.

### capability_nodes

Projection of the registries for graph lookup.

- `tenant_id`, `ref` (PK)
- `kind`: `tool | skill | mcp_server | plugin`
- `name`, `version`, `owner`, `source`
- `risk`, `trust`, `service_id`
- `labels JSONB`
- `synced_at`

The sync is an idempotent upsert from the tool registry, extension runtime and
plugin inventory. It runs at startup and after extension changes.

### capability_relations

- `tenant_id`, `src_ref`, `dst_ref`, `relation`
- `relation`: `provides | belongs_to | depends_on | alternative_to | composes | precedes | related_to`
- `source`: `declared | derived`
- `weight`, `attrs JSONB`, `created_at`
- PK `(tenant_id, src_ref, dst_ref, relation, source)`

Declared edges are written by admins. Derived edges are produced by the sync
(tool → MCP server, skill → tenant, capability → service). Runtime usage is
not written here; it comes from `step_capability_refs`.

### run_thoughts

- `id` UUID, `tenant_id`, `run_id`, `step_id`, `seq`
- `kind`: `plan | observation | hypothesis | evidence | decision | action | verification | reflection | conclusion`
- `summary`, `detail_ref`
- `status`: `UNVERIFIED | VERIFIED | REFUTED | FAILED | OK | UNKNOWN`
- `confidence` (an estimate, never a calibrated probability)
- `structured` boolean
- `attrs JSONB`, `created_at`

One reason step may emit several thoughts. `seq` orders them within a step;
the UI orders steps by `agent_steps.seq`.

### thought_refs

- `tenant_id`, `thought_id`, `ref`, `relation`
- `relation`: `based_on | supports | refutes | uses | produced | cites`
- PK `(tenant_id, thought_id, ref, relation)`

### step_capability_refs

- `tenant_id`, `run_id`, `step_id`, `capability_ref`
- `role`: `used | considered`
- `created_at`
- PK `(tenant_id, step_id, capability_ref, role)`

Tool steps record `used` with the descriptor snapshot at execution time.
Reason steps record `considered` for injected skills and the chosen tool.

### memory_shards

- `tenant_id`, `memory_id`, `shard_key`
- `weight` (scope 1.0, declared 1.0, incident 1.0, capability 0.8, run 0.4)
- `source`: `scope | capability | incident | run | declared`
- PK `(tenant_id, memory_id, shard_key)`

Shard key format: `org:<tenant>`, `service:<id>`, `capability:<ref>`,
`incident:<id>`, `agent:<id>`, `run:<id>`.

### graph_edges view

A read-only union of:

- `step_capability_refs`: step `uses` capability
- `run_thoughts`/`thought_refs`: step `produces` thought, thought `cites` refs
- `step_memory_hits`: step `cites` memory
- `memory_shards`: memory `belongs_to` shard
- `capability_relations`: declared/derived capability edges
- `service_dependencies`: service `depends_on` service
- `agent_steps`: step `belongs_to` run
- `agent_runs`: run `belongs_to` incident when present

### graph_nodes view

A read-only union of display metadata for every node kind:

- `capability_nodes`: capability, skill, MCP server, plugin
- `agent_memories`: memory title, status, trust
- distinct `service_id` values from incidents, runs and memories
- `incidents`: incident labels
- `agent_runs`, `agent_steps`: run and step labels
- `run_thoughts`: thought kind and summary

The view exposes `(tenant_id, ref, kind, label, attrs, updated_at)`. Labels
are resolved at read time; the graph never copies mutable metadata.

The views are queried with depth ≤ 3 and hard caps (200 nodes, 500 edges).

## Runtime: explicit decision trace

The harness keeps native tool calling. It does not store hidden reasoning.

### Prompt contract

Profile `sre-diagnosis@4` pins `prompt_prefix_version=sre-prefix-v5`.

Before a tool call the model emits one line:

```
THOUGHT {"plan":"...","expected":"...","confidence":0.6}
```

followed by the native tool call. The parser accepts the last such line,
strips it from displayed text, and never fails a step when the line is
missing or malformed. The fallback stores the first two sentences of the
visible text as a plan with `structured=false`.

`submit_conclusion` gains `plan`, `reflection` and
`verification{expected,observed,verdict}`. `answer`, `reason_summary` and
`hypotheses` remain required/compatible.

### Producing thoughts

`executeReason` produces:

- `plan` from the parsed THOUGHT line or the fallback summary
- `decision` with the chosen tool or conclusion and the reason summary
- `action` for the chosen tool, referencing the tool call and capability ref
- one `hypothesis` per decision hypothesis
- on final: `verification`, `reflection` and `conclusion` when supplied

`executeTool` produces:

- `observation` on success or failure, referencing the observation artifact
  and the capability ref

`StepResult` carries `Thoughts`, `ThoughtRefs` and `Capabilities`.
`CommitSuccess` writes them in the same transaction as the step success, so a
step cannot succeed without its trace. The `step.succeeded` event carries
only counts and refs; the full trace is read through the steps API.

### Capability refs

`tools.CapabilityRef(descriptor)` derives `tool:<name>@<version>`.
`Descriptor` gains an optional `Plugin` field filled by the plugin host.

The reason step records considered skills from the prompt manifest and the
chosen tool. The tool step records the executed descriptor. MCP ownership is
derived from `Source=mcp:<server>`.

### Prompt capability map

Profiles may enable `capability_map`. When enabled, section `[E]` renders at
most five capabilities and two relationships each, capped at 1200 bytes:

```
[capability map]
tool:mcp.grafana.query provided by mcp-server:grafana; service: checkout;
recent verified lesson: pool exhaustion during peak.
```

The map is built from graph neighbors of the active service/incident and
only includes memories allowed by sensitivity and trust. The section hash and
the map ref join the prompt manifest.

## Memory sharding

`scope/scope_id` stays. Shards are additive retrieval indexes and graph edges.

Backfill:

- `service` scope → `service:<scope_id>`
- `incident` scope → `incident:<scope_id>`
- `agent` scope → `agent:<scope_id>`
- `organization` → `org:<tenant>`
- run refs in `source_refs` → `run:<id>` plus that run's used capabilities

The consolidator attaches shards when it creates a memory:

- service from goal/incident
- incident from the run
- capabilities from the run's `step_capability_refs` (`used`)
- strong tool-specific lessons get a capability shard with higher weight

Memory creation and shard inserts share one transaction. Existing redaction,
poison checks, lifecycle and dedup are unchanged. A capability shard never
promotes a memory to `ACTIVE`.

### Retrieval

`activeShards` is derived from the goal service, incident, profile/agent, the
visible capabilities with a service edge, and recently used capabilities for
the same service/incident. It is capped at 20.

The SQL hard filters remain first (tenant, status, sensitivity, expiry, scope
predicate). The join with `memory_shards` only affects ranking:

```
score = RRF * (1 + shard_weight * affinity)
```

`affinity` is capped so shard proximity cannot override text/vector
relevance. Organization memories without shards remain candidates. Service
dependency expansion stays; capability expansion is one hop only.

Pinned memory lines gain a short `shard=capability:...` tag.

## Desktop

### Trace

Reason steps show a compact chain: plan, decision, hypotheses, observation,
verification, reflection, conclusion. Each item carries capability chips
(tool/skill/MCP), memory chips, hypothesis status and the confidence estimate.
Tool steps link their observation to the used capability. The UI states that
this is an explicit harness trace, not hidden reasoning. `THOUGHT` control
lines never reach the rendered answer.

### Graph page

A new top-level "Graph" entry provides:

- search by name, ref, service, tool or memory text
- filters for node kind, relation and time
- a 1–3 hop SVG neighborhood laid out in rings, without a physics engine
- a detail panel with metadata, grouped neighbors, recent runs and memories
- jumps to Customize for capabilities and to the chat for runs

Nodes are keyboard-focusable buttons; edges are static SVG lines. Graphs are
truncated at 200 nodes with an explicit notice.

### Plugin, Customize and Settings

Capability cards show owner, service, last use, related memory count and a
"view in graph" action. Settings gains a Knowledge group for the capability
map flag, thought retention and a projection rebuild action.

## API

- `GET /v1/runs/{id}/steps` returns thoughts, capability refs and memory refs.
- `GET /v1/graph/search?q=&kinds=&limit=` returns matching nodes.
- `GET /v1/graph/{ref}?depth=2&relations=` returns `{nodes, edges, truncated}`.
- `GET /v1/capabilities?service=&used_since=` returns capability stats.
- All endpoints are tenant-scoped. Labels are rendered as text.

## Rollout

1. Graph foundation: schema, sync, backfill, graph API, graph page. No change
   to prompts or runs.
2. CoT capture: prompt v5, profile v4, thought parsing, commit writes, trace.
3. Memory shards: consolidator shards, shard-aware retrieval, memory tags.
4. Declared metadata and prompt map: admin relations, Knowledge settings,
   capability map behind a profile flag.

Each phase is additive. Profiles v1–v3 and `sre-prefix-v4` remain replayable.
Old runs have no thoughts and fall back to `reason_summary`.

## Compatibility and performance

- Migrations are additive; no column rewrites. Backfill is idempotent and
  batched per tenant.
- A step commit adds at most 10 thoughts and 10 capability refs, inserted in
  batches inside the existing transaction.
- Graph queries are depth-limited and node-capped; normal queries target
  <100 ms.
- `activeShards` is capped at 20; memory candidates stay capped at 500.
- SSE gains no required events; new fields are optional to old clients.

## Security

- Every new table carries `tenant_id` and RLS.
- Thought summaries pass through `memory.Redact` before storage.
- Capability shards store refs only, never tool output.
- Graph and shard metadata never bypass sensitivity, trust, expiry or
  approval.
- No hidden reasoning is stored or shown.

## Testing

- Unit: ref parsing, THOUGHT parsing (valid/missing/malformed), shard keys,
  ranking boost caps, graph traversal truncation.
- Integration (local PostgreSQL): migrations and RLS, transactional writes,
  run thoughts, memory shards, retrieval, idempotent backfill, old-run
  compatibility.
- Frontend: old/new run traces, graph page, keyboard, dark mode, no-glass,
  large graph truncation.
- Evaluation: existing datasets before/after; answer quality, tokens and
  latency; capability map A/B.

## Acceptance

1. A new run shows plan → action → observation → reflection → conclusion,
   linked to capabilities and memories.
2. The graph can answer which capabilities a service used, which memories
   relate to them and which runs used them.
3. A second run on the same service recalls capability-sharded lessons.
4. Old runs, old profiles and existing evaluations show no regression, and
   latency/token growth stays within budget.
