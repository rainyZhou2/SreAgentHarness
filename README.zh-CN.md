# go-sre-harness

[English](README.md) | **简体中文**

依据设计 RFC [`GoAgentHarness.zh-CN.md`](GoAgentHarness.zh-CN.md)（v1.1.3，[English](GoAgentHarness.md)）实现的 SRE Cognitive Agent Harness。覆盖 RFC 的 M1–M7 以及 v1.1 新增章节（对话/分支、模型网关、成本、Checkpoint、Compaction、Profile 灰度）。GPU 推理集群（vLLM/SGLang 路由、KV offload）未实现。

## 能力一览

| 领域 | 能力 | 位置 |
|---|---|---|
| **Durable Core** | Run/Step/Attempt 状态机，无空洞 `event_seq` 的事件日志 | `internal/store/pg`、`migrations/` |
| | Step 粒度租约 + Fence Token：短事务领取 → 事务外执行（心跳）→ 带 Fence 提交 | `pg/claim.go`、`pg/commit.go`、`kernel/worker.go` |
| | Transactional Outbox → NATS JetStream；Reconciler 回收租约、过期审批、唤醒空闲 Run | `pg/reconcile.go`、`scheduler/`、`notify/` |
| | 租户公平：每租户同时运行 Run 上限，超额延迟领取 | `pg/claim.go`（`HARNESS_MAX_RUNNING_PER_TENANT`） |
| **干预** | pause / resume / cancel / inject_evidence / approve / reject / resolve_unknown | `pg/commands.go` |
| | v1.1 命令：override_result、replace_pending_input（服务端重算意图）、retry、migrate_profile | `pg/commands_v11.go` |
| **安全执行** | 审批绑定 `action_hash`、一次性 Grant、过期作废；R3 与未注册工具拒绝 | `policy/`、`pg/claim.go` |
| | 副作用账本：结果未知进入 `NEEDS_HUMAN`，绝不自动重放；工具可声明"未派发"（`ErrNotDispatched`） | `pg/commit.go`、`kernel/worker.go` |
| **对话** | Session / Turn / Branch、single_writer_queue、分支 fork、消息历史 | `pg/conversation.go`、`api/conversation.go` |
| **模型网关** | Alias → 硬过滤 → 软评分路由、熔断、预算与并发准入、仅在"未发送"时 fallback | `internal/gateway` |
| | Claude（官方 Go SDK，默认 `claude-opus-5-5`，adaptive thinking、prompt cache）与 OpenAI 兼容适配器 | `gateway/anthropic`、`gateway/openai` |
| | 请求/尝试/用量账本（ESTIMATED → REPORTED 修订），成本查询，Token 流片段落盘 + SSE delta | `pg/model.go`、`stream/` |
| **上下文** | Prompt Manifest（分区哈希、`rendered_request_hash`），Checkpoint 与 fork，确定性 Compaction | `cognitive/assembler.go`、`kernel/compact.go`、`pg/checkpoint.go` |
| **长期记忆** | 写入脱敏 + 投毒检测；全文 + 向量 + 依赖图三路召回，RRF 融合，冲突裁决，按 Step 钉住 | `internal/memory`、`pg/memory.go` |
| | 冷路径 Consolidator：Run → Experience → 候选记忆；操作员反馈只把候选记忆推进到 VALIDATING（由 approver 激活）或否决 | `memory/consolidate.go` |
| **评估与演进** | 从真实 Run 录制数据集（冻结后带 manifest hash），离线回放（写工具一律拦截） | `internal/eval` |
| | 指标（Wilson 下界、分层）+ 非劣性门禁；冻结字段不可演进；提议人不能自批 | `eval/eval.go`、`eval/service.go`、`profile/` |
| | 金丝雀：按路由键稳定分流、护栏自动回滚、人工 promote / rollback | `eval/service.go` |
| **工具** | Prometheus（`prom.query`、`prom.alerts`，R0） | `tools/sretools/prometheus.go` |
| | Kubernetes（`k8s.deployment_status` R0；`k8s.rollout_restart` R2，**默认 dry-run**，幂等，附回滚方案） | `tools/sretools/kubernetes.go` |
| | MCP（stdio / Streamable HTTP，官方 Go SDK）：只注册白名单工具，风险等级由运维配置，输出按 T4 处理，熔断隔离 | `tools/mcpexec` |
| | Remote Executor（gRPC `ExecutorService`，mTLS），按幂等键缓存结果 | `tools/remote`、`proto/executor/v1` |
| **安全** | 认证：静态 Token（仅存 SHA-256）、OIDC JWT（JWKS 轮换）、开发模式请求头 | `internal/auth` |
| | RBAC：viewer / operator / approver / admin，按路由和命令授权；可要求审批人≠发起人 | `api/authz.go` |
| | `security_audit_log` 审计；Postgres RLS（API 请求按租户设置 `app.tenant_id`） | `pg/security.go` |
| **可观测** | `/metrics`（Prometheus 文本格式，无高基数标签），OpenTelemetry tracing（OTLP/HTTP） | `internal/obs` |
| **保留策略** | 清理已发布 outbox、过期 Token 片段（行 + 对象）、过期记忆标记 STALE、可选审计保留期 | `internal/retention` |

依赖方向：`kernel` 只依赖 `domain`、`cognitive`、`tools` 等接口，不 import pg / NATS / 模型 SDK；Claude SDK 只出现在 `gateway/anthropic`。

## 插件框架（Everything is a plugin）

内核只保留**不可替换**的部分：Run/Step 状态机、租约与 Fence、事务提交、Outbox 发布、Reconciler、Worker 池和 API 核心路由。其余一切都是插件，通过编译期注册 + 配置文件启用：

```
插件包 init() ── plugin.Register(kind, name, factory) ──▶ 注册表
                                                          │
HARNESS_CONFIG (YAML/JSON) ── plugins.<kind>: [specs] ────┤
环境变量（未配置文件时的默认值）────────────────────────────┘
                                                          ▼
internal/app 按阶段构建：telemetry → artifact_store → tools → profile_source → policy
  → stream_bus → broker → route_policy → model_provider（组合路由策略 → Gateway）
  → embedder → memory_retriever → context_assembler → strategy → compactor → grader
  → event_sink → job → authenticator → api
```

| Kind | 接口 | 内置插件 | 多实例 |
|---|---|---|---|
| `telemetry` | `TelemetryExporter`（可选实现 `kernel.Observer`） | `otlp` | ✓ |
| `artifact_store` | `artifact.Store` | `fs`、`memory` | |
| `tools` | `ToolSource` | `demo`、`prometheus`、`kubernetes`、`mcp`、`remote` | ✓ |
| `policy` | `policy.Gate` | `default` | |
| `stream_bus` | `stream.Bus` | `local`、`nats` | |
| `broker` | `MessageBroker` | `local`、`nats` | |
| `model_provider` | `ModelBackend`（适配器 + 端点 + alias） | `scripted`、`anthropic`、`openai` | ✓ |
| `embedder` | `memory.Embedder` | `hashing` | |
| `memory_retriever` | `kernel.MemoryRetriever` | `hybrid` | |
| `strategy` | `StrategyBuilder`（按 profile 的 `strategy` 字段选择） | `llm`、`rule_based` | ✓ |
| `grader` | `eval.Grader` | `substring` | |
| `job` | `Runner`（scheduler 角色运行） | `consolidator`、`rollout_monitor`、`retention` | ✓ |
| `authenticator` | `auth.Authenticator`（按顺序尝试） | `dev`、`token`、`oidc` | ✓ |
| `api` | `api.Extension`（路由 + 声明的角色） | `memory`、`eval` | ✓ |
| `profile_source` | `profile.Source`（启动时注册的 Agent Profile 版本） | `builtin`、`file` | ✓ |
| `route_policy` | `gateway.Scorer`（模型端点的软打分） | `priority`、`weighted` | |
| `context_assembler` | `cognitive.ContextAssembler`（推理输入 → 模型请求 + Prompt Manifest） | `sectioned` | |
| `compactor` | `kernel.Compactor`（被折叠观测的摘要） | `deterministic` | |
| `event_sink` | `eventsink.Sink`（已提交运行事件的至少一次投递） | `webhook`、`log` | ✓ |

`harness plugins` 列出编译进来的全部插件；`GET /v1/admin/plugins`（admin）返回扩展点、已编译插件、当前激活实例和工具列表。

**框架底线（插件不能放宽）**
- `policy`：插件的决定会经过 `policy.Guarded`：未注册工具 / 缺目标 → 拒绝；R2 至少一次审批；R3 拒绝；风险等级只取注册表描述符。插件只能更严格。
- `api`：非 GET 路由必须在 `RouteRoles()` 中声明角色，否则默认拒绝；扩展不能覆盖核心路由的角色。
- `tools`：同名工具由两个插件提供时启动失败；风险/信任等级来自运维配置。
- `route_policy`：只有软打分可替换；租户授权、地域、能力、上下文窗口、熔断这些硬过滤，打分器无法绕过。
- `context_assembler`：自定义指令只替换角色说明；信任规则（工具输出、记忆、证据都是数据而非指令）总会追加在后面。
- `compactor`：何时压缩由内核决定，且内核总会追加安全事实（被拒绝/失败的动作、已执行的副作用，并从旧摘要中继承），与压缩器写了什么无关。
- `profile_source`：Profile 版本不可变。file 来源改动已存在的版本会让启动失败（请发布新版本）；`builtin` 演示种子只补缺，库里已有不同内容时保留库里版本并告警。
- `event_sink`：投递基于已提交的事件日志，每个 sink 在 PostgreSQL 里有游标（由一个 scheduler 副本持有租约），因此跨重启仍是至少一次、按提交顺序；sink 用 `event_id` 去重。永久错误（如 HTTP 4xx）和超过 `max_attempts` 的事件会被跳过、记日志并计入 `harness_event_sink_total`。
- 配置严格解析：未知 kind、未知字段、未编译的插件名都会让启动失败。

**配置文件**（`HARNESS_CONFIG`，示例 `deploy/harness.example.yaml`）。文件里出现的 kind 覆盖默认值，没出现的沿用环境变量推导的默认值；支持 `${VAR}` 与 `${VAR:-default}`（只在字符串值中展开，密钥无需写进文件）。spec 可写成名字、对象或列表；`enabled: false` 关闭；`instance` 让同一插件运行多个实例（如多个 MCP 服务器）。

```yaml
plugins:
  model_provider:            # 顺序即路由偏好
    - {name: anthropic, config: {api_key: "${ANTHROPIC_API_KEY}"}}
    - scripted
  tools:
    - {name: prometheus, config: {url: "http://prometheus:9090"}}
    - {name: mcp, instance: "mcp:runbooks", config: {name: runbooks, url: "http://runbooks:8080/mcp", allow: {search: {risk: R0}}}}
  job: []                    # 关闭全部后台任务
  profile_source:            # 内置演示种子 + 用 git 管理的版本化 Profile 文件
    - builtin
    - {name: file, config: {path: /etc/harness/profiles}}
  route_policy: {name: weighted, config: {cost_usd: 100, failures: 20}}
  context_assembler: {name: sectioned, config: {instructions_file: /etc/harness/instructions.md}}
  compactor: {name: deterministic, config: {keep: 3}}
  event_sink:
    - {name: webhook, instance: slack-approvals, config: {url: "${SLACK_WEBHOOK_URL}", format: slack, types: ["approval.requested", "run.failed"]}}
    - {name: webhook, instance: siem, config: {url: "https://siem.internal/hooks/harness", secret: "${SIEM_HMAC}", start_from: earliest}}
settings:
  default_profile: sre-diagnosis   # POST /v1/runs 未指定 profile 时使用
  default_profile_version: 1
models:
  circuit_failure_threshold: 3
  circuit_cooldown: 30s
```

Profile 文件（`*.yaml`/`*.yml`/`*.json`，单个定义或列表）形如 `{id: ops-triage, version: 3, tenant?: t1, spec: {strategy: llm, tools: [prom.query], max_steps: 12}}`；未写 tenant 的定义写入该来源的 `tenants`（默认 `settings.default_tenant`）。Webhook sink 会发送 `X-Harness-Event`、`X-Harness-Event-Id`，配置 `secret` 时还有 `X-Harness-Signature: sha256=<HMAC>`。

**Profile 约束的执行。** 每个 run 绑定启动时的 profile 版本，冻结字段由内核、存储层和命令 API 共同执行（同一套判定逻辑 `profile.Rules`）：

| 字段 | 执行方式 |
|---|---|
| `tools` | 白名单（名字、`prefix.*`、`*`）。模型只看得到这些工具；其他调用会被规划为跳过的步骤（"not allowed by profile"），执行前还会再检查一次。`[]` 表示不允许任何工具；**不写该字段**保持旧行为（全部已注册工具），启动时打警告。 |
| `risk_overrides` | `{工具: R0..R3}`，只能**调高**风险等级（调低会被忽略）。调高后的等级写入 ActionIntent。 |
| `approval_rules` | `{"min_approvals": {"R1": 1, "R2": 2, "k8s.rollout_restart": 2}}`：让原本放行的工具需要审批，或提高审批人数；不会放宽 deny。对 `replace_pending_input` 同样生效。 |
| `policy_version` | 设置后，若当前 policy 版本不一致，工具调用一律拒绝。 |
| `max_steps` | 对该 profile 的 run 降低全局 `HARNESS_MAX_STEPS_PER_RUN`。 |
| `allowed_aliases` | `model_alias` 必须在其中；部署默认 alias 不在其中时改用第一个允许的 alias。 |
| `compact_every` / `memory_top_k` | 对该 profile 覆盖 worker / 检索器的默认值。 |

非法值（未知风险等级、错误的模式、`approval_rules` 未知字段、alias 不在 `allowed_aliases` 中）在 profile 来自 profile source 时会让启动失败。

**编写插件**：外部模块只依赖公开包 `go-sre-harness/pkg/sdk`（类型别名 + `Register` + 全部 Kind），再用 `pkg/harness.Main()` 组装自己的二进制。完整示例见 `examples/plugins/hello`（一个工具、一个更严格的策略 `freeze_window`、一个 API 路由）和 `examples/custom-harness`：

```go
func init() {
	sdk.Register(sdk.Tools, "hello", "greeting tool",
		func(ctx context.Context, h *sdk.Host, c sdk.Config) (sdk.ToolSource, error) {
			return sdk.StaticTools(&sdk.FuncTool{Desc: sdk.ToolDescriptor{Name: "example.hello", Risk: sdk.RiskR0}, Fn: ...}), nil
		})
}
```

```bash
HARNESS_CONFIG=examples/custom-harness/harness.yaml HARNESS_DATABASE_URL=... go run ./examples/custom-harness all
```

插件可以用 `Host` 访问前序阶段的共享服务（Store、Artifacts、Tools、Gateway、Embedder、Eval、Server 等），用 `Host.Provide/Lookup` 在插件间共享对象；实现 `Close()`/`Shutdown(ctx)` 的实例在退出时按逆序关闭，实现 `Run(ctx) error` 的实例随进程启动。外部模块也可以用 `sdk.NewKind` 声明自己的扩展点。

## 快速开始（单进程）

需要 Go 1.26+ 和 PostgreSQL 14+；NATS 可选。

```bash
createdb harness
```

```bash
HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

不设置模型 Key 时，网关路由到确定性的 scripted 模型，完整走一遍账本、Manifest、流式路径。接入 Claude：

```bash
ANTHROPIC_API_KEY=sk-ant-... HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

另开终端跑演示（创建 Run → 等待 R2 审批 → 批准 → SSE 事件 → 结论）：

```bash
scripts/demo.sh "remediate high latency service=checkout"
```

默认认证模式是 `dev`（信任 `X-Tenant-ID` / `X-Actor-ID`），只允许监听回环地址。

### 桌面客户端

[`desktop/`](desktop/README.zh-CN.md) 是 Codex / Claude 桌面版风格的原生对话客户端（Wails：Go + 系统 WebView，约 9 MB）。在会话里和 Agent 对话，每条回复都带可折叠的工作过程：记忆检索、推理摘要、模型流式输出，以及带参数和结果的工具调用。高风险的工具调用会直接在对话里显示批准/拒绝卡片，停止按钮可以取消当前回复。**Customize** 页面管理租户的模型、MCP 服务器、HTTP 工具和技能（见下文）。在输入框里键入 `/` 会弹出技能和工具的选择器。**Approvals** 汇总所有对话中待处理的审批，新的审批还会以 macOS 系统通知的形式提醒。对话支持重命名、搜索、归档、删除，以及导出为 Markdown。它是独立的 Go 模块，只通过 `/v1` API 与服务端通信。

```bash
desktop/build.sh run
```

## 生产部署

### 认证

生成 Token 哈希并写入 Token 文件（格式见 `deploy/tokens.example.json`）：

```bash
go run ./cmd/harness hash-token 'my-secret-token'
```

```bash
HARNESS_AUTH_MODE=token,oidc HARNESS_AUTH_TOKENS_FILE=/etc/harness/tokens.json HARNESS_OIDC_ISSUER=https://idp.example.com HARNESS_OIDC_AUDIENCE=sre-harness go run ./cmd/harness api
```

数据库中的 `role_bindings` 会叠加到 Token/JWT 的角色上，可通过 `POST /v1/admin/role-bindings` 管理（admin）。

| 角色 | 允许 |
|---|---|
| viewer | 所有 GET |
| operator | 创建 Run/会话/轮次、pause/resume/cancel/inject、反馈、数据集与评估、提出演进候选 |
| approver | approve/reject/resolve_unknown/override_result、激活记忆、批准演进候选 |
| admin | 以上全部 + migrate_profile、灰度启动/promote/rollback、服务依赖、角色与审计 |

开启 `HARNESS_REQUIRE_DISTINCT_APPROVER=true` 后，Run 发起人不能批准自己的工具操作。

> 本地 `harness` 角色若是 superuser，RLS 不生效；生产应使用非 superuser、无 `BYPASSRLS` 的角色。

### 工具配置

推荐在 `HARNESS_CONFIG` 的 `plugins.tools` 中配置（见上文）。旧方式 `HARNESS_TOOLS_CONFIG` 仍然有效：它指向 JSON 文件（示例：`deploy/tools.example.json`），启动时被转换为对应的工具插件。风险等级、白名单、目标参数都在此处由运维决定，MCP/Remote 的自我描述不会提升权限。K8s 重启默认 `dry_run: true`，需要显式设为 `false` 才会真正改动集群。

### 模型（按租户、运行时）

默认情况下，Run 使用路由策略里配置的模型（开发环境下是内置的 `scripted` provider）。租户管理员可以在 **Customize → Model** 页面，或通过 `PUT /v1/extensions/models/{name}`，为本租户配置自己的模型：

| Provider | Spec |
|---|---|
| `anthropic` | `{"provider":"anthropic","model":"claude-opus-5-5","api_key":"…","effort":"high"}`（model 默认为 `claude-opus-5-5`；effort 取 `low`…`max`） |
| `openai`（兼容接口） | `{"provider":"openai","model":"…","base_url":"https://…/v1","api_key":"…"}` |

配置的模型会成为网关中的一个端点（`tenant:<tenant>:<name>`），只对该租户开放。它优先于全局端点承接 `sre-reasoning` 和 `sre-fast` 两个别名，失败时自动回退到全局端点。使用 `llm` 策略时，由模型自行选择工具，但仍受同一套 profile、策略和审批规则约束。`api_key` 只写不读。`POST /v1/extensions/models/{name}/test` 会发一条简短请求做测试，不保存配置；`GET /v1/extensions` 返回 `current_model`，即新 Run 实际会解析到的端点和模型。`model` 类型需要先执行迁移 `0007_tenant_models.sql`。

### MCP 服务器、HTTP 工具与技能（按租户、运行时）

除了通过 `plugins.tools` 编译进来的工具，租户管理员还可以在运行时添加扩展：在桌面客户端的 **Customize** 页面里，或通过 `/v1/extensions` API。扩展按租户存储在 `tenant_extensions` 表中（启用 RLS），每个进程会在 `HARNESS_EXTENSIONS_SYNC_INTERVAL` 内重新加载。

| 类型 | 说明 | 工具名 |
|---|---|---|
| MCP 服务器 | streamable-HTTP MCP 服务器。点 "Connect & list tools" 探测其工具；只有管理员勾选的工具会暴露给 Agent，每个工具的风险等级（R0–R3）和可选的目标参数由运维指定。本地 stdio 服务器默认拒绝，除非设置 `HARNESS_MCP_ALLOW_STDIO=true`。 | `mcp.<server>.<tool>` |
| HTTP 工具 | 一个接收调用 JSON 参数的端点（POST；R0 工具可用 GET 查询参数），请求带 `Idempotency-Key`、`X-Harness-Run-ID`、`X-Harness-Step-ID` 头 | `http.<name>` |
| 技能（Skill） | 一段 Markdown 操作手册（≤16 KiB），注入系统提示词（位于信任规则之前），Prompt manifest 记录 `name@hash`。`mode: "always"`（默认）对每个 Run 生效；`mode: "manual"` 只在当前消息（或本对话之前的消息）中出现 `/<name>` 时才生效 | — |

运行时工具不会绕过治理：
- Run 所用的 Profile 必须允许这些工具（默认 `sre-diagnosis@2` 允许 `demo.*`、`mcp.*`、`http.*`，`@1` 不允许）；
- 策略与 R2/R3 审批底线照常生效；
- MCP 的描述和输出都标记为不可信（T4）；
- 租户工具不会覆盖同名的全局工具；
- 请求头和环境变量的值只写不读：API 返回空值，提交空值时保留已存储的值；但如果目的地（`url`、`base_url`、`provider`、`command`、`args`）变了，必须重新填写密钥（`422 secrets_required`）；
- 租户扩展不能连接回环、链路本地和云元数据地址（在连接时按解析后的地址检查）；`HARNESS_EXTENSIONS_ALLOW_NETWORKS` 可以添加例外，元数据地址始终禁止；
- 所有修改仅限 admin，并记入审计日志。

本地试用：

```bash
go run ./examples/mcp-demo
```

启动 harness 时设置 `HARNESS_EXTENSIONS_ALLOW_NETWORKS=127.0.0.0/8`，租户扩展才能访问它。它会在 `http://127.0.0.1:8090/mcp` 提供一个 MCP 服务器（runbook_search、oncall_lookup、page_oncall），以及一个 `POST /deploys` 端点。在 Customize 中添加后，在对话里输入例如：`use mcp.demo.runbook_search {"query":"pool"}`。

### 多进程 / 容器

```bash
nats-server -js
```

```bash
export HARNESS_DATABASE_URL=postgres://localhost/harness HARNESS_NATS_URL=nats://127.0.0.1:4222
```

```bash
go run ./cmd/harness scheduler
```

```bash
HARNESS_WORKER_ID=w1 HARNESS_METRICS_ADDR=127.0.0.1:9091 go run ./cmd/harness worker
```

```bash
go run ./cmd/harness api
```

`scheduler` 角色运行 Outbox 发布和 Reconciler，以及已启用的 job（记忆 Consolidator、灰度护栏监控、保留策略清理）。

Docker Compose（Postgres + NATS + migrate + scheduler + 2 worker + api，Token 认证；先创建 `deploy/tokens.json`）：

```bash
docker compose -f deploy/docker-compose.yml up --build
```

Kubernetes 清单见 `deploy/k8s/harness.yaml`（外部 Postgres/NATS，Secret `harness`，ServiceAccount 只在白名单命名空间拥有 `get/patch deployments`）。FS Artifact 存储需要 RWX 卷；生产建议以 `artifact_store` 插件的形式用 S3/MinIO 实现同一接口。

## 配置（环境变量）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `HARNESS_DATABASE_URL` | — | 必填 |
| `HARNESS_CONFIG` | 空 | 插件配置文件（YAML/JSON）；为空时全部由下列环境变量推导 |
| `HARNESS_NATS_URL` | 空 | 为空时使用进程内 Broker（仅 `all` 模式） |
| `HARNESS_AUTO_MIGRATE` | `true` | 启动时执行迁移（advisory lock，多副本安全） |
| `HARNESS_HTTP_ADDR` | `127.0.0.1:8080` | |
| `HARNESS_METRICS_ADDR` | 空 | 非 api 角色单独暴露 `/metrics` |
| `HARNESS_ARTIFACT_DIR` | `./data/artifacts` | 内容寻址文件存储 |
| `HARNESS_DEFAULT_TENANT` | `tenant-a` | `builtin` 种子 Profile `sre-diagnosis@1` 与 `@2`（默认版本，允许 `mcp.*` / `http.*`）以及未写 tenant 的 Profile 定义所属租户 |
| `HARNESS_EVENT_WEBHOOK_URL` / `_FORMAT` / `_SECRET` | 空 / `json` / 空 | 启用一个 `webhook` 事件 sink（聊天工具用 `format: slack`） |
| `HARNESS_EVENT_TYPES` | 空（全部） | 该 sink 的事件过滤，逗号分隔，如 `approval.*,run.failed` |
| `HARNESS_AUTH_MODE` | `dev` | `token`、`oidc`、`dev` 逗号组合；`dev` 在非回环地址需 `HARNESS_ALLOW_DEV_AUTH=true` |
| `HARNESS_AUTH_TOKENS_FILE` | — | 静态 Token 文件 |
| `HARNESS_OIDC_ISSUER` / `_AUDIENCE` / `_JWKS_URL` / `_TENANT_CLAIM` / `_ROLES_CLAIM` | — / — / 发现 / `tenant_id` / `roles` | OIDC |
| `HARNESS_REQUIRE_DISTINCT_APPROVER` | `false` | 审批人不能是发起人 |
| `HARNESS_TOOLS_CONFIG` | 空 | 旧版工具配置文件；为空时只有 demo 工具 |
| `HARNESS_STRATEGY` | `llm` | `rule` 时完全不经模型网关 |
| `ANTHROPIC_API_KEY` / `HARNESS_ANTHROPIC_BASE_URL` / `HARNESS_ANTHROPIC_EFFORT` | — | Claude 适配器 |
| `HARNESS_OPENAI_BASE_URL` / `HARNESS_OPENAI_MODEL` / `HARNESS_OPENAI_API_KEY` | — | OpenAI 兼容适配器 |
| `HARNESS_MODEL_ALIAS` / `HARNESS_MODEL_EFFORT` | `sre-reasoning` / 空 | Profile 未指定时的默认值 |
| `HARNESS_MODEL_DAILY_BUDGET_USD` / `HARNESS_MODEL_MAX_INFLIGHT_PER_TENANT` | `0`（不限）/ `32` | 网关准入 |
| `HARNESS_MEMORY_RETRIEVAL` / `HARNESS_MEMORY_TOP_K` | `true` / `5` | 每个 Run 首个推理前检索记忆 |
| `HARNESS_CHECKPOINTS` | `true` | 每个成功 Step 写 Checkpoint |
| `HARNESS_BRANCH_POLICY` / `HARNESS_MAX_PENDING_TURNS` | `single_writer_queue` / `8` | 对话并发策略 |
| `HARNESS_MAX_RUNNING_PER_TENANT` | `0`（不限） | 租户公平 |
| `HARNESS_WORKER_ID` / `HARNESS_WORKER_MAX_INFLIGHT` | `<host>-<pid>` / `90` | |
| `HARNESS_LEASE_TTL` / `HARNESS_STEP_TIMEOUT` | `45s` / `2m` | 心跳间隔为 TTL/4 |
| `HARNESS_MAX_STEPS_PER_RUN` / `HARNESS_APPROVAL_TTL` | `100` / `30m` | |
| `HARNESS_RECONCILE_INTERVAL` / `HARNESS_CONSOLIDATE_INTERVAL` / `HARNESS_ROLLOUT_MONITOR_INTERVAL` | `10s` / `10s` / `1m` | |
| `HARNESS_RETENTION_INTERVAL` / `HARNESS_OUTBOX_RETENTION` / `HARNESS_AUDIT_RETENTION` | `5m` / `24h` / `0`（永久） | |
| `HARNESS_MCP_ALLOW_STDIO` / `HARNESS_EXTENSIONS_SYNC_INTERVAL` | `false` / `5s` | 是否允许以本地命令启动的租户 MCP 服务器 / 租户扩展的重新加载周期 |
| `HARNESS_EXTENSIONS_ALLOW_NETWORKS` | 空 | 逗号分隔的 CIDR 或地址，租户扩展可以访问这些默认禁止（回环、链路本地）的地址；本地试用可设为 `127.0.0.0/8` |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | 空 | 设置后启用 OTLP/HTTP tracing |

## API 速览

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/v1/runs` | 需 `Idempotency-Key`；`{"goal": "...", "profile_id"?, "profile_version"?}`；未指定版本时按灰度/渠道解析 |
| GET | `/v1/runs?status=&limit=` | 当前租户最新的 Run（含 goal 文本；`limit` 1..200，默认 50） |
| GET | `/v1/runs/{id}/steps?expand=io` | 步骤并内联输入/输出 Artifact（工具参数、观测结果、决策；每个最多 256 KiB） |
| GET | `/v1/runs/{id}` · `/steps` · `/approvals` · `/events` · `/events/stream` | SSE 支持 `Last-Event-ID`，`include_deltas=true` 附带 Token 流；已结束 Run 的流以一条 `stream.end` 消息（无 id）收尾 |
| POST | `/v1/runs/{id}/commands` | `202` 已应用 / `409` 版本冲突 / `422` 拒绝（附原因） / `403` 角色不足 |
| GET | `/v1/runs/{id}/model-requests` · `/cost` · `/stream-segments` · `/checkpoints` | 模型账本与 Checkpoint |
| POST | `/v1/runs/{id}/fork` | 从 Checkpoint 派生新 Run |
| GET | `/v1/sessions?status=active\|archived&q=&limit=` | 当前租户的会话（按最近使用排序，含轮次数）；`q` 按标题搜索 |
| PATCH | `/v1/sessions/{id}` | operator；`{"title"?, "status"?: "ACTIVE"\|"ARCHIVED"\|"DELETED"}`（删除后的会话永久隐藏，其 Run 仍保留） |
| GET | `/v1/approvals?limit=` | 租户内所有待处理审批，附带所属 Run、会话及会话标题 |
| POST/GET | `/v1/sessions…` | 会话、轮次、分支、消息 |
| POST/GET | `/v1/memories…` · `/v1/runs/{id}/feedback` · `/v1/incidents/{id}/memory-events` | 记忆生命周期与反馈 |
| POST/GET | `/v1/eval/datasets…` · `/v1/eval/runs` | 数据集录制、冻结、离线评估 |
| POST/GET | `/v1/evolution/candidates…` · `/v1/evolution/rollouts…` | 提议 → 门禁评估 → 批准 → 金丝雀 → promote/rollback |
| GET | `/v1/usage` · `/v1/whoami` · `/v1/admin/audit` · `/v1/admin/plugins` | |
| GET | `/v1/extensions` · `/v1/tools` | 租户的 MCP 服务器、HTTP 工具、技能及加载状态（密钥置空）；Run 可见的全部工具及 `allowed_by_profile` |
| PUT/DELETE | `/v1/extensions/{models\|mcp-servers\|http-tools\|skills}/{name}` | admin；`{"spec": {...}, "enabled"?, "expected_version"?}`（`0` 表示仅创建） |
| POST | `/v1/extensions/mcp-servers/{name}/probe` | admin；连接并列出服务器工具，不保存 |
| POST | `/v1/extensions/models/{name}/test` | admin；向模型发送一条简短请求做测试，不保存；返回 `{reply, model, latency_ms}`，失败返回 `502 model_failed` |
| GET | `/healthz` · `/metrics` | 无需认证 |

演进流程示例：

```json
POST /v1/evolution/candidates  {"profile_id":"sre-diagnosis","base_version":1,"patch":{"effort":"high"},"rationale":"..."}
POST /v1/evolution/candidates/{id}/evaluate  {"dataset_id":"…"}
POST /v1/evolution/candidates/{id}/approve   (另一位 approver)
POST /v1/evolution/candidates/{id}/rollout   {"traffic_percent":10,"guardrails":{"min_runs":20,"max_failure_rate":0.2}}
POST /v1/evolution/rollouts/{id}/promote
```

## 测试

每个测试使用独立 schema；未设置 `HARNESS_TEST_DATABASE_URL` 时数据库相关测试跳过。

```bash
HARNESS_TEST_DATABASE_URL=postgres://localhost/harness_test go test -race ./...
```

桌面客户端有自己的测试（`cd desktop && go test ./...`）。

覆盖重点：
- **状态层**：幂等、租户隔离、并发重复投递只产生一个租约、过期 Fence 无法提交、暂停/恢复/取消、`NEEDS_HUMAN` 恢复、KV、Outbox 互斥、v1.1 命令版本校验、租户公平。
- **端到端**（真实 Worker/Outbox/Reconciler/API）：
  - 诊断与审批流程、SSE 断点续传；
  - 模型网关 + Claude 适配器（假 HTTP 服务）、Compaction/fork/override；
  - 记忆召回钉住、投毒拦截、整合与反馈；
  - 演进门禁、自批拦截、金丝雀分流与自动回滚、promote；
  - 认证/RBAC/审计/职责分离、`/metrics`、保留策略。
- **插件框架**：注册/严格配置/重复实例/逆序关闭/Runner 收集；YAML + `${VAR}` 合并；用配置文件装配示例插件后完整跑通一个 Run，策略底线与 `/v1/admin/plugins` 权限。
- **适配器**：MCP（内存传输）、gRPC（bufconn）、Prometheus 与 K8s（假 HTTP 服务）、OIDC（真实 RSA 签名 JWT、伪造签名与算法混淆）。

## 已知限制

- **GPU 推理**：vLLM/SGLang 路由与 KV offload 未实现。
- **Artifact**：只有文件系统实现；Artifact 是内容寻址、共享的，因此 GC 只删除 Token 流片段对象。
- **离线评估**：每次评估使用独立的内存账本网关，成本记在报告中，不计入租户预算。
- **KV 过期**：读路径视为不存在，物理清理（写 tombstone 版本）尚未实现。
- **Python Worker**：通过 gRPC `ExecutorService` 或 MCP 接入即可，仓库只提供 Go 侧服务端示例。
- **`ErrNotDispatched`**：工具在派发前拒绝（参数非法、命名空间不在白名单）时 Step 按永久失败处理，Run 失败；不会进入 `NEEDS_HUMAN`。
- **进程内 Broker** 满了会丢消息，由 Reconciler 兜底，仅用于单进程模式和测试。
- **插件配置**只在启动时读取，修改后需要重启。
