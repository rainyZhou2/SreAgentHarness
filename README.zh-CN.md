# go-sre-harness

[English](README.md) | **简体中文**

面向 SRE 场景的认知型 Agent 运行时：把每一次推理和工具调用当作可恢复、可审计的分布式事务来执行。

## 核心能力

| 领域 | 能力 |
|---|---|
| **持久执行** | Run/Step/Attempt 状态机，Step 级租约 + Fence Token，进程崩溃后从断点恢复 |
| **安全执行** | R0–R3 风险分级、审批绑定 `action_hash`、一次性授权、副作用账本；结果未知进入 `NEEDS_HUMAN`，绝不自动重放 |
| **对话** | Session / Turn / Branch，单写者队列，分支 fork，消息与事件可回放 |
| **模型网关** | Alias → 硬过滤 → 软评分路由，熔断、预算与并发准入，仅在"未发送"时 fallback；支持 Claude、OpenAI 兼容、Gemini |
| **上下文与意图** | Prompt Manifest（分区哈希）、确定性压缩；每条消息先区分问答 / 诊断 / 澄清 |
| **长期记忆** | 全文 + 向量 + 依赖图三路召回，RRF 融合，投毒检测与冲突裁决 |
| **编排 DSL** | Profile 内嵌 flow 图：条件/循环、并行子 Run、变量插值、审批/人工输入、子流程与多 Agent；编译到 Step 引擎 |
| **联网能力** | `web.fetch` 开箱可用；`web.search` 支持 Brave / Tavily / Bing / 通用后端，输出按不可信内容处理 |
| **评估与演进** | 从真实 Run 录制数据集、离线回放、非劣性门禁、金丝雀与自动回滚 |
| **事件分发** | 提交事件 → 事件日志 → Relay → JetStream → SSE；断线按游标回补，稳态不查询数据库 |
| **插件化** | 除状态机、租约、事务提交、Outbox 和核心路由外，其余全部是可替换插件 |

## 实现要点

```
客户端（Web / 桌面端）
   │ REST + SSE
internal/api ── 扩展：memory / eval / extensions
   │
internal/kernel ── Durable Step Engine
   ├─ cognitive  意图分析、上下文装配、Prompt Manifest、策略
   ├─ gateway    路由、熔断、预算、请求与用量账本
   ├─ tools      MCP / gRPC / Kubernetes / Prometheus / HTTP
   └─ policy     风险门禁与审批
   │
PostgreSQL（唯一真相）  +  NATS JetStream（唤醒与事件分发）
```

- **短事务领取 → 事务外执行 → 带 Fence 提交**：租约丢失时结果直接丢弃，旧 worker 无法覆盖新状态。
- **无空洞事件日志 + Transactional Outbox**：状态、事件、唤醒消息同事务提交，Reconciler 兜底恢复。
- **事件平面**：Relay 以租约单活追尾事件日志，JetStream 按 Run 分发；SSE 网关补齐一次后只接收推送。
- **内核只依赖接口**：不 import PostgreSQL、NATS 或模型 SDK，适配器可独立替换和测试。

### 编排 DSL

Profile 可以设置 `strategy: "flow"` 并内嵌 `flow` 图。DSL 编译到现有 Step 引擎：父 Run 只做协调，agent / tool / subflow 节点各自成为子 Run，因此租约、Fence、审批、副作用账本和断点恢复全部复用。并行用子 Run 承载，join 节点汇总；子 Run 继承父 Profile 且只能收窄工具和权限。

```yaml
strategy: flow
tools: [demo.metrics, demo.restart]
max_steps: 40
flow:
  version: 1
  entry: triage
  limits: {max_steps: 20, max_children: 4, max_total_runs: 8, max_depth: 2}
  nodes:
    - id: triage
      type: agent
      role: triage
      tools: [demo.metrics]
      next: [{to: finish}]
    - id: finish
      type: end
      output: "${nodes.triage.output}"
```

节点类型：`agent` / `tool` / `subflow` / `parallel` / `join` / `approval` / `input` / `loop` / `end`。`GET /v1/runs/{id}/flow` 返回节点和子 Run 状态；`provide_input` 命令用于回答 `input` 节点。内置示例：`sre-diagnosis@8`（并行诊断 + 审批）与 `@9`（人工输入 + 子流程）。

桌面端不需要手动选择 Profile。自动会话以一个持久的 **plan step** 启动：Planner LLM 通过 `submit_flow` 提交 flow DSL，服务端用 `flow.Compile` 校验、做静态安全与预算分析、按错误自动修复，并把计划固定为 artifact。含副作用或 R2/R3 工具的生成计划会先进入 **plan review** 审批步骤，审批绑定 plan hash；图、输入或工具集变化都会让审批失效。审批通过后才追加 flow step 并执行。模型无法生成合法计划时，回退到配置的 runbook（默认 `sre-diagnosis@8`）或 ReAct。运行中展示生成计划、节点和子 Run 状态，并内联处理计划审批、flow 审批与 `input` 输入门。

## 设计上比较满意的地方

- **数据库是唯一真相，推送只是加速**：断线、丢消息、副本故障都不会造成状态不一致。
- **副作用不可重复**：审批绑定动作哈希，执行前二次校验意图；结果未知宁可交给人，也不自动重试。
- **插件只能收紧不能放宽**：策略底线、风险等级来源、信任规则、审计都由框架兜底。
- **没有 API Key 也能跑通全链路**：内置 scripted 模型，账本、Manifest、流式输出和工具校验全部走到。
- **演进闭环完整**：数据集冻结 → 离线回放 → 门禁 → 人工批准 → 金丝雀 → 自动回滚。

## 快速开始

需要 Go 1.26+ 和 PostgreSQL 14+，NATS 可选。

```bash
createdb harness
HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

接入模型：

```bash
ANTHROPIC_API_KEY=sk-ant-... HARNESS_DATABASE_URL=postgres://localhost/harness go run ./cmd/harness all
```

演示（创建 Run → R2 审批 → SSE → 结论）：

```bash
scripts/demo.sh "remediate high latency service=checkout"
```

默认使用 `dev` 认证，只监听回环地址。

## 桌面客户端

[`desktop/`](desktop/README.zh-CN.md) 是 Wails 原生客户端。默认进入 **本地运行** 模式：应用会启动自带的 SQLite harness core，执行事件写入追加式 JSONL 日志，SQLite 只作为查询索引，不需要 Docker、PostgreSQL、NATS 或命令行。切换到 **团队服务** 即可连接共享 harness。会话、流式推理、工具调用卡片、对话内审批、审批收件箱，以及模型 / MCP / HTTP 工具 / 技能管理在两种模式下一致；本地模式实现了同一套 `/v1/extensions`、命令、checkpoint/fork、memory、graph 和评测 API。命令带版本校验并支持跨重启幂等，取消 flow 会级联到子 Run，压缩摘要、记忆命中、checkpoint 和步骤输出修订都持久化在本地存储中。

```bash
desktop/build.sh run
```

## 部署

```bash
docker compose -f deploy/docker-compose.yml up --build
```

- 角色：`api` / `worker` / `scheduler`；单机可用 `all`，跨进程需要 NATS。
- Kubernetes 清单：`deploy/k8s/harness.yaml`。
- 完整插件与配置示例：`deploy/harness.example.yaml`。
- 生产环境使用非 superuser 数据库角色，RLS 才会生效。

## 常用配置

| 变量 | 说明 |
|---|---|
| `HARNESS_DATABASE_URL` | 必填 |
| `HARNESS_NATS_URL` | 多进程唤醒与事件分发 |
| `HARNESS_CONFIG` | 插件配置文件（YAML/JSON） |
| `HARNESS_AUTH_MODE` | `dev` / `token` / `oidc` |
| `HARNESS_SSE_MODE` | `push`（默认）/ `poll` |
| `HARNESS_MODEL_DAILY_BUDGET_USD` | 每租户日预算 |
| `HARNESS_REQUIRE_DISTINCT_APPROVER` | 审批人不能是发起人 |
| `HARNESS_AUTO_PROFILE` / `_VERSION` | 自动会话的运行 Profile，默认 `sre-diagnosis` / `10` |
| `HARNESS_PLANNER_MODEL_ALIAS` / `_EFFORT` | Planner 使用的模型与 effort |
| `HARNESS_PLANNER_FALLBACK_PROFILE` / `_VERSION` | Planner 失败时回退的 flow Profile，默认 `sre-diagnosis` / `8` |
| `HARNESS_PLANNER_MAX_REPAIRS` / `_MAX_NODES` | 计划修复次数与计划节点上限 |
| `HARNESS_PLAN_APPROVAL` | `auto`（默认）/ `always` / `never`；`auto` 对含写操作的生成计划要求审批 |
| `HARNESS_PLAN_APPROVERS` | 计划审批人数；R3 计划至少 2 人 |
| `HARNESS_PLANNER_MAX_CHILDREN` / `_TOTAL_RUNS` / `_DEPTH` / `_ITERATIONS` | 生成图的并行、总 Run、嵌套与循环上限 |
| `HARNESS_PLANNER_MAX_MODEL_CALLS` / `_MAX_ESTIMATED_TOKENS` | 静态模型调用数与输出 Token 上限 |
| `HARNESS_OUTPUT_OFFLOAD_BYTES` | 超过该大小的工具结果落盘并摘要，模型通过 `harness.artifact_query` 按需查询（默认 24576，负数关闭） |
| `HARNESS_CONTEXT_PROVIDER_TIMEOUT` / `HARNESS_CONTEXT_MAX_FACTS` | 环境事实 provider 的单个超时与事实条数上限 |
| `HARNESS_PLANNER_REACT_PATTERNS` / `_FLOW_PATTERNS` | 规划失败时的路由正则，`\|\|` 分隔；`none` 关闭该列表 |

生成计划会发出 `plan.generated`、`plan.validation_failed`、`plan.repaired`、`plan.fallback`、`plan.approval_requested`、`plan.approved`、`plan.rejected`、`plan.executed` 事件。Planner 指标在 `/metrics`：`harness_plan_total`、`harness_plan_duration_seconds`、`harness_plan_repairs_total`、`harness_plan_validation_failures_total`、`harness_plan_fallback_total`。

计划数据集会记录 plan artifact、分析结果、flow 状态、审批、结果、耗时与成本。`POST /v1/eval/datasets/{id}/plan-cases` 录入已结束的 Run，`POST /v1/eval/plan-runs` 离线回放冻结的数据集，并按变体（`react`、`fallback_flow`、`generated_flow`、`profile_flow`）汇总成功率、安全性、回放完成率、步数、耗时与成本。

其余配置见 `deploy/harness.example.yaml`。

## 触发器、策略规则与动作闭环

- **触发器**（`triggers:`）：`POST /v1/triggers/{name}` 把 Alertmanager、Grafana、PagerDuty、CI 等 webhook 转成运行。`items` 拆分事件，`filter` 过滤噪声，`resolve` 事件只追加证据。去重窗口内的重复事件以证据形式注入已有运行而不是新建；去重通过数据库共享，多副本结果一致。支持 HMAC 签名（`X-Harness-Signature: sha256=…`）和按触发器限流。`schedule:` 触发器在多个 scheduler 副本间每个周期只创建一次运行。
- **规则化策略**（`policy: {name: rules}`）：按工具、来源、标签、风险、环境、目标、参数、时间窗、频率和影响面匹配，最严格的命中规则生效，框架底线始终生效。环境按每次调用解析并绑定进 action intent。
- **时间与环境上下文**：运行固定 `now`、故障时间窗、服务和标签（`POST /v1/runs` 的 `context`，或由触发器渲染）。`context_provider` 插件在每个运行开始时补充拓扑、归属和近期变更，结果固定用于回放，并作为不可信数据渲染。配置了 `time_args` 的工具在模型未给出时间时自动补齐时间窗。
- **校验与补偿**（`tool_overlays.<tool>.lifecycle`）：副作用执行后内核轮询校验工具直到 `expect` 成立；校验失败时提出补偿动作，补偿始终需要审批。
- **大输出**：超大的工具结果存为 `tool-output` 制品并生成结构化摘要（日志级别、时间范围、高频模板；JSON 结构与数值统计），模型通过 `harness.artifact_query` 查询其余部分（head、tail、lines、grep、JSON path、时间范围）。
- **不可信输出**：所有工具结果、人工证据和压缩摘要都包在由内容哈希生成 id 的 `<<<DATA id=…>>>` 边界里，内容本身无法提前闭合；聊天模板控制符会被转义，像指令的文本（复用记忆投毒规则）在 prompt、observation 和 prompt manifest 中标记为 `injection_suspected`。内容只标记不删除。上下文压缩会在截断之外保留落盘 handle、校验结论和这些标记。
- **路由**：规划失败时，依次由上下文提示、模型请求的模式、配置的正则以及目标中提到的有副作用工具决定走兜底 flow 还是 ReAct。

桌面端从数据目录下的 `harness.local.json`（或 `HARNESS_LOCAL_CONFIG`）读取同样的 `policy`、`triggers`、`context_providers`、`tool_overlays` 配置。

## 可量化、可复用、可自我改进的推理

对 agent 显式的决策轨迹（计划、带置信度的假设、校验）做打分、复用和改进；从不读取模型提供方隐藏的推理。设计文档：`docs/superpowers/specs/2026-11-07-reasoning-quality-and-self-improvement-design.md`。

- **量化**：每个结束的 run 由 consolidator 打分，收到反馈后重新打分。指标包括：校准度（以 VERIFIED/REFUTED 结束的假设的 Brier 分数）、证据支撑率、重复工具调用、到首个结论的步数、最终校验结论和综合质量分。离线回放用同一套打分；若候选版本在 holdout 上的校准度、证据支撑率、冗余率或质量分退化，发布门禁直接不通过。
- **复用**：已完成、未被反馈否定且校验结论为 VERIFIED 的 run，会被提炼为*推理模式*，即一条 procedural 记忆。内容是按顺序检验过的假设及各自结论，以及得出首个结论前用到的工具。它走正常的记忆生命周期（CANDIDATE → 审批 → ACTIVE），每条模式都要人工激活。每条记忆的成功率和 Wilson 下界，按检索过它的 run 统计。
- **模式效果**：按同一服务中没用到该模式的 run 的成功率作为基线来比较，而不是用绝对成功率。模型显式引用（evidence 或假设 refs 里的 `memory:<id>`）的样本足够时，以引用为准。比基线显著且明显更差的模式会被排除。被排除的模式仍按 `reasoning.pattern_probe_rate`（上限 0.2）确定性地放给一小部分 run，这些 run 达到基线就恢复。已失效的条目会被清掉；列表到 500 条上限时，只保留证据最强的。运维人员也可以手动提议排除或恢复。
- **轨迹结构**：假设带稳定 `id`，换个说法也按同一个算。置信度覆盖率和结构化比例纳入度量、门禁和诊断；已定论但没给置信度的假设按 0.5 预测计。
- **改进**：`reasoning_improver` 定时任务（或 `POST /v1/reasoning/improve`）会诊断薄弱维度和表现差的 ACTIVE 模式，并提出一个 profile 候选版本，只改 `reasoning.playbook` 和 `reasoning.patterns_excluded`。playbook 是固定的、与领域无关的指导，≤2000 字符，经过投毒检查，并放在信任规则之前渲染。诊断时会排除 holdout 用例对应的 run。候选版本走现有流程：holdout 门禁 → 由非提议者的人工审批 → 灰度与自动回滚。灰度监控还会比较灰度组和对照组的推理质量，退化超过阈值就自动回滚；该检查默认开启，两组各至少 20 个已打分 run 才会判定。该循环从不审批、不发起灰度、不晋升，且同一时间只提一个变更。
- **保留期**：`retention` 任务按 `reasoning_retention`（默认 90 天）清理推理分数。
- **API**：`GET /v1/reasoning/summary`、`GET /v1/reasoning/runs/{run_id}`、`GET /v1/reasoning/patterns`、`POST /v1/reasoning/patterns/{memory_id}/exclude|reinstate`（operator）、`POST /v1/reasoning/improve`（admin）。桌面端同样打分和提炼，但只在手动触发时改进。

## API

`POST /v1/runs`、`GET /v1/runs/{id}/events/stream`（SSE）、`POST /v1/runs/{id}/commands`（审批 / 暂停 / 取消）、`/v1/sessions…`、`/v1/memories…`、`/v1/eval…`、`/v1/evolution…`、`/v1/extensions…`、`/v1/triggers…`、`/v1/reasoning…`、`/healthz`、`/metrics`。

## 测试

```bash
HARNESS_TEST_DATABASE_URL=postgres://localhost/harness_test go test -race ./...
```

未设置数据库时，数据库相关测试自动跳过。真实模型 tool-call 探针会调用实际 Provider，并在模型没有返回合法 `submit_flow` 计划时以非零退出：

```bash
ANTHROPIC_API_KEY=sk-ant-... go run ./cmd/plancheck -goal "审批后重启 checkout"
```

## 已知限制

- GPU 推理（vLLM/SGLang 路由、KV offload）未实现。
- Artifact 仅提供文件系统实现，生产建议以插件接入 S3/MinIO。
- 离线评估使用独立内存账本，成本不计入租户预算。
- 计划回放用录制的节点结果驱动 flow 状态机；含有限循环的图会做编译与安全检查，但不做逐步回放。
- 插件配置在启动时读取，修改后需要重启。
