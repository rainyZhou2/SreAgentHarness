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

Planner 把自己的输入也视为不可信：目标、会话历史、capability 描述以及失败尝试的节点输出都包在 `<<<DATA>>>` 边界内，并中和聊天模板控制符；工具描述来自插件和租户 MCP，看起来像注入指令的描述会同时从 prompt 和工具 schema 中移除。只要发现可疑输入，就记录在计划上，并强制生成的 flow 走计划审批，即使 `HARNESS_PLAN_APPROVAL=never`。生成的 flow 失败时不会立刻让 Run 失败：最多 `HARNESS_PLAN_MAX_REPLANS` 次，Planner 会拿到失败原因、已执行节点和已完成的副作用重新规划，新计划作为新的 flow generation 执行（全新状态与子 Run；失败的 generation 保留用于审计；plan hash 绑定 generation，旧审批不能复用）。

计划会从历史 Run 中改进。一个在模型生成的 flow 上成功完成的 Run（非 fallback、无注入标记、未被拒绝、无争议、反馈未标错）会被提炼为 *plan exemplar* 记忆（按服务作用域、按内容去重），走正常的记忆生命周期；Planner 会以不可信数据的形式看到与目标和服务最相近的 ACTIVE exemplar 与推理模式。每个 Run 记录计划指标（修复、fallback、重规划、审批请求/拒绝、注入、缓存命中），金丝雀和候选门禁会对比 fallback、修复、重规划、拒绝、成功和注入率。路由依次看 hint、请求的模式、点名的动作、正则列表，最后是分类器（默认对标注样例目标做 n-gram kNN，也可接 embedding 分类器；设置 `ClassifierFirst` 可让分类器先于正则）。每个生成或 fallback 的 flow 在审批前都会用模拟成功的子任务做 dry run：默认路径在自身限制内走不完的 flow 会被修复，未经审批就到达的副作用节点会被标出。计划审批附带可读摘要、有序步骤、风险、dry-run 路径，以及与上一 generation（重规划）或 fallback runbook 的 diff。未固定工具的 agent 节点会被固定到只读工具，不再一律按最高风险估计。Token 估算使用典型输出量，同时给出最坏情况。没有重规划和注入标记的计划按租户与输入缓存，命中后会针对当前工具目录重新校验。Profile 的 `planner` 段可以按租户收紧（不能放宽）这些设置：模型别名或模型阶梯、审批模式、节点/调用/Token/修复/重规划上限，以及按服务或目标正则匹配的 fallback 规则。

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
| `HARNESS_PLAN_MAX_REPLANS` | 生成的 flow 失败后在同一 Run 内重新规划的次数（默认 1，负数关闭）；新计划能看到已执行的节点和已完成的副作用，并作为新的 flow generation 执行 |
| `HARNESS_PLANNER_MAX_CHILDREN` / `_TOTAL_RUNS` / `_DEPTH` / `_ITERATIONS` | 生成图的并行、总 Run、嵌套与循环上限 |
| `HARNESS_PLANNER_MAX_MODEL_CALLS` / `_MAX_ESTIMATED_TOKENS` | 静态模型调用数与输出 Token 上限 |
| `HARNESS_OUTPUT_OFFLOAD_BYTES` | 超过该大小的工具结果落盘并摘要，模型通过 `harness.artifact_query` 按需查询（默认 24576，负数关闭） |
| `HARNESS_CONTEXT_PROVIDER_TIMEOUT` / `HARNESS_CONTEXT_MAX_FACTS` | 环境事实 provider 的单个超时与事实条数上限 |
| `HARNESS_PLANNER_REACT_PATTERNS` / `_FLOW_PATTERNS` | 规划失败时的路由正则，`\|\|` 分隔；`none` 关闭该列表 |
| `HARNESS_PLANNER_ROUTER_EXAMPLES` | `{"text","mode"}` 样例的 JSON 文件，替换内置路由分类器样例 |
| `HARNESS_PLANNER_FALLBACKS` | `{"name","services","goal_pattern","profile_id","version"}` 的 JSON 列表，优先于默认 runbook 匹配的 fallback 规则 |
| `HARNESS_PLANNER_MODEL_LADDER` | 逗号分隔的模型别名：第一个负责规划，每次修复升级到下一个 |
| `HARNESS_PLANNER_CACHE_TTL` / `_CACHE_SIZE` | 计划缓存时长（默认 `10m`，`0` 关闭）与条目数（默认 512） |
| `HARNESS_PLANNER_TYPICAL_OUTPUT_TOKENS` | Token 估算采用的单次输出量（默认上限的四分之一；负数按最坏情况） |
| `HARNESS_PLANNER_AUTO_PIN` / `_DRY_RUN` | 将未固定工具的 agent 节点固定到只读工具；审批前对计划做 dry run（默认都为 `true`） |
| `HARNESS_REASONING_VERIFY` | 对最终 task 结论做独立复核（默认 `true`；规则层无需模型） |
| `HARNESS_VERIFIER_MODEL_ALIAS` / `_EFFORT` / `_TIMEOUT` | 复核用的模型裁判（为空则只用规则）、effort 与超时（默认 `1m`） |
| `HARNESS_VERIFIER_MEMORY_IS_EVIDENCE` | 是否允许单独一条记忆支撑 VERIFIED 断言（默认 `false`） |
| `HARNESS_REASONING_MAX_BACKTRACKS` | 未被接受的结论退回 agent 的次数（默认 1，负数不退回） |
| `HARNESS_REASONING_SAMPLES` / `_MIN_AGREEMENT` | 最终结论的自一致性采样次数（默认 1：关闭）及所需一致率（默认 0.6） |
| `HARNESS_REASONING_CALIBRATION` / `HARNESS_CALIBRATION_MIN_FORECASTS` | 按 profile 做置信度校准（默认 `true`）及拟合所需样本数（默认 30） |
| `HARNESS_RUN_MAX_COST_USD` / `HARNESS_RUN_MAX_TOKENS` | 单个 Run 的模型花费上限（默认 0：关闭）；用到 80% 时提示 agent 收敛，用满时 kernel 不再调用模型、直接代为结论。profile 的 `run_limits` 只能收紧 |
| `HARNESS_LOOP_MAX_REPEATS` / `HARNESS_LOOP_STALL_STEPS` | 完全相同的调用（工具+参数）可重复执行的次数（默认 1，负数不限）；连续多少个工具结果没有新内容时提示停滞（默认 4，达到两倍时强制结论，负数关闭） |
| `HARNESS_MAX_PARALLEL_TOOLS` | 同一次模型响应中可并行执行的只读调用数（默认 4；1 表示每步一个调用） |
| `HARNESS_ARTIFACT_CACHE_BYTES` | worker 内存中缓存不可变 artifact、用于重建 agent 上下文（默认 64 MiB；负数关闭） |
| `HARNESS_TOOL_TOP_K` | 可见工具较多时，每个 Run 发送给模型的工具 schema 数（默认 30；负数全部发送）；其余只列名称，可用 `harness.load_tools` 加载 |
| `HARNESS_COMPACT_TOKENS` | 未压缩的工具结果超过约这么多 token 时压缩历史（默认 16000；负数只按步数） |
| `HARNESS_MODEL_ROUTINE_ALIAS` / `HARNESS_MODEL_ESCALATED_ALIAS` | 常规调查步使用的模型 alias，以及决定结果的步骤使用的 alias（默认沿用策略自身的 alias） |
| `HARNESS_SPECULATIVE_TOOLS` | 模型仍在流式输出时提前执行允许的只读调用，工具步直接复用结果（默认 `false`） |
| `HARNESS_SHORT_TERM_MESSAGES` | 原样保留的历史对话消息数（默认 12；更早的消息压缩为摘要；负数全部保留） |
| `HARNESS_EMBEDDING_BASE_URL` / `_MODEL` / `_DIMENSIONS` / `_API_KEY_ENV` | 桌面运行时：使用 OpenAI 兼容的 embeddings 接口替代哈希 embedder；密钥从 `_API_KEY_ENV` 指定的变量读取（默认 `HARNESS_EMBEDDING_API_KEY`）。服务端在 `HARNESS_CONFIG` 中配置 `embedder: {name: http, …}` |

生成计划会发出 `plan.generated`、`plan.validation_failed`、`plan.repaired`、`plan.fallback`、`plan.approval_requested`、`plan.approved`、`plan.rejected`、`plan.executed`、`plan.injection_suspected`、`plan.replan_requested`、`plan.replanned` 事件。Planner 指标在 `/metrics`：`harness_plan_total`、`harness_plan_duration_seconds`、`harness_plan_repairs_total`、`harness_plan_validation_failures_total`、`harness_plan_fallback_total`。

计划数据集会记录 plan artifact、分析结果、flow 状态、审批、结果、耗时与成本。`POST /v1/eval/datasets/{id}/plan-cases` 录入已结束的 Run，`POST /v1/eval/plan-runs` 离线回放冻结的数据集，并按变体（`react`、`fallback_flow`、`generated_flow`、`profile_flow`）汇总成功率、安全性、回放完成率、步数、耗时与成本。

其余配置见 `deploy/harness.example.yaml`。

## Agent 循环防护

- **Run 预算**：每个推理步之前 kernel 读取本 Run 的模型用量。达到 `HARNESS_RUN_MAX_COST_USD` / `HARNESS_RUN_MAX_TOKENS`（或 profile 更严格的 `run_limits.max_cost_usd` / `max_tokens`）的 80% 时，模型会收到 `[kernel notice]` 和"立即结论"指令；用满时 kernel 自己写最终答复（结果中带 `stopped`：截至当时的假设和最近的工具结果，标记为 UNVERIFIED）并跳过复核，循环中的 Run 不会超支。
- **只读调用并行**：一次响应请求多个工具时，若额外调用均已注册、对 agent 可见、无需审批、只读且不是重复调用，则与第一个调用一起放进 kernel 自有的 `harness.parallel` 步骤中并发执行（对模型隐藏，模型直接调用会被拒绝），上限 `max_parallel_tools`。每个调用各自成为一条观察（`obs:step<N>.<K>`），某个调用失败或在规划后发生变化只影响它自己。未执行的调用（有副作用、需审批、重复）记录在步骤摘要的 `deferred_not_run` 中；有副作用的调用始终单独走审批。
- **工具错误即观察**：只读调用的永久性失败（参数错误、资源不存在、权限）变成 `outcome=error` 的观察交给 agent 调整，不再让 Run 失败；可重试的瞬时错误仍按步骤重试，有副作用的调用保持 unknown-effect 处理。
- **修复反馈**：无法使用的响应（无法解析的工具调用、不符合 schema 的参数、无效决策、不存在的工具）以 `[correction]` 的形式把具体问题（及可用工具列表）回传给模型，每次尝试最多两次，之后才重试步骤；修复记录写入步骤摘要。
- **重复与停滞防护**：完全相同的调用（工具+规范化参数）已执行 `max_repeats` 次，或曾被拒绝、驳回、过期的，不再执行，而是记录为跳过的步骤并指向之前的结果。连续 `stall_steps` 个工具结果没有新内容（重复、错误、相同输出）时提示 agent 换思路；达到两倍时，再请求工具会被 kernel 结论替代。
- profile 可在 `run_limits` 下按 agent 设置（`max_cost_usd`、`max_tokens`、`max_repeats`、`stall_steps`、`max_parallel_tools`），只能比进程配置更严格。

## Agent 循环性能

- **上下文重建**：每个推理步都从 Run 历史重建上下文，以保证重试时完全确定。artifact 不可变，worker 会缓存自己写入和读取的 artifact（LRU，`HARNESS_ARTIFACT_CACHE_BYTES`），每步只需读取新增内容。同一 Run 的步骤本来就会在一个 worker 内连续执行（最多 20 步后才交回队列）。
- **Prompt 缓存**：历史区在步骤之间只会追加，因此请求在其末尾额外标记一个缓存断点（系统/工具前缀已有一个）；账本、提示和指令都放在断点之后。提供自动前缀缓存的厂商会忽略该标记。
- **工具筛选**：可见工具超过 `HARNESS_TOOL_TOP_K` 时，模型看到每个 Run 固定的一组工具：本 Run 用过或加载过的、内置工具、请求中点名的，再按与请求的词重叠排序补足；其余列在 `[more tools]` 中，可用 `harness.load_tools` 加载。选择在步骤之间不变，缓存前缀保持有效；任何可见工具仍会被接受。
- **模型路由**：配置 `HARNESS_MODEL_ROUTINE_ALIAS` / `HARNESS_MODEL_ESCALATED_ALIAS` 后，首次分析、可能或被要求的结论、停滞、结论被驳回后的那一步以及修复使用升级 alias，其余步骤使用常规 alias（步骤摘要中的 `model_tier`）。flow 节点自带的 alias 优先。
- **自适应停止**：账本收敛时（有 VERIFIED 且置信度 ≥ 0.8 的假设，且没有其他开放假设），在步数上限或停滞防护介入之前提示 agent 给出结论。
- **按 token 预算压缩**：除每 N 个工具步压缩一次外，未压缩结果超过 `HARNESS_COMPACT_TOKENS` 时也会压缩（此时只保留最新一个结果原文）。默认压缩器（`deterministic-v2`）每条结果一行、以可引用的 ref 开头，大的 JSON 对象按顶层字段呈现；被压缩的 ref（包括批量步骤的子调用）仍可引用。
- **只读调用提前执行**（`HARNESS_SPECULATIVE_TOOLS`）：模型流中每个工具调用一完成，若它可见、只读、无需审批且不是重复调用，就立即开始执行；执行同一调用（版本、风险、参数都相同）的工具步直接取用结果。未被使用的结果两分钟后过期；有副作用的调用从不提前执行。
- **可观测性**：每个步骤摘要带 `timings_ms`（queue、context、model、review、tool），并与提交耗时一起导出为 `harness_step_phase_seconds{kind,phase}`；`harness_agent_loop_events_total{event}` 统计修复、循环防护、提示、批量、收敛、路由、压缩、提前执行和 artifact 缓存命中。Run 结果中带 `loop`（步数、模型请求数、token、花费）。

## 记忆分层

| 层级 | 生命周期 | 内容 | 写入方 |
|---|---|---|---|
| 工作记忆 | 单个 Run | agent 的笔记：已确认的事实、约束、待查项；以及假设账本 | scope 为 `run` 的 `notes` |
| 短期记忆 | 单个会话（分支） | 最近 `HARNESS_SHORT_TERM_MESSAGES` 条消息原文、更早轮次的摘要、会话笔记 | scope 为 `session` 的 `notes` |
| 长期记忆 | 租户 | 事件、修复方案、推理模式、计划范例、agent 提议的事实（organization / service / incident） | consolidator、运维人员、scope 为 `long_term` 的 `notes`（作为 CANDIDATE） |
| 个性化记忆 | 单个用户 | 发起 Run 的用户的稳定偏好（语言、格式、默认值） | scope 为 `user` 的 `notes`，或用户通过 `/v1/me/memories` 管理 |

- **笔记**：agent 在 `THOUGHT` 行或结论中写 `"notes":[{"key","value","scope"}]`；重复写同一个 key 即更新，值为空即删除。kernel 从已存储的决策重建笔记，因此重试和压缩后依然存在，并在数据边界内以 `[working memory]` 小节呈现。每层最多 24 条、每条 500 字符。
- **短期记忆**：Run 结束时的会话笔记写入其结果（`memory.session`）；同一分支的下一轮从最近一个完成轮次的笔记开始，因此分叉出的分支只看到自己的历史。
- **个性化记忆**：用户发起的 Run 完成后，其 `user` 笔记按 key（`pref:<key>`，ACTIVE，`restricted`）为该用户新建、强化、修订（新版本）或归档记忆。个性化记忆与本 Run 检索到的记忆一起固定，只在该用户自己的 Run 中以 `[about the user]` 小节呈现；共享检索、`harness.memory_search` 和其他用户都看不到（管理员可列出）。指令性文本会被拒绝，偏好永远不能授权任何操作。用户通过 `GET /v1/me/memories`、`PUT|DELETE /v1/me/memories/{key}` 管理自己的偏好。
- **agent 提议的长期记忆**：已完成且未被否定的 Run 中的 `long_term` 笔记成为 CANDIDATE 记忆（反馈确认后为 VALIDATING），subject 为 `note:<key>`，与其他记忆一样走审核。

## 记忆更新机制

- **强化而非重复**：在同一 scope 中再次写入相同内容，或写入同 subject、近似重复（embedding 余弦 ≥ 0.95；无可比 embedding 时词元重叠 ≥ 0.85）且验证程度不高于已有记忆的内容时，若来源是新的，则记为对已有记忆的一次强化（`reinforcements`、`last_reinforced_at`，合并 `source_refs`）。被强化的记忆排序略微提升（最多 ×1.3）。
- **修订**：`PATCH /v1/memories/{id}`（approver；`expected_version`、`title` 和/或 `content`、`reason`）写入新版本：与新记忆一样脱敏和投毒检查，重新生成向量和检索词，记审计，状态不变。旧版本保留在 `agent_memory_versions` 中，引用旧版本的 ref 仍可解析；修订后与其他记忆内容重复时会被拒绝。
- **取代**（激活一条记忆会让同 subject、同 scope 的 ACTIVE 记忆变为 SUPERSEDED）、**生命周期迁移**、反馈和保留策略保持不变。整理过程是幂等的：反馈触发的重新整理不会重复强化或修订。

## 记忆检索

- **运行中检索**：第一个推理步看到的是按目标检索出的记忆。`harness.memory_search`（R0，参数 `query`，可选 `service`、`top_k` ≤ 10）用 agent 之后掌握的信息（假设、报错信息、组件）走同一条检索流水线（硬过滤、全文/向量/依赖图三路召回、RRF、信任与 profile 排除、冲突消解）。结果作为"需验证的过往经验"返回，带 `memory:<id>@<v>` ref；这些 ref 可被引用，并作为记忆证据交给独立验证器。工具可见时，停滞提示会建议检索记忆。
- **模型 embedding**：`http` embedder 调用任意 OpenAI 兼容的 `/embeddings` 接口（`base_url`、`model`、`api_key_env`、`dimensions`、`timeout`），带小型查询缓存；调用失败时检索跳过向量分支。向量只在同一 embedding 模型内比较，因此切换模型后由 consolidator 的 indexer（`index`，默认开启；`index_batch`，默认 50）按批为存活记忆重新生成向量，ACTIVE 优先。
- **中日韩文本**：分词把 CJK 连续字符切成重叠的二字组，全文检索（Postgres 写入 `search_terms` 并纳入 `search_tsv`）、本地存储的匹配和哈希 embedder（`hash-uni-bigram-v2`）都能在不依赖分词器的情况下命中 CJK 片段。migration 0018 之前写入的记忆由 indexer 补齐检索词。

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
- **引用校验**：每条 observation 头都带有自己的引用 `obs:step<N>`，证据为 `kv:<key>@<v>`，记忆为 `memory:<id>@<v>`。kernel 会把 `supports`、`refutes`、`evidence_refs` 中的每条引用与本次请求实际给模型看的内容对照解析（`obs:4`、`obs:<tool>`、offload handle 等别名会归一为标准形式）。指向不存在内容的引用存为 `unresolved:<原文>`，不计入证据支撑。没有有效引用就被判为 VERIFIED/REFUTED 的假设记为无据结论；VERIFIED 结论如果 verification 和已证实的假设都没有有效引用，视为模型自封：打分和统计按未验证处理，不算成功，也不会被提炼成模式。引用有效率、无据假设率和无据结论率会汇总进 summary，纳入发布门禁和金丝雀门禁，并在改进循环中触发 `citation` playbook 提示。
- **独立复核与回溯**：最终的 task 结论由不是它作者的校验器复核。规则层（始终开启）要求每条 VERIFIED/REFUTED 断言都引用真实存在的证据，失败的工具调用或单独一条记忆都不算证据，VERIFIED 结论至少要有一条被支持的已证实断言；可选的模型裁判（`HARNESS_VERIFIER_MODEL_ALIAS`，独立 prompt，看不到 agent 的历史）逐条判断所引证据是支持、矛盾还是不足。未获 SUPPORTED 的结论会通过 kernel 专属的 `harness.conclusion_review` 步骤（对模型隐藏，模型主动调用会被拒绝）退回 agent，最多 `HARNESS_REASONING_MAX_BACKTRACKS` 次，相关假设在假设账本中标为 CHALLENGED；否则 Run 完成，报告写入 `independent_verification`，未被接受的答案会被明确标注。打分时独立结论优先于 agent 自评：被反驳或证据不足的结论不算成功，也不会被提炼。
- **多假设与自一致性**：调查过程中 agent 在 THOUGHT 行里列出竞争假设（id、状态、概率）；kernel 从本 Run 的决策重建假设账本并在每一步展示，促使 agent 在开放假设间做区分性检查，首个假设被证伪时回到下一个。`HARNESS_REASONING_SAMPLES` > 1 时，最终 task 结论会采样多次，取一致结论最多的一簇；一致率低于 `HARNESS_REASONING_MIN_AGREEMENT` 视为证据不足并触发复核。
- **事后校准**：每个 Run 记录假设最初声明的置信度及最终结果（有独立结论时以其为准）。按租户 profile 用近期 Run 拟合保序回归校准器（优先使用经独立复核的样本）；kernel 在原始置信度旁记录校准后的值，并告诉模型其声明的置信度过去的实际命中率；summary 报告 ECE 和校准后 Brier，ECE 或被反驳率上升会让门禁不通过。
- **归纳式 Playbook**：在 `reasoning_improver` 任务上配置 `inducer_model_alias` 后，改进循环会把最差 Run 的摘要（假设及结论、工具、校验器意见，不含原始工具输出）交给模型归纳通用指导语。点名资源或 Run、像注入指令、削弱审批/复核/校验/信任规则、或与已有行重复的条目都会被拒绝；没有可用条目时退回模板指导语。候选版本仍需通过 holdout 门禁、非提议者审批和金丝雀。
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
