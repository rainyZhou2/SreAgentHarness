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

[`desktop/`](desktop/README.zh-CN.md) 是 Wails 原生客户端（约 9 MB）：会话、流式推理、工具调用卡片、对话内审批、审批收件箱，以及模型 / MCP / HTTP 工具 / 技能管理。

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

其余配置见 `deploy/harness.example.yaml`。

## API

`POST /v1/runs`、`GET /v1/runs/{id}/events/stream`（SSE）、`POST /v1/runs/{id}/commands`（审批 / 暂停 / 取消）、`/v1/sessions…`、`/v1/memories…`、`/v1/eval…`、`/v1/evolution…`、`/v1/extensions…`、`/healthz`、`/metrics`。

## 测试

```bash
HARNESS_TEST_DATABASE_URL=postgres://localhost/harness_test go test -race ./...
```

未设置数据库时，数据库相关测试自动跳过。

## 已知限制

- GPU 推理（vLLM/SGLang 路由、KV offload）未实现。
- Artifact 仅提供文件系统实现，生产建议以插件接入 S3/MinIO。
- 离线评估使用独立内存账本，成本不计入租户预算。
- 插件配置在启动时读取，修改后需要重启。
