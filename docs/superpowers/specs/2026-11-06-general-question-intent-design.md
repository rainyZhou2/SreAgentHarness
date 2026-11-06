# 普通问题意图识别与自适应回答

日期：2026-11-06
状态：已确认

## 背景

当前每个用户消息都会变成 `sre-diagnosis` 的一个 Run。系统提示词
（`internal/cognitive/assembler.go` 的 `DefaultInstructions`）固定为事故调查角色，
每一步都被要求「调用一个工具或 `submit_conclusion`」。结果是普通咨询类问题也被
推向「假设—证据—处置」模板，模型可能为了显得严谨而调用无关工具。

## 目标

- 每条用户消息先分析意图，再决定回答方式。
- 咨询类问题直接回答；诊断/操作类保持现有 ReAct、策略门禁与审批流程。
- 普通问答仍走 Run/Step 引擎，保留会话、账本、事件、断线恢复和审计。
- 回答格式自适应：问答只展示答案；诊断保留假设与证据结构。

## 非目标

- 不新增 `classify` 步骤或请求级预分类器。
- 不改变工具白名单、风险分级、审批底线。
- 不引入新的数据库表或事件类型。

## 行为契约

| intent | 触发条件 | 行为 |
|---|---|---|
| `question` | 询问知识、解释或现状，不要求改动 | 直接提交结论；需要实时事实时可使用工具，但不发起副作用操作 |
| `task` | 诊断、修复或执行动作 | 沿用现有调查循环与审批 |
| `clarification` | 请求含糊且贸然执行有风险 | 在答案中提出一个澄清问题并结束本轮 |

意图只影响回答组织方式，不影响策略门禁。工具调用仍由 `policy.Gate` 与审批流程约束。

## 组件

### domain

`Decision` 增加 `Intent`（`question|task|clarification`，可为空）。

### cognitive

- `ReasoningInput` 增加 `HasToolSteps`，由 worker 根据当前 Run 的历史填充。
- 重写 `DefaultInstructions`：先判断请求类型，再决定直接回答还是调查；明确禁止
  「为了显得认真而调用工具」。
- `[instruction]` 按阶段区分：
  - 首步（无工具结果）：判断请求类型，问答直接提交结论；
  - 已有工具结果：继续调查，调用一个工具或提交结论。
- `submit_conclusion` schema 增加 `intent`；解析时缺省为 `task`。
- `PromptPrefixVersion` 升为 `sre-prefix-v2`。

### kernel

- `renderContext` 填充 `HasToolSteps`。
- 最终 result artifact 与 step summary 记录 `intent`。
- 空 intent 的最终决策归一化为 `task`，保持旧行为。

### desktop

- `result.intent` 为 `question` 或 `clarification` 时只渲染答案，不渲染假设块，
  完成后不显示「Worked for …」调查过程。
- `task` 保持现状。

## 兼容性

- 模型或自定义 prompt 不返回 `intent`：默认 `task`，行为与改造前一致。
- 自定义 `context_assembler.instructions` 仍然覆盖角色说明，框架信任规则不变。
- 分类错误不会绕过策略与审批；最坏情况是多走一轮调查或答案缺少证据。

## 测试

- assembler：schema 含 `intent`；首步与后续步骤指令不同；prefix version 正确。
- decision 解析：三种 intent 与缺省值。
- kernel e2e（需要数据库）：scripted 模型返回 `question` 时单步完成、无 tool step、
  `result.intent=question`。
- 桌面端手工验证问答不显示假设与调查过程。

## 回滚

纯代码变更，无数据库迁移；回滚即恢复旧 prompt 与渲染逻辑。
