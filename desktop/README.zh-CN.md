# SRE Harness 桌面客户端

[English](README.md) | 中文

SRE harness 的原生桌面客户端，基于 [Wails v2](https://wails.io)：Go + 操作系统自带的 WebView，不打包浏览器（macOS 上约 9 MB）。它是独立的 Go 模块，只通过公开的 `/v1` API 与服务端通信，认证和角色与其他客户端完全一致。

## 功能

Codex / Claude 桌面版风格的对话客户端，基于 harness 的会话（session）和轮次（turn）。

- **对话列表**（侧边栏）：按最近使用排序，Agent 工作中的对话会显示圆点。**New chat** 用第一条消息新建对话，也可以点建议开头。搜索框按标题筛选对话，**Archived** 切换到已归档的对话。每个对话的 ⋯ 菜单（对话标题栏里也有）可以**重命名**、**归档**/取消归档、**导出为 Markdown**（逐轮导出问题、工具调用和结论）以及**删除**。点击标题栏中的对话标题也可以直接重命名。
- **回复**：你发出的每条消息是一个轮次，Agent 用一个 Run 来回答。每条回复包含：
  - **工作过程**：进行中时展开，完成后折叠成 "Worked for 9s · 2 tool calls"，内容有：
    - 记忆检索；
    - 推理摘要，思考时流式显示模型输出；
    - 工具调用（名称、参数、状态），点开可以看输出；
  - **结论**（简单 Markdown）和带状态与置信度的**假设**；
  - Run 编号、模型调用次数和 Token 数。
- **对话内审批**：工具需要审批时，卡片上显示风险等级、目标、过期时间和 action hash，可以**批准**或**拒绝**（拒绝时可填原因）。
- **审批收件箱**：侧边栏的 **Approvals** 汇总租户内所有对话中待处理的审批，并显示数量角标。可以在收件箱里直接批准或拒绝，也可以跳转到对应对话。客户端每隔几秒检查一次，每出现一条新审批就发送一条 macOS 系统通知，点击通知会打开收件箱。首次使用时 macOS 会请求通知权限。
- **输入框**：Enter 发送，Shift+Enter 换行；输入法确认候选词时按的 Enter 不会发送。Agent 工作时：
  - 新消息会排队，依次执行；
  - ■ 停止当前回复；
  - 回复下方可以**暂停**和**恢复**。

  输入 `/` 会弹出选择器，列出技能和工具：选技能会插入 `/<技能名>`，在本对话中启用一个 manual 技能；选工具会插入 `use <工具名> {…}`，参数按工具的 Schema 预先填好骨架。↑/↓ 切换，Enter 或 Tab 选中，Esc 关闭。
- **Customize**：租户的运行时扩展，分五个标签页。
  - **Model**：配置租户使用的模型，可选 Anthropic（选择模型和 effort）或 OpenAI 兼容端点（填写 Base URL 和模型名）。保存前可以点 **Test** 发一条简短请求测试连通性。页面顶部显示新 Run 实际使用的模型。配置模型后，由 Agent 自行选择要调用的工具。
  - **MCP servers**：填写 URL 和请求头后点 *Connect & list tools*，勾选允许 Agent 调用的工具，并为每个工具设置风险等级（R0–R3）和可选的目标参数。
  - **HTTP tools**：配置端点、方法、风险等级和输入 Schema。
  - **Skills**：Markdown 格式的操作手册，注入系统提示词。有两种模式：*always* 对每个 Run 生效；*manual* 只在提到 `/<名称>` 的对话中生效。
  - **Tools**：列出 Run 可用的全部工具，包括来源、风险等级以及默认 Profile 是否允许。

  查看需要 viewer 角色，修改需要 admin。请求头的值和 API Key 只写不读，留空即保留已保存的值。在对话里输入 `use <工具名> {json}` 可以让 Agent 调用指定工具。
- **插件**：服务端插件清单（需要 admin 角色）。

所有权限都由服务端校验。命令带上 Run 当前的 `expected_version`，界面过期时会得到 `409`，不会基于旧状态执行操作。

## 构建与运行

依赖：Go 1.26+。
- macOS：Xcode 命令行工具。
- Linux：`libgtk-3-dev`、`libwebkit2gtk-4.1-dev`。
- Windows：WebView2。

不需要 Node.js，也不需要 Wails CLI。

```bash
# 本地模式构建：打包 SQLite harness core。
./build.sh
./build.sh run
```

应用默认进入 **本地运行** 模式：

- 桌面进程启动自带的 SQLite harness core；
- UI 通过回环地址和每次启动随机生成的 Token 连接；
- 不需要 Docker、系统 PostgreSQL、NATS 或命令行；
- 数据保存在 `<用户配置目录>/sre-harness-desktop/local/`：
  ```text
  local/
    harness.db          SQLite 状态与查询索引
    events/local/*.jsonl 追加式执行日志（source of truth）
    artifacts/
    logs/
  ```
- 执行事件先写 JSONL；SQLite 只保存文件偏移索引，可通过 `RebuildEventIndex` 从 JSONL 重建。

在 Connection 中切换到 **团队服务** 即可连接共享 harness。团队模式支持开发请求头（仅本机调试）或 Bearer Token。设置保存在 `<用户配置目录>/sre-harness-desktop/settings.json`（权限 0600）；Token 只保存在内存中。

## 浏览器预览

同一个二进制可以不开窗口，而是把界面提供给浏览器，方便调界面和自动化检查。只监听回环地址。

```bash
SRE_DESKTOP_PREVIEW=127.0.0.1:34115 "build/bin/SRE Harness.app/Contents/MacOS/sre-harness-desktop"
```

然后打开 http://127.0.0.1:34115/。

## 目录

| 路径 | 作用 |
|---|---|
| `main.go` | Wails 窗口与选项；设置了 `SRE_DESKTOP_PREVIEW` 时进入预览模式 |
| `app.go` | 绑定给界面的方法（`window.go.main.App.*`）：会话、对话记录、发送、Run 视图、命令、实时订阅；所有 HTTP 请求都在 Go 里发出 |
| `inbox.go` | 对话搜索、重命名、归档、删除和 Markdown 导出；审批收件箱和系统通知 |
| `harness/` | 无 UI 依赖的 `/v1` 客户端：认证头、幂等键、错误解析、带续传的 SSE |
| `preview.go` | 浏览器预览桥（`/__call/<Method>`、`/__events`） |
| `frontend/dist/` | 界面：纯 HTML/CSS/JS，无构建步骤；服务端数据只以文本渲染 |

## 测试

```bash
go test ./...
```
