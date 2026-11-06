// SRE Harness desktop client: a chat UI over sessions and turns. Every
// network call goes through the Go side (window.go.main.App); live run
// events and model deltas arrive as "run:event" / "run:delta".
"use strict";

const api = () => window.go.main.App;
const $ = (sel, root = document) => root.querySelector(sel);

const TERMINAL = new Set(["COMPLETED", "FAILED", "CANCELLED"]);
const SUGGESTIONS = [
  "checkout p99 latency doubled after the last deploy",
  "remediate latency service=checkout",
  "payments error rate is spiking, what changed?",
];

const state = {
  who: null,
  sessions: [],
  sessionId: null,      // null = new chat
  session: null,
  turns: [],
  runs: new Map(),      // run_id -> RunView
  deltas: new Map(),    // run_id -> Map(step_id -> streamed text)
  watching: null,
  open: new Set(),      // expanded tool outputs (step ids)
  traceOpen: new Map(), // run_id -> bool (user toggled)
  view: "chat",
  sending: false,
  sessFilter: { status: "active", q: "" },
  approvals: [],
};

// ---------- DOM helpers: server data only ever becomes text nodes ----------
function h(tag, props, ...kids) {
  const el = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (v == null || v === false) continue;
    if (k === "class") el.className = v;
    else if (k.startsWith("on")) el.addEventListener(k.slice(2), v);
    else el.setAttribute(k, v === true ? "" : v);
  }
  for (const kid of kids.flat(Infinity)) {
    if (kid == null || kid === false) continue;
    el.append(kid instanceof Node ? kid : document.createTextNode(String(kid)));
  }
  return el;
}
const BADGE = { COMPLETED: "s-ok", VERIFIED: "s-ok", ACTIVE: "s-ok", UNVERIFIED: "s-warn", REFUTED: "s-bad", FAILED: "s-bad" };
const badge = (s) => h("span", { class: "badge " + (BADGE[s] || "s-idle") }, String(s || "?").replaceAll("_", " "));

function ago(ts) {
  if (!ts) return "";
  const d = (Date.now() - new Date(ts).getTime()) / 1000;
  if (d < 60) return "now";
  if (d < 3600) return Math.round(d / 60) + "m";
  if (d < 86400) return Math.round(d / 3600) + "h";
  if (d < 86400 * 7) return Math.round(d / 86400) + "d";
  return new Date(ts).toLocaleDateString();
}
function dur(a, b) {
  const s = Math.max(0, Math.round((new Date(b) - new Date(a)) / 1000));
  return s < 60 ? `${s}s` : `${Math.floor(s / 60)}m ${s % 60}s`;
}
let toastTimer;
function toast(msg, bad = false) {
  const t = $("#toast");
  t.textContent = msg;
  t.className = "toast" + (bad ? " bad" : "");
  t.hidden = false;
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => (t.hidden = true), bad ? 6000 : 2500);
}
const errText = (e) => (e && e.message) || String(e);

// Minimal Markdown (paragraphs, lists, fenced code, `code`, **bold**) built as DOM.
function md(text) {
  const root = h("div", { class: "md" });
  const inline = (s) => s.split(/(`[^`]+`|\*\*[^*]+\*\*)/).filter(Boolean).map((p) =>
    p.startsWith("`") && p.endsWith("`") ? h("code", {}, p.slice(1, -1))
      : p.startsWith("**") && p.endsWith("**") ? h("strong", {}, p.slice(2, -2)) : p);
  String(text || "").split(/```/).forEach((part, i) => {
    if (i % 2 === 1) return root.append(h("pre", {}, part.replace(/^\w*\n/, "")));
    for (const block of part.split(/\n{2,}/)) {
      const lines = block.split("\n").filter((l) => l.trim() !== "");
      if (!lines.length) continue;
      if (lines.every((l) => /^\s*[-*] /.test(l))) root.append(h("ul", {}, lines.map((l) => h("li", {}, inline(l.replace(/^\s*[-*] /, ""))))));
      else if (lines.every((l) => /^\s*\d+[.)] /.test(l))) root.append(h("ol", {}, lines.map((l) => h("li", {}, inline(l.replace(/^\s*\d+[.)] /, ""))))));
      else root.append(h("p", {}, lines.map((l, j) => [j ? h("br") : null, inline(l)])));
    }
  });
  return root;
}

// ---------- connection ----------
async function openConnection() {
  const s = await api().GetSettings();
  const f = $("#form-conn");
  f.server_url.value = s.server_url || "http://127.0.0.1:8080";
  const c = s.creds || {};
  f.mode.value = c.mode || "dev";
  f.tenant.value = c.tenant || "";
  f.actor.value = c.actor || "";
  f.roles.value = c.roles || "";
  f.token.value = "";
  $("#conn-error").hidden = true;
  syncMode();
  $("#dlg-conn").showModal();
}
function syncMode() {
  const mode = $("#form-conn").mode.value;
  document.querySelectorAll("#form-conn [data-mode]").forEach((el) => (el.hidden = el.dataset.mode !== mode));
}
async function submitConnection(ev) {
  ev.preventDefault();
  const f = ev.target;
  const settings = {
    server_url: f.server_url.value.trim(),
    creds: { mode: f.mode.value, tenant: f.tenant.value.trim(), actor: f.actor.value.trim(), roles: f.roles.value.trim(), token: f.token.value },
  };
  try {
    showWho(await api().Connect(settings));
    $("#dlg-conn").close();
    state.runs.clear();
    newChat();
    await loadSessions();
  } catch (e) {
    $("#conn-error").textContent = errText(e);
    $("#conn-error").hidden = false;
  }
}
function showWho(who) {
  state.who = who;
  $("#who").textContent = who ? `${who.subject} · ${who.tenant}` : "Not connected";
  $("#who").title = who ? `roles: ${(who.roles || []).join(", ")} · auth: ${who.method}` : "";
  $("#who-initial").textContent = who ? (who.subject || "?").slice(0, 1).toUpperCase() : "?";
}

// ---------- sidebar ----------
async function loadSessions() {
  let error = null;
  try {
    const f = state.sessFilter;
    const res = await api().SearchSessions(f.status, f.q, 100);
    if (f !== state.sessFilter) return; // filter changed meanwhile
    state.sessions = res.sessions || [];
  } catch (e) {
    error = errText(e);
  }
  renderSessions(error);
}
function renderSessions(error) {
  const ul = $("#chats");
  ul.replaceChildren();
  if (error && !state.sessions.length) return ul.append(h("li", { class: "empty" }, error));
  const archived = state.sessFilter.status === "archived";
  $("#chats-label").textContent = archived ? "Archived chats" : "Chats";
  $("#btn-archived").textContent = archived ? "‹ Back" : "Archived";
  if (!state.sessions.length) return ul.append(h("li", { class: "empty" },
    state.sessFilter.q ? "No matching chats" : archived ? "No archived chats" : "No chats yet"));
  for (const s of state.sessions) {
    ul.append(h("li", { class: s.id === state.sessionId && state.view === "chat" ? "active" : "", title: s.title || "", onclick: () => openSession(s.id) },
      h("span", { class: "chat-title" }, s.title || "Untitled chat"),
      s.active_turns > 0 ? h("span", { class: "pulse", title: "working" }) : h("span", { class: "chat-time" }, ago(s.last_activity_at)),
      h("button", { class: "row-more", title: "More", onclick: (e) => { e.stopPropagation(); chatMenu(e.currentTarget, s); } }, "⋯")));
  }
}

// ---------- chat management: rename, archive, export, delete ----------
function chatMenu(anchor, s) {
  const archived = s.status === "ARCHIVED";
  popMenu(anchor, [
    ["Rename…", () => renameChat(s)],
    [archived ? "Restore" : "Archive", () => archiveChat(s, !archived)],
    ["Export as Markdown", () => exportChat(s.id)],
    null,
    ["Delete…", () => deleteChat(s), "danger"],
  ]);
}

async function renameChat(s) {
  const title = await ask({ title: "Rename chat", label: "Title", value: s.title || "", ok: "Rename" });
  if (title == null || !title.trim()) return;
  try {
    await api().RenameSession(s.id, title.trim());
    if (state.session && state.sessionId === s.id) { state.session.title = title.trim(); renderHead(); }
    loadSessions();
  } catch (e) { toast(errText(e), true); }
}

async function archiveChat(s, archived) {
  try {
    await api().ArchiveSession(s.id, archived);
    toast(archived ? "Archived" : "Restored");
    if (state.sessionId === s.id) {
      if (state.session) state.session.status = archived ? "ARCHIVED" : "ACTIVE";
      if (archived && state.sessFilter.status === "active") newChat(); else renderHead();
    }
    loadSessions();
  } catch (e) { toast(errText(e), true); }
}

async function deleteChat(s) {
  const ok = await ask({ title: "Delete chat?", text: `“${s.title || "Untitled chat"}” will be removed from your chats. Its runs stay in the server's audit history.`, ok: "Delete", danger: true });
  if (!ok) return;
  try {
    await api().DeleteSession(s.id);
    if (state.sessionId === s.id) newChat();
    loadSessions();
    toast("Deleted");
  } catch (e) { toast(errText(e), true); }
}

async function exportChat(id) {
  try {
    const path = await api().ExportChat(id);
    if (path) toast("Saved to " + path);
  } catch (e) { toast(errText(e), true); }
}

// popMenu shows a small menu under anchor; items are [label, fn, class] or null (separator).
function popMenu(anchor, items) {
  const m = $("#menu");
  m.replaceChildren(...items.map((it) => it ? h("button", { class: "menu-item " + (it[2] || ""), onclick: () => { closeMenu(); it[1](); } }, it[0]) : h("div", { class: "menu-sep" })));
  const r = anchor.getBoundingClientRect();
  m.hidden = false;
  m.style.left = Math.min(r.left, window.innerWidth - m.offsetWidth - 8) + "px";
  m.style.top = (r.bottom + 4 + m.offsetHeight > window.innerHeight ? r.top - m.offsetHeight - 4 : r.bottom + 4) + "px";
}
function closeMenu() { $("#menu").hidden = true; }

// ask shows a confirm or input dialog; resolves to true/the value, or null when cancelled.
let askResolve = null;
function ask({ title, text = "", label = null, value = "", ok = "OK", danger = false }) {
  const d = $("#dlg-ask");
  $("#ask-title").textContent = title;
  $("#ask-text").textContent = text;
  $("#ask-text").hidden = !text;
  $("#ask-label").hidden = label == null;
  $("#ask-label-text").textContent = label || "";
  const input = $("#form-ask input[name=value]");
  input.value = value;
  $("#ask-ok").textContent = ok;
  $("#ask-ok").className = "btn " + (danger ? "danger-solid" : "primary");
  if (askResolve) askResolve(null);
  d.showModal();
  if (label != null) { input.focus(); input.select(); }
  return new Promise((res) => {
    askResolve = (v) => { askResolve = null; res(v); };
  });
}

// ---------- chat ----------
function newChat() {
  state.view = "chat";
  markSide();
  state.sessionId = null;
  state.session = null;
  state.turns = [];
  stopWatching();
  renderSessions();
  render();
  $("#input").focus();
}

async function openSession(id) {
  if (id !== state.sessionId || state.view !== "chat") {
    state.view = "chat";
    markSide();
    state.sessionId = id;
    state.turns = [];
    state.session = null;
    stopWatching();
    render();
  }
  renderSessions();
  await refreshSession(true);
}

async function refreshSession(scroll) {
  const id = state.sessionId;
  if (!id) return;
  let t;
  try {
    t = await api().Transcript(id);
  } catch (e) {
    return toast(errText(e), true);
  }
  if (id !== state.sessionId) return;
  state.session = t.view && t.view.session;
  state.turns = t.turns || [];
  // Load (or refresh) every run that is not cached in a terminal state.
  await Promise.all(state.turns.filter((x) => x.run_id).map((x) => {
    const cached = state.runs.get(x.run_id);
    return cached && TERMINAL.has(cached.run.run.status) ? null : loadRun(x.run_id, false);
  }));
  if (id !== state.sessionId) return;
  render(scroll);
  watchActive();
}

async function loadRun(runId, rerender = true) {
  try {
    const v = await api().RunView(runId);
    state.runs.set(runId, v);
    if (TERMINAL.has(v.run.run.status)) state.deltas.delete(runId);
  } catch (e) {
    /* keep the previous view */
  }
  if (rerender) render();
}

function activeTurn() {
  return state.turns.find((t) => t.run_id && (t.status === "RUNNING" || t.status === "ACCEPTED") &&
    !(state.runs.get(t.run_id) && TERMINAL.has(state.runs.get(t.run_id).run.run.status)));
}

async function watchActive() {
  const t = activeTurn();
  if (!t) return stopWatching();
  if (state.watching === t.run_id) return;
  const v = state.runs.get(t.run_id);
  state.watching = t.run_id;
  try {
    await api().WatchRun(t.run_id, v ? v.run.run.last_event_seq : 0);
  } catch (e) {
    state.watching = null;
    toast(errText(e), true);
  }
}
function stopWatching() {
  if (state.watching) api().UnwatchRun();
  state.watching = null;
}

let runTimer = null;
const pendingRuns = new Set();
let sessionPending = false;
function scheduleRun(runId, sessionToo) {
  pendingRuns.add(runId);
  if (sessionToo) sessionPending = true;
  clearTimeout(runTimer);
  runTimer = setTimeout(async () => {
    const all = [...pendingRuns];
    const sess = sessionPending;
    pendingRuns.clear();
    sessionPending = false;
    await Promise.all(all.map((r) => loadRun(r, false)));
    if (sess) {
      await refreshSession(false);
      loadSessions();
    } else {
      render();
    }
  }, 200);
}

// ---------- send / stop ----------
async function send(text) {
  text = text.trim();
  if (!text || state.sending) return;
  state.sending = true;
  updateComposer();
  try {
    if (!state.sessionId) {
      const title = text.length > 60 ? text.slice(0, 57) + "…" : text;
      const res = await api().CreateSession(title, "");
      state.sessionId = res.session.id;
      state.session = res.session;
    }
    state.turns.push({ id: "pending", status: "SENDING", text });
    $("#input").value = "";
    autosize();
    render(true);
    await api().SendMessage(state.sessionId, text);
    await refreshSession(true);
    loadSessions();
  } catch (e) {
    state.turns = state.turns.filter((t) => t.id !== "pending");
    render();
    $("#input").value = text;
    toast("Send failed: " + errText(e), true);
  } finally {
    state.sending = false;
    updateComposer();
  }
}

async function command(runId, type, opts = {}) {
  const v = state.runs.get(runId);
  if (!v) return;
  const req = { type, expected_version: opts.version ? v.run.run.version : 0, reason: opts.reason || "", payload: opts.payload || {} };
  try {
    const cmd = await api().Command(runId, req);
    if (cmd && cmd.status === "REJECTED") toast(`${type} rejected: ${cmd.reject_reason || "no reason"}`, true);
  } catch (e) {
    toast(`${type.replaceAll("_", " ")} failed: ${errText(e)}`, true);
  }
  scheduleRun(runId);
}
function stop() {
  const t = activeTurn();
  if (t) command(t.run_id, "cancel", { reason: "stopped from desktop client" });
}

let reasonCallback = null;
function askReason(title, ok, cb) {
  $("#reason-title").textContent = title;
  $("#reason-ok").textContent = ok;
  $("#form-reason").reason.value = "";
  reasonCallback = cb;
  $("#dlg-reason").showModal();
}

// ---------- rendering ----------
function render(scrollToEnd) {
  if (state.view !== "chat") return;
  const sc = $("#scroll");
  const nearBottom = sc.scrollHeight - sc.scrollTop - sc.clientHeight < 80;
  renderHead();
  const thread = $("#thread");
  if (!state.turns.length) {
    thread.replaceChildren(h("div", { class: "hero" },
      h("div", { class: "logo" }, "◆"),
      h("h1", {}, "What's going on?"),
      h("div", { class: "muted" }, "Describe an incident. The agent investigates with tools and asks you before risky actions."),
      h("div", { class: "chips" }, SUGGESTIONS.map((s) => h("button", { class: "chip", onclick: () => send(s) }, s)))));
  } else {
    thread.replaceChildren(...state.turns.map(renderTurn));
  }
  $("#composer-wrap").hidden = false;
  updateComposer();
  if (scrollToEnd || nearBottom) sc.scrollTop = sc.scrollHeight;
}

function renderHead() {
  const head = $("#chat-head");
  if (!state.sessionId || !state.session) {
    head.className = "chat-head";
    return head.replaceChildren();
  }
  let cost = 0, profile = "";
  for (const t of state.turns) {
    const v = t.run_id && state.runs.get(t.run_id);
    if (!v) continue;
    cost += (v.cost && v.cost.cost_usd) || 0;
    profile = `${v.run.run.profile_id}@${v.run.run.profile_version}`;
  }
  head.className = "chat-head bordered";
  const sess = { id: state.sessionId, title: state.session.title, status: state.session.status };
  head.replaceChildren(...[
    h("span", { class: "title editable", title: "Rename", onclick: () => renameChat(sess) }, state.session.title || "Untitled chat"),
    sess.status === "ARCHIVED" ? h("span", { class: "badge s-idle" }, "archived") : null,
    h("span", { class: "meta" }, [profile, `${state.turns.length} turn${state.turns.length === 1 ? "" : "s"}`, cost ? `$${cost.toFixed(4)}` : ""].filter(Boolean).join(" · ")),
    h("span", { class: "spacer" }),
    h("button", { class: "head-btn", title: "Export as Markdown", onclick: () => exportChat(sess.id) }, "Export"),
    h("button", { class: "head-btn", title: "More", onclick: (e) => chatMenu(e.currentTarget, sess) }, "⋯")].filter(Boolean));
}

function renderTurn(t) {
  const queued = t.status === "QUEUED" || t.status === "SENDING";
  const out = h("div", { class: "turn" }, h("div", { class: "msg-user" }, h("div", { class: "bubble" + (queued ? " queued" : "") }, t.text || "…")));
  if (t.status === "SENDING") return out;
  if (t.status === "QUEUED") {
    out.append(h("div", { class: "queued-note" }, "Queued · starts when the current reply finishes"));
    return out;
  }
  const v = t.run_id && state.runs.get(t.run_id);
  out.append(h("div", { class: "msg-agent" },
    h("div", { class: "agent-mark" + (v && !TERMINAL.has(v.run.run.status) ? " busy" : "") }, "◆"),
    h("div", { class: "agent-body" }, v ? renderRun(v) : h("div", { class: "shimmer" }, "Starting…"))));
  return out;
}

function renderRun(v) {
  const run = v.run.run;
  const status = run.status;
  const done = TERMINAL.has(status);
  const steps = ((v.steps && v.steps.steps) || []).slice().sort((a, b) => a.seq - b.seq);
  const approvals = (v.approvals && v.approvals.approvals) || [];
  const tools = steps.filter((s) => s.kind === "tool").length;

  const choice = state.traceOpen.get(run.id);
  const open = choice !== undefined ? choice : !done || status === "FAILED";
  const label = done
    ? `Worked for ${dur(run.created_at, run.updated_at)}${tools ? ` · ${tools} tool call${tools > 1 ? "s" : ""}` : ""}`
    : status === "WAITING_APPROVAL" ? "Waiting for your approval" : status === "PAUSED" ? "Paused" : "Working…";
  const parts = [h("button", { class: "trace-toggle", onclick: () => { state.traceOpen.set(run.id, !open); render(); } },
    h("span", { class: "caret" + (open ? " open" : "") }, "▶"),
    h("span", { class: !done && status !== "WAITING_APPROVAL" && status !== "PAUSED" ? "shimmer" : "" }, label))];
  if (open && steps.length) parts.push(h("div", { class: "trace" }, steps.map((s) => renderStep(run, s, approvals))));

  const result = v.run.result;
  if (status === "COMPLETED" && result) {
    if (result.answer) parts.push(h("div", { class: "answer" }, md(result.answer)));
    const hyps = result.hypotheses || [];
    if (hyps.length) parts.push(h("div", { class: "hyps" }, hyps.map((x) => h("div", { class: "hyp" },
      badge(x.status), h("span", {}, x.statement), h("span", { class: "conf" }, Math.round((x.confidence_estimate || 0) * 100) + "%")))));
  } else if (status === "FAILED") {
    parts.push(h("div", { class: "status-line" }, "Failed" + (run.failure_reason ? ": " + run.failure_reason : "")));
  } else if (status === "CANCELLED") {
    parts.push(h("div", { class: "status-line stopped" }, "Stopped"));
  } else if (status === "NEEDS_HUMAN") {
    parts.push(h("div", { class: "status-line" }, "Needs a human: " + (run.failure_reason || "the agent could not continue safely")));
  }

  const meta = [h("span", {}, "run …" + run.id.slice(-8))];
  if (v.cost && v.cost.model_requests) meta.push(h("span", {}, `${v.cost.model_requests} model call${v.cost.model_requests > 1 ? "s" : ""}`),
    h("span", {}, `${(v.cost.input_tokens || 0) + (v.cost.output_tokens || 0)} tokens`));
  const models = Object.keys((v.cost && v.cost.cost_micros_by_model) || {});
  if (models.length) meta.push(h("span", { class: "model-tag", title: "model" }, models.join(", ")));
  if (status === "PAUSED") meta.push(h("button", { onclick: () => command(run.id, "resume", { version: true }) }, "Resume"));
  else if (!done) meta.push(h("button", { onclick: () => command(run.id, "pause") }, "Pause"));
  parts.push(h("div", { class: "turn-meta" }, meta));
  return parts;
}

function argsText(args) {
  if (!args || typeof args !== "object") return "";
  return Object.entries(args).map(([k, v]) => `${k}=${typeof v === "object" ? JSON.stringify(v) : v}`).join("  ");
}

function renderStep(run, s, approvals) {
  const out = s.output || {};
  const running = s.status === "RUNNING" || s.status === "PENDING" || s.status === "RETRY_WAIT";
  if (s.kind === "retrieve_memory") {
    const hits = Array.isArray(out.hits) ? out.hits.length : 0;
    return h("div", { class: "item" }, h("div", { class: "item-row" }, h("span", { class: "ico" }, "⌕"),
      h("span", { class: running ? "shimmer" : "what" }, running ? "Searching memory…" : "Searched memory"),
      !running && h("span", {}, hits ? `${hits} relevant` : "nothing relevant")));
  }
  if (s.kind === "reason") {
    const delta = (state.deltas.get(run.id) || new Map()).get(s.id);
    if (running) {
      return h("div", { class: "item" },
        h("div", { class: "item-row" }, h("span", { class: "ico" }, "✦"), h("span", { class: "shimmer" }, "Thinking…")),
        h("div", { class: "streaming", id: "delta-" + s.id }, delta || ""));
    }
    const summary = out.reason_summary || s.status_reason || "";
    return h("div", { class: "item" }, h("div", { class: "item-row" }, h("span", { class: "ico" }, "✦"),
      h("span", { class: "thought" }, summary || (out.kind === "final" ? "Concluded" : "Reasoned"))));
  }
  if (s.kind === "tool") {
    const inp = s.input || {};
    const name = inp.name || s.executor_name;
    const approval = approvals.find((a) => a.step_id === s.id && a.status === "PENDING");
    const waiting = s.status === "WAITING_APPROVAL";
    const isOpen = state.open.has(s.id);
    let st;
    if (waiting) st = h("span", { class: "state warn" }, "needs approval");
    else if (running) st = h("span", { class: "state shimmer" }, "running");
    else if (s.status === "SUCCEEDED") {
      const bad = out.outcome && out.outcome !== "ok";
      st = h("span", { class: "state " + (bad ? "warn" : "ok") }, bad ? out.outcome : "✓");
    } else st = h("span", { class: "state bad" }, (s.status || "").toLowerCase().replaceAll("_", " "));
    const card = h("div", { class: "tool" + (waiting ? " wait" : "") },
      h("div", { class: "tool-head", onclick: () => { isOpen ? state.open.delete(s.id) : state.open.add(s.id); render(); } },
        h("span", { class: "ico" }, "⚙"), h("span", { class: "name" }, name), h("span", { class: "args" }, argsText(inp.args)), st));
    if (isOpen) {
      const body = s.output ? (out.content !== undefined ? out.content : out) : s.status_reason || "no output yet";
      card.append(h("pre", {}, typeof body === "string" ? body : JSON.stringify(body, null, 2)));
    }
    if (waiting && approval) {
      const payload = { approval_request_id: approval.id, action_hash: approval.action_hash };
      card.append(h("div", { class: "approve-box" },
        h("div", { class: "why" }, h("strong", {}, "Approval required. "),
          `${name} is a ${approval.risk_class} action and needs ${approval.required_approvals} approval${approval.required_approvals > 1 ? "s" : ""}.`),
        h("div", { class: "kv" },
          h("span", {}, "target"), h("b", { class: "mono" }, approval.target || "—"),
          h("span", {}, "expires"), h("b", {}, approval.expires_at ? new Date(approval.expires_at).toLocaleString() : "—"),
          h("span", {}, "action"), h("b", { class: "mono" }, approval.action_hash)),
        h("div", { class: "approve-actions" },
          h("button", { class: "btn approve", onclick: () => command(run.id, "approve_tool", { version: true, payload }) }, "Approve"),
          h("button", { class: "btn danger", onclick: () => askReason("Reject " + name, "Reject",
            (reason) => command(run.id, "reject_tool", { version: true, reason, payload })) }, "Reject"))));
    }
    return h("div", { class: "item" }, card);
  }
  return h("div", { class: "item" }, h("div", { class: "item-row" }, h("span", { class: "ico" }, "•"),
    h("span", { class: "what" }, s.kind), h("span", {}, (s.status || "").toLowerCase())));
}

// ---------- composer ----------
function autosize() {
  const ta = $("#input");
  ta.style.height = "auto";
  ta.style.height = Math.min(ta.scrollHeight, 220) + "px";
}
function updateComposer() {
  const btn = $("#send");
  const busy = !!activeTurn();
  const hasText = $("#input").value.trim() !== "";
  if (busy && !hasText) {
    btn.textContent = "■";
    btn.title = "Stop";
    btn.className = "send stop";
    btn.disabled = false;
  } else {
    btn.textContent = "↑";
    btn.title = busy ? "Send (queued after the current reply)" : "Send";
    btn.className = "send";
    btn.disabled = !hasText || state.sending;
  }
  $("#hint").textContent = busy ? "Working… new messages are queued · ■ stops the current reply" : "Enter to send · Shift+Enter for a new line · / for skills and tools";
}

// ---------- slash picker: "/" inserts a skill or a tool call ----------
const slash = { open: false, items: [], index: 0, start: 0, end: 0, cache: null, cachedAt: 0 };

async function slashData() {
  if (slash.cache && Date.now() - slash.cachedAt < 30000) return slash.cache;
  const [ext, tl] = await Promise.all([api().Extensions().catch(() => ({})), api().Tools().catch(() => ({}))]);
  const skills = (ext.skills || []).filter((x) => x.enabled).map((x) => ({ kind: "skill", name: x.name,
    desc: x.spec.description || "", manual: x.spec.mode === "manual" }));
  const tools = (tl.tools || []).filter((d) => d.allowed_by_profile).map((d) => ({ kind: "tool", name: d.name,
    desc: (d.description || "").replace(/^\[[^\]]*\]\s*/, ""), risk: d.risk, schema: d.input_schema, target: d.target_arg }));
  slash.cache = { skills, tools };
  slash.cachedAt = Date.now();
  return slash.cache;
}

function slashToken(ta) {
  const pos = ta.selectionStart;
  if (pos !== ta.selectionEnd) return null;
  const before = ta.value.slice(0, pos);
  const m = before.match(/(^|\s)\/([\w.\-]*)$/);
  return m ? { start: pos - m[2].length - 1, end: pos, query: m[2].toLowerCase() } : null;
}

async function updateSlash() {
  const ta = $("#input");
  const tok = slashToken(ta);
  if (!tok) return closeSlash();
  let data;
  try { data = await slashData(); } catch { return closeSlash(); }
  if (slashToken(ta)?.start !== tok.start) return; // input moved on meanwhile
  const q = tok.query;
  const match = (x) => !q || x.name.toLowerCase().includes(q) || x.desc.toLowerCase().includes(q);
  const items = [...data.skills.filter(match), ...data.tools.filter(match)].slice(0, 30);
  if (!items.length) return closeSlash();
  Object.assign(slash, { open: true, items, start: tok.start, end: tok.end, index: Math.min(slash.index, items.length - 1) });
  renderSlash();
}

function closeSlash() {
  slash.open = false;
  slash.index = 0;
  const el = $("#slash");
  if (el) el.hidden = true;
}

function renderSlash() {
  const el = $("#slash");
  let lastKind = null;
  const rows = [];
  slash.items.forEach((it, i) => {
    if (it.kind !== lastKind) {
      rows.push(h("div", { class: "slash-group" }, it.kind === "skill" ? "Skills" : "Tools · inserts “use <tool> {…}”"));
      lastKind = it.kind;
    }
    rows.push(h("div", { class: "slash-item" + (i === slash.index ? " on" : ""), onmousedown: (e) => { e.preventDefault(); pickSlash(i); },
      onmousemove: () => { if (slash.index !== i) { slash.index = i; renderSlash(); } } },
      h("span", { class: "mono" }, it.kind === "skill" ? "/" + it.name : it.name),
      it.kind === "skill" ? h("span", { class: "hint-tag" }, it.manual ? "this chat" : "always on") : h("span", { class: "risk-tag r" + (it.risk || "R0").slice(1) }, it.risk),
      h("span", { class: "muted small slash-desc" }, it.desc)));
  });
  el.replaceChildren(...rows);
  el.hidden = false;
  const on = el.querySelector(".slash-item.on");
  if (on) on.scrollIntoView({ block: "nearest" });
}

function argSkeleton(it) {
  let schema = it.schema;
  if (typeof schema === "string") { try { schema = JSON.parse(schema); } catch { schema = null; } }
  const keys = (schema && (schema.required || Object.keys(schema.properties || {}).slice(0, 2))) || [];
  const all = keys.length ? keys : (it.target ? it.target.split(",") : []);
  const obj = {};
  for (const k of all) obj[k.trim()] = "";
  return JSON.stringify(obj);
}

function pickSlash(i) {
  const it = slash.items[i];
  const ta = $("#input");
  const ins = it.kind === "skill" ? `/${it.name} ` : `use ${it.name} ${argSkeleton(it)}`;
  ta.value = ta.value.slice(0, slash.start) + ins + ta.value.slice(slash.end);
  // Put the caret inside the first "" of the arguments, if any.
  let caret = slash.start + ins.length;
  const q = ins.indexOf('""');
  if (it.kind === "tool" && q >= 0) caret = slash.start + q + 1;
  ta.focus();
  ta.setSelectionRange(caret, caret);
  closeSlash();
  autosize();
  updateComposer();
}

function slashKey(e) {
  if (!slash.open) return false;
  if (e.key === "ArrowDown" || e.key === "ArrowUp") {
    e.preventDefault();
    const n = slash.items.length;
    slash.index = (slash.index + (e.key === "ArrowDown" ? 1 : n - 1)) % n;
    renderSlash();
    return true;
  }
  if ((e.key === "Enter" || e.key === "Tab") && !e.isComposing && e.keyCode !== 229) {
    e.preventDefault();
    pickSlash(slash.index);
    return true;
  }
  if (e.key === "Escape") {
    e.preventDefault();
    closeSlash();
    return true;
  }
  return false;
}

// ---------- approvals inbox ----------
function updateInboxBadge() {
  const n = state.approvals.length;
  const el = $("#inbox-count");
  el.textContent = String(n);
  el.hidden = n === 0;
}

function showInbox() {
  state.view = "inbox";
  markSide();
  stopWatching();
  renderSessions();
  $("#chat-head").replaceChildren();
  $("#chat-head").className = "chat-head";
  $("#composer-wrap").hidden = true;
  renderInbox();
  api().RefreshApprovals();
}

function renderInbox() {
  if (state.view !== "inbox") return;
  const list = state.approvals;
  const cards = list.map((a) => h("div", { class: "inbox-card" },
    h("div", { class: "inbox-top" },
      h("span", { class: "risk-tag r" + String(a.risk_class || "R0").slice(1) }, a.risk_class),
      h("span", { class: "mono" }, a.tool_name),
      a.target ? h("span", { class: "muted" }, "on ", h("b", { class: "mono" }, a.target)) : null,
      h("span", { class: "spacer" }),
      h("span", { class: "muted small", title: new Date(a.created_at).toLocaleString() }, ago(a.created_at))),
    h("div", { class: "muted small" },
      a.session_title ? ["in “", a.session_title, "” · "] : ["run …", a.run_id.slice(-8), " · "],
      a.initiator_id ? `asked by ${a.initiator_id} · ` : "",
      `expires ${new Date(a.expires_at).toLocaleTimeString()}`,
      a.required_approvals > 1 ? ` · needs ${a.required_approvals} approvals` : ""),
    h("div", { class: "mono tiny-hash" }, a.action_hash),
    h("div", { class: "approve-actions" },
      h("button", { class: "btn approve", onclick: (e) => decide(e.currentTarget, a, true) }, "Approve"),
      h("button", { class: "btn", onclick: (e) => decide(e.currentTarget, a, false) }, "Reject"),
      h("span", { class: "spacer" }),
      a.session_id ? h("button", { class: "link-btn", onclick: () => openSession(a.session_id) }, "Open chat ›") : null)));
  $("#thread").replaceChildren(h("div", { class: "customize" },
    h("h1", {}, "Approvals"),
    h("p", { class: "muted lead" }, "Tool calls waiting for a decision in any chat of this tenant. New ones also arrive as system notifications."),
    list.length ? cards : h("div", { class: "empty-card muted" }, "Nothing is waiting for approval.")));
}

async function decide(btn, a, approve) {
  let reason = "";
  if (!approve) {
    reason = await ask({ title: "Reject " + a.tool_name, label: "Reason (optional)", ok: "Reject", danger: true });
    if (reason == null) return;
  }
  btn.disabled = true;
  try {
    const cmd = await api().ApproveFromInbox(a.run_id, a.id, a.action_hash, approve, reason);
    if (cmd && cmd.status === "REJECTED") toast(`Not applied: ${cmd.reject_reason || "rejected"}`, true);
    else toast(approve ? "Approved" : "Rejected");
    state.approvals = state.approvals.filter((x) => x.id !== a.id);
    updateInboxBadge();
    renderInbox();
  } catch (e) {
    toast(errText(e), true);
    btn.disabled = false;
  }
  api().RefreshApprovals();
}

// ---------- plugins ----------
async function showPlugins() {
  state.view = "plugins";
  markSide();
  stopWatching();
  renderSessions();
  $("#chat-head").replaceChildren();
  $("#chat-head").className = "chat-head";
  $("#composer-wrap").hidden = true;
  const thread = $("#thread");
  thread.replaceChildren(h("div", { class: "plugins" }, h("h1", {}, "Plugins"), h("div", { class: "muted" }, "Loading…")));
  let data;
  try {
    data = await api().Plugins();
  } catch (e) {
    const msg = errText(e);
    return thread.replaceChildren(h("div", { class: "plugins" }, h("h1", {}, "Plugins"),
      h("div", { class: "error" }, msg + (msg.includes("403") ? "\nThe plugin inventory needs the admin role." : ""))));
  }
  const active = new Set((data.active || []).map((p) => p.kind + "/" + p.name));
  const body = (data.kinds || []).map((k) => {
    const list = (data.compiled || []).filter((p) => p.kind === k.name);
    return h("div", { class: "kind" }, h("h3", {}, k.name, k.multi ? " · multiple" : ""), h("div", { class: "muted small" }, k.doc || ""),
      list.length > 0 && h("table", {}, h("tbody", {}, list.map((p) => h("tr", {},
        h("td", { class: "mono" }, p.name),
        h("td", {}, active.has(p.kind + "/" + p.name) ? badge("ACTIVE") : h("span", { class: "muted small" }, "available")),
        h("td", { class: "muted" }, p.doc || ""))))));
  });
  if ((data.tools || []).length) body.push(h("div", { class: "kind" }, h("h3", {}, "registered tools"), h("div", { class: "mono" }, data.tools.join("   "))));
  thread.replaceChildren(h("div", { class: "plugins" }, h("h1", {}, "Plugins"), body));
}

// ---------- customize: MCP servers, HTTP tools, skills, tools ----------
const RISKS = ["R0", "R1", "R2", "R3"];
const RISK_DOC = { R0: "read-only", R1: "low-impact write", R2: "change · needs approval", R3: "dangerous · needs approval" };
const TABS = [["model", "Model"], ["mcp", "MCP servers"], ["http", "HTTP tools"], ["skills", "Skills"], ["tools", "Tools"]];
const cz = { tab: "model", data: null, tools: null, edit: null };

function riskSelect(value, onchange) {
  const sel = h("select", { class: "risk", onchange: (e) => onchange(e.target.value) },
    RISKS.map((r) => h("option", { value: r, selected: r === value }, `${r} · ${RISK_DOC[r]}`)));
  return sel;
}
function statusBadge(x) {
  if (!x.enabled) return h("span", { class: "badge s-idle" }, "disabled");
  const st = x.status;
  if (!st) return h("span", { class: "badge s-idle" }, "syncing");
  if (st.state === "ready") return h("span", { class: "badge s-ok" }, "ready");
  if (st.state === "error") return h("span", { class: "badge s-bad", title: st.error || "" }, "error");
  return h("span", { class: "badge s-idle" }, st.state);
}
// kv editor rows for headers: [{k, v, saved}]
function kvEditor(rows, placeholderKey) {
  const box = h("div", { class: "kv-edit" });
  const draw = () => {
    box.replaceChildren(...rows.map((r, i) => h("div", { class: "kv-row" },
      h("input", { value: r.k, placeholder: placeholderKey, spellcheck: "false", oninput: (e) => (r.k = e.target.value) }),
      h("input", { value: r.v, type: "password", autocomplete: "off", placeholder: r.saved ? "saved · leave blank to keep" : "value",
        oninput: (e) => (r.v = e.target.value) }),
      h("button", { type: "button", class: "icon-btn", title: "Remove", onclick: () => { rows.splice(i, 1); draw(); } }, "×"))),
      h("button", { type: "button", class: "link-btn", onclick: () => { rows.push({ k: "", v: "" }); draw(); } }, "+ Add header"));
  };
  draw();
  return box;
}
const kvToMap = (rows) => Object.fromEntries(rows.filter((r) => r.k.trim()).map((r) => [r.k.trim(), r.v]));

async function showCustomize(tab) {
  if (tab) cz.tab = tab;
  state.view = "customize";
  stopWatching();
  renderSessions();
  $("#chat-head").replaceChildren();
  $("#chat-head").className = "chat-head";
  $("#composer-wrap").hidden = true;
  markSide();
  renderCustomize(true);
  try {
    const [data, tools] = await Promise.all([api().Extensions(), api().Tools()]);
    cz.data = data;
    cz.tools = tools;
    cz.error = null;
  } catch (e) {
    cz.error = errText(e);
  }
  renderCustomize();
}

function renderCustomize(loading) {
  if (state.view !== "customize") return;
  const tabs = h("div", { class: "tabs" }, TABS.map(([id, label]) =>
    h("button", { class: "tab" + (cz.tab === id ? " on" : ""), onclick: () => { cz.tab = id; cz.edit = null; renderCustomize(); } }, label,
      cz.data && id !== "tools" && id !== "model" ? h("span", { class: "count" }, String(listFor(id).length)) : null)));
  let body;
  if (loading && !cz.data) body = h("div", { class: "muted" }, "Loading…");
  else if (cz.error) body = h("div", { class: "error" }, cz.error);
  else if (cz.edit) body = cz.edit.render();
  else if (cz.tab === "tools") body = renderToolsTab();
  else if (cz.tab === "model") body = renderModelTab();
  else body = renderExtList(cz.tab);
  $("#thread").replaceChildren(h("div", { class: "customize" },
    h("h1", {}, "Customize"),
    h("p", { class: "muted lead" }, "Choose the model, connect MCP servers, add HTTP tools and write skills for every chat in this tenant. ",
      "Tools still run under the profile allowlist, policy and approvals. Changes need the admin role."),
    tabs, body));
}

function listFor(tab) {
  const d = cz.data || {};
  return (tab === "mcp" ? d.mcp_servers : tab === "http" ? d.http_tools : tab === "model" ? d.models : d.skills) || [];
}

function renderExtList(tab) {
  const items = listFor(tab);
  const add = { mcp: ["Add MCP server", () => editMCP(null)], http: ["Add HTTP tool", () => editHTTP(null)], skills: ["New skill", () => editSkill(null)] }[tab];
  const empty = {
    mcp: "No MCP servers yet. Add a streamable-HTTP server, pick which of its tools the agent may use and give each a risk class.",
    http: "No HTTP tools yet. An HTTP tool POSTs the call's JSON arguments to your endpoint and returns the response.",
    skills: "No skills yet. A skill is a playbook (instructions) added to the agent's system prompt in every run.",
  }[tab];
  return h("div", {},
    h("div", { class: "list-bar" }, h("button", { class: "btn primary", onclick: add[1] }, add[0])),
    items.length === 0 ? h("div", { class: "empty-card muted" }, empty) :
      items.map((x) => h("div", { class: "ext-card", onclick: () => (tab === "mcp" ? editMCP(x) : tab === "http" ? editHTTP(x) : editSkill(x)) },
        h("div", { class: "ext-main" },
          h("div", { class: "ext-title" }, h("span", { class: "mono" }, x.name), statusBadge(x)),
          h("div", { class: "muted small" }, extSummary(tab, x)),
          x.status && x.status.error ? h("div", { class: "error small" }, x.status.error) : null),
        h("div", { class: "ext-side muted small" }, "v" + x.version, " · ", x.updated_by || "", " · ", ago(x.updated_at)))));
}

function extSummary(tab, x) {
  const s = x.spec || {};
  if (tab === "mcp") {
    const allowed = Object.keys(s.allow || {});
    const loaded = (x.status && x.status.tools) || [];
    return [s.url || (s.command ? "$ " + s.command + " " + (s.args || []).join(" ") : ""),
      `${allowed.length} tool${allowed.length === 1 ? "" : "s"} allowed` + (x.status && x.status.state === "ready" ? `, ${loaded.length} loaded` : ""),
      (x.status && (x.status.missing || []).length) ? "missing on server: " + x.status.missing.join(", ") : ""].filter(Boolean).join(" · ");
  }
  if (tab === "http") return `${s.method || "POST"} ${s.url} · ${s.risk} · http.${x.name}`;
  return (s.mode === "manual" ? `/${x.name} · ` : "always on · ") + (s.description || "");
}

function editorShell(title, fields, actions) {
  return h("div", { class: "editor" },
    h("div", { class: "editor-head" }, h("button", { class: "link-btn", onclick: () => { cz.edit = null; renderCustomize(); } }, "‹ Back"), h("h2", {}, title)),
    fields, h("div", { class: "actions" }, actions));
}

async function saveExt(kind, name, spec, enabled, version) {
  try {
    await api().PutExtension(kind, name, spec, enabled, version || 0);
    toast("Saved " + name);
    cz.edit = null;
    await showCustomize();
  } catch (e) {
    const m = errText(e);
    toast(m.includes("403") ? "Changing extensions needs the admin role." : m, true);
  }
}
async function deleteExt(kind, name) {
  if (!(await ask({ title: `Delete ${name}?`, text: "Runs will no longer see it.", ok: "Delete", danger: true }))) return;
  try {
    await api().DeleteExtension(kind, name);
    toast("Deleted " + name);
    cz.edit = null;
    await showCustomize();
  } catch (e) {
    toast(errText(e), true);
  }
}

// --- MCP server editor: connect, list remote tools, choose and classify ---
function editMCP(x) {
  const spec = (x && x.spec) || {};
  const f = {
    name: x ? x.name : "", url: spec.url || "", enabled: x ? x.enabled : true,
    headers: Object.keys(spec.headers || {}).map((k) => ({ k, v: "", saved: true })),
    allow: JSON.parse(JSON.stringify(spec.allow || {})),
    remote: null, probing: false, probeError: null,
    command: spec.command || "", args: (spec.args || []).join(" "), callTimeout: spec.call_timeout || "",
  };
  const buildSpec = (withAllow) => {
    const out = { allow: withAllow ? f.allow : {} };
    if (f.url.trim()) out.url = f.url.trim();
    if (f.command.trim()) { out.command = f.command.trim(); if (f.args.trim()) out.args = f.args.trim().split(/\s+/); }
    const hd = kvToMap(f.headers);
    if (Object.keys(hd).length) out.headers = hd;
    if (f.callTimeout.trim()) out.call_timeout = f.callTimeout.trim();
    return out;
  };
  const probe = async () => {
    if (!f.name.trim()) return toast("Give the server a name first.", true);
    f.probing = true; f.probeError = null; renderCustomize();
    try {
      const r = await api().ProbeMCP(f.name.trim(), buildSpec(false));
      f.remote = r.tools || [];
    } catch (e) {
      f.probeError = errText(e);
    }
    f.probing = false;
    renderCustomize();
  };
  cz.edit = {
    render() {
      const nameIn = h("input", { value: f.name, placeholder: "e.g. grafana", spellcheck: "false", disabled: !!x, oninput: (e) => (f.name = e.target.value) });
      const remote = f.remote || Object.keys(f.allow).map((n) => ({ name: n, description: "", input_schema: null, stored: true }));
      const toolRows = remote.map((t) => {
        const pol = f.allow[t.name];
        const props = Object.keys((t.input_schema && t.input_schema.properties) || {});
        const def = t.read_only_hint ? "R0" : "R2";
        return h("tr", { class: pol ? "" : "off" },
          h("td", {}, h("input", { type: "checkbox", checked: !!pol, onchange: (e) => {
            if (e.target.checked) f.allow[t.name] = { risk: def }; else delete f.allow[t.name];
            renderCustomize();
          } })),
          h("td", {}, h("div", { class: "mono" }, t.name), h("div", { class: "muted small" }, t.description || (t.stored ? "(connect to refresh)" : "")),
            t.read_only_hint ? h("span", { class: "hint-tag" }, "read-only hint") : null,
            t.destructive_hint ? h("span", { class: "hint-tag bad" }, "destructive hint") : null),
          h("td", {}, pol ? riskSelect(pol.risk, (v) => (pol.risk = v)) : h("span", { class: "muted small" }, "not exposed")),
          h("td", {}, pol && props.length ? h("select", { onchange: (e) => { if (e.target.value) pol.target_arg = e.target.value; else delete pol.target_arg; } },
            h("option", { value: "" }, "no target"), props.map((p) => h("option", { value: p, selected: pol.target_arg === p }, p))) : null));
      });
      const fields = h("div", {},
        h("div", { class: "grid2" },
          h("label", {}, "Name ", nameIn, h("span", { class: "muted small" }, "Tools appear as mcp.", f.name || "<name>", ".<tool>")),
          h("label", {}, "Server URL (streamable HTTP)", h("input", { value: f.url, placeholder: "http://127.0.0.1:8090/mcp", spellcheck: "false", oninput: (e) => (f.url = e.target.value) }))),
        h("label", {}, "Headers (e.g. Authorization) — stored on the server, never shown again"),
        kvEditor(f.headers, "Header"),
        h("details", { class: "adv" }, h("summary", {}, "Advanced"),
          h("div", { class: "grid2" },
            h("label", {}, "Local command (stdio; the server must set HARNESS_MCP_ALLOW_STDIO)", h("input", { value: f.command, placeholder: "npx", oninput: (e) => (f.command = e.target.value) })),
            h("label", {}, "Arguments", h("input", { value: f.args, placeholder: "-y @scope/server", oninput: (e) => (f.args = e.target.value) })),
            h("label", {}, "Call timeout", h("input", { value: f.callTimeout, placeholder: "30s", oninput: (e) => (f.callTimeout = e.target.value) })))),
        h("div", { class: "probe-bar" },
          h("button", { class: "btn", type: "button", disabled: f.probing, onclick: probe }, f.probing ? "Connecting…" : (f.remote ? "Reconnect" : "Connect & list tools")),
          h("span", { class: "muted small" }, f.remote ? `${f.remote.length} tools on the server · tick the ones the agent may call` : "Only ticked tools are exposed to the agent.")),
        f.probeError ? h("div", { class: "error small" }, f.probeError) : null,
        remote.length ? h("table", { class: "tool-table" }, h("thead", {}, h("tr", {}, h("th", {}), h("th", {}, "Tool"), h("th", {}, "Risk"), h("th", {}, "Target arg"))), h("tbody", {}, toolRows)) : null,
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " Enabled"));
      return editorShell(x ? "MCP server · " + x.name : "Add MCP server", fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("mcp-servers", x.name) }, "Delete") : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, "Cancel"),
        h("button", { class: "btn primary", onclick: () => {
          if (!f.name.trim()) return toast("Name is required.", true);
          if (!Object.keys(f.allow).length) return toast("Tick at least one tool (Connect & list tools).", true);
          saveExt("mcp-servers", f.name.trim(), buildSpec(true), f.enabled, x && x.version);
        } }, "Save")]);
    },
  };
  renderCustomize();
  if (x) probe();
}

// --- HTTP tool editor ---
function editHTTP(x) {
  const s = (x && x.spec) || {};
  const f = {
    name: x ? x.name : "", url: s.url || "", method: s.method || "POST", description: s.description || "", risk: s.risk || "R0",
    targetArg: s.target_arg || "", timeout: s.timeout || "", enabled: x ? x.enabled : true,
    headers: Object.keys(s.headers || {}).map((k) => ({ k, v: "", saved: true })),
    schema: s.input_schema ? JSON.stringify(s.input_schema, null, 2) : '{\n  "type": "object",\n  "properties": {\n    "service": {"type": "string"}\n  },\n  "required": ["service"]\n}',
  };
  cz.edit = {
    render() {
      const fields = h("div", {},
        h("div", { class: "grid2" },
          h("label", {}, "Name ", h("input", { value: f.name, disabled: !!x, placeholder: "e.g. deploy_history", spellcheck: "false", oninput: (e) => (f.name = e.target.value) }),
            h("span", { class: "muted small" }, "The tool is called http.", f.name || "<name>")),
          h("label", {}, "Risk", riskSelect(f.risk, (v) => (f.risk = v)))),
        h("label", {}, "Description (shown to the model)", h("input", { value: f.description, placeholder: "Recent deploys of a service", oninput: (e) => (f.description = e.target.value) })),
        h("div", { class: "grid-url" },
          h("label", {}, "Method", h("select", { onchange: (e) => (f.method = e.target.value) }, ["POST", "GET"].map((m) => h("option", { selected: m === f.method }, m)))),
          h("label", {}, "Endpoint URL", h("input", { value: f.url, placeholder: "https://ops.internal/api/deploys", spellcheck: "false", oninput: (e) => (f.url = e.target.value) }))),
        h("label", {}, "Headers — stored on the server, never shown again"),
        kvEditor(f.headers, "Header"),
        h("label", {}, "Input schema (JSON Schema, type object)", h("textarea", { rows: 7, class: "mono", spellcheck: "false", oninput: (e) => (f.schema = e.target.value) }, f.schema)),
        h("div", { class: "grid2" },
          h("label", {}, "Target argument (policy scope, optional)", h("input", { value: f.targetArg, placeholder: "service", oninput: (e) => (f.targetArg = e.target.value) })),
          h("label", {}, "Timeout", h("input", { value: f.timeout, placeholder: "15s", oninput: (e) => (f.timeout = e.target.value) }))),
        h("p", { class: "muted small" }, "GET tools must be R0. Calls carry Idempotency-Key, X-Harness-Run-ID and X-Harness-Step-ID headers."),
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " Enabled"));
      return editorShell(x ? "HTTP tool · " + x.name : "Add HTTP tool", fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("http-tools", x.name) }, "Delete") : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, "Cancel"),
        h("button", { class: "btn primary", onclick: () => {
          let schema;
          try { schema = JSON.parse(f.schema || "{}"); } catch (e) { return toast("Input schema is not valid JSON: " + errText(e), true); }
          const spec = { description: f.description, url: f.url.trim(), method: f.method, risk: f.risk, input_schema: schema };
          const hd = kvToMap(f.headers);
          if (Object.keys(hd).length) spec.headers = hd;
          if (f.targetArg.trim()) spec.target_arg = f.targetArg.trim();
          if (f.timeout.trim()) spec.timeout = f.timeout.trim();
          saveExt("http-tools", f.name.trim(), spec, f.enabled, x && x.version);
        } }, "Save")]);
    },
  };
  renderCustomize();
}

// --- skill editor ---
function editSkill(x) {
  const s = (x && x.spec) || {};
  const f = { name: x ? x.name : "", description: s.description || "", instructions: s.instructions || "", mode: s.mode || "always", enabled: x ? x.enabled : true };
  cz.edit = {
    render() {
      const fields = h("div", {},
        h("div", { class: "grid2" },
          h("label", {}, "Name", h("input", { value: f.name, disabled: !!x, placeholder: "e.g. db-triage", spellcheck: "false", oninput: (e) => (f.name = e.target.value) })),
          h("label", {}, "When to use it", h("input", { value: f.description, placeholder: "Database incidents", oninput: (e) => (f.description = e.target.value) }))),
        h("label", {}, "Instructions (Markdown) — added to the system prompt of every run",
          h("textarea", { rows: 14, class: "mono", placeholder: "1. Search the runbooks first (use mcp.demo.runbook_search).\n2. Check connection-pool metrics before suggesting a restart.\n3. Cite the runbook you followed.",
            oninput: (e) => { f.instructions = e.target.value; $("#skill-size").textContent = sizeText(f.instructions); } }, f.instructions)),
        h("div", { class: "muted small", id: "skill-size" }, sizeText(f.instructions)),
        h("div", { class: "seg" },
          h("label", {}, h("input", { type: "radio", name: "mode", checked: f.mode !== "manual", onchange: () => (f.mode = "always") }), " Always on (every run)"),
          h("label", {}, h("input", { type: "radio", name: "mode", checked: f.mode === "manual", onchange: () => (f.mode = "manual") }),
            " Only in chats that invoke it with /", f.name || "name")),
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " Enabled"));
      return editorShell(x ? "Skill · " + x.name : "New skill", fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("skills", x.name) }, "Delete") : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, "Cancel"),
        h("button", { class: "btn primary", onclick: () => saveExt("skills", f.name.trim(), { description: f.description, instructions: f.instructions, mode: f.mode }, f.enabled, x && x.version) }, "Save")]);
    },
  };
  renderCustomize();
}
const sizeText = (t) => `${new TextEncoder().encode(t || "").length.toLocaleString()} / 16,384 bytes`;

// --- model: which LLM the agent uses for this tenant ---
const ANTHROPIC_MODELS = [["claude-opus-5-5", "Claude Opus 5.5 · most capable"], ["claude-sonnet-5-5", "Claude Sonnet 5.5 · balanced"],
  ["claude-haiku-5-5", "Claude Haiku 5.5 · fastest"]];
const EFFORTS = ["", "low", "medium", "high", "xhigh", "max"];

function renderModelTab() {
  const cur = (cz.data && cz.data.current_model) || {};
  const models = listFor("model");
  const curBox = h("div", { class: "current-model" + (cur.tenant_configured ? " on" : "") },
    h("div", { class: "muted small" }, "New runs use"),
    h("div", { class: "cm-name" }, cur.model || "—"),
    h("div", { class: "muted small" }, cur.error ? cur.error :
      cur.tenant_configured ? "configured for this tenant" :
      cur.model === "rule-based-sre" ? "the built-in scripted demo model — it only calls tools when you write “use <tool> {json}”. Add a model below to let the agent reason and pick tools itself." :
      "the deployment's default model"));
  return h("div", {}, curBox,
    h("div", { class: "list-bar" }, h("button", { class: "btn primary", onclick: () => editModel(null) }, models.length ? "Add another model" : "Add a model")),
    models.length === 0 ? null : models.map((x) => h("div", { class: "ext-card", onclick: () => editModel(x) },
      h("div", { class: "ext-main" },
        h("div", { class: "ext-title" }, h("span", { class: "mono" }, x.name), statusBadge(x)),
        h("div", { class: "muted small" }, (x.spec.provider === "anthropic" ? "Anthropic · " : "OpenAI-compatible · ") + x.spec.model +
          (x.spec.base_url ? " · " + x.spec.base_url : "") + (x.spec.effort ? " · effort " + x.spec.effort : ""))),
      h("div", { class: "ext-side muted small" }, "v" + x.version, " · ", x.updated_by || "", " · ", ago(x.updated_at)))),
    h("p", { class: "muted small" }, "The key is stored on the server and used only for this tenant's runs; it is never sent back to clients. ",
      "If several models are enabled, the first by name is preferred and the others are fallbacks."));
}

function editModel(x) {
  const s = (x && x.spec) || {};
  const f = { name: x ? x.name : "claude", provider: s.provider || "anthropic", model: s.model || "claude-opus-5-5", apiKey: "",
    baseURL: s.base_url || "", effort: s.effort || "", ctx: s.context_window || "", maxOut: s.max_output || "", enabled: x ? x.enabled : true,
    testing: false, test: null, testErr: null };
  const spec = () => {
    const o = { provider: f.provider, model: f.model.trim(), api_key: f.apiKey };
    if (f.baseURL.trim()) o.base_url = f.baseURL.trim();
    if (f.provider === "anthropic" && f.effort) o.effort = f.effort;
    if (f.provider === "openai") {
      if (+f.ctx > 0) o.context_window = +f.ctx;
      if (+f.maxOut > 0) o.max_output = +f.maxOut;
    }
    return o;
  };
  const test = async () => {
    if (!f.name.trim()) return toast("Name is required.", true);
    f.testing = true; f.test = null; f.testErr = null; renderCustomize();
    try { f.test = await api().TestModel(f.name.trim(), spec()); } catch (e) { f.testErr = errText(e); }
    f.testing = false; renderCustomize();
  };
  cz.edit = {
    render() {
      const anth = f.provider === "anthropic";
      const known = ANTHROPIC_MODELS.some(([id]) => id === f.model);
      const fields = h("div", {},
        h("div", { class: "seg" }, [["anthropic", "Anthropic (Claude)"], ["openai", "OpenAI-compatible (vLLM, Ollama, …)"]].map(([v, l]) =>
          h("label", {}, h("input", { type: "radio", name: "prov", checked: f.provider === v, disabled: false, onchange: () => {
            f.provider = v; f.model = v === "anthropic" ? "claude-opus-5-5" : ""; f.test = f.testErr = null; renderCustomize();
          } }), " ", l))),
        h("div", { class: "grid2" },
          h("label", {}, "Name", h("input", { value: f.name, disabled: !!x, spellcheck: "false", oninput: (e) => (f.name = e.target.value) })),
          anth ? h("label", {}, "Model", h("select", { onchange: (e) => { f.model = e.target.value; renderCustomize(); } },
              ANTHROPIC_MODELS.map(([id, l]) => h("option", { value: id, selected: id === f.model }, l)),
              h("option", { value: known ? "custom" : f.model, selected: !known }, known ? "Other…" : f.model)))
            : h("label", {}, "Model", h("input", { value: f.model, placeholder: "qwen3-32b", spellcheck: "false", oninput: (e) => (f.model = e.target.value) }))),
        anth && !known ? h("label", {}, "Model ID", h("input", { value: f.model === "custom" ? "" : f.model, placeholder: "claude-…", oninput: (e) => (f.model = e.target.value) })) : null,
        h("label", {}, anth ? "Anthropic API key" : "API key (optional for self-hosted servers)",
          h("input", { type: "password", autocomplete: "off", value: f.apiKey, placeholder: x && x.spec.api_key !== undefined ? "saved · leave blank to keep" : (anth ? "sk-ant-…" : ""),
            oninput: (e) => (f.apiKey = e.target.value) })),
        h("div", { class: "grid2" },
          h("label", {}, anth ? "Base URL (optional)" : "Base URL", h("input", { value: f.baseURL, placeholder: anth ? "https://api.anthropic.com" : "http://127.0.0.1:8000/v1",
            spellcheck: "false", oninput: (e) => (f.baseURL = e.target.value) })),
          anth ? h("label", {}, "Effort", h("select", { onchange: (e) => (f.effort = e.target.value) },
              EFFORTS.map((v) => h("option", { value: v, selected: v === f.effort }, v || "default"))))
            : h("div", { class: "grid2" },
              h("label", {}, "Context window", h("input", { value: f.ctx, placeholder: "128000", oninput: (e) => (f.ctx = e.target.value) })),
              h("label", {}, "Max output", h("input", { value: f.maxOut, placeholder: "16000", oninput: (e) => (f.maxOut = e.target.value) })))),
        h("div", { class: "probe-bar" },
          h("button", { class: "btn", type: "button", disabled: f.testing, onclick: test }, f.testing ? "Testing…" : "Test connection"),
          f.test ? h("span", { class: "ok small" }, `✓ ${f.test.model} replied “${f.test.reply}” in ${f.test.latency_ms} ms`) : null),
        f.testErr ? h("div", { class: "error small" }, f.testErr) : null,
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " Enabled"));
      return editorShell(x ? "Model · " + x.name : "Add a model", fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("models", x.name) }, "Delete") : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, "Cancel"),
        h("button", { class: "btn primary", onclick: () => {
          if (!f.name.trim() || !f.model.trim() || f.model === "custom") return toast("Name and model are required.", true);
          saveExt("models", f.name.trim(), spec(), f.enabled, x && x.version);
        } }, "Save")]);
    },
  };
  renderCustomize();
}

// --- tools: everything a run of this tenant can be offered ---
function renderToolsTab() {
  const t = cz.tools || {};
  const list = t.tools || [];
  const src = (d) => d.source || "built-in";
  return h("div", {},
    h("p", { class: "muted small" }, "Default profile ", h("span", { class: "mono" }, t.profile || "?"),
      ". Tools it does not allow are hidden from runs that use it. In a chat, “use <tool> {json}” asks the agent to call a tool."),
    h("table", { class: "tool-table" },
      h("thead", {}, h("tr", {}, h("th", {}, "Tool"), h("th", {}, "Source"), h("th", {}, "Risk"), h("th", {}, "Profile"))),
      h("tbody", {}, list.map((d) => h("tr", { class: d.allowed_by_profile ? "" : "off" },
        h("td", {}, h("div", { class: "mono" }, d.name), h("div", { class: "muted small" }, (d.description || "").replace(/^\[[^\]]*\]\s*/, ""))),
        h("td", { class: "small" }, src(d)),
        h("td", {}, h("span", { class: "risk-tag r" + (d.risk || "R0").slice(1) }, d.risk), d.trust ? h("span", { class: "muted small" }, " " + d.trust) : null),
        h("td", {}, d.allowed_by_profile ? h("span", { class: "badge s-ok" }, "allowed") : h("span", { class: "badge s-idle" }, "blocked")),
      )))));
}

function markSide() {
  $("#btn-inbox").classList.toggle("on", state.view === "inbox");
  $("#btn-customize").classList.toggle("on", state.view === "customize");
  $("#btn-plugins").classList.toggle("on", state.view === "plugins");
}

// ---------- wiring ----------
function bind() {
  $("#btn-conn").onclick = openConnection;
  $("#btn-new").onclick = newChat;
  $("#btn-plugins").onclick = showPlugins;
  $("#btn-customize").onclick = () => showCustomize();
  $("#btn-inbox").onclick = showInbox;
  $("#btn-archived").onclick = () => {
    state.sessFilter = { ...state.sessFilter, status: state.sessFilter.status === "archived" ? "active" : "archived" };
    loadSessions();
  };
  let searchTimer;
  $("#chat-search").addEventListener("input", (e) => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => { state.sessFilter = { ...state.sessFilter, q: e.target.value }; loadSessions(); }, 200);
  });
  $("#form-ask").addEventListener("submit", (ev) => {
    ev.preventDefault();
    const label = !$("#ask-label").hidden;
    const v = label ? $("#form-ask input[name=value]").value : true;
    $("#dlg-ask").close();
    if (askResolve) askResolve(v);
  });
  $("#dlg-ask").addEventListener("close", () => { if (askResolve) askResolve(null); });
  document.addEventListener("mousedown", (e) => { if (!e.target.closest("#menu") && !e.target.closest(".row-more, .head-btn")) closeMenu(); });
  document.addEventListener("keydown", (e) => { if (e.key === "Escape") closeMenu(); });
  window.runtime.EventsOn("approvals:update", (list) => {
    state.approvals = list || [];
    updateInboxBadge();
    renderInbox();
  });
  window.runtime.EventsOn("approvals:open", (info) => {
    if (info && info.session_id) openSession(info.session_id);
    else showInbox();
  });
  $("#form-conn").addEventListener("submit", submitConnection);
  $("#form-conn").addEventListener("change", syncMode);
  $("#form-reason").addEventListener("submit", (ev) => {
    ev.preventDefault();
    const reason = ev.target.reason.value.trim();
    $("#dlg-reason").close();
    if (reasonCallback) reasonCallback(reason);
  });
  document.querySelectorAll("[data-close]").forEach((b) => (b.onclick = () => b.closest("dialog").close()));

  const input = $("#input");
  input.addEventListener("input", () => { autosize(); updateComposer(); updateSlash(); });
  input.addEventListener("click", updateSlash);
  input.addEventListener("blur", () => setTimeout(closeSlash, 100));
  input.addEventListener("keydown", (e) => {
    if (slashKey(e)) return;
    // isComposing / 229: Enter that confirms an IME candidate (e.g. Chinese input) must not send.
    if (e.key === "Enter" && !e.shiftKey && !e.isComposing && e.keyCode !== 229) {
      e.preventDefault();
      send(input.value);
    }
  });
  $("#composer").addEventListener("submit", (e) => {
    e.preventDefault();
    if ($("#send").classList.contains("stop")) stop();
    else send(input.value);
  });

  window.runtime.EventsOn("run:event", (msg) => {
    if (!msg || !state.runs.has(msg.run_id)) return;
    const type = msg.event.event_type;
    const final = type === "run.completed" || type === "run.failed" || type === "run.cancelled" || type === "turn.finalized";
    scheduleRun(msg.run_id, final);
  });
  window.runtime.EventsOn("run:delta", (msg) => {
    if (!msg || !msg.delta || !state.runs.has(msg.run_id)) return;
    const d = msg.delta;
    const text = d.event && d.event.text;
    if (!text) return;
    if (!state.deltas.has(msg.run_id)) state.deltas.set(msg.run_id, new Map());
    const m = state.deltas.get(msg.run_id);
    m.set(d.step_id, (m.get(d.step_id) || "") + text);
    const el = document.getElementById("delta-" + d.step_id);
    if (el) {
      el.textContent = m.get(d.step_id);
      const sc = $("#scroll");
      if (sc.scrollHeight - sc.scrollTop - sc.clientHeight < 120) sc.scrollTop = sc.scrollHeight;
    } else {
      scheduleRun(msg.run_id);
    }
  });
  window.runtime.EventsOn("run:stream-error", (msg) => {
    if (msg && msg.run_id === state.watching) {
      state.watching = null;
      toast("Live updates stopped: " + msg.error, true);
    }
  });
  setInterval(() => { if (!document.hidden) loadSessions(); }, 5000);
  // Safety net for missed stream events: refresh the active run periodically.
  setInterval(() => { const t = activeTurn(); if (t && !document.hidden) scheduleRun(t.run_id); }, 6000);
}

async function boot() {
  bind();
  render();
  if (await api().Connected()) {
    try {
      showWho(await api().Whoami());
      await loadSessions();
      return;
    } catch (e) {
      toast(errText(e), true);
    }
  }
  openConnection();
}

window.addEventListener("DOMContentLoaded", boot);
