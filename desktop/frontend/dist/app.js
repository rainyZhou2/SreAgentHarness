// SRE Harness desktop client: a chat UI over sessions and turns. Every
// network call goes through the Go side (window.go.main.App); live run
// events and model deltas arrive as "run:event" / "run:delta".
"use strict";

const api = () => window.go.main.App;
const $ = (sel, root = document) => root.querySelector(sel);

const TERMINAL = new Set(["COMPLETED", "FAILED", "CANCELLED"]);
const SUGGESTIONS = [
  "What's the difference between a Deployment and a StatefulSet?",
  "Why did checkout p99 latency rise this week?",
  "Restart the checkout deployment and confirm recovery",
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
  demoModel: null,      // true when the tenant runs on the bundled scripted model
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
// icon draws a 24px line icon (one family, 1.75 stroke; see .ico-svg).
const ICONS = {
  search: '<circle cx="11" cy="11" r="6.5"/><path d="m20 20-4.2-4.2"/>',
  spark: '<path d="M12 3.5 13.7 9l5.3 1.7-5.3 1.8L12 18l-1.7-5.5L5 10.7 10.3 9z"/>',
  tool: '<path d="M14.5 6.2a4 4 0 0 0-5.3 5.3L4.5 16.2V19.5h3.3l4.7-4.7a4 4 0 0 0 5.3-5.3l-2.5 2.5-2.3-.5-.5-2.3z"/>',
  dot: '<circle cx="12" cy="12" r="2.5"/>',
  up: '<path d="M12 19V5M6 11l6-6 6 6"/>',
  stop: '<rect x="7" y="7" width="10" height="10" rx="2" fill="currentColor" stroke="none"/>',
  book: '<path d="M5 5.5A2.5 2.5 0 0 1 7.5 3H19v15H7.5A2.5 2.5 0 0 0 5 20.5z"/><path d="M5 20.5A2.5 2.5 0 0 1 7.5 18H19v3H7.5"/>',
  sliders: '<path d="M4 7h10M18 7h2M4 17h4M12 17h8"/><circle cx="16" cy="7" r="2"/><circle cx="10" cy="17" r="2"/>',
};
const kbd = (k) => h("kbd", {}, k);
function icon(name) {
  const el = document.createElementNS("http://www.w3.org/2000/svg", "svg");
  el.setAttribute("viewBox", "0 0 24 24");
  el.setAttribute("class", "ico-svg");
  el.setAttribute("aria-hidden", "true");
  el.innerHTML = ICONS[name]; // static markup above, never server data
  return el;
}
// actionable makes a non-button surface reachable from the keyboard.
function actionable(el, fn) {
  el.setAttribute("role", "button");
  el.setAttribute("tabindex", "0");
  el.addEventListener("keydown", (e) => {
    if (e.key === "Enter" || e.key === " ") { e.preventDefault(); fn(e); }
  });
  return el;
}
const BADGE = { COMPLETED: "s-ok", VERIFIED: "s-ok", ACTIVE: "s-ok", UNVERIFIED: "s-warn", REFUTED: "s-bad", FAILED: "s-bad" };
const badge = (s) => h("span", { class: "badge " + (BADGE[s] || "s-idle") }, t(String(s || "?")).replaceAll("_", " "));

function ago(ts) {
  if (!ts) return "";
  const d = (Date.now() - new Date(ts).getTime()) / 1000;
  if (d < 60) return t("now");
  if (d < 3600) return t("{n}m", { n: Math.round(d / 60) });
  if (d < 86400) return t("{n}h", { n: Math.round(d / 3600) });
  if (d < 86400 * 7) return t("{n}d", { n: Math.round(d / 86400) });
  return new Date(ts).toLocaleDateString(locale());
}
function dur(a, b) {
  const s = Math.max(0, Math.round((new Date(b) - new Date(a)) / 1000));
  return s < 60 ? t("{s}s", { s }) : t("{m}m {s}s", { m: Math.floor(s / 60), s: s % 60 });
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
  f.server_url.focus();
  f.server_url.select();
}
function syncMode() {
  const mode = $("#form-conn").mode.value;
  document.querySelectorAll("#form-conn [data-mode]").forEach((el) => (el.hidden = el.dataset.mode !== mode));
  document.querySelectorAll("#form-conn .seg label").forEach((el) => {
    const input = el.querySelector("input");
    el.classList.toggle("on", !!(input && input.checked));
  });
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
    loadModelHint();
  } catch (e) {
    $("#conn-error").textContent = errText(e);
    $("#conn-error").hidden = false;
  }
}
function showWho(who) {
  state.who = who;
  $("#who").textContent = who ? `${who.subject} · ${who.tenant}` : t("Not connected");
  $("#who").title = who ? t("roles: {roles} · auth: {method}", { roles: (who.roles || []).join(", "), method: who.method }) : "";
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
  $("#chats-label").textContent = t(archived ? "Archived chats" : "Chats");
  $("#btn-archived").textContent = t(archived ? "‹ Back" : "Archived");
  if (!state.sessions.length) return ul.append(h("li", { class: "empty" },
    t(state.sessFilter.q ? "No matching chats" : archived ? "No archived chats" : "No chats yet")));
  for (const s of state.sessions) {
    ul.append(h("li", { class: s.id === state.sessionId && state.view === "chat" ? "active" : "", title: s.title || "", onclick: () => openSession(s.id) },
      h("span", { class: "chat-title" }, s.title || t("Untitled chat")),
      s.active_turns > 0 ? h("span", { class: "pulse", title: t("working") }) : h("span", { class: "chat-time" }, ago(s.last_activity_at)),
      h("button", { class: "row-more", title: t("More"), onclick: (e) => { e.stopPropagation(); chatMenu(e.currentTarget, s); } }, "⋯")));
  }
}

// ---------- chat management: rename, archive, export, delete ----------
function chatMenu(anchor, s) {
  const archived = s.status === "ARCHIVED";
  popMenu(anchor, [
    [t("Rename…"), () => renameChat(s)],
    [t(archived ? "Restore" : "Archive"), () => archiveChat(s, !archived)],
    [t("Export as Markdown"), () => exportChat(s.id)],
    null,
    [t("Delete…"), () => deleteChat(s), "danger"],
  ]);
}

async function renameChat(s) {
  const title = await ask({ title: t("Rename chat"), label: t("Title"), value: s.title || "", ok: t("Rename") });
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
    toast(t(archived ? "Archived" : "Restored"));
    if (state.sessionId === s.id) {
      if (state.session) state.session.status = archived ? "ARCHIVED" : "ACTIVE";
      if (archived && state.sessFilter.status === "active") newChat(); else renderHead();
    }
    loadSessions();
  } catch (e) { toast(errText(e), true); }
}

async function deleteChat(s) {
  const ok = await ask({ title: t("Delete chat?"), text: t("“{title}” will be removed from your chats. Its runs stay in the server's audit history.",
    { title: s.title || t("Untitled chat") }), ok: t("Delete"), danger: true });
  if (!ok) return;
  try {
    await api().DeleteSession(s.id);
    if (state.sessionId === s.id) newChat();
    loadSessions();
    toast(t("Deleted"));
  } catch (e) { toast(errText(e), true); }
}

async function exportChat(id) {
  try {
    const path = await api().ExportChat(id);
    if (path) toast(t("Saved to {path}", { path }));
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
function ask({ title, text = "", label = null, value = "", ok = t("OK"), danger = false }) {
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
    toast(t("Send failed: {err}", { err: errText(e) }), true);
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
    if (cmd && cmd.status === "REJECTED") toast(t("{cmd} rejected: {reason}", { cmd: type, reason: cmd.reject_reason || t("no reason") }), true);
  } catch (e) {
    toast(t("{cmd} failed: {err}", { cmd: type.replaceAll("_", " "), err: errText(e) }), true);
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
      h("img", { class: "hero-logo", src: "logo.png", alt: "" }),
      h("h1", {}, t("What can I help with?")),
      h("div", { class: "muted" }, t("Tell me what's on your mind. I'll work out what you need and help from there.")),
      h("div", { class: "chips" }, SUGGESTIONS.map((s) => h("button", { class: "chip", onclick: () => send(t(s)) }, t(s)))),
      state.demoModel ? h("div", { class: "model-hint" },
        h("span", {}, t("You're on the built-in scripted model. Add a real model for better answers.")),
        h("button", { class: "link-btn", onclick: () => showSettings("models") }, t("Configure a model"))) : null));
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
    h("span", { class: "title editable", title: t("Rename"), onclick: () => renameChat(sess) }, state.session.title || t("Untitled chat")),
    sess.status === "ARCHIVED" ? h("span", { class: "badge s-idle" }, t("archived")) : null,
    h("span", { class: "meta" }, [profile, tn(state.turns.length, "{n} turn", "{n} turns"), cost ? `$${cost.toFixed(4)}` : ""].filter(Boolean).join(" · ")),
    h("span", { class: "spacer" }),
    h("button", { class: "head-btn", title: t("Export as Markdown"), onclick: () => exportChat(sess.id) }, t("Export")),
    h("button", { class: "head-btn", title: t("More"), onclick: (e) => chatMenu(e.currentTarget, sess) }, "⋯")].filter(Boolean));
}

function renderTurn(t) {
  const queued = t.status === "QUEUED" || t.status === "SENDING";
  const out = h("div", { class: "turn" }, h("div", { class: "msg-user" }, h("div", { class: "bubble" + (queued ? " queued" : "") }, t.text || "…")));
  if (t.status === "SENDING") return out;
  if (t.status === "QUEUED") {
    out.append(h("div", { class: "queued-note" }, t("Queued · starts when the current reply finishes")));
    return out;
  }
  const v = t.run_id && state.runs.get(t.run_id);
  out.append(h("div", { class: "msg-agent" },
    h("div", { class: "agent-mark" + (v && !TERMINAL.has(v.run.run.status) ? " busy" : "") }, h("img", { src: "logo.png", alt: "" })),
    h("div", { class: "agent-body" }, v ? renderRun(v) : h("div", { class: "shimmer" }, t("Starting…")))));
  return out;
}

function renderRun(v) {
  const run = v.run.run;
  const status = run.status;
  const done = TERMINAL.has(status);
  const steps = ((v.steps && v.steps.steps) || []).slice().sort((a, b) => a.seq - b.seq);
  const approvals = (v.approvals && v.approvals.approvals) || [];
  const tools = steps.filter((s) => s.kind === "tool").length;

  const result = v.run.result;
  let intent = (result && result.intent) || "";
  if (!intent) {
    for (let i = steps.length - 1; i >= 0; i--) {
      const o = steps[i].output;
      if (steps[i].kind === "reason" && o && o.intent) { intent = o.intent; break; }
    }
  }
  const conversational = intent === "question" || intent === "clarification";

  const choice = state.traceOpen.get(run.id);
  const open = choice !== undefined ? choice : !done || status === "FAILED";
  const label = done
    ? t("Worked for {d}", { d: dur(run.created_at, run.updated_at) }) + (tools ? " · " + tn(tools, "{n} tool call", "{n} tool calls") : "")
    : t(status === "WAITING_APPROVAL" ? "Waiting for your approval" : status === "PAUSED" ? "Paused" : "Working…");
  const parts = [];
  if (!conversational) {
    parts.push(h("button", { class: "trace-toggle", onclick: () => { state.traceOpen.set(run.id, !open); render(); } },
      h("span", { class: "caret" + (open ? " open" : "") }, "▶"),
      h("span", { class: !done && status !== "WAITING_APPROVAL" && status !== "PAUSED" ? "shimmer" : "" }, label)));
    if (open && steps.length) parts.push(h("div", { class: "trace" }, steps.map((s) => renderStep(run, s, approvals))));
  } else if (!done) {
    parts.push(h("div", { class: "trace-toggle" }, h("span", { class: "shimmer" }, label)));
  }

  if (status === "COMPLETED" && result) {
    if (result.answer) parts.push(h("div", { class: "answer" }, md(result.answer)));
    const hyps = result.hypotheses || [];
    if (!conversational && hyps.length) parts.push(h("div", { class: "hyps" }, hyps.map((x) => h("div", { class: "hyp" },
      badge(x.status), h("span", {}, x.statement), h("span", { class: "conf" }, Math.round((x.confidence_estimate || 0) * 100) + "%")))));
  } else if (status === "FAILED") {
    parts.push(h("div", { class: "status-line" }, run.failure_reason ? t("Failed: {reason}", { reason: run.failure_reason }) : t("Failed")));
  } else if (status === "CANCELLED") {
    parts.push(h("div", { class: "status-line stopped" }, t("Stopped")));
  } else if (status === "NEEDS_HUMAN") {
    parts.push(h("div", { class: "status-line" }, t("Needs a human: {reason}", { reason: run.failure_reason || t("the agent could not continue safely") })));
  }

  const meta = [h("span", {}, "run …" + run.id.slice(-8))];
  if (v.cost && v.cost.model_requests) meta.push(h("span", {}, tn(v.cost.model_requests, "{n} model call", "{n} model calls")),
    h("span", {}, t("{n} tokens", { n: (v.cost.input_tokens || 0) + (v.cost.output_tokens || 0) })));
  const models = Object.keys((v.cost && v.cost.cost_micros_by_model) || {});
  if (models.length) meta.push(h("span", { class: "model-tag", title: t("model") }, models.join(", ")));
  if (status === "PAUSED") meta.push(h("button", { onclick: () => command(run.id, "resume", { version: true }) }, t("Resume")));
  else if (!done) meta.push(h("button", { onclick: () => command(run.id, "pause") }, t("Pause")));
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
    return h("div", { class: "item" }, h("div", { class: "item-row" }, h("span", { class: "ico" }, icon("search")),
      h("span", { class: running ? "shimmer" : "what" }, t(running ? "Searching memory…" : "Searched memory")),
      !running && h("span", {}, hits ? t("{n} relevant", { n: hits }) : t("nothing relevant"))));
  }
  if (s.kind === "reason") {
    const delta = (state.deltas.get(run.id) || new Map()).get(s.id);
    if (running) {
      return h("div", { class: "item" },
        h("div", { class: "item-row" }, h("span", { class: "ico" }, icon("spark")), h("span", { class: "shimmer" }, t("Thinking…"))),
        h("div", { class: "streaming", id: "delta-" + s.id }, delta || ""));
    }
    const summary = out.reason_summary || s.status_reason || "";
    return h("div", { class: "item" }, h("div", { class: "item-row" }, h("span", { class: "ico" }, icon("spark")),
      h("span", { class: "thought" }, summary || t(out.kind === "final" ? "Concluded" : "Reasoned"))));
  }
  if (s.kind === "tool") {
    const inp = s.input || {};
    const name = inp.name || s.executor_name;
    const approval = approvals.find((a) => a.step_id === s.id && a.status === "PENDING");
    const waiting = s.status === "WAITING_APPROVAL";
    const isOpen = state.open.has(s.id);
    let st;
    if (waiting) st = h("span", { class: "state warn" }, t("needs approval"));
    else if (running) st = h("span", { class: "state shimmer" }, t("running"));
    else if (s.status === "SUCCEEDED") {
      const bad = out.outcome && out.outcome !== "ok";
      st = h("span", { class: "state " + (bad ? "warn" : "ok") }, bad ? t(out.outcome) : "✓");
    } else st = h("span", { class: "state bad" }, t((s.status || "").toLowerCase()).replaceAll("_", " "));
    const card = h("div", { class: "tool" + (waiting ? " wait" : "") },
      h("div", { class: "tool-head", onclick: () => { isOpen ? state.open.delete(s.id) : state.open.add(s.id); render(); } },
        h("span", { class: "ico" }, icon("tool")), h("span", { class: "name" }, name), h("span", { class: "args" }, argsText(inp.args)), st));
    if (isOpen) {
      const body = s.output ? (out.content !== undefined ? out.content : out) : s.status_reason || t("no output yet");
      card.append(h("pre", {}, typeof body === "string" ? body : JSON.stringify(body, null, 2)));
    }
    if (waiting && approval) {
      const payload = { approval_request_id: approval.id, action_hash: approval.action_hash };
      card.append(h("div", { class: "approve-box" },
        h("div", { class: "why" }, h("strong", {}, t("Approval required. ")),
          tn(approval.required_approvals, "{name} is a {risk} action and needs {n} approval.", "{name} is a {risk} action and needs {n} approvals.",
            { name, risk: approval.risk_class })),
        h("div", { class: "kv" },
          h("span", {}, t("target")), h("b", { class: "mono" }, approval.target || "—"),
          h("span", {}, t("expires")), h("b", {}, approval.expires_at ? new Date(approval.expires_at).toLocaleString(locale()) : "—"),
          h("span", {}, t("action")), h("b", { class: "mono" }, approval.action_hash)),
        h("div", { class: "approve-actions" },
          h("button", { class: "btn approve", onclick: () => command(run.id, "approve_tool", { version: true, payload }) }, t("Approve")),
          h("button", { class: "btn danger", onclick: () => askReason(t("Reject {name}", { name }), t("Reject"),
            (reason) => command(run.id, "reject_tool", { version: true, reason, payload })) }, t("Reject")))));
    }
    return h("div", { class: "item" }, card);
  }
  return h("div", { class: "item" }, h("div", { class: "item-row" }, h("span", { class: "ico" }, icon("dot")),
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
  const stopping = busy && !hasText;
  const mode = stopping ? "stop" : "up";
  if (btn.dataset.icon !== mode) { btn.replaceChildren(icon(mode)); btn.dataset.icon = mode; }
  btn.title = t(stopping ? "Stop" : busy ? "Send (queued after the current reply)" : "Send");
  btn.className = "send" + (stopping ? " stop" : "");
  btn.disabled = !stopping && (!hasText || state.sending);
  $("#hint").replaceChildren(...(busy
    ? [t("Working… new messages are queued · ■ stops the current reply")]
    : [kbd("↵"), " " + t("send"), h("span", { class: "hint-sep" }), kbd("⇧ ↵"), " " + t("new line")]));
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
  const empty = !data.skills.length && !data.tools.length;
  Object.assign(slash, { open: true, items, empty, start: tok.start, end: tok.end, index: Math.max(0, Math.min(slash.index, items.length - 1)) });
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
  const head = h("div", { class: "slash-head" },
    h("span", { class: "slash-title" }, t("Skills & tools")),
    h("span", { class: "slash-keys" }, kbd("↑↓"), " " + t("navigate"), kbd("↵"), " " + t("insert"), kbd("esc"), " " + t("close")));
  const rows = [head];
  if (!slash.items.length) {
    rows.push(h("div", { class: "slash-empty" }, t(slash.empty ? "No skills or tools are available yet. Add them under Customize." : "No matching skills or tools")));
  }
  let lastKind = null;
  slash.items.forEach((it, i) => {
    if (it.kind !== lastKind) {
      const n = slash.items.filter((x) => x.kind === it.kind).length;
      rows.push(h("div", { class: "slash-group" },
        h("span", {}, t(it.kind === "skill" ? "Skills" : "Tools")), h("span", { class: "slash-count" }, String(n)),
        it.kind === "tool" ? h("span", { class: "slash-group-hint" }, t("inserts “use <tool> {…}”")) : null));
      lastKind = it.kind;
    }
    const tag = it.kind === "skill"
      ? h("span", { class: "hint-tag" }, t(it.manual ? "this chat" : "always on"))
      : h("span", { class: "risk-tag r" + (it.risk || "R0").slice(1) }, it.risk);
    rows.push(h("div", { class: "slash-item" + (i === slash.index ? " on" : ""), role: "option", "aria-selected": i === slash.index ? "true" : null,
      onmousedown: (e) => { e.preventDefault(); pickSlash(i); },
      onmousemove: () => { if (slash.index !== i) { slash.index = i; renderSlash(); } } },
      h("span", { class: "slash-ico" }, icon(it.kind === "skill" ? "book" : "tool")),
      h("span", { class: "slash-text" },
        h("span", { class: "slash-name mono" }, it.kind === "skill" ? "/" + it.name : it.name),
        it.desc ? h("span", { class: "slash-desc" }, it.desc) : null),
      tag));
  });
  el.replaceChildren(...rows);
  el.hidden = false;
  const on = el.querySelector(".slash-item.on");
  if (on) on.scrollIntoView({ block: "nearest" });
}

// openSlash inserts "/" at the caret (the picker button) and opens the picker.
function openSlash() {
  const ta = $("#input");
  const pos = ta.selectionStart ?? ta.value.length;
  const before = ta.value.slice(0, pos);
  const ins = before === "" || /\s$/.test(before) ? "/" : " /";
  ta.value = before + ins + ta.value.slice(ta.selectionEnd ?? pos);
  ta.focus();
  ta.setSelectionRange(pos + ins.length, pos + ins.length);
  autosize();
  updateComposer();
  updateSlash();
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
  if (!slash.items.length && e.key !== "Escape") return false;
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
      a.target ? h("span", { class: "muted" }, t("on") + " ", h("b", { class: "mono" }, a.target)) : null,
      h("span", { class: "spacer" }),
      h("span", { class: "muted small", title: new Date(a.created_at).toLocaleString(locale()) }, ago(a.created_at))),
    h("div", { class: "muted small" },
      a.session_title ? t("in “{title}”", { title: a.session_title }) + " · " : ["run …", a.run_id.slice(-8), " · "],
      a.initiator_id ? t("asked by {who}", { who: a.initiator_id }) + " · " : "",
      t("expires {time}", { time: new Date(a.expires_at).toLocaleTimeString(locale()) }),
      a.required_approvals > 1 ? " · " + t("needs {n} approvals", { n: a.required_approvals }) : ""),
    h("div", { class: "mono tiny-hash" }, a.action_hash),
    h("div", { class: "approve-actions" },
      h("button", { class: "btn approve", onclick: (e) => decide(e.currentTarget, a, true) }, t("Approve")),
      h("button", { class: "btn", onclick: (e) => decide(e.currentTarget, a, false) }, t("Reject")),
      h("span", { class: "spacer" }),
      a.session_id ? h("button", { class: "link-btn", onclick: () => openSession(a.session_id) }, t("Open chat ›")) : null)));
  $("#thread").replaceChildren(h("div", { class: "customize" },
    h("h1", {}, t("Approvals")),
    h("p", { class: "muted lead" }, t("Tool calls waiting for a decision in any chat of this tenant. New ones also arrive as system notifications.")),
    list.length ? cards : h("div", { class: "empty-card muted" }, t("Nothing is waiting for approval."))));
}

async function decide(btn, a, approve) {
  let reason = "";
  if (!approve) {
    reason = await ask({ title: t("Reject {name}", { name: a.tool_name }), label: t("Reason (optional)"), ok: t("Reject"), danger: true });
    if (reason == null) return;
  }
  btn.disabled = true;
  try {
    const cmd = await api().ApproveFromInbox(a.run_id, a.id, a.action_hash, approve, reason);
    if (cmd && cmd.status === "REJECTED") toast(t("Not applied: {reason}", { reason: cmd.reject_reason || t("rejected") }), true);
    else toast(t(approve ? "Approved" : "Rejected"));
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
  thread.replaceChildren(h("div", { class: "plugins" }, h("h1", {}, t("Plugins")), h("div", { class: "muted" }, t("Loading…"))));
  let data;
  try {
    data = await api().Plugins();
  } catch (e) {
    const msg = errText(e);
    return thread.replaceChildren(h("div", { class: "plugins" }, h("h1", {}, t("Plugins")),
      h("div", { class: "error" }, msg + (msg.includes("403") ? "\n" + t("The plugin inventory needs the admin role.") : ""))));
  }
  const active = new Set((data.active || []).map((p) => p.kind + "/" + p.name));
  const body = (data.kinds || []).map((k) => {
    const list = (data.compiled || []).filter((p) => p.kind === k.name);
    return h("section", { class: "kind" }, h("h3", {}, k.name, k.multi ? " · " + t("multiple") : ""), h("div", { class: "muted small" }, k.doc || ""),
      list.length > 0 && h("div", { class: "plugin-grid" }, list.map((p) => h("article", { class: "plugin-card" },
        h("div", { class: "plugin-top" },
          h("span", { class: "plugin-name mono" }, p.name),
          active.has(p.kind + "/" + p.name) ? badge("ACTIVE") : h("span", { class: "badge s-idle" }, t("available"))),
        p.doc ? h("p", { class: "plugin-doc" }, p.doc) : null))));
  });
  if ((data.tools || []).length) body.push(h("section", { class: "kind" }, h("h3", {}, t("registered tools")),
    h("div", { class: "plugin-tools" }, data.tools.map((x) => h("span", { class: "plugin-chip mono" }, x)))));
  thread.replaceChildren(h("div", { class: "plugins" }, h("h1", {}, t("Plugins")), body));
}

// ---------- gallery: bundled skill catalog ----------
const GALLERY_CATS = ["Diagnosis", "Changes", "Observability", "Incident response"];
const GALLERY = [
  {
    name: "latency-triage", cat: "Diagnosis", tags: ["p99", "latency", "dependencies"],
    en: {
      title: "Latency triage",
      desc: "Narrow a p99 regression to a service, a dependency or a resource before changing anything.",
      instructions: `1. Fix the window, service and endpoint; say whether all replicas are affected.
2. Compare p50 with p99 and request rate. A flat p50 with a rising p99 points to contention, GC or a slow dependency.
3. Check downstream latency and errors before blaming this service.
4. Correlate the start with deploys, config changes, traffic shifts and dependency incidents.
5. State the leading hypothesis with evidence, then the smallest safe next check. No restart or scale-out without approval.`,
    },
    zh: {
      title: "延迟排查",
      desc: "在动手前把 p99 升高定位到服务、依赖或资源。",
      instructions: `1. 先确定时间窗口、服务和接口，并确认是否所有副本都受影响。
2. 对比 p50 与 p99 以及请求量。p50 平稳而 p99 升高，通常指向资源争抢、GC 或慢依赖。
3. 先看下游依赖的延迟和错误，再判断是否是本服务的问题。
4. 把开始时间与发布、配置变更、流量变化和依赖故障对齐。
5. 给出有证据支持的主要假设，以及代价最小的下一步检查；未经批准不要重启或扩容。`,
    },
  },
  {
    name: "error-spike", cat: "Diagnosis", tags: ["5xx", "4xx", "errors"],
    en: {
      title: "Error spike triage",
      desc: "Split an error spike by class, endpoint and replica, then find the single change behind it.",
      instructions: `1. Split errors by status class, endpoint, replica and client. 4xx and 5xx have different owners.
2. Check a single dependency, certificate, quota or auth change that could explain the spike.
3. Sample the actual error response and log line; quote the concrete message.
4. Correlate with deploys and feature flags in the same window.
5. Propose one diagnostic step and one reversible mitigation with its blast radius.`,
    },
    zh: {
      title: "错误突增排查",
      desc: "按状态码、接口和副本拆分错误，再找出背后的那一次变更。",
      instructions: `1. 按状态码类别、接口、副本和客户端拆分错误。4xx 和 5xx 的责任方不同。
2. 检查是否有单一的依赖、证书、配额或认证变更可以解释这次突增。
3. 抽取真实的错误响应和日志行，引用具体报错。
4. 与同一时间窗口内的发布和功能开关对齐。
5. 给出一个诊断步骤和一个可回滚的缓解措施，并说明影响范围。`,
    },
  },
  {
    name: "dependency-outage", cat: "Diagnosis", tags: ["timeout", "circuit breaker", "retries"],
    en: {
      title: "Dependency outage",
      desc: "Tell a slow dependency from a failing one, and avoid retry storms that make it worse.",
      instructions: `1. List failing dependencies and whether calls time out, fail fast or return 5xx.
2. Inspect circuit breakers, retries and queue depth; retries can amplify an outage.
3. Find the last healthy timestamp and any provider status page.
4. Recommend load shedding or cached responses only if reversible and approved.
5. Re-check the dependency health endpoint before declaring recovery.`,
    },
    zh: {
      title: "依赖故障",
      desc: "区分依赖变慢还是失败，并避免重试风暴把故障放大。",
      instructions: `1. 列出失败的依赖，判断是超时、快速失败还是返回 5xx。
2. 检查熔断、重试和队列深度；重试可能放大故障。
3. 找到最后一个健康时间点，以及依赖方是否有状态公告。
4. 只有在可回滚且经过批准时，才建议降级或返回缓存。
5. 宣布恢复前，重新检查依赖的健康检查端点。`,
    },
  },
  {
    name: "resource-saturation", cat: "Diagnosis", tags: ["cpu", "memory", "disk"],
    en: {
      title: "Resource saturation",
      desc: "Separate a leak from a traffic burst and check whether a restart would only postpone the problem.",
      instructions: `1. Check CPU throttling, memory working set vs limit, disk, inodes and file descriptors.
2. Tell a leak (steady growth) from a burst (traffic-shaped).
3. Check whether a restart would only postpone the problem.
4. Recommend the smallest change and the metric that proves it worked.`,
    },
    zh: {
      title: "资源饱和",
      desc: "区分内存泄漏和流量突增，并判断重启是否只是拖延问题。",
      instructions: `1. 检查 CPU 限流、内存工作集与限制、磁盘、inode 和文件描述符。
2. 区分泄漏（持续增长）和突发（跟随流量）。
3. 判断重启是否只是把问题往后拖。
4. 给出最小的改动，以及能证明它生效的指标。`,
    },
  },
  {
    name: "db-pool", cat: "Diagnosis", tags: ["database", "pool", "locks"],
    en: {
      title: "Database pool pressure",
      desc: "Read pool exhaustion together with query latency, locks and transaction length.",
      instructions: `1. Compare pool in-use, wait time and wait timeouts with query latency.
2. Look for slow queries, lock waits and long transactions.
3. Check pool size against concurrency and the database connection limit.
4. Prefer query, timeout and transaction fixes over raising the pool blindly.
5. Verify recovery with all four signals, not just the error count.`,
    },
    zh: {
      title: "数据库连接池压力",
      desc: "把连接池耗尽与查询延迟、锁等待和事务长度放在一起看。",
      instructions: `1. 把连接池占用、等待时间和等待超时与查询延迟放在一起对比。
2. 查找慢查询、锁等待和长事务。
3. 检查连接池大小与并发量、数据库连接上限是否匹配。
4. 优先修查询、超时和事务，而不是盲目调大连接池。
5. 用四项信号验证恢复，而不是只看错误数。`,
    },
  },
  {
    name: "rollout-check", cat: "Changes", tags: ["deploy", "canary", "readiness"],
    en: {
      title: "Rollout check",
      desc: "Verify a rollout with old-vs-new signals before declaring it healthy.",
      instructions: `1. Identify the rollout, revision and target replicas.
2. Compare old and new error rate, latency and saturation over at least one full window.
3. Verify readiness, health checks and traffic distribution across replicas.
4. If unhealthy, pause or roll back with explicit criteria rather than waiting.`,
    },
    zh: {
      title: "发布检查",
      desc: "在宣布发布成功前，用新旧版本对比信号做验证。",
      instructions: `1. 确认发布对象、版本和目标副本数。
2. 至少观察一个完整窗口，对比新旧版本的错误率、延迟和资源饱和度。
3. 检查就绪状态、健康检查和副本间的流量分布。
4. 如果异常，按明确标准暂停或回滚，而不是继续等待。`,
    },
  },
  {
    name: "safe-rollback", cat: "Changes", tags: ["rollback", "revision", "schema"],
    en: {
      title: "Safe rollback",
      desc: "Check that a rollback is actually safe before running it, especially with schema changes.",
      instructions: `1. Confirm the current revision, the previous healthy revision and the exact rollback command.
2. Check for schema or stateful changes that a rollback cannot undo.
3. Define success: error rate, latency and saturation return to baseline.
4. Execute only after approval and record before/after evidence.`,
    },
    zh: {
      title: "安全回滚",
      desc: "执行前先确认回滚是否真的安全，尤其是涉及数据库结构变更时。",
      instructions: `1. 确认当前版本、上一个健康版本和准确的回滚命令。
2. 检查是否有回滚无法撤销的结构或状态变更。
3. 定义成功标准：错误率、延迟和资源饱和度回到基线。
4. 只有经过批准才执行，并记录回滚前后的证据。`,
    },
  },
  {
    name: "log-triage", cat: "Observability", tags: ["logs", "signatures", "window"],
    en: {
      title: "Log triage",
      desc: "Group logs by error signature instead of scrolling, and start from the earliest abnormal line.",
      instructions: `1. Start from the symptom and an exact time window; avoid scanning everything.
2. Group lines by error signature, not raw text; count them against the baseline.
3. Prefer the earliest abnormal line over the loudest one.
4. Correlate with deploy and dependency events; quote the signature.`,
    },
    zh: {
      title: "日志排查",
      desc: "按错误签名归并日志，从最早出现的异常行开始。",
      instructions: `1. 从现象和明确的时间窗口开始，不要全量扫日志。
2. 按错误签名而不是原始文本归并，并与基线对比数量。
3. 优先关注最早出现的异常行，而不是最吵的那条。
4. 与发布和依赖事件对齐，并引用具体签名。`,
    },
  },
  {
    name: "metric-baseline", cat: "Observability", tags: ["baseline", "trend", "anomaly"],
    en: {
      title: "Metric baseline",
      desc: "Compare against the same time-of-day baseline and separate a level shift from a short spike.",
      instructions: `1. Compare the current window with the same time-of-day baseline from previous days.
2. Separate a level shift from a spike, and check whether the change persists.
3. Read rate, errors and duration together.
4. Report metric, delta and confidence; avoid causal claims without evidence.`,
    },
    zh: {
      title: "指标基线对比",
      desc: "用同一时段的基线做对比，区分水平偏移和短时尖峰。",
      instructions: `1. 用前几天的同一时段基线对比当前窗口。
2. 区分水平偏移和尖峰，并判断变化是否持续。
3. 把请求量、错误和耗时放在一起看。
4. 报告指标、变化幅度和置信度；没有证据不要下因果结论。`,
    },
  },
  {
    name: "incident-update", cat: "Incident response", tags: ["status", "impact", "comms"],
    en: {
      title: "Incident update",
      desc: "Write a short, factual status update that separates impact, knowns and unknowns.",
      instructions: `1. Lead with impact: who is affected, since when, how severe.
2. Separate what is known, what is being checked and what is still unknown.
3. Give the next update time and the owner.
4. Keep it short and factual; no speculation or blame.`,
    },
    zh: {
      title: "故障通报",
      desc: "写一段简短、事实优先的状态更新，区分影响、已知和未知。",
      instructions: `1. 先说影响：谁受影响、从什么时候开始、严重程度如何。
2. 区分已经确认的、正在排查的和仍然未知的内容。
3. 给出下次更新时间和负责人。
4. 保持简短和事实化，不猜测、不追责。`,
    },
  },
  {
    name: "postmortem", cat: "Incident response", tags: ["timeline", "actions", "review"],
    en: {
      title: "Postmortem draft",
      desc: "Turn evidence into a timeline, contributing factors and a few owned follow-ups.",
      instructions: `1. Build a timestamped timeline from evidence, not memory.
2. Separate the trigger, contributing factors and detection gaps.
3. Note what went well and what slowed recovery.
4. Propose a few owned actions with dates; avoid vague "improve monitoring".`,
    },
    zh: {
      title: "复盘草稿",
      desc: "把证据整理成时间线、促成因素和少量有负责人的后续动作。",
      instructions: `1. 基于证据而不是记忆，整理带时间戳的时间线。
2. 区分触发因素、促成因素和发现环节的缺口。
3. 记录哪些做法有效、哪些拖慢了恢复。
4. 给出少量有负责人和日期的动作；不要写“加强监控”这类空话。`,
    },
  },
];
const gal = { data: null, q: "", cat: "All", error: null };
const galText = (item) => (i18n.lang === "zh" ? item.zh : item.en);

async function showGallery() {
  state.view = "gallery";
  markSide();
  stopWatching();
  renderSessions();
  $("#chat-head").replaceChildren();
  $("#chat-head").className = "chat-head";
  $("#composer-wrap").hidden = true;
  renderGallery(true);
  try {
    gal.data = await api().Extensions();
    gal.error = null;
  } catch (e) {
    gal.error = errText(e);
  }
  renderGallery();
}

function renderGallery(loading) {
  if (state.view !== "gallery") return;
  const cats = [["All", t("All")], ...GALLERY_CATS.map((c) => [c, t(c)])];
  $("#thread").replaceChildren(h("div", { class: "gallery" },
    h("h1", {}, t("Gallery")),
    h("p", { class: "muted lead" }, t("Ready-made playbooks for common SRE work. Add one and it becomes a skill in Customize.")),
    h("div", { class: "gallery-bar" },
      h("input", { class: "gallery-search", type: "search", placeholder: t("Search skills"), value: gal.q,
        autocomplete: "off", spellcheck: "false", oninput: (e) => { gal.q = e.target.value; paintGallery(); } }),
      h("div", { class: "gallery-cats" }, cats.map(([key, label]) =>
        h("button", { class: "gallery-cat" + (gal.cat === key ? " on" : ""), "data-cat": key,
          onclick: () => { gal.cat = key; paintGallery(); } }, label)))),
    gal.error ? h("p", { class: "error small" }, gal.error) : null,
    h("div", { class: "gallery-list", id: "gallery-list" })));
  paintGallery(loading);
}

function paintGallery(loading) {
  const box = $("#gallery-list");
  if (!box) return;
  document.querySelectorAll(".gallery-cat").forEach((b) => b.classList.toggle("on", b.dataset.cat === gal.cat));
  const q = gal.q.trim().toLowerCase();
  const items = GALLERY.filter((it) => (gal.cat === "All" || it.cat === gal.cat) &&
    (!q || [galText(it).title, galText(it).desc, it.name, ...it.tags].join(" ").toLowerCase().includes(q)));
  if (loading && !gal.data) return box.replaceChildren(h("div", { class: "muted" }, t("Loading…")));
  if (!items.length) return box.replaceChildren(h("div", { class: "empty-card" },
    h("div", { class: "empty-mark" }, icon("search")),
    h("div", { class: "empty-title" }, t("No matching skills")),
    h("p", { class: "muted small" }, t("Try another keyword or category."))));
  const installed = new Set(((gal.data && gal.data.skills) || []).map((s) => s.name));
  box.replaceChildren(...items.map((it) => galleryCard(it, installed.has(it.name))));
}

function galleryCard(it, installed) {
  const c = galText(it);
  return h("article", { class: "gallery-card" },
    h("div", { class: "gallery-top" },
      h("span", { class: "gallery-mark" }, icon("spark")),
      h("div", { class: "gallery-titles" },
        h("div", { class: "gallery-title" }, c.title),
        h("div", { class: "gallery-cat-tag" }, t(it.cat)))),
    h("p", { class: "gallery-desc" }, c.desc),
    h("div", { class: "gallery-tags" }, it.tags.map((tag) => h("span", { class: "gallery-tag" }, tag))),
    h("div", { class: "gallery-foot" },
      h("span", { class: "gallery-slug" }, "/" + it.name),
      installed
        ? h("span", { class: "gallery-installed" },
            h("span", { class: "badge s-ok" }, t("Installed")),
            h("button", { class: "link-btn", onclick: () => openInstalledSkill(it.name) }, t("Open")))
        : h("button", { class: "btn", onclick: (e) => installSkill(it, e.currentTarget) }, t("Add"))));
}

async function installSkill(it, btn) {
  btn.disabled = true;
  const c = galText(it);
  try {
    await api().PutExtension("skills", it.name, { description: c.desc, instructions: c.instructions, mode: "manual" }, true, 0);
  } catch (e) {
    btn.disabled = false;
    const m = errText(e);
    toast(m.includes("403") ? t("Adding skills needs the admin role.") : m, true);
    return;
  }
  toast(t("Added {name}", { name: c.title }));
  try { gal.data = await api().Extensions(); gal.error = null; } catch { /* keep the catalog usable */ }
  paintGallery();
}

async function openInstalledSkill(name) {
  await showCustomize("skills");
  const x = ((cz.data && cz.data.skills) || []).find((s) => s.name === name);
  if (x) editSkill(x);
}

// ---------- customize: MCP servers, HTTP tools, skills, tools ----------
const RISKS = ["R0", "R1", "R2", "R3"];
const RISK_DOC = { R0: "read-only", R1: "low-impact write", R2: "change · needs approval", R3: "dangerous · always denied" };
const TABS = [["mcp", "MCP servers"], ["http", "HTTP tools"], ["web", "Web search"], ["skills", "Skills"], ["tools", "Tools"]];
const TAB_ICONS = { mcp: "tool", http: "dot", web: "search", skills: "spark" };
const EMPTY_TITLES = { mcp: "No MCP servers yet", http: "No HTTP tools yet", web: "No search backend yet", skills: "No skills yet" };
const cz = { tab: "mcp", data: null, tools: null, edit: null };

// Extension editors live in Customize and (models) in Settings; these redraw
// or reload whichever page is open.
function extRerender() { if (state.view === "settings") renderSettings(); else renderCustomize(); }
async function extReload() { if (state.view === "settings") await showSettings(); else await showCustomize(); }

function riskSelect(value, onchange) {
  const sel = h("select", { class: "risk", onchange: (e) => onchange(e.target.value) },
    RISKS.map((r) => h("option", { value: r, selected: r === value }, `${r} · ${t(RISK_DOC[r])}`)));
  return sel;
}
function statusBadge(x) {
  if (!x.enabled) return h("span", { class: "badge s-idle" }, t("disabled"));
  const st = x.status;
  if (!st) return h("span", { class: "badge s-idle" }, t("syncing"));
  if (st.state === "ready") return h("span", { class: "badge s-ok" }, t("ready"));
  if (st.state === "error") return h("span", { class: "badge s-bad", title: st.error || "" }, t("error"));
  return h("span", { class: "badge s-idle" }, st.state);
}
// kv editor rows for headers: [{k, v, saved}]
function kvEditor(rows, placeholderKey) {
  const box = h("div", { class: "kv-edit" });
  const draw = () => {
    box.replaceChildren(...rows.map((r, i) => h("div", { class: "kv-row" },
      h("input", { value: r.k, placeholder: placeholderKey, spellcheck: "false", oninput: (e) => (r.k = e.target.value) }),
      h("input", { value: r.v, type: "password", autocomplete: "off", placeholder: t(r.saved ? "saved · leave blank to keep" : "value"),
        oninput: (e) => (r.v = e.target.value) }),
      h("button", { type: "button", class: "icon-btn", title: t("Remove"), onclick: () => { rows.splice(i, 1); draw(); } }, "×"))),
      h("button", { type: "button", class: "link-btn", onclick: () => { rows.push({ k: "", v: "" }); draw(); } }, t("+ Add header")));
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
    h("button", { class: "tab" + (cz.tab === id ? " on" : ""), onclick: () => { cz.tab = id; cz.edit = null; renderCustomize(); } }, t(label),
      cz.data && id !== "tools" ? h("span", { class: "count" }, String(listFor(id).length)) : null)));
  let body;
  if (loading && !cz.data) body = h("div", { class: "muted" }, t("Loading…"));
  else if (cz.error) body = h("div", { class: "error" }, cz.error);
  else if (cz.edit) body = cz.edit.render();
  else if (cz.tab === "tools") body = renderToolsTab();
  else body = renderExtList(cz.tab);
  $("#thread").replaceChildren(h("div", { class: "customize" },
    h("header", { class: "cz-head" },
      h("div", { class: "cz-mark" }, icon("sliders")),
      h("div", { class: "cz-title" },
        h("h1", {}, t("Customize")),
        h("p", { class: "muted lead" }, t("Connect MCP servers, add HTTP tools and write skills for every chat in this tenant. Models are under Settings. "),
          t("Tools still run under the profile allowlist, policy and approvals. Changes need the admin role.")))),
    tabs, body));
}

function listFor(tab) {
  const d = cz.data || {};
  return (tab === "mcp" ? d.mcp_servers : tab === "http" ? d.http_tools : tab === "web" ? d.web_tools : tab === "model" ? d.models : d.skills) || [];
}

function renderExtList(tab) {
  const items = listFor(tab);
  const add = { mcp: [t("Add MCP server"), () => editMCP(null)], http: [t("Add HTTP tool"), () => editHTTP(null)],
    web: [t("Add search backend"), () => editWeb(null)], skills: [t("New skill"), () => editSkill(null)] }[tab];
  const empty = {
    mcp: "Add a streamable-HTTP server, pick which of its tools the agent may use and give each a risk class.",
    http: "An HTTP tool POSTs the call's JSON arguments to your endpoint and returns the response.",
    web: "web.fetch works without configuration; add a search provider to let the agent look up current facts.",
    skills: "A skill is a playbook (instructions) added to the agent's system prompt in every run.",
  }[tab];
  const edit = { mcp: editMCP, http: editHTTP, web: editWeb, skills: editSkill }[tab];
  return h("div", {},
    h("div", { class: "list-bar" }, h("button", { class: "btn primary", onclick: add[1] }, add[0])),
    items.length === 0 ? h("div", { class: "empty-card" },
      h("div", { class: "empty-mark" }, icon(TAB_ICONS[tab])),
      h("div", { class: "empty-title" }, t(EMPTY_TITLES[tab])),
      h("div", { class: "muted small" }, t(empty))) :
      items.map((x) => {
        const open = () => edit(x);
        return actionable(h("div", { class: "ext-card", onclick: open },
          h("div", { class: "ext-ico" }, icon(TAB_ICONS[tab])),
          h("div", { class: "ext-main" },
            h("div", { class: "ext-title" }, h("span", { class: "mono" }, x.name), statusBadge(x)),
            h("div", { class: "muted small" }, extSummary(tab, x)),
            x.status && x.status.error ? h("div", { class: "error small" }, x.status.error) : null),
          h("div", { class: "ext-side muted small" }, "v" + x.version, " · ", x.updated_by || "", " · ", ago(x.updated_at))), open);
      }));
}

function extSummary(tab, x) {
  const s = x.spec || {};
  if (tab === "mcp") {
    const allowed = Object.keys(s.allow || {});
    const loaded = (x.status && x.status.tools) || [];
    return [s.url || (s.command ? "$ " + s.command + " " + (s.args || []).join(" ") : ""),
      tn(allowed.length, "{n} tool allowed", "{n} tools allowed") + (x.status && x.status.state === "ready" ? ", " + t("{n} loaded", { n: loaded.length }) : ""),
      (x.status && (x.status.missing || []).length) ? t("missing on server: {list}", { list: x.status.missing.join(", ") }) : ""].filter(Boolean).join(" · ");
  }
  if (tab === "http") return `${s.method || "POST"} ${s.url} · ${s.risk} · http.${x.name}`;
  if (tab === "web") return `${s.provider || "?"}${s.endpoint ? " · " + s.endpoint : ""} · web.${x.name} · ${s.max_results || 5} ${t("results")}`;
  return (s.mode === "manual" ? `/${x.name} · ` : t("always on") + " · ") + (s.description || "");
}

function editorShell(title, fields, actions) {
  return h("div", { class: "editor" },
    h("div", { class: "editor-head" }, h("button", { class: "link-btn", onclick: () => { cz.edit = null; extRerender(); } }, t("‹ Back")), h("h2", {}, title)),
    fields, h("div", { class: "actions" }, actions));
}

async function saveExt(kind, name, spec, enabled, version) {
  try {
    await api().PutExtension(kind, name, spec, enabled, version || 0);
    toast(t("Saved {name}", { name }));
    cz.edit = null;
    await extReload();
  } catch (e) {
    const m = errText(e);
    toast(m.includes("403") ? t("Changing extensions needs the admin role.") : m, true);
  }
}
async function deleteExt(kind, name) {
  if (!(await ask({ title: t("Delete {name}?", { name }), text: t("Runs will no longer see it."), ok: t("Delete"), danger: true }))) return;
  try {
    await api().DeleteExtension(kind, name);
    toast(t("Deleted {name}", { name }));
    cz.edit = null;
    await extReload();
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
    if (!f.name.trim()) return toast(t("Give the server a name first."), true);
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
      const nameIn = h("input", { value: f.name, placeholder: t("e.g. {x}", { x: "grafana" }), spellcheck: "false", disabled: !!x, oninput: (e) => (f.name = e.target.value) });
      const remote = f.remote || Object.keys(f.allow).map((n) => ({ name: n, description: "", input_schema: null, stored: true }));
      const toolRows = remote.map((rt) => {
        const pol = f.allow[rt.name];
        const props = Object.keys((rt.input_schema && rt.input_schema.properties) || {});
        const def = rt.read_only_hint ? "R0" : "R2";
        return h("tr", { class: pol ? "" : "off" },
          h("td", {}, h("input", { type: "checkbox", checked: !!pol, onchange: (e) => {
            if (e.target.checked) f.allow[rt.name] = { risk: def }; else delete f.allow[rt.name];
            renderCustomize();
          } })),
          h("td", {}, h("div", { class: "mono" }, rt.name), h("div", { class: "muted small" }, rt.description || (rt.stored ? t("(connect to refresh)") : "")),
            rt.read_only_hint ? h("span", { class: "hint-tag" }, t("read-only hint")) : null,
            rt.destructive_hint ? h("span", { class: "hint-tag bad" }, t("destructive hint")) : null),
          h("td", {}, pol ? riskSelect(pol.risk, (v) => (pol.risk = v)) : h("span", { class: "muted small" }, t("not exposed"))),
          h("td", {}, pol && props.length ? h("select", { onchange: (e) => { if (e.target.value) pol.target_arg = e.target.value; else delete pol.target_arg; } },
            h("option", { value: "" }, t("no target")), props.map((p) => h("option", { value: p, selected: pol.target_arg === p }, p))) : null));
      });
      const fields = h("div", {},
        h("div", { class: "grid2" },
          h("label", {}, t("Name") + " ", nameIn, h("span", { class: "muted small" }, t("Tools appear as {prefix}", { prefix: `mcp.${f.name || "<name>"}.<tool>` }))),
          h("label", {}, t("Server URL (streamable HTTP)"), h("input", { value: f.url, placeholder: "http://127.0.0.1:8090/mcp", spellcheck: "false", oninput: (e) => (f.url = e.target.value) }))),
        h("label", {}, t("Headers (e.g. Authorization) — stored on the server, never shown again")),
        kvEditor(f.headers, t("Header")),
        h("details", { class: "adv" }, h("summary", {}, t("Advanced")),
          h("div", { class: "grid2" },
            h("label", {}, t("Local command (stdio; the server must set HARNESS_MCP_ALLOW_STDIO)"), h("input", { value: f.command, placeholder: "npx", oninput: (e) => (f.command = e.target.value) })),
            h("label", {}, t("Arguments"), h("input", { value: f.args, placeholder: "-y @scope/server", oninput: (e) => (f.args = e.target.value) })),
            h("label", {}, t("Call timeout"), h("input", { value: f.callTimeout, placeholder: "30s", oninput: (e) => (f.callTimeout = e.target.value) })))),
        h("div", { class: "probe-bar" },
          h("button", { class: "btn", type: "button", disabled: f.probing, onclick: probe }, t(f.probing ? "Connecting…" : (f.remote ? "Reconnect" : "Connect & list tools"))),
          h("span", { class: "muted small" }, f.remote ? t("{n} tools on the server · tick the ones the agent may call", { n: f.remote.length }) : t("Only ticked tools are exposed to the agent."))),
        f.probeError ? h("div", { class: "error small" }, f.probeError) : null,
        remote.length ? h("table", { class: "tool-table" }, h("thead", {}, h("tr", {}, h("th", {}), h("th", {}, t("Tool")), h("th", {}, t("Risk")), h("th", {}, t("Target arg")))), h("tbody", {}, toolRows)) : null,
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " " + t("Enabled")));
      return editorShell(x ? t("MCP server · {name}", { name: x.name }) : t("Add MCP server"), fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("mcp-servers", x.name) }, t("Delete")) : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, t("Cancel")),
        h("button", { class: "btn primary", onclick: () => {
          if (!f.name.trim()) return toast(t("Name is required."), true);
          if (!Object.keys(f.allow).length) return toast(t("Tick at least one tool (Connect & list tools)."), true);
          saveExt("mcp-servers", f.name.trim(), buildSpec(true), f.enabled, x && x.version);
        } }, t("Save"))]);
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
          h("label", {}, t("Name") + " ", h("input", { value: f.name, disabled: !!x, placeholder: t("e.g. {x}", { x: "deploy_history" }), spellcheck: "false", oninput: (e) => (f.name = e.target.value) }),
            h("span", { class: "muted small" }, t("The tool is called {name}", { name: "http." + (f.name || "<name>") }))),
          h("label", {}, t("Risk"), riskSelect(f.risk, (v) => (f.risk = v)))),
        h("label", {}, t("Description (shown to the model)"), h("input", { value: f.description, placeholder: t("Recent deploys of a service"), oninput: (e) => (f.description = e.target.value) })),
        h("div", { class: "grid-url" },
          h("label", {}, t("Method"), h("select", { onchange: (e) => (f.method = e.target.value) }, ["POST", "GET"].map((m) => h("option", { selected: m === f.method }, m)))),
          h("label", {}, t("Endpoint URL"), h("input", { value: f.url, placeholder: "https://ops.internal/api/deploys", spellcheck: "false", oninput: (e) => (f.url = e.target.value) }))),
        h("label", {}, t("Headers — stored on the server, never shown again")),
        kvEditor(f.headers, t("Header")),
        h("label", {}, t("Input schema (JSON Schema, type object)"), h("textarea", { rows: 7, class: "mono", spellcheck: "false", oninput: (e) => (f.schema = e.target.value) }, f.schema)),
        h("div", { class: "grid2" },
          h("label", {}, t("Target argument (policy scope, optional)"), h("input", { value: f.targetArg, placeholder: "service", oninput: (e) => (f.targetArg = e.target.value) })),
          h("label", {}, t("Timeout"), h("input", { value: f.timeout, placeholder: "15s", oninput: (e) => (f.timeout = e.target.value) }))),
        h("p", { class: "muted small" }, t("GET tools must be R0. Calls carry Idempotency-Key, X-Harness-Run-ID and X-Harness-Step-ID headers.")),
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " " + t("Enabled")));
      return editorShell(x ? t("HTTP tool · {name}", { name: x.name }) : t("Add HTTP tool"), fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("http-tools", x.name) }, t("Delete")) : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, t("Cancel")),
        h("button", { class: "btn primary", onclick: () => {
          let schema;
          try { schema = JSON.parse(f.schema || "{}"); } catch (e) { return toast(t("Input schema is not valid JSON: {err}", { err: errText(e) }), true); }
          const spec = { description: f.description, url: f.url.trim(), method: f.method, risk: f.risk, input_schema: schema };
          const hd = kvToMap(f.headers);
          if (Object.keys(hd).length) spec.headers = hd;
          if (f.targetArg.trim()) spec.target_arg = f.targetArg.trim();
          if (f.timeout.trim()) spec.timeout = f.timeout.trim();
          saveExt("http-tools", f.name.trim(), spec, f.enabled, x && x.version);
        } }, t("Save"))]);
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
          h("label", {}, t("Name"), h("input", { value: f.name, disabled: !!x, placeholder: t("e.g. {x}", { x: "db-triage" }), spellcheck: "false", oninput: (e) => (f.name = e.target.value) })),
          h("label", {}, t("When to use it"), h("input", { value: f.description, placeholder: t("Database incidents"), oninput: (e) => (f.description = e.target.value) }))),
        h("label", {}, t("Instructions (Markdown) — added to the system prompt of every run"),
          h("textarea", { rows: 14, class: "mono", placeholder: t("1. Search the runbooks first (use mcp.demo.runbook_search).\n2. Check connection-pool metrics before suggesting a restart.\n3. Cite the runbook you followed."),
            oninput: (e) => { f.instructions = e.target.value; $("#skill-size").textContent = sizeText(f.instructions); } }, f.instructions)),
        h("div", { class: "muted small", id: "skill-size" }, sizeText(f.instructions)),
        h("div", { class: "seg" },
          h("label", {}, h("input", { type: "radio", name: "mode", checked: f.mode !== "manual", onchange: () => (f.mode = "always") }), " " + t("Always on (every run)")),
          h("label", {}, h("input", { type: "radio", name: "mode", checked: f.mode === "manual", onchange: () => (f.mode = "manual") }),
            " " + t("Only in chats that invoke it with /{name}", { name: f.name || "name" }))),
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " " + t("Enabled")));
      return editorShell(x ? t("Skill · {name}", { name: x.name }) : t("New skill"), fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("skills", x.name) }, t("Delete")) : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, t("Cancel")),
        h("button", { class: "btn primary", onclick: () => saveExt("skills", f.name.trim(), { description: f.description, instructions: f.instructions, mode: f.mode }, f.enabled, x && x.version) }, t("Save"))]);
    },
  };
  renderCustomize();
}
// --- web search backend ---
function editWeb(x) {
  const s = (x && x.spec) || {};
  const f = { name: x ? x.name : "search", provider: s.provider || "brave", endpoint: s.endpoint || "",
    key: "", keySaved: !!x, maxResults: s.max_results || 5, enabled: x ? x.enabled : true };
  cz.edit = {
    render() {
      const fields = h("div", {},
        h("div", { class: "grid2" },
          h("label", {}, t("Name"), h("input", { value: f.name, disabled: !!x, placeholder: "search", spellcheck: "false", oninput: (e) => (f.name = e.target.value) })),
          h("label", {}, t("Provider"), h("select", { onchange: (e) => (f.provider = e.target.value) },
            ["brave", "tavily", "bing", "generic"].map((p) => h("option", { value: p, selected: f.provider === p }, p))))),
        h("label", {}, t("Endpoint (optional; required for generic)"),
          h("input", { value: f.endpoint, placeholder: f.provider === "generic" ? "https://search.internal/api" : t("provider default"),
            spellcheck: "false", oninput: (e) => (f.endpoint = e.target.value) })),
        h("div", { class: "grid2" },
          h("label", {}, t("API key"), h("input", { value: f.key, type: "password", autocomplete: "off",
            placeholder: f.keySaved ? t("saved · leave blank to keep") : "", oninput: (e) => (f.key = e.target.value) })),
          h("label", {}, t("Max results"), h("input", { value: f.maxResults, type: "number", min: "1", max: "10",
            oninput: (e) => (f.maxResults = e.target.value) }))),
        h("p", { class: "muted small" }, t("web.fetch works without configuration. A search backend adds web.{name} for current facts.", { name: f.name || "search" })),
        h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " " + t("Enabled")));
      return editorShell(x ? t("Web search · {name}", { name: x.name }) : t("Add search backend"), fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("web-tools", x.name) }, t("Delete")) : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; renderCustomize(); } }, t("Cancel")),
        h("button", { class: "btn primary", onclick: () => saveWeb(f, x) }, t("Save"))]);
    },
  };
  renderCustomize();
}
function saveWeb(f, x) {
  const name = f.name.trim();
  if (!name) return toast(t("Give the search backend a name first."), true);
  if (f.provider === "generic" && !f.endpoint.trim()) return toast(t("The generic provider needs an endpoint."), true);
  const spec = { provider: f.provider, max_results: Number(f.maxResults) || 5 };
  if (f.endpoint.trim()) spec.endpoint = f.endpoint.trim();
  if (f.key.trim()) spec.api_key = f.key.trim();
  saveExt("web-tools", name, spec, f.enabled, x && x.version);
}
const sizeText = (s) => t("{n} / 16,384 bytes", { n: new TextEncoder().encode(s || "").length.toLocaleString(locale()) });

// --- models: which LLM the agent uses for this tenant (Settings → Models) ---
const ANTHROPIC_MODELS = [["claude-opus-5-5", "Claude Opus 5.5", "most capable"], ["claude-sonnet-5-5", "Claude Sonnet 5.5", "balanced"],
  ["claude-haiku-5-5", "Claude Haiku 5.5", "fastest"]];
const EFFORTS = ["", "low", "medium", "high", "xhigh", "max"];
// Vendor templates. Most vendors speak the OpenAI chat-completions protocol,
// so a preset is only a base URL and example model IDs; Anthropic and Gemini
// use their native adapters. Example IDs age quickly: "Fetch models" asks
// the vendor for the IDs the key can use.
const PRESETS = [
  { id: "openai", name: "OpenAI", mark: "AI", provider: "openai", base: "https://api.openai.com/v1", mct: true,
    models: ["gpt-6.1-sol", "gpt-6-luna", "gpt-5.6-terra"], key: "sk-…" },
  { id: "anthropic", name: "Anthropic Claude", mark: "A", provider: "anthropic", base: "", models: ANTHROPIC_MODELS.map((m) => m[0]), key: "sk-ant-…" },
  { id: "gemini", name: "Google Gemini", mark: "G", provider: "gemini", base: "", models: ["gemini-flash-latest", "gemini-pro-latest", "gemini-3.8-flash"], key: "AIza…" },
  { id: "deepseek", name: "DeepSeek", mark: "DS", provider: "openai", base: "https://api.deepseek.com/v1", models: ["deepseek-v4-pro", "deepseek-v4-flash"], key: "sk-…" },
  { id: "qwen", name: "Qwen (Alibaba Cloud)", mark: "通", provider: "openai", base: "https://dashscope.aliyuncs.com/compatible-mode/v1",
    models: ["qwen3.7-plus", "qwen3.7-max", "qwen3.6-flash"], key: "sk-…" },
  { id: "qianfan", name: "ERNIE (Baidu Qianfan)", mark: "文", provider: "openai", base: "https://qianfan.baidubce.com/v2", models: [], key: "bce-v3/…" },
  { id: "zhipu", name: "Zhipu GLM", mark: "智", provider: "openai", base: "https://open.bigmodel.cn/api/paas/v4", models: ["glm-5"], key: "" },
  { id: "kimi", name: "Kimi (Moonshot AI)", mark: "K", provider: "openai", base: "https://api.moonshot.cn/v1", models: [], key: "sk-…" },
  { id: "doubao", name: "Doubao (Volcengine Ark)", mark: "豆", provider: "openai", base: "https://ark.cn-beijing.volces.com/api/v3", models: [],
    key: "", modelHint: "Ark endpoint ID (ep-…) or model name" },
  { id: "custom", name: "OpenAI-compatible / self-hosted", mark: "</>", provider: "openai", base: "", models: [], key: "",
    basePlaceholder: "http://127.0.0.1:8000/v1" },
];
const presetOf = (spec) => PRESETS.find((p) => p.id === spec.preset) ||
  PRESETS.find((p) => p.provider === spec.provider && p.id !== "custom" && (p.provider !== "openai" || p.base === spec.base_url)) ||
  PRESETS[PRESETS.length - 1];
const presetMark = (p) => h("span", { class: "preset-mark" }, p.mark);

function modelLine(x) {
  const s = x.spec || {};
  return [t(presetOf(s).name), s.model, s.provider === "openai" && s.base_url ? s.base_url : "",
    s.effort ? t("effort {v}", { v: s.effort }) : ""].filter(Boolean).join(" · ");
}

function renderModelTab() {
  const cur = (cz.data && cz.data.current_model) || {};
  const models = listFor("model").slice().sort((a, b) => (b.spec.default ? 1 : 0) - (a.spec.default ? 1 : 0) || a.name.localeCompare(b.name));
  const curBox = h("div", { class: "current-model" + (cur.tenant_configured ? " on" : "") },
    h("div", { class: "muted small" }, t("New runs use")),
    h("div", { class: "cm-name" }, cur.model || "—"),
    h("div", { class: "muted small" }, cur.error ? cur.error :
      t(cur.tenant_configured ? "configured for this tenant" :
        cur.model === "rule-based-sre" ? "the built-in scripted demo model — it only calls tools when you write “use <tool> {json}”. Add a model below to let the agent reason and pick tools itself." :
        "the deployment's default model")));
  return h("div", {}, curBox,
    h("div", { class: "list-bar" }, h("button", { class: "btn primary", onclick: () => pickPreset() }, t(models.length ? "Add another model" : "Add a model"))),
    models.map((x) => h("div", { class: "ext-card", onclick: () => editModel(x) },
      presetMark(presetOf(x.spec)),
      h("div", { class: "ext-main" },
        h("div", { class: "ext-title" }, h("span", { class: "mono" }, x.name),
          x.spec.default ? h("span", { class: "badge s-info" }, t("default")) : null, statusBadge(x)),
        h("div", { class: "muted small" }, modelLine(x)),
        x.status && x.status.error ? h("div", { class: "error small" }, x.status.error) : null),
      h("div", { class: "ext-side" },
        !x.spec.default && x.enabled ? h("button", { class: "btn", onclick: (e) => { e.stopPropagation(); setDefaultModel(x); } }, t("Make default")) : null,
        h("div", { class: "muted small" }, "v" + x.version, " · ", ago(x.updated_at))))),
    h("p", { class: "muted small" }, t("The key is stored on the server and used only for this tenant's runs; it is never sent back to clients. "),
      t("The default model is tried first; the others are fallbacks.")));
}

// setDefaultModel marks x as the tenant's default and clears the flag on the
// others. Keys stay on the server: blank keys keep the stored ones.
async function setDefaultModel(x) {
  const strip = (spec) => { const o = { ...spec }; delete o.api_key; return o; };
  try {
    for (const m of listFor("model")) {
      if (m.name !== x.name && m.spec.default) await api().PutExtension("models", m.name, { ...strip(m.spec), default: false }, m.enabled, m.version);
    }
    await api().PutExtension("models", x.name, { ...strip(x.spec), default: true }, x.enabled, x.version);
    toast(t("{name} is now the default model", { name: x.name }));
    await extReload();
  } catch (e) {
    const m = errText(e);
    toast(m.includes("403") ? t("Changing extensions needs the admin role.") : m, true);
  }
}

// pickPreset shows the vendor grid before the model editor.
function pickPreset() {
  cz.edit = {
    render() {
      return editorShell(t("Add a model"), h("div", {},
        h("p", { class: "muted small" }, t("Choose the vendor. Vendors with an OpenAI-compatible API only need a base URL, a key and a model ID.")),
        h("div", { class: "preset-grid" }, PRESETS.map((p) => h("button", { class: "preset-card", onclick: () => editModel(null, p) },
          presetMark(p),
          h("span", { class: "preset-text" }, h("span", { class: "preset-name" }, t(p.name)),
            h("span", { class: "muted small" }, p.provider === "openai" ? t("OpenAI-compatible") : p.provider === "gemini" ? "Gemini API" : "Messages API")))))),
      [h("span", { class: "spacer" }), h("button", { class: "btn ghost", onclick: () => { cz.edit = null; extRerender(); } }, t("Cancel"))]);
    },
  };
  extRerender();
}

function editModel(x, preset) {
  const s = (x && x.spec) || {};
  const p = preset || presetOf(s);
  const f = { name: x ? x.name : uniqueModelName(p.id), provider: s.provider || p.provider, preset: p.id,
    model: s.model || p.models[0] || "", apiKey: "", baseURL: x ? s.base_url || "" : p.base, effort: s.effort || "",
    ctx: s.context_window || "", maxOut: s.max_output || "", mct: x ? !!s.max_completion_tokens : !!p.mct,
    isDefault: x ? !!s.default : listFor("model").length === 0, enabled: x ? x.enabled : true,
    testing: false, test: null, testErr: null, listing: false, listed: null, listErr: null };
  const spec = () => {
    const o = { provider: f.provider, preset: f.preset, model: f.model.trim(), api_key: f.apiKey };
    if (f.baseURL.trim()) o.base_url = f.baseURL.trim();
    if (f.provider === "anthropic" && f.effort) o.effort = f.effort;
    if (f.provider !== "anthropic") {
      if (+f.ctx > 0) o.context_window = +f.ctx;
      if (+f.maxOut > 0) o.max_output = +f.maxOut;
    }
    if (f.provider === "openai" && f.mct) o.max_completion_tokens = true;
    if (f.isDefault) o.default = true;
    return o;
  };
  const test = async () => {
    if (!f.name.trim() || !f.model.trim()) return toast(t("Name and model are required."), true);
    f.testing = true; f.test = null; f.testErr = null; extRerender();
    try { f.test = await api().TestModel(f.name.trim(), spec()); } catch (e) { f.testErr = errText(e); }
    f.testing = false; extRerender();
  };
  const fetchModels = async () => {
    if (!f.name.trim()) return toast(t("Name is required."), true);
    f.listing = true; f.listErr = null; extRerender();
    try { f.listed = ((await api().ListModels(f.name.trim(), spec())) || {}).models || []; } catch (e) { f.listErr = errText(e); }
    f.listing = false; extRerender();
  };
  cz.edit = {
    render() {
      const choices = [...new Set([...(f.listed || []), ...p.models])];
      const keyHint = x && s.api_key !== undefined ? t("saved · leave blank to keep") : p.key;
      const fields = h("div", {},
        h("div", { class: "editor-preset" }, presetMark(p), h("div", {}, h("div", { class: "preset-name" }, t(p.name)),
          h("div", { class: "muted small" }, f.provider === "openai" ? t("OpenAI-compatible chat completions") :
            f.provider === "gemini" ? "Gemini generateContent" : "Anthropic Messages"))),
        h("div", { class: "grid-name-model" },
          h("label", {}, t("Name"), h("input", { value: f.name, disabled: !!x, spellcheck: "false", oninput: (e) => (f.name = e.target.value) })),
          h("label", {}, t("Model"),
            h("div", { class: "input-with-btn" },
              h("input", { value: f.model, list: "model-choices", spellcheck: "false", placeholder: p.modelHint ? t(p.modelHint) : "",
                oninput: (e) => (f.model = e.target.value) }),
              h("button", { type: "button", class: "btn", disabled: f.listing, onclick: fetchModels }, t(f.listing ? "Loading…" : "Fetch models"))),
            h("datalist", { id: "model-choices" }, choices.map((m) => h("option", { value: m }))))),
        f.listErr ? h("div", { class: "error small" }, f.listErr) : null,
        f.listed ? h("div", { class: "model-chips" }, f.listed.length ? f.listed.slice(0, 40).map((m) =>
          h("button", { type: "button", class: "chip" + (m === f.model ? " on" : ""), onclick: () => { f.model = m; extRerender(); } }, m))
          : h("span", { class: "muted small" }, t("The vendor returned no models for this key."))) :
          h("p", { class: "muted small" }, t("Example model IDs may be outdated; “Fetch models” lists the ones your key can use.")),
        h("label", {}, t("API key"), h("input", { type: "password", autocomplete: "off", value: f.apiKey, placeholder: keyHint,
          oninput: (e) => (f.apiKey = e.target.value) })),
        h("div", { class: "grid2" },
          h("label", {}, t(f.provider === "openai" ? "Base URL" : "Base URL (optional)"), h("input", { value: f.baseURL, spellcheck: "false",
            placeholder: p.basePlaceholder || (f.provider === "anthropic" ? "https://api.anthropic.com" : f.provider === "gemini" ? "https://generativelanguage.googleapis.com/v1beta" : ""),
            oninput: (e) => (f.baseURL = e.target.value) })),
          f.provider === "anthropic" ? h("label", {}, t("Effort"), h("select", { onchange: (e) => (f.effort = e.target.value) },
            EFFORTS.map((v) => h("option", { value: v, selected: v === f.effort }, v || t("default"))))) : h("div")),
        f.provider !== "anthropic" ? h("details", { class: "adv" }, h("summary", {}, t("Advanced")),
          h("div", { class: "grid2" },
            h("label", {}, t("Context window"), h("input", { value: f.ctx, placeholder: f.provider === "gemini" ? "1048576" : "128000", oninput: (e) => (f.ctx = e.target.value) })),
            h("label", {}, t("Max output"), h("input", { value: f.maxOut, placeholder: f.provider === "gemini" ? "65536" : "16000", oninput: (e) => (f.maxOut = e.target.value) }))),
          f.provider === "openai" ? h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.mct, onchange: (e) => (f.mct = e.target.checked) }),
            " " + t("Send max_completion_tokens (OpenAI reasoning models)")) : null) : null,
        h("div", { class: "probe-bar" },
          h("button", { class: "btn", type: "button", disabled: f.testing, onclick: test }, t(f.testing ? "Testing…" : "Test connection")),
          f.test ? h("span", { class: "ok small" }, t("✓ {model} replied “{reply}” in {ms} ms", { model: f.test.model, reply: f.test.reply, ms: f.test.latency_ms })) : null),
        f.testErr ? h("div", { class: "error small" }, f.testErr) : null,
        h("div", { class: "check-row" },
          h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.isDefault, onchange: (e) => (f.isDefault = e.target.checked) }), " " + t("Default model")),
          h("label", { class: "check" }, h("input", { type: "checkbox", checked: f.enabled, onchange: (e) => (f.enabled = e.target.checked) }), " " + t("Enabled"))));
      return editorShell(x ? t("Model · {name}", { name: x.name }) : t("Add a model"), fields, [
        x ? h("button", { class: "btn danger", onclick: () => deleteExt("models", x.name) }, t("Delete")) : null,
        h("span", { class: "spacer" }),
        h("button", { class: "btn ghost", onclick: () => { cz.edit = null; extRerender(); } }, t("Cancel")),
        h("button", { class: "btn primary", onclick: async () => {
          if (!f.name.trim() || !f.model.trim()) return toast(t("Name and model are required."), true);
          if (f.isDefault) for (const m of listFor("model")) {
            if (m.name !== f.name.trim() && m.spec.default) {
              const o = { ...m.spec, default: false }; delete o.api_key;
              try { await api().PutExtension("models", m.name, o, m.enabled, m.version); } catch (e) { return toast(errText(e), true); }
            }
          }
          saveExt("models", f.name.trim(), spec(), f.enabled, x && x.version);
        } }, t("Save"))]);
    },
  };
  extRerender();
}

function uniqueModelName(base) {
  const taken = new Set(listFor("model").map((m) => m.name));
  let n = base, i = 2;
  while (taken.has(n)) n = `${base}-${i++}`;
  return n;
}

// --- tools: everything a run of this tenant can be offered ---
function renderToolsTab() {
  const tl = cz.tools || {};
  const list = tl.tools || [];
  const src = (d) => (!d.source || d.source === "native") ? t("built-in") : d.source;
  return h("div", {},
    h("p", { class: "muted small" }, t("Default profile") + " ", h("span", { class: "mono" }, tl.profile || "?"),
      t(". Tools it does not allow are hidden from runs that use it. In a chat, “use <tool> {json}” asks the agent to call a tool.")),
    h("div", { class: "tool-grid" }, list.map((d) => {
      const desc = (d.description || "").replace(/^\[[^\]]*\]\s*/, "");
      const allowed = !!d.allowed_by_profile;
      const on = allowed && d.enabled !== false;
      return h("article", { class: "tool-card" + (on ? "" : " off"), title: desc },
        h("div", { class: "tool-card-head" },
          h("span", { class: "tool-name mono" }, d.name),
          h("span", { class: "risk-tag r" + (d.risk || "R0").slice(1) }, d.risk),
          !allowed ? h("span", { class: "badge s-idle" }, t("blocked"))
            : on ? h("span", { class: "badge s-ok" }, t("allowed")) : h("span", { class: "badge s-idle" }, t("disabled")),
          h("label", { class: "switch", title: allowed ? t("Hide this tool from runs of this tenant") : t("The profile does not allow this tool") },
            h("input", { type: "checkbox", checked: on, disabled: !allowed, "aria-label": t("Toggle {name}", { name: d.name }), onchange: (e) => toggleTool(d.name, e.target.checked) }),
            h("span", { class: "track" }))),
        h("p", { class: "tool-desc" }, desc),
        h("div", { class: "tool-foot" },
          h("span", { class: "tool-source" }, src(d)),
          d.trust ? h("span", { class: "tool-trust" }, d.trust) : null));
    })));
}

async function toggleTool(name, enabled) {
  try {
    await api().SetToolEnabled(name, enabled);
    toast(t(enabled ? "Enabled {name}" : "Disabled {name}", { name }));
    cz.tools = await api().Tools();
  } catch (e) {
    toast(errText(e), true);
  }
  renderCustomize();
}

// ---------- settings: appearance, connection, models, notifications, about ----------
const SETTINGS_TABS = [["appearance", "Appearance"], ["connection", "Connection & account"], ["models", "Models"],
  ["notifications", "Notifications"], ["about", "About"]];
const st = { tab: "appearance", settings: null, about: null, aboutLoading: false };
const POLL_CHOICES = [5, 8, 15, 30, 60];

async function showSettings(tab) {
  if (tab) { st.tab = tab; cz.edit = null; }
  state.view = "settings";
  stopWatching();
  renderSessions();
  $("#chat-head").replaceChildren();
  $("#chat-head").className = "chat-head";
  $("#composer-wrap").hidden = true;
  markSide();
  st.settings = await api().GetSettings().catch(() => st.settings || {});
  if (st.tab === "models") {
    renderSettings(true);
    try {
      cz.data = await api().Extensions();
      cz.error = null;
    } catch (e) {
      cz.error = errText(e);
    }
  }
  if (st.tab === "about" && !st.about) loadAbout();
  renderSettings();
}

function renderSettings(loading) {
  if (state.view !== "settings") return;
  const nav = h("nav", { class: "settings-nav" }, SETTINGS_TABS.map(([id, label]) =>
    h("button", { class: "settings-tab" + (st.tab === id ? " on" : ""), onclick: () => showSettings(id) }, t(label))));
  const body = {
    appearance: renderAppearance, connection: renderConnection, notifications: renderNotifications, about: renderAbout,
    models: () => loading && !cz.data ? h("div", { class: "muted" }, t("Loading…")) : cz.error ? h("div", { class: "error" }, cz.error)
      : cz.edit ? cz.edit.render() : renderModelTab(),
  }[st.tab]();
  $("#thread").replaceChildren(h("div", { class: "settings" },
    h("h1", {}, t("Settings")),
    h("div", { class: "settings-body" }, nav, h("section", { class: "settings-panel" }, body))));
}

// row is one setting: label and help on the left, control on the right.
const row = (label, help, control) => h("div", { class: "set-row" },
  h("div", { class: "set-text" }, h("div", { class: "set-label" }, label), help ? h("div", { class: "muted small" }, help) : null),
  h("div", { class: "set-control" }, control));
const group = (title, ...rows) => h("div", { class: "set-group" }, title ? h("h3", {}, title) : null, h("div", { class: "set-card" }, rows));

function segmented(value, options, onchange) {
  return h("div", { class: "segmented" }, options.map(([v, label]) =>
    h("button", { type: "button", class: v === value ? "on" : "", onclick: () => onchange(v) }, label)));
}
function toggle(checked, onchange, label) {
  return h("label", { class: "switch", title: label || "" },
    h("input", { type: "checkbox", checked, "aria-label": label || "", onchange: (e) => onchange(e.target.checked) }), h("span", {}));
}

async function savePrefs(patch) {
  const prefs = { ...prefsOf(st.settings), ...patch };
  try {
    await api().SetPreferences(prefs);
    st.settings = { ...st.settings, ...prefs };
    applyPrefs(st.settings);
    renderSettings();
  } catch (e) {
    toast(errText(e), true);
  }
}
const prefsOf = (s) => ({ theme: (s && s.theme) || "", no_glass: !!(s && s.no_glass), notify_off: !!(s && s.notify_off),
  poll_seconds: (s && s.poll_seconds) || 0 });

function renderAppearance() {
  const s = st.settings || {};
  return h("div", {},
    group(t("Language"),
      row(t("Interface language"), t("The language applies to this app, its notifications and exported chats. Replies from the agent are not translated."),
        segmented(i18n.lang, [["en", "English"], ["zh", "中文"]], (v) => setLanguage(v, true)))),
    group(t("Theme"),
      row(t("Color scheme"), t("System follows the macOS appearance."),
        segmented(s.theme || "", [["", t("System")], ["light", t("Light")], ["dark", t("Dark")]], (v) => savePrefs({ theme: v }))),
      row(t("Glass effect"), t("Translucent sidebar, buttons and floating panels. Turn off for solid surfaces."),
        toggle(!s.no_glass, (on) => savePrefs({ no_glass: !on }), t("Glass effect")))));
}

function renderConnection() {
  const s = st.settings || {};
  const who = state.who;
  const c = s.creds || {};
  return h("div", {},
    group(t("Account"),
      row(t("Status"), null, who ? h("span", { class: "badge s-ok" }, t("Connected")) : h("span", { class: "badge s-idle" }, t("Not connected"))),
      who ? row(t("Signed in as"), null, h("span", { class: "mono" }, `${who.subject} · ${who.tenant}`)) : null,
      who ? row(t("Roles"), null, h("span", { class: "mono" }, (who.roles || []).join(", ") || "—")) : null,
      who ? row(t("Authentication"), null, h("span", { class: "mono" }, who.method || c.mode || "—")) : null),
    group(t("Server"),
      row(t("Server URL"), null, h("span", { class: "mono" }, s.server_url || "—")),
      row(t("Sign-in method"), null, h("span", {}, t(c.mode === "bearer" ? "Bearer token" : "Development headers")))),
    h("div", { class: "set-actions" },
      h("button", { class: "btn", onclick: openConnection }, t("Edit connection…")),
      who ? h("button", { class: "btn danger", onclick: signOut }, t("Sign out")) : null));
}

async function signOut() {
  if (!(await ask({ title: t("Sign out?"), text: t("The app stops talking to the server until you connect again."), ok: t("Sign out"), danger: true }))) return;
  try {
    await api().Disconnect();
    showWho(null);
    state.sessions = [];
    state.approvals = [];
    state.demoModel = null;
    updateInboxBadge();
    renderSettings();
    renderSessions();
  } catch (e) {
    toast(errText(e), true);
  }
}

function renderNotifications() {
  const s = st.settings || {};
  const poll = s.poll_seconds || 8;
  return h("div", {},
    group(t("Approvals"),
      row(t("System notifications"), t("Show a macOS notification when a tool call needs your approval."),
        toggle(!s.notify_off, (on) => savePrefs({ notify_off: !on }), t("System notifications"))),
      row(t("Refresh interval"), t("How often the approvals inbox checks the server."),
        h("select", { class: "set-select", onchange: (e) => savePrefs({ poll_seconds: +e.target.value }) },
          POLL_CHOICES.map((n) => h("option", { value: n, selected: n === poll }, t("{n} seconds", { n })))))));
}

async function loadAbout() {
  st.aboutLoading = true;
  renderSettings();
  try { st.about = await api().About(); } catch (e) { st.about = { error: errText(e) }; }
  st.log = await api().RecentLog(30).catch(() => []);
  st.aboutLoading = false;
  renderSettings();
}

function renderAbout() {
  const a = st.about;
  if (!a) return h("div", { class: "muted" }, t("Loading…"));
  const srv = a.server || {};
  const health = srv.ok ? h("span", { class: "badge s-ok" }, t("Healthy · {ms} ms", { ms: srv.latency_ms }))
    : h("span", { class: "badge s-bad", title: srv.error || "" }, t("Unreachable"));
  const events = (st.log || []).map((e) => h("div", { class: "log-row" },
    h("span", { class: "log-time mono" }, new Date(e.time).toLocaleTimeString(locale())),
    h("span", { class: "log-level " + e.level }, e.level), h("span", { class: "log-msg" }, e.msg)));
  return h("div", {},
    h("div", { class: "about-head" }, h("img", { class: "about-logo", src: "logo.png", alt: "" }),
      h("div", {}, h("div", { class: "about-name" }, "SRE Harness"), h("div", { class: "muted small" }, t("Version {v}", { v: a.version || "?" })))),
    group(t("Client"),
      row(t("Version"), null, h("span", { class: "mono" }, a.version || "—")),
      row(t("Platform"), null, h("span", { class: "mono" }, `${a.os || "?"}/${a.arch || "?"} · ${a.go || ""}`)),
      row(t("Settings file"), null, h("span", { class: "mono small path" }, a.settings_path || "—"))),
    group(t("Server"),
      row(t("Server URL"), null, h("span", { class: "mono" }, a.server_url || "—")),
      row(t("Health"), srv.error || null, st.aboutLoading ? h("span", { class: "muted small" }, t("Checking…")) : health)),
    h("div", { class: "set-actions" },
      h("button", { class: "btn", disabled: st.aboutLoading, onclick: loadAbout }, t("Check again")),
      h("button", { class: "btn", onclick: exportDiagnostics }, t("Export diagnostics…"))),
    h("p", { class: "muted small" }, t("Diagnostics contain the client version, settings without the token, server health and recent client events. No chat content.")),
    group(t("Recent events"), events.length ? h("div", { class: "log" }, events) : row(t("Nothing logged yet."), null, null)));
}

async function exportDiagnostics() {
  try {
    const path = await api().ExportDiagnostics();
    if (path) toast(t("Saved to {path}", { path }));
  } catch (e) {
    toast(errText(e), true);
  }
}

// applyPrefs applies theme and glass to the page (and the native window).
function applyPrefs(s) {
  const theme = (s && s.theme) || "";
  if (theme) document.documentElement.dataset.theme = theme; else delete document.documentElement.dataset.theme;
  document.documentElement.classList.toggle("no-glass", !!(s && s.no_glass));
  const rt = window.runtime || {};
  const fn = theme === "dark" ? rt.WindowSetDarkTheme : theme === "light" ? rt.WindowSetLightTheme : rt.WindowSetSystemDefaultTheme;
  if (typeof fn === "function") fn();
}

// setLanguage switches the UI language and redraws the current view.
async function setLanguage(lang, save) {
  i18n.lang = lang === "zh" ? "zh" : "en";
  applyStatic();
  if (save) {
    try { await api().SetLanguage(i18n.lang); } catch (e) { toast(errText(e), true); }
  }
  showWho(state.who);
  renderSessions();
  if (state.view === "chat") render();
  else if (state.view === "inbox") renderInbox();
  else if (state.view === "customize") renderCustomize();
  else if (state.view === "settings") renderSettings();
  else if (state.view === "plugins") showPlugins();
}

function markSide() {
  $("#btn-inbox").classList.toggle("on", state.view === "inbox");
  $("#btn-customize").classList.toggle("on", state.view === "customize");
  $("#btn-gallery").classList.toggle("on", state.view === "gallery");
  $("#btn-plugins").classList.toggle("on", state.view === "plugins");
  $("#btn-settings").classList.toggle("on", state.view === "settings");
}

// ---------- wiring ----------
function bind() {
  $("#btn-conn").onclick = openConnection;
  $("#btn-new").onclick = newChat;
  $("#btn-plugins").onclick = showPlugins;
  $("#btn-customize").onclick = () => showCustomize();
  $("#btn-gallery").onclick = showGallery;
  $("#btn-inbox").onclick = showInbox;
  $("#btn-settings").onclick = () => showSettings();
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
  $("#btn-slash").addEventListener("mousedown", (e) => e.preventDefault()); // keep the caret in the input
  $("#btn-slash").onclick = openSlash;
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
      toast(t("Live updates stopped: {err}", { err: msg.error }), true);
    }
  });
  setInterval(() => { if (!document.hidden) loadSessions(); }, 5000);
  // Safety net for missed stream events: refresh the active run periodically.
  setInterval(() => { const t = activeTurn(); if (t && !document.hidden) scheduleRun(t.run_id); }, 6000);
}

async function loadModelHint() {
  try {
    const data = await api().Extensions();
    const cur = data.current_model || {};
    state.demoModel = !cur.tenant_configured && cur.model === "rule-based-sre";
  } catch {
    state.demoModel = null;
  }
  if (state.view === "chat" && !state.turns.length) render();
}

async function boot() {
  // The native window is translucent (macOS vibrancy shows behind the sidebar);
  // browser preview has no desktop behind it and keeps an opaque root.
  document.documentElement.classList.toggle("native", !!(window.runtime && window.runtime.WindowSetTitle));
  // Saved choice first; otherwise follow the system language.
  const saved = await api().GetSettings().catch(() => ({}));
  i18n.lang = saved.language || ((navigator.language || "").toLowerCase().startsWith("zh") ? "zh" : "en");
  st.settings = saved;
  applyPrefs(saved);
  applyStatic();
  bind();
  render();
  if (await api().Connected()) {
    try {
      showWho(await api().Whoami());
      await loadSessions();
      loadModelHint();
      return;
    } catch (e) {
      toast(errText(e), true);
    }
  }
  openConnection();
}

window.addEventListener("DOMContentLoaded", boot);
