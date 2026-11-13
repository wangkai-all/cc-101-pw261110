/* 异步导出采集台前端逻辑（原生 JS，无依赖） */
"use strict";

const PHASES = [
  { key: "WAITING", label: "等待" },
  { key: "GENERATING", label: "生成" },
  { key: "DOWNLOADING", label: "下载" },
  { key: "VERIFYING", label: "校验" },
  { key: "INGESTING", label: "入库" },
];
const PHASE_ORDER = Object.fromEntries(PHASES.map((p, i) => [p.key, i]));
const PHASE_LABEL = {
  WAITING: "等待", GENERATING: "生成", DOWNLOADING: "下载",
  VERIFYING: "校验", INGESTING: "入库",
  INGESTED: "已入库", FAILED: "失败", CANCELLED: "已取消",
};

let currentTaskId = null;
let taskCache = new Map();

const $ = (id) => document.getElementById(id);
const fmtTime = (s) => (s ? s.replace("T", " ").replace("Z", " UTC").slice(0, 23) : "-");
const escapeHtml = (s) => String(s ?? "").replace(/[&<>"']/g,
  (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));

async function api(method, path, body) {
  const resp = await fetch(path, {
    method,
    headers: body ? { "Content-Type": "application/json" } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });
  const data = await resp.json().catch(() => ({}));
  if (!resp.ok) {
    const msg = data?.error?.message || `HTTP ${resp.status}`;
    throw new Error(msg);
  }
  return data;
}

function setMsg(el, text, kind = "") {
  el.textContent = text;
  el.className = "msg" + (kind ? " " + kind : "");
  if (text) setTimeout(() => { if (el.textContent === text) el.textContent = ""; }, 5000);
}

// ---------------- 创建导出 ----------------
function readFault() {
  return {
    task_seconds: Number($("fTaskSeconds").value) || 12,
    link_ttl: Number($("fLinkTtl").value) || 15,
    interrupts: Number($("fInterrupts").value) || 0,
    rate_limit_hits: Number($("fRateHits").value) || 0,
    retry_after: Number($("fRetryAfter").value) || 3,
    download_delay: Number($("fDownloadDelay").value) || 0,
    incomplete: $("fIncomplete").checked,
    fail: $("fFail").checked,
  };
}

$("btnCreate").addEventListener("click", async () => {
  const bizDate = $("bizDate").value;
  if (!bizDate) { setMsg($("createMsg"), "请先选择业务日期", "err"); return; }
  $("btnCreate").disabled = true;
  try {
    const res = await api("POST", "/api/exports", { biz_date: bizDate, fault: readFault() });
    if (res.deduplicated) {
      setMsg($("createMsg"), `该日期已有有效任务 #${res.task.id}，直接返回，未重复建单`, "ok");
    } else if (res.reused) {
      setMsg($("createMsg"), `已复用任务 #${res.task.id} 发起第 ${res.task.attempt} 次尝试`, "ok");
    } else {
      setMsg($("createMsg"), `导出任务 #${res.task.id} 已创建`, "ok");
    }
    await refreshTasks();
    openDetail(res.task.id);
  } catch (e) {
    setMsg($("createMsg"), "创建失败：" + e.message, "err");
  } finally {
    $("btnCreate").disabled = false;
  }
});

// ---------------- 任务列表 ----------------
async function refreshTasks() {
  const tasks = await api("GET", "/api/tasks");
  taskCache = new Map(tasks.map((t) => [t.id, t]));
  const body = $("taskBody");
  if (!tasks.length) {
    body.innerHTML = '<tr><td colspan="9" class="empty">暂无任务</td></tr>';
  } else {
    body.innerHTML = tasks.map(taskRow).join("");
    body.querySelectorAll("tr.clickable").forEach((tr) => {
      tr.addEventListener("click", (e) => {
        if (e.target.closest("button")) return;
        openDetail(Number(tr.dataset.id));
      });
    });
    body.querySelectorAll("button.act-cancel").forEach((b) =>
      b.addEventListener("click", () => cancelTask(Number(b.dataset.id))));
    body.querySelectorAll("button.act-retry").forEach((b) =>
      b.addEventListener("click", () => retryTask(Number(b.dataset.id))));
  }
  if (currentTaskId) refreshDetail();
  refreshOverview();
}

function taskRow(t) {
  const terminal = ["INGESTED", "FAILED", "CANCELLED"].includes(t.status);
  const cancelling = t.cancel_requested && !terminal;
  let actions = "";
  if (!terminal && t.status !== "INGESTING") {
    actions = `<button class="btn btn-danger act-cancel" data-id="${t.id}">取消</button>`;
  }
  if (t.status === "FAILED" || t.status === "CANCELLED") {
    actions = `<button class="btn btn-primary act-retry" data-id="${t.id}">重试此日期</button>`;
  }
  if (t.status === "INGESTING") actions = '<span class="kv">入库中不可取消</span>';
  return `<tr class="clickable" data-id="${t.id}">
    <td>${t.id}</td>
    <td><b>${escapeHtml(t.biz_date)}</b></td>
    <td>${PHASE_LABEL[t.phase] || t.phase}</td>
    <td>${cancelling
      ? '<span class="badge badge-cancelling">取消中…</span>'
      : `<span class="badge badge-${t.status}">${PHASE_LABEL[t.status] || t.status}</span>`}</td>
    <td>${t.attempt}</td>
    <td class="mono">${escapeHtml(t.remote_task_id || "-")}</td>
    <td class="mono">${fmtTime(t.link_expires_at)}</td>
    <td class="mono">${fmtTime(t.updated_at)}</td>
    <td>${actions}</td>
  </tr>`;
}

async function cancelTask(id) {
  if (!confirm("确认取消该任务？已取消后可选择该日期重试。")) return;
  try {
    await api("POST", `/api/tasks/${id}/cancel`);
  } catch (e) {
    alert("取消失败：" + e.message);
  }
  await refreshTasks();
}

async function retryTask(id) {
  try {
    const res = await api("POST", `/api/tasks/${id}/retry`);
    openDetail(res.id);
  } catch (e) {
    alert("重试失败：" + e.message);
  }
  await refreshTasks();
}

async function refreshOverview() {
  try {
    const o = await api("GET", "/api/overview");
    const parts = Object.entries(o.status_counts)
      .map(([k, v]) => `${PHASE_LABEL[k] || k} ${v}`);
    $("overviewText").textContent = `任务 ${o.tasks_total} · ` + (parts.join(" / ") || "-");
    $("healthDot").className = "dot dot-ok";
    $("healthText").textContent = "服务正常";
  } catch {
    $("healthDot").className = "dot dot-bad";
    $("healthText").textContent = "服务不可用";
  }
}

// ---------------- 任务详情 ----------------
async function openDetail(id) {
  currentTaskId = id;
  $("detailCard").classList.remove("hidden");
  await refreshDetail();
  $("detailCard").scrollIntoView({ behavior: "smooth", block: "start" });
}

$("btnCloseDetail").addEventListener("click", () => {
  currentTaskId = null;
  $("detailCard").classList.add("hidden");
});

document.querySelectorAll(".tab").forEach((tab) => {
  tab.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((t) => t.classList.remove("active"));
    document.querySelectorAll(".tab-panel").forEach((p) => p.classList.add("hidden"));
    tab.classList.add("active");
    $("tab-" + tab.dataset.tab).classList.remove("hidden");
  });
});

async function refreshDetail() {
  if (!currentTaskId) return;
  let d;
  try {
    d = await api("GET", `/api/tasks/${currentTaskId}/detail`);
  } catch {
    currentTaskId = null;
    $("detailCard").classList.add("hidden");
    return;
  }
  const t = d.task;
  $("detailTitle").textContent = `#${t.id} · ${t.biz_date} · 第 ${t.attempt} 次尝试`;
  renderStepper(t, d.events);
  renderFailBanner(t);
  renderRequests(d.request_logs);
  renderFiles(d.file_records);
  renderEvents(d.events);
}

function renderStepper(t, events) {
  const eventTimes = {};
  for (const e of events) {
    if (!eventTimes[e.event_type]) eventTimes[e.event_type] = e.created_at;
  }
  const stageEnter = {
    WAITING: eventTimes.TASK_CREATED || t.created_at,
    GENERATING: eventTimes.EXPORT_CREATED,
    DOWNLOADING: eventTimes.EXPORT_READY,
    VERIFYING: eventTimes.DOWNLOAD_COMPLETED,
    INGESTING: eventTimes.VERIFICATION_PASSED,
  };
  const curIdx = PHASE_ORDER[t.phase] ?? (t.status === "INGESTED" ? PHASES.length : -1);
  let html = "";
  PHASES.forEach((p, i) => {
    let cls = "";
    if (t.status === "FAILED" && i === curIdx) cls = "failed";
    else if (t.status === "CANCELLED" && i === curIdx) cls = "cancelled";
    else if (t.status === "INGESTED" || i < curIdx) cls = "done";
    else if (i === curIdx) cls = "active";
    const time = stageEnter[p.key] ? fmtTime(stageEnter[p.key]) : "";
    html += `<div class="step ${cls}">
      <span class="step-name">${p.label}${i === curIdx && t.status === "GENERATING" ? "…" : ""}</span>
      <span class="step-time">${time}</span>
    </div>`;
  });
  const finalCls = t.status === "INGESTED" ? "done"
    : t.status === "FAILED" ? "failed"
    : t.status === "CANCELLED" ? "cancelled" : "";
  html += `<div class="step ${finalCls}">
    <span class="step-name">${PHASE_LABEL[t.status] === "已入库" || PHASE_LABEL[t.status] === "失败" || PHASE_LABEL[t.status] === "已取消" ? PHASE_LABEL[t.status] : "完成"}</span>
    <span class="step-time">${t.status === "INGESTED" ? fmtTime(t.updated_at) : ""}</span>
  </div>`;
  $("pipelineStepper").innerHTML = html;
}

function renderFailBanner(t) {
  const b = $("failBanner");
  if (t.status === "FAILED") {
    b.classList.remove("hidden");
    b.innerHTML = `<b>任务失败（阶段：${escapeHtml(t.fail_stage || "-")}）</b><br>${escapeHtml(t.fail_reason || "-")}
      <div class="kv" style="margin-top:6px">失败文件保留在隔离区 quarantine/，不会进入入库查询；可点击「重试此日期」重新导出。</div>`;
  } else if (t.status === "CANCELLED") {
    b.classList.remove("hidden");
    b.innerHTML = "<b>任务已取消</b>，未产生入库数据；可点击「重试此日期」重新导出。";
  } else {
    b.classList.add("hidden");
  }
}

function prettyBody(raw) {
  if (raw == null || raw === "") return null;
  if (typeof raw === "string") {
    try { return JSON.stringify(JSON.parse(raw), null, 2); }
    catch { return raw; }
  }
  return JSON.stringify(raw, null, 2);
}

function renderRequests(logs) {
  if (!logs.length) {
    $("tab-requests").innerHTML = '<p class="kv">暂无请求记录</p>';
    return;
  }
  $("tab-requests").innerHTML = logs.slice().reverse().map((l, idx) => {
    const codeClass = l.status_code ? `status-${String(l.status_code)[0]}xx` : "status-0";
    const reqBody = prettyBody(l.request_body);
    const respBody = prettyBody(l.response_body);
    const headers = prettyBody(l.response_headers);
    const ra = l.retry_after ? `<span class="badge badge-cancelling">Retry-After: ${l.retry_after}s</span>` : "";
    return `<div class="req-item">
      <div class="req-head" data-toggle="${idx}">
        <span class="method ${l.method}">${l.method}</span>
        <span class="status-code ${codeClass}">${l.status_code || "NET"}</span>
        <span class="req-url">${escapeHtml(l.url)}</span>
        ${ra}
        <span class="event-time">${fmtTime(l.created_at)}</span>
        <span class="kv">▸</span>
      </div>
      <div class="req-body hidden" data-body="${idx}">
        ${l.error ? `<div class="banner banner-fail" style="margin:0 0 8px">${escapeHtml(l.error)}</div>` : ""}
        ${reqBody ? '<div class="lbl">请求体</div><pre>' + escapeHtml(reqBody) + "</pre>" : ""}
        ${headers ? '<div class="lbl">响应头</div><pre>' + escapeHtml(headers) + "</pre>" : ""}
        ${respBody ? '<div class="lbl">响应体</div><pre>' + escapeHtml(respBody) + "</pre>" : ""}
        ${l.url.includes("/download") ? '<div class="kv">二进制响应体已保存为本地文件，详见「文件记录」标签</div>' : ""}
      </div>
    </div>`;
  }).join("");
  $("tab-requests").querySelectorAll(".req-head").forEach((h) => {
    h.addEventListener("click", () => {
      $("tab-requests").querySelector(`[data-body="${h.dataset.toggle}"]`).classList.toggle("hidden");
    });
  });
}

function renderFiles(records) {
  if (!records.length) {
    $("tab-files").innerHTML = '<p class="kv">暂无文件记录（下载完成后生成）</p>';
    return;
  }
  $("tab-files").innerHTML = records.map((r) => {
    const manifest = prettyBody(r.manifest);
    const checks = JSON.parse(r.check_detail || "[]");
    const locBadge = r.location === "ingested"
      ? '<span class="badge badge-INGESTED">已入库目录</span>'
      : r.location === "quarantine"
        ? '<span class="badge badge-FAILED">隔离区（失败保留）</span>'
        : '<span class="badge badge-GENERATING">下载目录</span>';
    const checkHtml = checks.map((c) =>
      `<div class="check-row">
        <span class="${c.ok ? "check-ok" : "check-bad"}">${c.ok ? "✓" : "✗"}</span>
        <b>${escapeHtml(c.name)}</b>
        <span class="kv">期望 ${escapeHtml(JSON.stringify(c.expected))} / 实际 ${escapeHtml(JSON.stringify(c.actual))}</span>
      </div>`).join("");
    return `<div class="file-item">
      <div class="file-head">
        ${locBadge}
        <span class="mono">${escapeHtml(r.local_path || "-")}</span>
        <span class="kv">大小 ${r.actual_size ?? "-"} / 期望 ${r.expected_size ?? "-"} 字节</span>
        <span class="${r.check_ok ? "check-ok" : "check-bad"}">${r.check_ok ? "校验通过" : "校验失败"}</span>
      </div>
      <div class="req-body">
        <div class="lbl">清单核对（大小 · SHA256 · 数据时间范围 · 行数 · 业务日期）</div>
        ${checkHtml}
        <div class="lbl" style="margin-top:8px">SHA256</div>
        <div class="kv mono">期望 ${escapeHtml(r.expected_sha256 || "-")}<br>实际 ${escapeHtml(r.actual_sha256 || "-")}</div>
        <div class="lbl" style="margin-top:8px">时间范围</div>
        <div class="kv mono">期望 ${escapeHtml(r.expected_time_start || "-")} ~ ${escapeHtml(r.expected_time_end || "-")}
          <br>实际 ${escapeHtml(r.actual_time_start || "-")} ~ ${escapeHtml(r.actual_time_end || "-")}</div>
        ${manifest ? '<div class="lbl" style="margin-top:8px">清单原文</div><pre>' + escapeHtml(manifest) + "</pre>" : ""}
      </div>
    </div>`;
  }).join("");
}

function renderEvents(events) {
  if (!events.length) {
    $("tab-events").innerHTML = '<p class="kv">暂无事件</p>';
    return;
  }
  $("tab-events").innerHTML = events.slice().reverse().map((e) => {
    const detail = e.detail ? "<pre>" + escapeHtml(prettyBody(e.detail)) + "</pre>" : "";
    return `<div class="event-item">
      <div class="event-head">
        <span class="event-type">${escapeHtml(e.event_type)}</span>
        <span class="event-time">${fmtTime(e.created_at)}</span>
      </div>
      ${detail ? `<div class="req-body">${detail}</div>` : ""}
    </div>`;
  }).join("");
}

// ---------------- 入库查询 ----------------
async function runQuery(bizDate) {
  const url = "/api/ingested/data?limit=200" + (bizDate ? `&biz_date=${encodeURIComponent(bizDate)}` : "");
  const res = await api("GET", url);
  $("queryMsg").textContent = `共 ${res.total} 行（失败/取消任务的数据不会出现）`;
  const body = $("dataBody");
  if (!res.rows.length) {
    body.innerHTML = '<tr><td colspan="8" class="empty">无数据</td></tr>';
    return;
  }
  body.innerHTML = res.rows.map((r) => `<tr>
    <td>${r.id}</td><td>${r.task_pk}</td><td><b>${escapeHtml(r.biz_date)}</b></td>
    <td>${r.row_no}</td><td class="mono">${escapeHtml(r.biz_time)}</td>
    <td>${escapeHtml(r.metric)}</td><td>${escapeHtml(r.value)}</td>
    <td class="mono">${fmtTime(r.ingested_at)}</td>
  </tr>`).join("");
}

$("btnQuery").addEventListener("click", () => runQuery($("queryDate").value).catch((e) =>
  setMsg($("queryMsg"), "查询失败：" + e.message, "err")));
$("btnQueryAll").addEventListener("click", () => {
  $("queryDate").value = "";
  runQuery(null).catch((e) => setMsg($("queryMsg"), "查询失败：" + e.message, "err"));
});

// ---------------- 轮询刷新 ----------------
$("bizDate").value = new Date().toISOString().slice(0, 10);
refreshTasks();
setInterval(refreshTasks, 1500);
