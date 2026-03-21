const aliases = [
  "server_stats",
  "sessions",
  "tokens",
  "workers",
  "consul_cluster_summary",
];

const POLL_INTERVAL_MS = 15000;
let timerId = null;

const globalStatus = document.getElementById("global-status");
const lastRefresh = document.getElementById("last-refresh");
const refreshBtn = document.getElementById("refresh-btn");

function panelFor(alias) {
  return document.querySelector(`.panel[data-alias="${alias}"]`);
}

function setStatus(el, label, cls) {
  el.textContent = label;
  el.className = `status-pill ${cls}`;
}

function setPanel(alias, statusLabel, statusClass, content) {
  const panel = panelFor(alias);
  if (!panel) {
    return;
  }
  const statusEl = panel.querySelector("[data-status]");
  const contentEl = panel.querySelector("[data-content]");
  setStatus(statusEl, statusLabel, statusClass);
  contentEl.textContent = content;
}

async function fetchAlias(alias) {
  const response = await fetch(`/dashboard/key?name=${encodeURIComponent(alias)}`, {
    method: "GET",
    cache: "no-store",
  });

  if (response.status === 404) {
    setPanel(alias, "Empty", "status-empty", "No key value in Redis yet.");
    return true;
  }

  if (response.status === 503) {
    setPanel(alias, "Degraded", "status-degraded", "Backend cannot reach Redis.");
    return false;
  }

  if (!response.ok) {
    const text = await response.text();
    setPanel(alias, "Error", "status-error", text || "Unexpected backend error.");
    return false;
  }

  try {
    const json = await response.json();
    const pretty = JSON.stringify(json, null, 2);
    setPanel(alias, "OK", "status-ok", pretty);
    return true;
  } catch (error) {
    setPanel(alias, "Error", "status-error", `Invalid JSON payload: ${error}`);
    return false;
  }
}

async function refreshDashboard() {
  setStatus(globalStatus, "Loading", "status-loading");
  const results = await Promise.all(aliases.map((alias) => fetchAlias(alias)));
  const allOk = results.every((result) => result);
  setStatus(globalStatus, allOk ? "Healthy" : "Partial", allOk ? "status-ok" : "status-degraded");
  lastRefresh.textContent = `Last refresh: ${new Date().toLocaleTimeString()}`;
}

function startPolling() {
  if (timerId) {
    window.clearInterval(timerId);
  }
  timerId = window.setInterval(refreshDashboard, POLL_INTERVAL_MS);
}

refreshBtn.addEventListener("click", () => {
  refreshDashboard();
});

refreshDashboard();
startPolling();
