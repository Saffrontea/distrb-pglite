/**
 * 分散デモ UI
 *
 * ダッシュボードサーバー (port 3000) を起動し、
 * ブラウザからノードの状態・イベント伝播・スナップショットを可視化する。
 */

import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { WebSocketHub } from "@distrb-pglite/node";
import { EdgeNodeServer } from "./server.js";

// ─── types ────────────────────────────────────────────────────────────────────

type NodeStatus = {
  nodeId: string;
  url: string;
  online: boolean;
  eventCount: number;
  stats: { sensor_id: string; count: string; avg_reading: string }[];
  error?: string;
};

type LogEntry = { ts: string; msg: string; kind: string };
type SqlResponse = {
  nodeId: string;
  mode: "query" | "exec";
  rows: Record<string, unknown>[];
  columns: string[];
  rowCount: number;
  durationMs: number;
};

// ─── dashboard state ──────────────────────────────────────────────────────────

const nodes = new Map<string, EdgeNodeServer>();
const statuses = new Map<string, NodeStatus>();
const log: LogEntry[] = [];
let baseDir = "";
let casDir = "";
let hubUrl = "";
let lateCounter = 0;

function addLog(msg: string, kind = "info"): void {
  const ts = new Date().toLocaleTimeString("ja-JP", { hour12: false });
  log.unshift({ ts, msg, kind });
  if (log.length > 80) log.pop();
}

// ─── polling ──────────────────────────────────────────────────────────────────

async function pollNode(nodeId: string, url: string): Promise<void> {
  try {
    const [statusRes, statsRes] = await Promise.all([
      fetch(`${url}/status`),
      fetch(`${url}/stats`),
    ]);
    const status = (await statusRes.json()) as { online: boolean; eventCount: number };
    const statsData = (await statsRes.json()) as { stats: NodeStatus["stats"] };

    const prev = statuses.get(nodeId);
    const next: NodeStatus = {
      nodeId,
      url,
      online: status.online,
      eventCount: status.eventCount,
      stats: statsData.stats,
    };

    if (prev) {
      if (prev.online !== next.online) {
        addLog(`${nodeId} → ${next.online ? "● オンライン" : "○ オフライン"}`, "status");
      }
      if (next.eventCount > prev.eventCount) {
        const delta = next.eventCount - prev.eventCount;
        addLog(`${nodeId} +${delta} イベント (計 ${next.eventCount} 件)`, "sync");
      }
    }
    statuses.set(nodeId, next);
  } catch {
    const prev = statuses.get(nodeId);
    statuses.set(nodeId, {
      nodeId,
      url,
      online: false,
      eventCount: prev?.eventCount ?? 0,
      stats: prev?.stats ?? [],
      error: "unreachable",
    });
  }
}

async function pollAll(): Promise<void> {
  await Promise.all([...nodes.entries()].map(([id, srv]) => pollNode(id, srv.baseUrl)));
}

// ─── action proxy ─────────────────────────────────────────────────────────────

async function proxyPost(targetUrl: string, body?: unknown): Promise<unknown> {
  const res = await fetch(targetUrl, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

async function handleAction(nodeId: string, action: string, body: unknown): Promise<unknown> {
  const srv = nodes.get(nodeId);
  if (!srv) throw new Error(`node not found: ${nodeId}`);

  switch (action) {
    case "write": {
      const sensorId = `sensor-${nodeId}`;
      const reading = Math.round(Math.random() * 100);
      await proxyPost(`${srv.baseUrl}/readings`, { sensorId, reading });
      addLog(`${nodeId} → 書き込み ${sensorId}=${reading}`, "write");
      return { ok: true };
    }
    case "write-burst": {
      const { count = 5 } = body as { count?: number };
      for (let i = 0; i < count; i++) {
        const sensorId = `sensor-${nodeId}-${i % 3}`;
        const reading = Math.round(20 + Math.random() * 80);
        await proxyPost(`${srv.baseUrl}/readings`, { sensorId, reading });
      }
      addLog(`${nodeId} → burst write ${count} 件`, "write");
      return { ok: true, count };
    }
    case "disconnect":
      await proxyPost(`${srv.baseUrl}/disconnect`);
      addLog(`${nodeId} 切断`, "status");
      return { ok: true };
    case "connect": {
      const result = (await proxyPost(`${srv.baseUrl}/connect`)) as {
        applied?: number;
        catchup?: Array<{ peerNodeId: string; applied: number }>;
      };
      addLog(`${nodeId} 再接続`, "status");
      const applied = result.applied ?? 0;
      if (result.catchup && result.catchup.length > 0) {
        const peers = result.catchup.map((item) => `${item.peerNodeId}:${item.applied}`).join(", ");
        addLog(`${nodeId} auto catchup +${applied} 件 (${peers})`, "catchup");
      }
      return { ok: true, applied };
    }
    case "catchup": {
      const { peerNodeId } = body as { peerNodeId: string };
      const result = (await proxyPost(`${srv.baseUrl}/catchup`, { peerNodeId })) as {
        applied: number;
      };
      addLog(`${nodeId} ← catchup(${peerNodeId}) +${result.applied} 件`, "catchup");
      return result;
    }
    case "snapshot": {
      const snap = (await proxyPost(`${srv.baseUrl}/create-snapshot`)) as {
        snapshotId: string;
        blocks: number;
      };
      addLog(`${nodeId} スナップショット作成 (${snap.blocks} blk)`, "snapshot");
      return snap;
    }
    case "pull-snapshot": {
      const { sourceNodeId } = body as {
        sourceNodeId: string;
      };
      if (sourceNodeId === nodeId) {
        throw new Error("source and target must be different");
      }
      const source = nodes.get(sourceNodeId);
      if (!source) {
        throw new Error(`source node not found: ${sourceNodeId}`);
      }

      throw new Error(
        `existing node snapshot pull is not a bootstrap target: add a late joiner via snapshot instead (${sourceNodeId} -> new replica)`,
      );
    }
    case "add-late-joiner": {
      const { via, peerNodeId } = body as { via: "catchup" | "snapshot"; peerNodeId: string };
      lateCounter++;
      const lateId = `late-${lateCounter}`;
      const latePort = 4100 + lateCounter;
      const lateSrv = await EdgeNodeServer.create({
        nodeId: lateId,
        hubUrl,
        dataDir: join(baseDir, lateId),
        casDir,
        port: latePort,
        partitionId: "tenant:jp",
        peerNodeIds: [...nodes.keys()],
      });
      await lateSrv.listen();
      nodes.set(lateId, lateSrv);
      addLog(`${lateId} 参加 (via ${via})`, "status");

      const peer = nodes.get(peerNodeId) ?? nodes.get("node-0")!;

      if (via === "snapshot") {
        // スナップショット bootstrap
        await proxyPost(`${peer.baseUrl}/create-snapshot`);
        const catalogManifest = await (await fetch(`${peer.baseUrl}/catalog`)).json();
        const { entries } = (await (await fetch(`${peer.baseUrl}/snapshot-entries`)).json()) as {
          entries: unknown[];
        };
        const pull = (await proxyPost(`${lateSrv.baseUrl}/pull-snapshot`, {
          catalogManifest,
          snapshotEntries: entries,
          sourceCasDir: casDir,
          requirement: "bootstrap",
        })) as { transferredBlocks: number; snapshotId: string };
        addLog(
          `${lateId} pull snapshot ${pull.transferredBlocks} blk (${pull.snapshotId})`,
          "snapshot",
        );
      }

      // delta catchup
      const cu = (await proxyPost(`${lateSrv.baseUrl}/catchup`, { peerNodeId: peer.nodeId })) as {
        applied: number;
      };
      addLog(`${lateId} catchup +${cu.applied} 件`, "catchup");
      return { ok: true, nodeId: lateId, url: lateSrv.baseUrl };
    }
    default:
      throw new Error(`unknown action: ${action}`);
  }
}

async function handleSql(body: unknown): Promise<SqlResponse> {
  const { nodeId, sql } = body as { nodeId?: string; sql?: string };
  if (!nodeId || !sql) {
    throw new Error("nodeId and sql are required");
  }
  const srv = nodes.get(nodeId);
  if (!srv) {
    throw new Error(`node not found: ${nodeId}`);
  }
  const result = (await proxyPost(`${srv.baseUrl}/sql`, { sql })) as Omit<SqlResponse, "nodeId">;
  addLog(`${nodeId} SQL ${result.mode} (${result.rowCount} rows, ${result.durationMs}ms)`, "sql");
  return { nodeId, ...result };
}

// ─── HTTP dispatch ─────────────────────────────────────────────────────────────

function cors(res: ServerResponse): void {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

async function dispatch(req: IncomingMessage, res: ServerResponse): Promise<void> {
  cors(res);
  if (req.method === "OPTIONS") {
    res.writeHead(204);
    res.end();
    return;
  }

  const url = new URL(req.url ?? "/", "http://localhost");
  const path = url.pathname;

  // serve dashboard
  if (path === "/" || path === "/index.html") {
    res.writeHead(200, { "Content-Type": "text/html; charset=utf-8" });
    res.end(HTML);
    return;
  }

  // status API (poll)
  if (path === "/api/status" && req.method === "GET") {
    await pollAll();
    res.writeHead(200, { "Content-Type": "application/json" });
    res.end(
      JSON.stringify({
        nodes: [...statuses.values()],
        log: log.slice(0, 50),
      }),
    );
    return;
  }

  if (path === "/api/sql" && req.method === "POST") {
    try {
      const body = await readJson(req);
      const result = await handleSql(body);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
    return;
  }

  // node action: POST /api/nodes/:nodeId/:action
  const m = path.match(/^\/api\/nodes\/([^/]+)\/([^/]+)$/);
  if (m && req.method === "POST") {
    const [, nodeId, action] = m;
    let body: unknown = {};
    try {
      body = await readJson(req);
    } catch {
      /* ignore */
    }

    try {
      const result = await handleAction(nodeId, action, body);
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
    return;
  }

  res.writeHead(404);
  res.end();
}

async function readJson(req: IncomingMessage): Promise<unknown> {
  return new Promise<unknown>((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => {
      try {
        resolve(data ? JSON.parse(data) : {});
      } catch (e) {
        reject(e);
      }
    });
    req.on("error", reject);
  });
}

// ─── HTML ─────────────────────────────────────────────────────────────────────

const HTML = /* html */ `<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<title>distrb-pglite playground</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:Inter,ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;background:#f4f6f8;color:#18212f;min-height:100vh;display:flex;flex-direction:column}
header{height:54px;padding:0 18px;border-bottom:1px solid #cfd7e3;display:flex;align-items:center;gap:16px;background:#ffffff}
header h1{font-size:15px;color:#12355b;font-weight:700;letter-spacing:0}
header .hub{font-size:12px;color:#66768a}
.toolbar{padding:10px 18px;border-bottom:1px solid #d8e0ea;display:flex;gap:12px;background:#eef3f8;align-items:center;flex-wrap:wrap}
.flow-controls{display:flex;align-items:center;gap:8px;flex-wrap:wrap}
.control-group{display:flex;align-items:center;gap:6px;font-size:11px;color:#66768a}
.arrow{color:#66768a;font-size:13px}
button{font-family:inherit;font-size:12px;padding:6px 10px;border-radius:6px;border:1px solid #b8c4d2;cursor:pointer;background:#ffffff;color:#263548;transition:background .15s,border-color .15s}
button:hover{background:#eef3f8;border-color:#8798ac}
button.primary{background:#1464a5;border-color:#1464a5;color:#fff}
button.primary:hover{background:#0f548c}
button.danger{background:#b42318;border-color:#b42318;color:#fff}
button.danger:hover{background:#941b12}
button.success{background:#18794e;border-color:#18794e;color:#fff}
button.success:hover{background:#12643f}
.main{display:grid;grid-template-columns:minmax(380px,1fr) minmax(420px,1.1fr) 320px;flex:1;min-height:0}
.nodes-panel{overflow-y:auto;padding:14px;display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:10px;align-content:start}
.sql-panel{border-left:1px solid #d8e0ea;background:#ffffff;display:flex;flex-direction:column;min-width:0}
.sql-head{padding:12px 14px;border-bottom:1px solid #d8e0ea;display:flex;align-items:center;gap:8px}
.sql-head h2{font-size:13px;color:#344054;font-weight:700;margin-right:auto}
select{font-family:inherit;font-size:12px;border:1px solid #b8c4d2;border-radius:6px;background:#fff;padding:5px 8px;color:#263548}
.sql-samples{padding:8px 14px;border-bottom:1px solid #e4e9f0;display:flex;gap:6px;flex-wrap:wrap}
.sql-samples button{font-size:11px;padding:4px 8px}
textarea{font-family:"SFMono-Regular",Consolas,"Liberation Mono",monospace;font-size:12px;line-height:1.5;border:0;border-bottom:1px solid #d8e0ea;resize:vertical;min-height:136px;padding:12px 14px;color:#18212f;background:#fbfcfe;outline:none}
.sql-result-meta{padding:8px 14px;font-size:12px;color:#66768a;border-bottom:1px solid #e4e9f0}
.sql-result{flex:1;overflow:auto;padding:12px 14px}
.sql-result table{width:max-content;min-width:100%;border-collapse:collapse;font-size:12px}
.sql-result th{position:sticky;top:0;background:#eef3f8;color:#344054;text-align:left;border:1px solid #d8e0ea;padding:6px 8px;font-weight:600}
.sql-result td{border:1px solid #e4e9f0;padding:5px 8px;vertical-align:top;font-family:"SFMono-Regular",Consolas,"Liberation Mono",monospace}
.sql-empty{font-size:12px;color:#66768a}
.sql-error{white-space:pre-wrap;color:#b42318;font-family:"SFMono-Regular",Consolas,"Liberation Mono",monospace;font-size:12px}
.log-panel{border-left:1px solid #d8e0ea;display:flex;flex-direction:column;background:#ffffff}
.log-panel h2{padding:10px 14px;font-size:11px;color:#66768a;border-bottom:1px solid #d8e0ea;text-transform:uppercase;letter-spacing:.08em}
.log-list{flex:1;overflow-y:auto;padding:8px 0;font-size:11px}
.log-entry{padding:5px 14px;line-height:1.45;border-bottom:1px solid #eef1f5;display:flex;gap:8px}
.log-entry .ts{color:#98a6b8;flex-shrink:0;font-size:10px;padding-top:1px}
.log-entry .msg{color:#344054}
.log-entry.write .msg{color:#18794e}
.log-entry.sync .msg{color:#1464a5}
.log-entry.snapshot .msg{color:#6f42c1}
.log-entry.catchup .msg{color:#ad5700}
.log-entry.status .msg{color:#66768a}
.log-entry.sql .msg{color:#0f766e}
.node-card{background:#ffffff;border:1px solid #d8e0ea;border-radius:8px;padding:14px;transition:border-color .2s;min-width:0}
.node-card.offline{border-color:#d92d2033;opacity:.84}
.node-header{display:flex;align-items:center;gap:8px;margin-bottom:10px}
.node-id{font-size:13px;font-weight:700;color:#18212f}
.badge{font-size:10px;padding:1px 6px;border-radius:10px;margin-left:auto}
.badge.online{background:#e9f7ef;color:#18794e;border:1px solid #a8dfc3}
.badge.offline{background:#fff0ee;color:#b42318;border:1px solid #f4b4ad}
.event-count{font-size:34px;font-weight:750;color:#1464a5;line-height:1;margin-bottom:4px;transition:color .3s}
.event-count.flash{color:#18794e}
.event-label{font-size:10px;color:#66768a;margin-bottom:10px}
.stats-table{width:100%;border-collapse:collapse;font-size:10px;margin-bottom:10px}
.stats-table th{color:#66768a;text-align:left;padding:2px 4px;border-bottom:1px solid #e4e9f0}
.stats-table td{padding:2px 4px;color:#344054}
.stats-table tr:first-child td{padding-top:4px}
.actions{display:flex;flex-wrap:wrap;gap:4px;margin-top:8px}
.actions button{font-size:10px;padding:3px 7px}
@media (max-width:1100px){.main{grid-template-columns:1fr}.log-panel{min-height:220px;border-left:0;border-top:1px solid #d8e0ea}.sql-panel{border-left:0;border-top:1px solid #d8e0ea}.nodes-panel{max-height:none}}
</style>
</head>
<body>
<header>
  <h1>distrb-pglite playground</h1>
  <span class="hub" id="hub-info">読み込み中...</span>
</header>
<div class="toolbar">
  <div class="flow-controls">
    <span class="control-group">source <select id="flow-source"></select></span>
    <span class="arrow">→</span>
    <span class="control-group">target <select id="flow-target"></select></span>
    <button onclick="writeBurst()">Burst write</button>
    <button onclick="createSourceSnapshot()">Snapshot source</button>
    <button onclick="catchupTarget()">Catchup → target</button>
    <button class="primary" onclick="pullSnapshot()">Pull snapshot → new replica</button>
  </div>
  <button onclick="addLateJoiner('catchup')">+ late joiner catchup</button>
  <button onclick="addLateJoiner('snapshot')">+ late joiner snapshot</button>
  <span style="margin-left:auto;font-size:11px;color:#484f58" id="conv-status"></span>
</div>
<div class="main">
  <div class="nodes-panel" id="nodes"></div>
  <div class="sql-panel">
    <div class="sql-head">
      <h2>SQL</h2>
      <select id="sql-node"></select>
      <button class="primary" onclick="runSql()">Run</button>
    </div>
    <div class="sql-samples">
      <button onclick="sampleSql('readings')">readings</button>
      <button onclick="sampleSql('stats')">stats</button>
      <button onclick="sampleSql('events')">events</button>
      <button onclick="sampleSql('partitions')">partitions</button>
      <button onclick="sampleSql('insert')">insert</button>
    </div>
    <textarea id="sql-input" spellcheck="false">SELECT sensor_id, reading, unit, source_node, recorded_at
FROM sensor_readings
ORDER BY recorded_at DESC
LIMIT 20;</textarea>
    <div class="sql-result-meta" id="sql-meta">ready</div>
    <div class="sql-result" id="sql-result"><div class="sql-empty">No result</div></div>
  </div>
  <div class="log-panel">
    <h2>イベントログ</h2>
    <div class="log-list" id="log"></div>
  </div>
</div>

<script>
const BASE = '';
let prevCounts = {};

async function api(method, path, body) {
  const res = await fetch(BASE + path, {
    method,
    headers: body ? {'Content-Type':'application/json'} : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  if (!res.ok) {
    const err = await res.json().catch(() => ({}));
    throw new Error(err.error || res.statusText);
  }
  return res.json();
}

function renderNode(s) {
  const flashing = (prevCounts[s.nodeId] ?? s.eventCount) < s.eventCount;
  const countClass = flashing ? 'event-count flash' : 'event-count';
  const statsRows = s.stats.slice(0, 5).map(r =>
    \`<tr><td>\${r.sensor_id}</td><td>\${r.count}</td><td>\${Number(r.avg_reading).toFixed(1)}</td></tr>\`
  ).join('');

  const toggleBtn = s.online
    ? \`<button class="danger" onclick="action('\${s.nodeId}','disconnect')">切断</button>\`
    : \`<button class="success" onclick="action('\${s.nodeId}','connect')">接続</button>\`;

  return \`
    <div class="node-card \${s.online ? '' : 'offline'}" id="card-\${s.nodeId}">
      <div class="node-header">
        <span class="node-id">\${s.nodeId}</span>
        <span class="badge \${s.online ? 'online' : 'offline'}">\${s.online ? '● online' : '○ offline'}</span>
      </div>
      <div class="\${countClass}" id="count-\${s.nodeId}">\${s.eventCount}</div>
      <div class="event-label">イベント数</div>
      \${statsRows ? \`<table class="stats-table">
        <tr><th>sensor</th><th>件数</th><th>平均</th></tr>
        \${statsRows}
      </table>\` : '<div style="font-size:10px;color:#484f58;margin-bottom:10px">データなし</div>'}
      <div class="actions">
        <button class="primary" onclick="action('\${s.nodeId}','write')">書込</button>
        \${toggleBtn}
        <button onclick="catchup('\${s.nodeId}')">CU</button>
        <button onclick="action('\${s.nodeId}','snapshot')">Snap</button>
        <button onclick="setFlowTarget('\${s.nodeId}')">Target</button>
        <button onclick="setSqlNode('\${s.nodeId}')">SQL</button>
      </div>
    </div>
  \`;
}

function updateLog(entries) {
  const el = document.getElementById('log');
  el.innerHTML = entries.map(e =>
    \`<div class="log-entry \${e.kind}"><span class="ts">\${e.ts}</span><span class="msg">\${e.msg}</span></div>\`
  ).join('');
}

function updateConvergence(nodes) {
  const counts = nodes.map(n => n.eventCount);
  const max = Math.max(...counts);
  const all = counts.every(c => c === max);
  const el = document.getElementById('conv-status');
  if (max === 0) { el.textContent = ''; return; }
  el.textContent = all
    ? \`✓ 収束済み (\${max} 件)\`
    : \`⏳ 収束中… [\${counts.join('/')}]\`;
  el.style.color = all ? '#3fb950' : '#ffa657';
}

function updateNodeSelect(nodes) {
  updateSelect('sql-node', nodes, 0);
  updateSelect('flow-source', nodes, 0);
  updateSelect('flow-target', nodes, Math.min(1, nodes.length - 1));
}

function updateSelect(id, nodes, fallbackIndex) {
  const select = document.getElementById(id);
  const current = select.value;
  select.innerHTML = nodes.map(n => \`<option value="\${n.nodeId}">\${n.nodeId}</option>\`).join('');
  if (nodes.some(n => n.nodeId === current)) select.value = current;
  else if (nodes[fallbackIndex]) select.value = nodes[fallbackIndex].nodeId;
}

async function poll() {
  try {
    const { nodes, log } = await api('GET', '/api/status');
    document.getElementById('hub-info').textContent = \`\${nodes.length} ノード稼働中\`;

    const container = document.getElementById('nodes');
    const html = nodes.map(renderNode).join('');
    container.innerHTML = html;

    // flash animation
    nodes.forEach(s => {
      if ((prevCounts[s.nodeId] ?? s.eventCount) < s.eventCount) {
        const el = document.getElementById(\`count-\${s.nodeId}\`);
        if (el) {
          el.classList.add('flash');
          setTimeout(() => el.classList.remove('flash'), 600);
        }
      }
      prevCounts[s.nodeId] = s.eventCount;
    });

    updateLog(log);
    updateConvergence(nodes);
    updateNodeSelect(nodes);
  } catch(e) {
    console.error(e);
  }
}

function sampleSql(name) {
  const samples = {
    readings: \`SELECT sensor_id, reading, unit, source_node, recorded_at
FROM sensor_readings
ORDER BY recorded_at DESC
LIMIT 20;\`,
    stats: \`SELECT sensor_id,
       round(avg(reading)::numeric, 2) AS avg_reading,
       min(reading) AS min_reading,
       max(reading) AS max_reading,
       count(*)::int AS count
FROM sensor_readings
GROUP BY sensor_id
ORDER BY sensor_id;\`,
    events: \`SELECT source_node_id, event_id::text, partition_id, payload
FROM distrb_events
ORDER BY event_id DESC
LIMIT 20;\`,
    partitions: \`SELECT * FROM distrb_partitions ORDER BY partition_id;\`,
    insert: \`INSERT INTO sensor_readings (event_id, sensor_id, reading, unit, source_node)
VALUES ('manual-' || floor(random() * 1000000000)::text, 'manual-sql', 72.5, 'C', 'sql');\`,
  };
  document.getElementById('sql-input').value = samples[name];
}

async function runSql() {
  const nodeId = document.getElementById('sql-node').value;
  const sql = document.getElementById('sql-input').value;
  const meta = document.getElementById('sql-meta');
  const resultEl = document.getElementById('sql-result');
  meta.textContent = 'running...';
  resultEl.innerHTML = '';
  try {
    const result = await api('POST', '/api/sql', { nodeId, sql });
    meta.textContent = \`\${result.nodeId} · \${result.mode} · \${result.rowCount} rows · \${result.durationMs}ms\`;
    renderSqlResult(result);
  } catch(e) {
    meta.textContent = 'error';
    resultEl.innerHTML = \`<div class="sql-error">\${escapeHtml(e.message)}</div>\`;
  }
}

function renderSqlResult(result) {
  const el = document.getElementById('sql-result');
  if (!result.rows.length) {
    el.innerHTML = '<div class="sql-empty">OK</div>';
    return;
  }
  const columns = result.columns.length ? result.columns : Object.keys(result.rows[0] || {});
  el.innerHTML = \`<table><thead><tr>\${columns.map(c => \`<th>\${escapeHtml(c)}</th>\`).join('')}</tr></thead><tbody>\${
    result.rows.map(row => \`<tr>\${columns.map(c => \`<td>\${escapeHtml(formatCell(row[c]))}</td>\`).join('')}</tr>\`).join('')
  }</tbody></table>\`;
}

function formatCell(value) {
  if (value === null || value === undefined) return '';
  if (typeof value === 'object') return JSON.stringify(value);
  return String(value);
}

function escapeHtml(value) {
  return String(value)
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

async function action(nodeId, act, body) {
  try {
    return await api('POST', \`/api/nodes/\${nodeId}/\${act}\`, body);
  } catch(e) {
    alert(\`エラー: \${e.message}\`);
    throw e;
  }
}

async function catchup(nodeId) {
  const peer = document.getElementById('flow-source').value || firstOtherNode(nodeId);
  await action(nodeId, 'catchup', { peerNodeId: peer });
}

async function writeBurst() {
  const source = document.getElementById('flow-source').value;
  await action(source, 'write-burst', { count: 8 });
}

async function createSourceSnapshot() {
  const source = document.getElementById('flow-source').value;
  await action(source, 'snapshot');
}

async function catchupTarget() {
  const source = document.getElementById('flow-source').value;
  const target = document.getElementById('flow-target').value;
  if (!source || !target || source === target) {
    alert('source と target には別ノードを選んでください');
    return;
  }
  await action(target, 'catchup', { peerNodeId: source });
}

async function pullSnapshot() {
  const source = document.getElementById('flow-source').value;
  if (!source) {
    alert('source を選んでください');
    return;
  }
  await api('POST', \`/api/nodes/\${source}/add-late-joiner\`, { via: 'snapshot', peerNodeId: source });
  prevCounts = {};
  await poll();
}

function setFlowTarget(nodeId) {
  const target = document.getElementById('flow-target');
  const source = document.getElementById('flow-source');
  target.value = nodeId;
  if (source.value === nodeId) {
    source.value = firstOtherNode(nodeId);
  }
}

function setSqlNode(nodeId) {
  document.getElementById('sql-node').value = nodeId;
}

function firstOtherNode(nodeId) {
  const allNodes = [...document.querySelectorAll('.node-card')].map(el => el.id.replace('card-',''));
  return allNodes.find(id => id !== nodeId) ?? allNodes[0] ?? 'node-0';
}

async function addLateJoiner(via) {
  const peer = document.getElementById('flow-source').value || 'node-0';
  try {
    await api('POST', \`/api/nodes/\${peer}/add-late-joiner\`, { via, peerNodeId: peer });
    prevCounts = {};
  } catch(e) {
    alert(\`エラー: \${e.message}\`);
  }
}

poll();
setInterval(poll, 600);
</script>
</body>
</html>`;

// ─── main ─────────────────────────────────────────────────────────────────────

async function run(): Promise<void> {
  baseDir = await mkdtemp(join(tmpdir(), "distrb-ui-"));
  casDir = join(baseDir, "cas");

  const hubPort = 4000;
  hubUrl = `ws://127.0.0.1:${hubPort}`;
  const hub = new WebSocketHub({ port: hubPort });

  // 3 initial nodes
  const initialNodeIds = ["node-0", "node-1", "node-2"];
  for (let i = 0; i < 3; i++) {
    const nodeId = initialNodeIds[i];
    const srv = await EdgeNodeServer.create({
      nodeId,
      hubUrl,
      dataDir: join(baseDir, nodeId),
      casDir,
      port: 4001 + i,
      partitionId: "tenant:jp",
      peerNodeIds: initialNodeIds.filter((peer) => peer !== nodeId),
    });
    await srv.listen();
    nodes.set(nodeId, srv);
  }

  addLog("3 ノードで起動", "status");

  const dashboard = createServer((req, res) => {
    dispatch(req, res).catch((err) => {
      res.writeHead(500);
      res.end(String(err));
    });
  });

  dashboard.listen(3000, "0.0.0.0", () => {
    console.log(`\n${"═".repeat(50)}`);
    console.log("  ダッシュボード: http://0.0.0.0:3000");
    console.log(`  ノード:  node-0 :4001  node-1 :4002  node-2 :4003`);
    console.log(`  Ctrl+C で終了`);
    console.log(`${"═".repeat(50)}\n`);
  });

  process.on("SIGINT", async () => {
    console.log("\n終了中...");
    dashboard.close();
    await Promise.all([...nodes.values()].map((n) => n.close()));
    await hub.close();
    await rm(baseDir, { recursive: true, force: true });
    process.exit(0);
  });
}

run().catch((err) => {
  console.error(err);
  process.exit(1);
});
