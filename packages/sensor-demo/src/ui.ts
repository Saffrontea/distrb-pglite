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

// ─── dashboard state ──────────────────────────────────────────────────────────

const nodes = new Map<string, EdgeNodeServer>();
const statuses = new Map<string, NodeStatus>();
const log: LogEntry[] = [];
let baseDir = "";
let casDir  = "";
let hubUrl  = "";
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
    const status = await statusRes.json() as { online: boolean; eventCount: number };
    const statsData = await statsRes.json() as { stats: NodeStatus["stats"] };

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
      nodeId, url,
      online: false,
      eventCount: prev?.eventCount ?? 0,
      stats: prev?.stats ?? [],
      error: "unreachable",
    });
  }
}

async function pollAll(): Promise<void> {
  await Promise.all(
    [...nodes.entries()].map(([id, srv]) => pollNode(id, srv.baseUrl))
  );
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

async function handleAction(
  nodeId: string,
  action: string,
  body: unknown,
): Promise<unknown> {
  const srv = nodes.get(nodeId);
  if (!srv) throw new Error(`node not found: ${nodeId}`);

  switch (action) {
    case "write": {
      const sensorId = `sensor-${nodeId}`;
      const reading  = Math.round(Math.random() * 100);
      await proxyPost(`${srv.baseUrl}/readings`, { sensorId, reading });
      addLog(`${nodeId} → 書き込み ${sensorId}=${reading}`, "write");
      return { ok: true };
    }
    case "disconnect":
      await proxyPost(`${srv.baseUrl}/disconnect`);
      addLog(`${nodeId} 切断`, "status");
      return { ok: true };
    case "connect":
    {
      const result = await proxyPost(`${srv.baseUrl}/connect`) as {
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
      const result = await proxyPost(`${srv.baseUrl}/catchup`, { peerNodeId }) as { applied: number };
      addLog(`${nodeId} ← catchup(${peerNodeId}) +${result.applied} 件`, "catchup");
      return result;
    }
    case "snapshot": {
      const snap = await proxyPost(`${srv.baseUrl}/create-snapshot`) as { snapshotId: string; blocks: number };
      addLog(`${nodeId} スナップショット作成 (${snap.blocks} blk)`, "snapshot");
      return snap;
    }
    case "add-late-joiner": {
      const { via, peerNodeId } = body as { via: "catchup" | "snapshot"; peerNodeId: string };
      lateCounter++;
      const lateId  = `late-${lateCounter}`;
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
        const { entries } = await (await fetch(`${peer.baseUrl}/snapshot-entries`)).json() as { entries: unknown[] };
        await proxyPost(`${lateSrv.baseUrl}/pull-snapshot`, {
          catalogManifest,
          snapshotEntries: entries,
          sourceCasDir: casDir,
          requirement: "bootstrap",
        });
        addLog(`${lateId} スナップショット bootstrap 完了`, "snapshot");
      }

      // delta catchup
      const cu = await proxyPost(`${lateSrv.baseUrl}/catchup`, { peerNodeId: peer.nodeId }) as { applied: number };
      addLog(`${lateId} catchup +${cu.applied} 件`, "catchup");
      return { ok: true, nodeId: lateId, url: lateSrv.baseUrl };
    }
    default:
      throw new Error(`unknown action: ${action}`);
  }
}

// ─── HTTP dispatch ─────────────────────────────────────────────────────────────

function cors(res: ServerResponse): void {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

async function dispatch(req: IncomingMessage, res: ServerResponse): Promise<void> {
  cors(res);
  if (req.method === "OPTIONS") { res.writeHead(204); res.end(); return; }

  const url  = new URL(req.url ?? "/", "http://localhost");
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
    res.end(JSON.stringify({
      nodes: [...statuses.values()],
      log: log.slice(0, 50),
    }));
    return;
  }

  // node action: POST /api/nodes/:nodeId/:action
  const m = path.match(/^\/api\/nodes\/([^/]+)\/([^/]+)$/);
  if (m && req.method === "POST") {
    const [, nodeId, action] = m;
    let body: unknown = {};
    try {
      body = await new Promise<unknown>((resolve, reject) => {
        let data = "";
        req.on("data", (c) => (data += c));
        req.on("end", () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
        req.on("error", reject);
      });
    } catch { /* ignore */ }

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

// ─── HTML ─────────────────────────────────────────────────────────────────────

const HTML = /* html */`<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<title>distrb-pglite ダッシュボード</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:ui-monospace,monospace;background:#0d1117;color:#e6edf3;min-height:100vh;display:flex;flex-direction:column}
header{padding:12px 20px;border-bottom:1px solid #21262d;display:flex;align-items:center;gap:16px;background:#161b22}
header h1{font-size:14px;color:#58a6ff;letter-spacing:.05em}
header .hub{font-size:11px;color:#8b949e}
.toolbar{padding:8px 20px;border-bottom:1px solid #21262d;display:flex;gap:8px;background:#0d1117}
button{font-family:inherit;font-size:11px;padding:4px 10px;border-radius:4px;border:1px solid #30363d;cursor:pointer;background:#21262d;color:#c9d1d9;transition:background .15s}
button:hover{background:#30363d}
button.primary{background:#1f6feb;border-color:#388bfd;color:#fff}
button.primary:hover{background:#388bfd}
button.danger{background:#da3633;border-color:#f85149;color:#fff}
button.danger:hover{background:#f85149}
button.success{background:#238636;border-color:#2ea043;color:#fff}
button.success:hover{background:#2ea043}
.main{display:flex;flex:1;overflow:hidden}
.nodes-panel{flex:1;overflow-y:auto;padding:16px;display:flex;flex-wrap:wrap;gap:12px;align-content:flex-start}
.log-panel{width:320px;border-left:1px solid #21262d;display:flex;flex-direction:column;background:#161b22}
.log-panel h2{padding:10px 14px;font-size:11px;color:#8b949e;border-bottom:1px solid #21262d;text-transform:uppercase;letter-spacing:.08em}
.log-list{flex:1;overflow-y:auto;padding:8px 0;font-size:11px}
.log-entry{padding:4px 14px;line-height:1.5;border-bottom:1px solid #21262d22;display:flex;gap:8px}
.log-entry .ts{color:#484f58;flex-shrink:0;font-size:10px;padding-top:1px}
.log-entry .msg{color:#c9d1d9}
.log-entry.write .msg{color:#3fb950}
.log-entry.sync .msg{color:#58a6ff}
.log-entry.snapshot .msg{color:#d2a8ff}
.log-entry.catchup .msg{color:#ffa657}
.log-entry.status .msg{color:#8b949e}
.node-card{background:#161b22;border:1px solid #30363d;border-radius:8px;padding:14px;min-width:200px;max-width:240px;flex:1;transition:border-color .2s}
.node-card.offline{border-color:#da363355;opacity:.85}
.node-header{display:flex;align-items:center;gap:8px;margin-bottom:10px}
.node-id{font-size:12px;font-weight:600;color:#e6edf3}
.badge{font-size:10px;padding:1px 6px;border-radius:10px;margin-left:auto}
.badge.online{background:#1a3726;color:#3fb950;border:1px solid #2ea04333}
.badge.offline{background:#2d1a1a;color:#f85149;border:1px solid #da363333}
.event-count{font-size:36px;font-weight:700;color:#58a6ff;line-height:1;margin-bottom:4px;transition:color .3s}
.event-count.flash{color:#3fb950}
.event-label{font-size:10px;color:#484f58;margin-bottom:10px}
.stats-table{width:100%;border-collapse:collapse;font-size:10px;margin-bottom:10px}
.stats-table th{color:#484f58;text-align:left;padding:2px 4px;border-bottom:1px solid #21262d}
.stats-table td{padding:2px 4px;color:#8b949e}
.stats-table tr:first-child td{padding-top:4px}
.actions{display:flex;flex-wrap:wrap;gap:4px;margin-top:8px}
.actions button{font-size:10px;padding:3px 7px}
</style>
</head>
<body>
<header>
  <h1>⬡ distrb-pglite ダッシュボード</h1>
  <span class="hub" id="hub-info">読み込み中...</span>
</header>
<div class="toolbar">
  <button class="primary" onclick="addLateJoiner('catchup')">+ 遅延参加 (catchup)</button>
  <button class="primary" onclick="addLateJoiner('snapshot')">+ 遅延参加 (snapshot)</button>
  <span style="margin-left:auto;font-size:11px;color:#484f58" id="conv-status"></span>
</div>
<div class="main">
  <div class="nodes-panel" id="nodes"></div>
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
  } catch(e) {
    console.error(e);
  }
}

async function action(nodeId, act, body) {
  try {
    await api('POST', \`/api/nodes/\${nodeId}/\${act}\`, body);
  } catch(e) {
    alert(\`エラー: \${e.message}\`);
  }
}

async function catchup(nodeId) {
  const allNodes = [...document.querySelectorAll('.node-card')].map(el => el.id.replace('card-',''));
  const peer = allNodes.find(id => id !== nodeId) ?? 'node-0';
  await action(nodeId, 'catchup', { peerNodeId: peer });
}

async function addLateJoiner(via) {
  const allNodes = [...document.querySelectorAll('.node-card')].map(el => el.id.replace('card-',''));
  const peer = allNodes[0] ?? 'node-0';
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
  casDir  = join(baseDir, "cas");

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

run().catch((err) => { console.error(err); process.exit(1); });
