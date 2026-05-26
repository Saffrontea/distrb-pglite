import { PGlite } from "@electric-sql/pglite";
import pgliteDataUrl from "../../node_modules/@electric-sql/pglite/dist/pglite.data?url";
import pgliteWasmUrl from "../../node_modules/@electric-sql/pglite/dist/pglite.wasm?url";
import {
  InMemoryPartitionStore,
  SyncNode,
  type EventEnvelope,
  type PartitionStore,
} from "@distrb-pglite/core";
import {
  BrowserPGliteCasFilesystem,
  BrowserSha256BlobStore,
  BrowserWebSocketTransport,
  OpfsCasBlockStore,
  browserManifestFilesEqualInventory,
  diffMissingBrowserCasBlocks,
  gcBrowserCasBlocks,
  inventoryBrowserFilesystem,
  restoreBrowserFilesystemFromCas,
  snapshotBrowserFilesystemToCas,
  transferMissingBrowserCasBlocks,
  type BrowserCasBlockStore,
  type BrowserCasManifest,
} from "@distrb-pglite/browser";
import "./style.css";

type QueryRow = Record<string, unknown>;

type BrowserNode = {
  id: string;
  role: FactoryRole;
  lineId: string;
  fs: BrowserPGliteCasFilesystem;
  blobs: BrowserSha256BlobStore;
  db: PGlite;
  store: SensorBrowserStore;
  syncNode?: SyncNode;
  relayOnline: boolean;
  eventCount: number;
  stats: NodeStats;
};

type LogEntry = {
  time: string;
  kind: "write" | "snapshot" | "restore" | "sql" | "system" | "gc" | "sync";
  message: string;
};

type FactoryRole = "line" | "quality" | "maintenance" | "warehouse";

type FactoryPayload = {
  eventType: "production" | "inspection" | "downtime" | "material" | "maintenance";
  lineId: string;
  stationId: string;
  workOrder: string;
  sku: string;
  quantity: number;
  defectCode?: string;
  severity?: "info" | "warn" | "critical";
  status: "open" | "closed";
  reading?: number;
  unit?: string;
};

type NodeStats = {
  events: number;
  units: number;
  defects: number;
  openIssues: number;
  latest: string;
};

declare const __DISTRB_RELAY_PORT__: number | undefined;

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS factory_events (
    id          text PRIMARY KEY,
    event_type  text NOT NULL,
    line_id     text NOT NULL,
    station_id  text NOT NULL,
    work_order  text NOT NULL,
    sku         text NOT NULL,
    quantity    integer NOT NULL DEFAULT 0,
    defect_code text,
    severity    text NOT NULL DEFAULT 'info',
    status      text NOT NULL DEFAULT 'closed',
    reading     numeric,
    unit        text,
    source_node text NOT NULL,
    recorded_at timestamptz NOT NULL DEFAULT now()
  );
`;

class SensorBrowserStore implements PartitionStore {
  private readonly inner: InMemoryPartitionStore;

  constructor(
    nodeId: string,
    private readonly db: PGlite,
    private readonly onAppliedRemote: () => Promise<void>,
  ) {
    this.inner = new InMemoryPartitionStore(nodeId);
  }

  registerPartition(partitionId: string) {
    return this.inner.registerPartition(partitionId);
  }

  hasPartition(partitionId: string) {
    return this.inner.hasPartition(partitionId);
  }

  appendLocal<TPayload>(input: Parameters<InMemoryPartitionStore["appendLocal"]>[0]) {
    return this.inner.appendLocal<TPayload>(input as never);
  }

  listEventsAfter(...args: Parameters<InMemoryPartitionStore["listEventsAfter"]>) {
    return this.inner.listEventsAfter(...args);
  }

  listEventsAfterCursors(...args: Parameters<InMemoryPartitionStore["listEventsAfterCursors"]>) {
    return this.inner.listEventsAfterCursors(...args);
  }

  getHighWatermark(partitionId: string) {
    return this.inner.getHighWatermark(partitionId);
  }

  createSnapshotMetadata(partitionId: string) {
    return this.inner.createSnapshotMetadata(partitionId);
  }

  async applyRemote(event: EventEnvelope): Promise<boolean> {
    const applied = await this.inner.applyRemote(event);
    if (applied) {
      await insertFactoryEvent(this.db, event);
      await this.onAppliedRemote();
    }
    return applied;
  }
}

const app = document.querySelector<HTMLDivElement>("#app");
if (!app) {
  throw new Error("missing #app");
}
const appRoot = app;

const nodes = new Map<string, BrowserNode>();
const logs: LogEntry[] = [];
const relayPort = typeof __DISTRB_RELAY_PORT__ === "number" ? __DISTRB_RELAY_PORT__ : 4174;
const relayUrl = `ws://${location.hostname || "127.0.0.1"}:${relayPort}`;
const partitionId = "tenant:jp";
let sourceNodeId = "line-a-edge";
let targetNodeId = "plant-control";
let selectedSqlNodeId = "plant-control";
let lastManifest: BrowserCasManifest | null = null;
let sourceCasStore: BrowserCasBlockStore;
let targetCasStore: BrowserCasBlockStore;
let opfsEnabled = false;
let casRelayEnabled = false;
let pgliteFsBundlePromise: Promise<Blob> | undefined;
let pgliteWasmModulePromise: Promise<WebAssembly.Module> | undefined;
let simulationRunning = true;
let simulationBusy = false;
let simulationTimer: ReturnType<typeof setInterval> | undefined;
let incidentTimer: ReturnType<typeof setInterval> | undefined;
let sqlText = defaultSql();
let sqlMetaText = "ready";
let sqlResultHtml = '<div class="empty">Run a query</div>';

async function boot(): Promise<void> {
  sourceCasStore = await openCasStore("source", "distrb-pglite-browser-source-cas");
  targetCasStore = await openCasStore("target", "distrb-pglite-browser-target-cas");
  await addNode("line-a-edge", "line", "LINE-A");
  await addNode("qa-gate", "quality", "LINE-A");
  await addNode("maintenance-bay", "maintenance", "LINE-B");
  await addNode("plant-control", "warehouse", "PLANT");
  await connectRelay("line-a-edge");
  await connectRelay("qa-gate");
  await connectRelay("plant-control");
  await writeFactoryEvent("line-a-edge", "production");
  await writeFactoryEvent("line-a-edge", "production");
  await writeFactoryEvent("qa-gate", "inspection");
  await writeFactoryEvent("maintenance-bay", "downtime");
  addLog(
    "system",
    `factory dashboard using ${opfsEnabled ? "OPFS" : "memory"} CAS + ${casRelayEnabled ? "HTTP relay CAS" : "local-only CAS"}`,
  );
  render();
  startSimulation();
}

async function createPGlite(fs: BrowserPGliteCasFilesystem): Promise<PGlite> {
  return new PGlite({
    fs,
    relaxedDurability: false,
    fsBundle: await loadPGliteFsBundle(),
    wasmModule: await loadPGliteWasmModule(),
  });
}

function loadPGliteFsBundle(): Promise<Blob> {
  pgliteFsBundlePromise ??= fetch(pgliteDataUrl).then(async (response) => {
    if (!response.ok) {
      throw new Error(`failed to load PGlite FS bundle: ${response.status} ${response.url}`);
    }
    return response.blob();
  });
  return pgliteFsBundlePromise;
}

function loadPGliteWasmModule(): Promise<WebAssembly.Module> {
  pgliteWasmModulePromise ??= fetch(pgliteWasmUrl).then(async (response) => {
    if (!response.ok) {
      throw new Error(`failed to load PGlite WASM module: ${response.status} ${response.url}`);
    }
    return WebAssembly.compile(await response.arrayBuffer());
  });
  return pgliteWasmModulePromise;
}

async function addNode(
  id = `workcell-${nodes.size + 1}`,
  role: FactoryRole = roleForIndex(nodes.size),
  lineId = lineForIndex(nodes.size),
): Promise<void> {
  const blobs = new BrowserSha256BlobStore();
  const fs = new BrowserPGliteCasFilesystem(blobs);
  const db = await createPGlite(fs);
  await db.waitReady;
  await db.exec(SCHEMA);
  const store = new SensorBrowserStore(id, db, async () => {
    const node = nodes.get(id);
    if (node) {
      node.stats = await readNodeStats(node.db);
      node.eventCount = node.stats.events;
      addLog("sync", `${id} applied remote event`);
      render(false);
    }
  });
  const stats = await readNodeStats(db);
  nodes.set(id, { id, role, lineId, fs, blobs, db, store, relayOnline: false, eventCount: stats.events, stats });
  if (!sourceNodeId) sourceNodeId = id;
  if (!targetNodeId) targetNodeId = id;
  selectedSqlNodeId = selectedSqlNodeId || id;
  addLog("system", `${id} joined as ${role} node on ${lineId}`);
}

async function resetNode(id: string): Promise<void> {
  const existing = requireNode(id);
  await existing.syncNode?.stop().catch(() => {});
  await existing.db.close();
  const blobs = new BrowserSha256BlobStore();
  const fs = new BrowserPGliteCasFilesystem(blobs);
  const db = await createPGlite(fs);
  await db.waitReady;
  await db.exec(SCHEMA);
  const store = new SensorBrowserStore(id, db, async () => {
    const node = nodes.get(id);
    if (node) {
      node.stats = await readNodeStats(node.db);
      node.eventCount = node.stats.events;
      addLog("sync", `${id} applied remote event`);
      render(false);
    }
  });
  const stats = await readNodeStats(db);
  nodes.set(id, {
    id,
    role: existing.role,
    lineId: existing.lineId,
    fs,
    blobs,
    db,
    store,
    relayOnline: false,
    eventCount: stats.events,
    stats,
  });
  addLog("system", `${id} reset`);
  render();
}

async function connectRelay(nodeId: string): Promise<void> {
  const node = requireNode(nodeId);
  if (node.relayOnline) {
    return;
  }

  try {
    const syncNode = new SyncNode(
      node.id,
      node.store,
      new BrowserWebSocketTransport({
        url: relayUrl,
        reconnect: { maxAttempts: 8, baseDelayMs: 250, maxDelayMs: 5_000 },
      }),
    );
    await syncNode.start({ autoRecoverSubscriptions: true });
    await syncNode.subscribe(partitionId);
    node.syncNode = syncNode;
    node.relayOnline = true;
    addLog("sync", `${node.id} connected to relay ${relayUrl}`);
  } catch (error) {
    node.relayOnline = false;
    addLog("sync", `${node.id} relay unavailable: ${String(error)}`);
  }
  render(false);
}

async function disconnectRelay(nodeId: string): Promise<void> {
  const node = requireNode(nodeId);
  await node.syncNode?.stop().catch(() => {});
  node.syncNode = undefined;
  node.relayOnline = false;
  addLog("sync", `${node.id} disconnected from relay`);
  render(false);
}

async function writeFactoryEvent(
  nodeId: string,
  preferredType?: FactoryPayload["eventType"],
): Promise<void> {
  const node = requireNode(nodeId);
  const payload = createFactoryPayload(node, preferredType);
  if (node.syncNode) {
    const event = await node.syncNode.write<FactoryPayload>({
      partitionId,
      schemaVersion: 1,
      payload,
    });
    await insertFactoryEvent(node.db, event);
    addLog("write", `${nodeId} relayed ${eventSummary(payload)}`);
  } else {
    const event = await node.store.appendLocal<FactoryPayload>({
      partitionId,
      schemaVersion: 1,
      payload,
    });
    await insertFactoryEvent(node.db, event);
    addLog("write", `${nodeId} recorded offline ${eventSummary(payload)}`);
  }
  node.stats = await readNodeStats(node.db);
  node.eventCount = node.stats.events;
  render();
}

async function catchupTargetFromSource(): Promise<void> {
  const target = requireNode(targetNodeId);
  if (!target.syncNode) {
    await connectRelay(target.id);
  }
  const applied = await requireNode(targetNodeId).syncNode?.catchUpFrom(sourceNodeId, partitionId) ?? 0;
  target.stats = await readNodeStats(target.db);
  target.eventCount = target.stats.events;
  addLog("sync", `${targetNodeId} catchup from ${sourceNodeId}: +${applied}`);
  render(false);
}

async function writeBurst(nodeId: string, count = 5): Promise<void> {
  for (let index = 0; index < count; index++) {
    await writeFactoryEvent(nodeId);
  }
}

function startSimulation(): void {
  stopSimulationTimers();
  simulationRunning = true;
  simulationTimer = setInterval(() => {
    void simulationStep();
  }, 1_800);
  incidentTimer = setInterval(() => {
    void incidentStep();
  }, 6_500);
}

function stopSimulation(): void {
  simulationRunning = false;
  stopSimulationTimers();
  addLog("system", "shift automation paused");
  render(false);
}

function resumeSimulation(): void {
  if (simulationRunning) return;
  addLog("system", "shift automation resumed");
  startSimulation();
  render(false);
}

function stopSimulationTimers(): void {
  if (simulationTimer) clearInterval(simulationTimer);
  if (incidentTimer) clearInterval(incidentTimer);
  simulationTimer = undefined;
  incidentTimer = undefined;
}

async function simulationStep(): Promise<void> {
  if (!simulationRunning || simulationBusy || nodes.size === 0) return;
  simulationBusy = true;
  try {
    const nodeList = [...nodes.values()];
    const producer = pick(nodeList);
    await writeFactoryEvent(producer.id);

    if (Math.random() > 0.72) {
      const target = nodeList.find((node) => node.id === "plant-control") ?? pick(nodeList);
      const peer = pick(nodeList.filter((node) => node.id !== target.id));
      if (target.syncNode && peer) {
        const applied = await target.syncNode.catchUpFrom(peer.id, partitionId);
        target.stats = await readNodeStats(target.db);
        target.eventCount = target.stats.events;
        addLog("sync", `${target.id} auto catchup from ${peer.id}: +${applied}`);
        render(false);
      }
    }
  } finally {
    simulationBusy = false;
  }
}

async function incidentStep(): Promise<void> {
  if (!simulationRunning || nodes.size === 0) return;
  const nodeList = [...nodes.values()];
  const online = nodeList.filter((node) => node.relayOnline);
  const offline = nodeList.filter((node) => !node.relayOnline);

  if (Math.random() > 0.55 && online.length > 2) {
    const node = pick(online.filter((candidate) => candidate.id !== "plant-control"));
    if (node) {
      await disconnectRelay(node.id);
      addLog("system", `${node.id} network segment isolated`);
      await writeFactoryEvent(node.id, node.role === "maintenance" ? "downtime" : eventTypeForRole(node.role));
    }
    return;
  }

  if (offline.length > 0) {
    const node = pick(offline);
    await connectRelay(node.id);
    const peers = nodeList.filter((candidate) => candidate.id !== node.id && candidate.relayOnline);
    const peer = peers.length ? pick(peers) : undefined;
    if (node.syncNode && peer) {
      const applied = await node.syncNode.catchUpFrom(peer.id, partitionId);
      node.stats = await readNodeStats(node.db);
      node.eventCount = node.stats.events;
      addLog("sync", `${node.id} rejoined and caught up from ${peer.id}: +${applied}`);
      render(false);
    }
    return;
  }

  const maintenance = nodeList.find((node) => node.role === "maintenance") ?? pick(nodeList);
  await writeFactoryEvent(maintenance.id, "downtime");
}

async function createSnapshot(): Promise<void> {
  const source = requireNode(sourceNodeId);
  lastManifest = await snapshotBrowserFilesystemToCas({
    fs: source.fs.asSnapshotDataFilesystem(),
    casStore: sourceCasStore,
    chunkSize: 16 * 1024,
    snapshotId: `browser-${Date.now()}`,
  });
  const inventory = await inventoryBrowserFilesystem({ fs: source.fs.asSnapshotDataFilesystem() });
  const matches = browserManifestFilesEqualInventory(lastManifest, inventory);
  addLog(
    "snapshot",
    `${source.id} snapshot ${lastManifest.files.length} files / ${blockCount(lastManifest)} blocks (${matches ? "verified" : "inventory mismatch"})`,
  );
  render();
}

async function restoreSnapshot(): Promise<void> {
  if (!lastManifest) {
    await createSnapshot();
  }
  const manifest = lastManifest!;
  const target = requireNode(targetNodeId);
  await target.syncNode?.stop().catch(() => {});
  await target.db.close();
  const missing = await transferMissingBrowserCasBlocks({
    manifest,
    sourceCasStore,
    targetCasStore,
  });
  const blobs = new BrowserSha256BlobStore();
  const fs = new BrowserPGliteCasFilesystem(blobs);
  await restoreBrowserFilesystemFromCas({
    manifest,
    casStore: targetCasStore,
    fs: fs.asSnapshotDataFilesystem(),
  });
  const db = await createPGlite(fs);
  await db.waitReady;
  const stats = await readNodeStats(db);
  const store = new SensorBrowserStore(target.id, db, async () => {
    const node = nodes.get(target.id);
    if (node) {
      node.stats = await readNodeStats(node.db);
      node.eventCount = node.stats.events;
      addLog("sync", `${target.id} applied remote event`);
      render(false);
    }
  });
  nodes.set(target.id, {
    id: target.id,
    role: target.role,
    lineId: target.lineId,
    fs,
    blobs,
    db,
    store,
    relayOnline: false,
    eventCount: stats.events,
    stats,
  });
  addLog("restore", `${target.id} restored ${manifest.snapshotId}; transferred ${missing.length} missing blocks`);
  selectedSqlNodeId = target.id;
  render();
  await runSql();
}

async function diffBlocks(): Promise<void> {
  if (!lastManifest) {
    await createSnapshot();
  }
  const available = await targetCasStore.listBlockHashes();
  const missing = diffMissingBrowserCasBlocks(lastManifest!, available);
  addLog("snapshot", `${targetNodeId} is missing ${missing.length} blocks for ${lastManifest!.snapshotId}`);
  render();
}

async function runGc(): Promise<void> {
  const result = await gcBrowserCasBlocks({
    casStore: targetCasStore,
    retainedManifests: lastManifest ? [lastManifest] : [],
  });
  addLog("gc", `target CAS GC deleted ${result.deleted.length}, retained ${result.retained.length}`);
  render();
}

async function runSql(): Promise<void> {
  const node = requireNode(selectedSqlNodeId);
  const input = document.querySelector<HTMLTextAreaElement>("#sql-input");
  if (!input) return;

  sqlText = input.value;
  const started = performance.now();
  try {
    const result = await node.db.query<QueryRow>(sqlText);
    const duration = Math.round(performance.now() - started);
    sqlMetaText = `${node.id}: ${result.rows.length} rows in ${duration}ms`;
    sqlResultHtml = renderTable(result.rows);
    addLog("sql", `${node.id} query returned ${result.rows.length} rows`);
  } catch (error) {
    sqlMetaText = `${node.id}: query failed`;
    sqlResultHtml = `<pre class="error">${escapeHtml(String(error))}</pre>`;
    addLog("sql", `${node.id} query failed`);
  }
  render(false);
}

async function readNodeStats(db: PGlite): Promise<NodeStats> {
  const result = await db.query<{
    events: string;
    units: string;
    defects: string;
    open_issues: string;
    latest: string | null;
  }>(`
    SELECT
      count(*)::int AS events,
      coalesce(sum(quantity) FILTER (WHERE event_type = 'production'), 0)::int AS units,
      count(*) FILTER (WHERE event_type = 'inspection' AND defect_code IS NOT NULL)::int AS defects,
      count(*) FILTER (WHERE status = 'open')::int AS open_issues,
      max(recorded_at)::text AS latest
    FROM factory_events
  `);
  const row = result.rows[0];
  return {
    events: Number(row?.events ?? 0),
    units: Number(row?.units ?? 0),
    defects: Number(row?.defects ?? 0),
    openIssues: Number(row?.open_issues ?? 0),
    latest: row?.latest ? new Date(row.latest).toLocaleTimeString("ja-JP", { hour12: false }) : "none",
  };
}

async function insertFactoryEvent(db: PGlite, event: EventEnvelope): Promise<void> {
  const payload = event.payload as FactoryPayload;
  await db.query(
    `INSERT INTO factory_events (
       id, event_type, line_id, station_id, work_order, sku, quantity,
       defect_code, severity, status, reading, unit, source_node
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
     ON CONFLICT (id) DO NOTHING`,
    [
      event.eventId,
      payload.eventType,
      payload.lineId,
      payload.stationId,
      payload.workOrder,
      payload.sku,
      payload.quantity,
      payload.defectCode ?? null,
      payload.severity ?? "info",
      payload.status,
      payload.reading ?? null,
      payload.unit ?? null,
      event.sourceNodeId,
    ],
  );
}

async function openCasStore(namespace: string, rootName: string): Promise<BrowserCasBlockStore> {
  const local = await openLocalCasStore(rootName);
  const relay = new RelayCasBlockStore(namespace);
  casRelayEnabled = await relay.isAvailable();
  return casRelayEnabled
    ? new MirroredCasBlockStore(relay, local)
    : local;
}

async function openLocalCasStore(rootName: string): Promise<OpfsCasBlockStore | MemoryCasBlockStore> {
  if ("storage" in navigator && "getDirectory" in navigator.storage) {
    try {
      opfsEnabled = true;
      return await OpfsCasBlockStore.open(rootName);
    } catch {
      opfsEnabled = false;
    }
  }
  return new MemoryCasBlockStore();
}

function requireNode(id: string): BrowserNode {
  const node = nodes.get(id);
  if (!node) {
    throw new Error(`node not found: ${id}`);
  }
  return node;
}

function addLog(kind: LogEntry["kind"], message: string): void {
  logs.unshift({
    kind,
    message,
    time: new Date().toLocaleTimeString("ja-JP", { hour12: false }),
  });
  logs.splice(80);
}

function render(includeSqlResult = true): void {
  const nodeList = [...nodes.values()];
  const fleet = fleetStats(nodeList);
  appRoot.innerHTML = `
    <header>
      <div>
        <h1>Distributed Factory Control</h1>
        <p>${nodeList.length} browser nodes · ${opfsEnabled ? "OPFS" : "memory"} CAS · ${casRelayEnabled ? "HTTP relay CAS" : "local CAS"} · WebSocket event relay</p>
      </div>
      <div class="header-actions">
        <span class="${simulationRunning ? "sim-live" : "sim-paused"}">${simulationRunning ? "shift running" : "shift paused"}</span>
        <button data-action="toggle-sim">${simulationRunning ? "Pause shift" : "Run shift"}</button>
        <button data-action="add-node">Add workcell</button>
      </div>
    </header>
    <main>
      <section class="toolbar">
        <label>Source ${select("source-node", nodeList, sourceNodeId)}</label>
        <label>Target ${select("target-node", nodeList, targetNodeId)}</label>
        <button class="primary" data-action="write-source">Record event</button>
        <button data-action="burst-source">Run shift</button>
        <button data-action="connect-source">Connect source</button>
        <button data-action="connect-target">Connect target</button>
        <button data-action="catchup">Catchup target</button>
        <button data-action="snapshot">Snapshot</button>
        <button data-action="diff">Diff</button>
        <button class="primary" data-action="restore">Restore to target</button>
        <button data-action="gc">GC target CAS</button>
      </section>
      <section class="kpis">
        <div><span>Fleet events</span><strong>${fleet.events}</strong></div>
        <div><span>Units reported</span><strong>${fleet.units}</strong></div>
        <div><span>Quality defects</span><strong>${fleet.defects}</strong></div>
        <div><span>Open issues</span><strong>${fleet.openIssues}</strong></div>
        <div><span>Relay online</span><strong>${fleet.online}/${nodeList.length}</strong></div>
      </section>
      <section class="content">
        <div class="nodes">
          <div class="panel-title">Workcell nodes</div>
          ${nodeList.map(renderNode).join("")}
        </div>
        <div class="sql">
          <div class="sql-head">
            <h2>Local plant SQL</h2>
            ${select("sql-node", nodeList, selectedSqlNodeId)}
            <button class="primary" data-action="run-sql">Run</button>
          </div>
          <div class="samples">
            <button data-sql="events">events</button>
            <button data-sql="line">line board</button>
            <button data-sql="quality">quality</button>
            <button data-sql="files">storage</button>
          </div>
          <textarea id="sql-input" spellcheck="false">${escapeHtml(sqlText)}</textarea>
          <div id="sql-meta">${escapeHtml(sqlMetaText)}</div>
          <div id="sql-result">${sqlResultHtml}</div>
        </div>
        <aside>
          <h2>Log</h2>
          <div class="log">${logs.map(renderLog).join("")}</div>
        </aside>
      </section>
    </main>
  `;
  bindEvents();
}

function renderNode(node: BrowserNode): string {
  const isSource = node.id === sourceNodeId;
  const isTarget = node.id === targetNodeId;
  const stale = node.stats.latest === "none" ? "no local events" : `latest ${node.stats.latest}`;
  return `
    <article class="node">
      <div class="node-title">
        <strong>${node.id}</strong>
        <span>${node.role}</span>
        <span>${node.lineId}</span>
        ${isSource ? '<span>source</span>' : ""}
        ${isTarget ? '<span>target</span>' : ""}
        ${node.relayOnline ? '<span>relay</span>' : ""}
      </div>
      <div class="node-metrics">
        <div><strong>${node.stats.events}</strong><span>events</span></div>
        <div><strong>${node.stats.units}</strong><span>units</span></div>
        <div><strong>${node.stats.defects}</strong><span>defects</span></div>
        <div><strong>${node.stats.openIssues}</strong><span>open</span></div>
      </div>
      <p>${stale}</p>
      <div class="node-actions">
        <button data-write="${node.id}">Record</button>
        <button data-connect="${node.id}">${node.relayOnline ? "Disconnect" : "Connect"}</button>
        <button data-reset="${node.id}">Reset</button>
        <button data-use-sql="${node.id}">SQL</button>
      </div>
    </article>
  `;
}

function renderLog(log: LogEntry): string {
  return `<div class="log-row ${log.kind}"><time>${log.time}</time><span>${escapeHtml(log.message)}</span></div>`;
}

function renderTable(rows: QueryRow[]): string {
  if (rows.length === 0) {
    return '<div class="empty">No rows</div>';
  }
  const columns = Object.keys(rows[0] ?? {});
  return `
    <table>
      <thead><tr>${columns.map((column) => `<th>${escapeHtml(column)}</th>`).join("")}</tr></thead>
      <tbody>
        ${rows.map((row) => `<tr>${columns.map((column) => `<td>${escapeHtml(formatCell(row[column]))}</td>`).join("")}</tr>`).join("")}
      </tbody>
    </table>
  `;
}

function bindEvents(): void {
  document.querySelector<HTMLSelectElement>("#source-node")?.addEventListener("change", (event) => {
    sourceNodeId = (event.target as HTMLSelectElement).value;
    render(false);
  });
  document.querySelector<HTMLSelectElement>("#target-node")?.addEventListener("change", (event) => {
    targetNodeId = (event.target as HTMLSelectElement).value;
    render(false);
  });
  document.querySelector<HTMLSelectElement>("#sql-node")?.addEventListener("change", (event) => {
    selectedSqlNodeId = (event.target as HTMLSelectElement).value;
    render(false);
  });
  document.querySelector<HTMLTextAreaElement>("#sql-input")?.addEventListener("input", (event) => {
    sqlText = (event.target as HTMLTextAreaElement).value;
  });
  document.querySelector("[data-action='add-node']")?.addEventListener("click", () => void addNode().then(() => render()));
  document.querySelector("[data-action='toggle-sim']")?.addEventListener("click", () => {
    if (simulationRunning) {
      stopSimulation();
    } else {
      resumeSimulation();
    }
  });
  document.querySelector("[data-action='write-source']")?.addEventListener("click", () => void writeFactoryEvent(sourceNodeId));
  document.querySelector("[data-action='burst-source']")?.addEventListener("click", () => void writeBurst(sourceNodeId));
  document.querySelector("[data-action='connect-source']")?.addEventListener("click", () => void connectRelay(sourceNodeId));
  document.querySelector("[data-action='connect-target']")?.addEventListener("click", () => void connectRelay(targetNodeId));
  document.querySelector("[data-action='catchup']")?.addEventListener("click", () => void catchupTargetFromSource());
  document.querySelector("[data-action='snapshot']")?.addEventListener("click", () => void createSnapshot());
  document.querySelector("[data-action='diff']")?.addEventListener("click", () => void diffBlocks());
  document.querySelector("[data-action='restore']")?.addEventListener("click", () => void restoreSnapshot());
  document.querySelector("[data-action='gc']")?.addEventListener("click", () => void runGc());
  document.querySelector("[data-action='run-sql']")?.addEventListener("click", () => void runSql());
  document.querySelectorAll<HTMLButtonElement>("[data-write]").forEach((button) => {
    button.addEventListener("click", () => void writeFactoryEvent(button.dataset.write!));
  });
  document.querySelectorAll<HTMLButtonElement>("[data-connect]").forEach((button) => {
    button.addEventListener("click", () => {
      const id = button.dataset.connect!;
      const node = requireNode(id);
      void (node.relayOnline ? disconnectRelay(id) : connectRelay(id));
    });
  });
  document.querySelectorAll<HTMLButtonElement>("[data-reset]").forEach((button) => {
    button.addEventListener("click", () => void resetNode(button.dataset.reset!));
  });
  document.querySelectorAll<HTMLButtonElement>("[data-use-sql]").forEach((button) => {
    button.addEventListener("click", () => {
      selectedSqlNodeId = button.dataset.useSql!;
      render(false);
    });
  });
  document.querySelectorAll<HTMLButtonElement>("[data-sql]").forEach((button) => {
    button.addEventListener("click", () => {
      setSqlSample(button.dataset.sql!);
    });
  });
}

function select(id: string, nodeList: BrowserNode[], selected: string): string {
  return `
    <select id="${id}">
      ${nodeList.map((node) => `<option value="${node.id}" ${node.id === selected ? "selected" : ""}>${node.id}</option>`).join("")}
    </select>
  `;
}

function setSqlSample(name: string): void {
  const input = document.querySelector<HTMLTextAreaElement>("#sql-input");
  if (name === "line") {
    sqlText = `SELECT line_id,
       sum(quantity) FILTER (WHERE event_type = 'production')::int AS units,
       count(*) FILTER (WHERE status = 'open')::int AS open_issues,
       max(recorded_at) AS latest
FROM factory_events
GROUP BY line_id
ORDER BY line_id;`;
  } else if (name === "quality") {
    sqlText = `SELECT line_id, station_id, defect_code, severity, count(*)::int AS count
FROM factory_events
WHERE event_type = 'inspection' AND defect_code IS NOT NULL
GROUP BY line_id, station_id, defect_code, severity
ORDER BY count DESC, line_id;`;
  } else if (name === "files") {
    sqlText = `SELECT pg_relation_filepath('factory_events') AS relation_path,
       pg_database_size(current_database()) AS database_size;`;
  } else {
    sqlText = defaultSql();
  }
  if (input) input.value = sqlText;
}

function defaultSql(): string {
  return `SELECT event_type, line_id, station_id, work_order, sku, quantity,
       coalesce(defect_code, '') AS defect, severity, status, source_node, recorded_at
FROM factory_events
ORDER BY recorded_at DESC
LIMIT 20;`;
}

function createFactoryPayload(
  node: BrowserNode,
  preferredType = eventTypeForRole(node.role),
): FactoryPayload {
  const sku = pick(["AX-14", "BX-22", "CX-90", "DX-08"]);
  const workOrder = `WO-${String(Math.floor(1200 + Math.random() * 80))}`;
  const stationId = stationFor(node);

  if (preferredType === "production") {
    return {
      eventType: "production",
      lineId: node.lineId,
      stationId,
      workOrder,
      sku,
      quantity: Math.floor(18 + Math.random() * 34),
      severity: "info",
      status: "closed",
      reading: Math.round((82 + Math.random() * 10) * 10) / 10,
      unit: "oee",
    };
  }

  if (preferredType === "inspection") {
    const hasDefect = Math.random() > 0.55;
    return {
      eventType: "inspection",
      lineId: node.lineId,
      stationId,
      workOrder,
      sku,
      quantity: 0,
      defectCode: hasDefect ? pick(["scratch", "torque-low", "vision-fail", "label-miss"]) : undefined,
      severity: hasDefect ? pick(["warn", "critical"]) : "info",
      status: hasDefect ? "open" : "closed",
      reading: Math.round((96 + Math.random() * 3.5) * 10) / 10,
      unit: "yield",
    };
  }

  if (preferredType === "material") {
    return {
      eventType: "material",
      lineId: node.lineId,
      stationId,
      workOrder,
      sku,
      quantity: Math.floor(40 + Math.random() * 90),
      severity: "info",
      status: "closed",
      unit: "kits",
    };
  }

  return {
    eventType: preferredType,
    lineId: node.lineId,
    stationId,
    workOrder,
    sku,
    quantity: 0,
    defectCode: preferredType === "downtime" ? pick(["jam", "overheat", "air-pressure", "changeover"]) : undefined,
    severity: preferredType === "downtime" ? pick(["warn", "critical"]) : "warn",
    status: "open",
    reading: Math.floor(6 + Math.random() * 24),
    unit: "min",
  };
}

function eventTypeForRole(role: FactoryRole): FactoryPayload["eventType"] {
  if (role === "quality") return "inspection";
  if (role === "maintenance") return Math.random() > 0.35 ? "maintenance" : "downtime";
  if (role === "warehouse") return "material";
  return "production";
}

function stationFor(node: BrowserNode): string {
  if (node.role === "quality") return "QA-02";
  if (node.role === "maintenance") return "MNT-01";
  if (node.role === "warehouse") return "WH-01";
  return `${node.lineId}-CELL-${Math.floor(1 + Math.random() * 4)}`;
}

function roleForIndex(index: number): FactoryRole {
  return (["line", "quality", "maintenance", "warehouse"] as const)[index % 4];
}

function lineForIndex(index: number): string {
  return ["LINE-A", "LINE-B", "LINE-C", "PLANT"][index % 4];
}

function pick<T>(values: readonly T[]): T {
  return values[Math.floor(Math.random() * values.length)]!;
}

function eventSummary(payload: FactoryPayload): string {
  const issue = payload.defectCode ? ` ${payload.defectCode}` : "";
  const qty = payload.quantity ? ` x${payload.quantity}` : "";
  return `${payload.eventType} ${payload.lineId}/${payload.stationId}${qty}${issue}`;
}

function fleetStats(nodeList: BrowserNode[]): NodeStats & { online: number } {
  return nodeList.reduce(
    (total, node) => ({
      events: total.events + node.stats.events,
      units: total.units + node.stats.units,
      defects: total.defects + node.stats.defects,
      openIssues: total.openIssues + node.stats.openIssues,
      latest: total.latest,
      online: total.online + (node.relayOnline ? 1 : 0),
    }),
    { events: 0, units: 0, defects: 0, openIssues: 0, latest: "", online: 0 },
  );
}

function blockCount(manifest: BrowserCasManifest): number {
  return manifest.files.reduce((count, file) => count + file.blocks.length, 0);
}

function formatCell(value: unknown): string {
  if (value == null) return "";
  if (typeof value === "object") return JSON.stringify(value);
  return String(value);
}

function escapeHtml(value: string): string {
  return value
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;");
}

class MemoryCasBlockStore {
  private readonly blocks = new Map<string, Uint8Array>();

  async readBlock(hash: string): Promise<Uint8Array> {
    const content = this.blocks.get(hash);
    if (!content) {
      throw new Error(`CAS block not found: ${hash}`);
    }
    return new Uint8Array(content);
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    this.blocks.set(hash, new Uint8Array(content));
  }

  async listBlockHashes(): Promise<Set<string>> {
    return new Set(this.blocks.keys());
  }

  async deleteBlock(hash: string): Promise<void> {
    this.blocks.delete(hash);
  }
}

class RelayCasBlockStore implements BrowserCasBlockStore {
  constructor(private readonly namespace: string) {}

  async isAvailable(): Promise<boolean> {
    try {
      await this.listBlockHashes();
      return true;
    } catch {
      return false;
    }
  }

  async readBlock(hash: string): Promise<Uint8Array> {
    const response = await fetch(this.blockUrl(hash));
    if (!response.ok) {
      throw new Error(`relay CAS read failed: ${response.status} ${hash}`);
    }
    return new Uint8Array(await response.arrayBuffer());
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    const response = await fetch(this.blockUrl(hash), {
      method: "PUT",
      headers: { "Content-Type": "application/octet-stream" },
      body: new Uint8Array(content),
    });
    if (!response.ok) {
      throw new Error(`relay CAS write failed: ${response.status} ${hash}`);
    }
  }

  async listBlockHashes(): Promise<Set<string>> {
    const response = await fetch(this.namespaceUrl());
    if (!response.ok) {
      throw new Error(`relay CAS list failed: ${response.status}`);
    }
    const body = await response.json() as { hashes?: string[] };
    return new Set(body.hashes ?? []);
  }

  async deleteBlock(hash: string): Promise<void> {
    const response = await fetch(this.blockUrl(hash), { method: "DELETE" });
    if (!response.ok) {
      throw new Error(`relay CAS delete failed: ${response.status} ${hash}`);
    }
  }

  private namespaceUrl(): string {
    return `/__distrb/cas/${encodeURIComponent(this.namespace)}`;
  }

  private blockUrl(hash: string): string {
    return `${this.namespaceUrl()}/${hash}`;
  }
}

class MirroredCasBlockStore implements BrowserCasBlockStore {
  constructor(
    private readonly primary: BrowserCasBlockStore,
    private readonly mirror: BrowserCasBlockStore,
  ) {}

  async readBlock(hash: string): Promise<Uint8Array> {
    try {
      return await this.primary.readBlock(hash);
    } catch {
      return this.mirror.readBlock(hash);
    }
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    await Promise.all([
      this.primary.writeBlock(hash, content),
      this.mirror.writeBlock(hash, content),
    ]);
  }

  async listBlockHashes(): Promise<Set<string>> {
    return this.primary.listBlockHashes();
  }

  async deleteBlock(hash: string): Promise<void> {
    await Promise.allSettled([
      this.primary.deleteBlock(hash),
      this.mirror.deleteBlock(hash),
    ]);
  }
}

void boot().catch((error) => {
  app.innerHTML = `<main class="fatal"><h1>Browser playground failed</h1><pre>${escapeHtml(String(error))}</pre></main>`;
});
