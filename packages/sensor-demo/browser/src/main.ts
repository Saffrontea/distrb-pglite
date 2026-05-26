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
  fs: BrowserPGliteCasFilesystem;
  blobs: BrowserSha256BlobStore;
  db: PGlite;
  store: SensorBrowserStore;
  syncNode?: SyncNode;
  relayOnline: boolean;
  eventCount: number;
};

type LogEntry = {
  time: string;
  kind: "write" | "snapshot" | "restore" | "sql" | "system" | "gc" | "sync";
  message: string;
};

type SensorPayload = {
  sensorId: string;
  reading: number;
  unit?: string;
};

declare const __DISTRB_RELAY_PORT__: number | undefined;

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS sensor_readings (
    id          text PRIMARY KEY,
    sensor_id   text NOT NULL,
    reading     numeric NOT NULL,
    unit        text NOT NULL DEFAULT 'C',
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
      await insertReading(this.db, event);
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
let sourceNodeId = "browser-a";
let targetNodeId = "browser-b";
let selectedSqlNodeId = "browser-a";
let lastManifest: BrowserCasManifest | null = null;
let sourceCasStore: BrowserCasBlockStore;
let targetCasStore: BrowserCasBlockStore;
let opfsEnabled = false;
let casRelayEnabled = false;
let pgliteFsBundlePromise: Promise<Blob> | undefined;
let pgliteWasmModulePromise: Promise<WebAssembly.Module> | undefined;

async function boot(): Promise<void> {
  sourceCasStore = await openCasStore("source", "distrb-pglite-browser-source-cas");
  targetCasStore = await openCasStore("target", "distrb-pglite-browser-target-cas");
  await addNode("browser-a");
  await addNode("browser-b");
  await connectRelay("browser-a");
  await connectRelay("browser-b");
  await writeReading("browser-a", "temp-a", 22);
  await writeReading("browser-a", "temp-b", 24);
  await writeReading("browser-b", "temp-c", 20);
  addLog(
    "system",
    `${opfsEnabled ? "OPFS" : "memory"} local CAS + ${casRelayEnabled ? "HTTP relay CAS" : "local-only CAS"} active`,
  );
  render();
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

async function addNode(id = `browser-${nodes.size + 1}`): Promise<void> {
  const blobs = new BrowserSha256BlobStore();
  const fs = new BrowserPGliteCasFilesystem(blobs);
  const db = await createPGlite(fs);
  await db.waitReady;
  await db.exec(SCHEMA);
  const store = new SensorBrowserStore(id, db, async () => {
    const node = nodes.get(id);
    if (node) {
      node.eventCount = await rowCount(node.db);
      addLog("sync", `${id} applied remote event`);
      render(false);
    }
  });
  nodes.set(id, { id, fs, blobs, db, store, relayOnline: false, eventCount: 0 });
  if (!sourceNodeId) sourceNodeId = id;
  if (!targetNodeId) targetNodeId = id;
  selectedSqlNodeId = selectedSqlNodeId || id;
  addLog("system", `${id} opened with BrowserPGliteCasFilesystem`);
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
      node.eventCount = await rowCount(node.db);
      addLog("sync", `${id} applied remote event`);
      render(false);
    }
  });
  nodes.set(id, { id, fs, blobs, db, store, relayOnline: false, eventCount: 0 });
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

async function writeReading(
  nodeId: string,
  sensorId = `sensor-${Math.floor(Math.random() * 5) + 1}`,
  reading = Math.round((18 + Math.random() * 12) * 10) / 10,
): Promise<void> {
  const node = requireNode(nodeId);
  if (node.syncNode) {
    const event = await node.syncNode.write<SensorPayload>({
      partitionId,
      schemaVersion: 1,
      payload: { sensorId, reading },
    });
    await insertReading(node.db, event);
    addLog("write", `${nodeId} relayed ${sensorId}=${reading}`);
  } else {
    await node.db.query(
      `INSERT INTO sensor_readings (id, sensor_id, reading, source_node)
       VALUES ($1, $2, $3, $4)`,
      [crypto.randomUUID(), sensorId, reading, nodeId],
    );
    await node.store.appendLocal<SensorPayload>({
      partitionId,
      schemaVersion: 1,
      payload: { sensorId, reading },
    });
    addLog("write", `${nodeId} inserted local-only ${sensorId}=${reading}`);
  }
  node.eventCount = await rowCount(node.db);
  render();
}

async function catchupTargetFromSource(): Promise<void> {
  const target = requireNode(targetNodeId);
  if (!target.syncNode) {
    await connectRelay(target.id);
  }
  const applied = await requireNode(targetNodeId).syncNode?.catchUpFrom(sourceNodeId, partitionId) ?? 0;
  target.eventCount = await rowCount(target.db);
  addLog("sync", `${targetNodeId} catchup from ${sourceNodeId}: +${applied}`);
  render(false);
}

async function writeBurst(nodeId: string, count = 5): Promise<void> {
  for (let index = 0; index < count; index++) {
    await writeReading(
      nodeId,
      `sensor-${index % 3}`,
      Math.round((18 + Math.random() * 14) * 10) / 10,
    );
  }
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
  const count = await rowCount(db);
  const store = new SensorBrowserStore(target.id, db, async () => {
    const node = nodes.get(target.id);
    if (node) {
      node.eventCount = await rowCount(node.db);
      addLog("sync", `${target.id} applied remote event`);
      render(false);
    }
  });
  nodes.set(target.id, { id: target.id, fs, blobs, db, store, relayOnline: false, eventCount: count });
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
  const meta = document.querySelector<HTMLDivElement>("#sql-meta");
  const output = document.querySelector<HTMLDivElement>("#sql-result");
  if (!input || !meta || !output) return;

  const started = performance.now();
  try {
    const result = await node.db.query<QueryRow>(input.value);
    const duration = Math.round(performance.now() - started);
    meta.textContent = `${node.id}: ${result.rows.length} rows in ${duration}ms`;
    output.innerHTML = renderTable(result.rows);
    addLog("sql", `${node.id} query returned ${result.rows.length} rows`);
  } catch (error) {
    meta.textContent = `${node.id}: query failed`;
    output.innerHTML = `<pre class="error">${escapeHtml(String(error))}</pre>`;
    addLog("sql", `${node.id} query failed`);
  }
  render(false);
}

async function rowCount(db: PGlite): Promise<number> {
  const result = await db.query<{ count: string }>("SELECT count(*)::int AS count FROM sensor_readings");
  return Number(result.rows[0]?.count ?? 0);
}

async function insertReading(db: PGlite, event: EventEnvelope): Promise<void> {
  const payload = event.payload as SensorPayload;
  await db.query(
    `INSERT INTO sensor_readings (id, sensor_id, reading, unit, source_node)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (id) DO NOTHING`,
    [
      event.eventId,
      payload.sensorId,
      payload.reading,
      payload.unit ?? "C",
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
  appRoot.innerHTML = `
    <header>
      <div>
        <h1>distrb-pglite browser playground</h1>
        <p>Browser PGlite + ${opfsEnabled ? "OPFS" : "memory"} CAS + ${casRelayEnabled ? "HTTP relay CAS" : "local CAS"} + WebSocket event relay</p>
      </div>
      <button data-action="add-node">Add node</button>
    </header>
    <main>
      <section class="toolbar">
        <label>Source ${select("source-node", nodeList, sourceNodeId)}</label>
        <label>Target ${select("target-node", nodeList, targetNodeId)}</label>
        <button class="primary" data-action="write-source">Write</button>
        <button data-action="burst-source">Burst</button>
        <button data-action="connect-source">Connect source</button>
        <button data-action="connect-target">Connect target</button>
        <button data-action="catchup">Catchup target</button>
        <button data-action="snapshot">Snapshot</button>
        <button data-action="diff">Diff</button>
        <button class="primary" data-action="restore">Restore to target</button>
        <button data-action="gc">GC target CAS</button>
      </section>
      <section class="content">
        <div class="nodes">${nodeList.map(renderNode).join("")}</div>
        <div class="sql">
          <div class="sql-head">
            <h2>SQL</h2>
            ${select("sql-node", nodeList, selectedSqlNodeId)}
            <button class="primary" data-action="run-sql">Run</button>
          </div>
          <div class="samples">
            <button data-sql="readings">readings</button>
            <button data-sql="stats">stats</button>
            <button data-sql="files">files</button>
          </div>
          <textarea id="sql-input" spellcheck="false">${escapeHtml(currentSql())}</textarea>
          <div id="sql-meta">${includeSqlResult ? "ready" : document.querySelector("#sql-meta")?.textContent ?? "ready"}</div>
          <div id="sql-result">${includeSqlResult ? '<div class="empty">Run a query</div>' : document.querySelector("#sql-result")?.innerHTML ?? ""}</div>
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
  return `
    <article class="node">
      <div class="node-title">
        <strong>${node.id}</strong>
        ${isSource ? '<span>source</span>' : ""}
        ${isTarget ? '<span>target</span>' : ""}
        ${node.relayOnline ? '<span>relay</span>' : ""}
      </div>
      <div class="count">${node.eventCount}</div>
      <p>local SQL rows</p>
      <div class="node-actions">
        <button data-write="${node.id}">Write</button>
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
  document.querySelector("[data-action='add-node']")?.addEventListener("click", () => void addNode().then(() => render()));
  document.querySelector("[data-action='write-source']")?.addEventListener("click", () => void writeReading(sourceNodeId));
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
    button.addEventListener("click", () => void writeReading(button.dataset.write!));
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
  if (!input) return;
  if (name === "stats") {
    input.value = `SELECT sensor_id, round(avg(reading)::numeric, 2) AS avg_reading, count(*)::int AS count
FROM sensor_readings
GROUP BY sensor_id
ORDER BY sensor_id;`;
  } else if (name === "files") {
    input.value = `SELECT pg_relation_filepath('sensor_readings') AS relation_path,
       pg_database_size(current_database()) AS database_size;`;
  } else {
    input.value = `SELECT sensor_id, reading, unit, source_node, recorded_at
FROM sensor_readings
ORDER BY recorded_at DESC
LIMIT 20;`;
  }
}

function currentSql(): string {
  return document.querySelector<HTMLTextAreaElement>("#sql-input")?.value
    ?? `SELECT sensor_id, reading, unit, source_node, recorded_at
FROM sensor_readings
ORDER BY recorded_at DESC
LIMIT 20;`;
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
