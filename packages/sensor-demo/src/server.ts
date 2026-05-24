import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { mkdir } from "node:fs/promises";
import { PGlite } from "@electric-sql/pglite";
import {
  SyncNode,
  type EventEnvelope,
  type PartitionStore,
} from "@distrb-pglite/core";
import type {
  StorageCatalogManifest,
  StoragePullRequirement,
  StorageSnapshotCatalogEntry,
} from "@distrb-pglite/core";
import {
  P2PWebSocketTransport,
  PGlitePartitionStore,
  WebSocketTransport,
  bindStoragePartition,
  createManagedPartitionCasSnapshot,
  createStorageBindingGroup,
  listStorageSnapshotCatalogEntries,
  loadStorageCatalogManifest,
  pullPartitionCasSnapshot,
  type CasManifest,
} from "@distrb-pglite/node";

// ─── application schema ───────────────────────────────────────────────────────

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS sensor_groups (id serial PRIMARY KEY);
  CREATE TABLE IF NOT EXISTS sensor_readings (
    event_id    text PRIMARY KEY,
    sensor_id   text NOT NULL,
    reading     numeric NOT NULL,
    unit        text NOT NULL DEFAULT 'C',
    recorded_at timestamptz NOT NULL DEFAULT now(),
    source_node text NOT NULL
  );
`;

export const GROUP_ID = "sensors";

// ─── event payload ────────────────────────────────────────────────────────────

export type SensorPayload = {
  sensorId: string;
  reading: number;
  unit?: string;
};

// ─── SensorPartitionStore ─────────────────────────────────────────────────────
// applyRemote でイベントを受け取ったとき、sensor_readings にも書き込む。

class SensorPartitionStore implements PartitionStore {
  constructor(readonly inner: PGlitePartitionStore, private readonly db: PGlite) {}

  registerPartition(id: string) { return this.inner.registerPartition(id); }
  hasPartition(id: string) { return this.inner.hasPartition(id); }
  appendLocal<T>(input: Parameters<PGlitePartitionStore["appendLocal"]>[0]) {
    return this.inner.appendLocal<T>(input as never);
  }
  listEventsAfter(...a: Parameters<PGlitePartitionStore["listEventsAfter"]>) {
    return this.inner.listEventsAfter(...a);
  }
  listEventsAfterCursors(...a: Parameters<PGlitePartitionStore["listEventsAfterCursors"]>) {
    return this.inner.listEventsAfterCursors(...a);
  }
  getHighWatermark(id: string) { return this.inner.getHighWatermark(id); }
  createSnapshotMetadata(id: string) { return this.inner.createSnapshotMetadata(id); }

  async applyRemote(event: EventEnvelope): Promise<boolean> {
    const applied = await this.inner.applyRemote(event);
    if (applied) {
      const p = event.payload as SensorPayload;
      await this.db.query(
        `INSERT INTO sensor_readings (event_id, sensor_id, reading, unit, source_node)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (event_id) DO NOTHING`,
        [event.eventId, p.sensorId, p.reading, p.unit ?? "C", event.sourceNodeId],
      );
    }
    return applied;
  }
}

// ─── EdgeNodeServer ───────────────────────────────────────────────────────────

export type EdgeServerConfig = {
  nodeId: string;
  hubUrl?: string;
  transport?: "hub" | "p2p";
  p2pPublicUrl?: string;
  p2pPort?: number;
  p2pSeeds?: string[];
  dataDir: string;
  casDir: string;
  port: number;
  partitionId: string;
  snapshotScope?: "catalog-and-data" | "data-only";
  peerNodeIds?: string[];
  autoCatchupOnConnect?: boolean;
};

type ReadingRow = {
  event_id: string; sensor_id: string; reading: string;
  unit: string; recorded_at: string; source_node: string;
};
type StatsRow = {
  sensor_id: string; avg_reading: string; min_reading: string;
  max_reading: string; count: string; last_seen: string;
};
type SqlResponse = {
  mode: "query" | "exec";
  rows: Record<string, unknown>[];
  columns: string[];
  rowCount: number;
  affectedRows?: number;
  durationMs: number;
};

export class EdgeNodeServer {
  private db!: PGlite;
  private inner!: PGlitePartitionStore;
  private store!: SensorPartitionStore;
  private syncNode!: SyncNode;
  private online = false;
  private readonly http;

  private constructor(private config: EdgeServerConfig) {
    this.http = createServer(this.dispatch.bind(this));
  }

  static async create(config: EdgeServerConfig): Promise<EdgeNodeServer> {
    const s = new EdgeNodeServer(config);
    await s.initDb();
    await s.initSync();
    await s.setupBindings();
    return s;
  }

  get baseUrl() { return `http://127.0.0.1:${this.config.port}`; }
  get nodeId() { return this.config.nodeId; }

  listen(): Promise<void> {
    return new Promise((resolve) => this.http.listen(this.config.port, "127.0.0.1", resolve));
  }

  async close(): Promise<void> {
    if (this.online) await this.syncNode.stop();
    await new Promise<void>((r, j) => this.http.close((e) => (e ? j(e) : r())));
    await this.db.close();
  }

  // ─── init ─────────────────────────────────────────────────────

  private async initDb(): Promise<void> {
    await mkdir(this.config.dataDir, { recursive: true });
    await mkdir(this.config.casDir, { recursive: true });
    this.db = new PGlite(this.config.dataDir, { relaxedDurability: false });
    await this.db.exec(SCHEMA);
    this.inner = new PGlitePartitionStore(this.config.nodeId, this.db);
    await this.inner.migrate();
    this.store = new SensorPartitionStore(this.inner, this.db);
  }

  private async initSync(): Promise<void> {
    this.syncNode = this.createSyncNode();
    await this.syncNode.start();
    await this.syncNode.subscribe(this.config.partitionId);
    this.online = true;
  }

  private createSyncNode(): SyncNode {
    if ((this.config.transport ?? "hub") === "p2p") {
      if (!this.config.p2pPublicUrl || !this.config.p2pPort) {
        throw new Error("p2p transport requires p2pPublicUrl and p2pPort");
      }
      return new SyncNode(this.config.nodeId, this.store, new P2PWebSocketTransport({
        publicUrl: this.config.p2pPublicUrl,
        port: this.config.p2pPort,
        seeds: this.config.p2pSeeds ?? [],
      }));
    }

    if (!this.config.hubUrl) {
      throw new Error("hub transport requires hubUrl");
    }
    return new SyncNode(this.config.nodeId, this.store, new WebSocketTransport(this.config.hubUrl));
  }

  private async setupBindings(): Promise<void> {
    await createStorageBindingGroup({ db: this.db, groupId: GROUP_ID, rootRelation: "sensor_groups" });
    await bindStoragePartition({
      db: this.db,
      groupId: GROUP_ID,
      partitionId: this.config.partitionId,
      relations: ["sensor_readings"],
      snapshotScope: this.config.snapshotScope ?? "catalog-and-data",
    });
  }

  // ─── HTTP dispatch ────────────────────────────────────────────

  private async dispatch(req: IncomingMessage, res: ServerResponse): Promise<void> {
    const url   = new URL(req.url ?? "/", "http://localhost");
    const path  = url.pathname;
    const meth  = req.method ?? "GET";
    const body  = meth !== "GET" ? await readJson(req) : {};

    const routes: Record<string, () => Promise<unknown>> = {
      "GET /status":            () => this.getStatus(),
      "POST /subscribe":        () => this.postSubscribe(body),
      "POST /readings":         () => this.postReading(body),
      "POST /readings-offline": () => this.postReadingOffline(body),
      "GET /readings":          () => this.getReadings(url.searchParams),
      "GET /stats":             () => this.getStats(),
      "GET /events":            () => this.getEvents(url.searchParams),
      "POST /sql":              () => this.postSql(body),
      "POST /catchup":          () => this.postCatchup(body),
      "POST /disconnect":       () => this.postDisconnect(),
      "POST /connect":          () => this.postConnect(),
      "POST /create-snapshot":  () => this.postCreateSnapshot(),
      "GET /catalog":           () => this.getCatalog(),
      "GET /snapshot-entries":  () => this.getSnapshotEntries(),
      "POST /pull-snapshot":    () => this.postPullSnapshot(body),
    };

    const handler = routes[`${meth} ${path}`];
    if (!handler) {
      res.writeHead(404, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: "not found", path }));
      return;
    }

    try {
      const result = await handler();
      res.writeHead(200, { "Content-Type": "application/json" });
      res.end(JSON.stringify(result));
    } catch (err) {
      res.writeHead(500, { "Content-Type": "application/json" });
      res.end(JSON.stringify({ error: String(err) }));
    }
  }

  // ─── handlers ─────────────────────────────────────────────────

  private async getStatus() {
    const events = await this.inner.listEventsAfterCursors(this.config.partitionId, new Map());
    return { nodeId: this.config.nodeId, online: this.online, partitionId: this.config.partitionId, eventCount: events.length };
  }

  private async postSubscribe(body: unknown) {
    const { partitionId } = body as { partitionId: string };
    await this.syncNode.subscribe(partitionId);
    return { ok: true, partitionId };
  }

  private async postReading(body: unknown) {
    if (!this.online) throw new Error("node is offline — use /readings-offline");
    const p = body as SensorPayload;
    const event = await this.syncNode.write({ partitionId: this.config.partitionId, schemaVersion: 1, payload: p });
    await this.db.query(
      `INSERT INTO sensor_readings (event_id, sensor_id, reading, unit, source_node)
       VALUES ($1, $2, $3, $4, $5) ON CONFLICT (event_id) DO NOTHING`,
      [event.eventId, p.sensorId, p.reading, p.unit ?? "C", this.config.nodeId],
    );
    return { eventId: event.eventId, sensorId: p.sensorId, reading: p.reading };
  }

  private async postReadingOffline(body: unknown) {
    const p = body as SensorPayload;
    const event = await this.inner.appendLocal({ partitionId: this.config.partitionId, schemaVersion: 1, payload: p });
    await this.db.query(
      `INSERT INTO sensor_readings (event_id, sensor_id, reading, unit, source_node)
       VALUES ($1, $2, $3, $4, $5) ON CONFLICT (event_id) DO NOTHING`,
      [event.eventId, p.sensorId, p.reading, p.unit ?? "C", this.config.nodeId],
    );
    return { eventId: event.eventId, sensorId: p.sensorId, reading: p.reading, offline: true };
  }

  private async getReadings(params: URLSearchParams) {
    const limit = Number(params.get("limit") ?? "100");
    const result = await this.db.query<ReadingRow>(
      `SELECT event_id, sensor_id, reading, unit, recorded_at, source_node
       FROM sensor_readings ORDER BY recorded_at DESC LIMIT $1`,
      [limit],
    );
    return { rows: result.rows, count: result.rows.length };
  }

  private async getStats() {
    const result = await this.db.query<StatsRow>(
      `SELECT sensor_id,
              round(avg(reading)::numeric, 2) AS avg_reading,
              round(min(reading)::numeric, 2) AS min_reading,
              round(max(reading)::numeric, 2) AS max_reading,
              count(*)::int                   AS count,
              max(recorded_at)                AS last_seen
       FROM sensor_readings GROUP BY sensor_id ORDER BY sensor_id`,
    );
    return { stats: result.rows, nodeId: this.config.nodeId };
  }

  private async getEvents(params: URLSearchParams) {
    const limit = Number(params.get("limit") ?? "50");
    const events = await this.inner.listEventsAfter(this.config.partitionId);
    return { events: events.slice(-limit), count: events.length };
  }

  private async postSql(body: unknown): Promise<SqlResponse> {
    const { sql } = body as { sql?: string };
    if (!sql || sql.trim().length === 0) {
      throw new Error("sql is required");
    }

    const started = Date.now();
    if (returnsRows(sql)) {
      const result = await this.db.query<Record<string, unknown>>(sql);
      const rows = result.rows.map(normalizeRow);
      return {
        mode: "query",
        rows,
        columns: columnsForRows(rows),
        rowCount: rows.length,
        affectedRows: result.affectedRows,
        durationMs: Date.now() - started,
      };
    }

    await this.db.exec(sql);
    return {
      mode: "exec",
      rows: [],
      columns: [],
      rowCount: 0,
      durationMs: Date.now() - started,
    };
  }

  private async postCatchup(body: unknown) {
    const { peerNodeId, partitionId } = body as { peerNodeId: string; partitionId?: string };
    const applied = await this.syncNode.catchUpFrom(peerNodeId, partitionId ?? this.config.partitionId);
    return { applied };
  }

  private async postDisconnect() {
    await this.syncNode.stop();
    this.online = false;
    return { ok: true, online: false };
  }

  private async postConnect() {
    this.syncNode = this.createSyncNode();
    await this.syncNode.start();
    await this.syncNode.subscribe(this.config.partitionId);
    this.online = true;
    const catchup = this.config.autoCatchupOnConnect === false
      ? []
      : await this.catchupKnownPeers();
    return {
      ok: true,
      online: true,
      catchup,
      applied: catchup.reduce((sum, result) => sum + result.applied, 0),
    };
  }

  private async catchupKnownPeers(): Promise<Array<{ peerNodeId: string; applied: number }>> {
    const results: Array<{ peerNodeId: string; applied: number }> = [];
    for (const peerNodeId of this.config.peerNodeIds ?? []) {
      if (peerNodeId === this.config.nodeId) {
        continue;
      }
      const applied = await this.syncNode.catchUpFrom(peerNodeId, this.config.partitionId);
      results.push({ peerNodeId, applied });
    }
    return results;
  }

  private async postCreateSnapshot() {
    const managed = await createManagedPartitionCasSnapshot({
      db: this.db,
      store: this.inner,
      groupId: GROUP_ID,
      partitionId: this.config.partitionId,
      dataDir: this.config.dataDir,
      casDir: this.config.casDir,
    });
    const blocks = managed.snapshot.manifest.files.reduce((n, f) => n + f.blocks.length, 0);
    return {
      snapshotId: managed.snapshot.snapshotId,
      files: managed.snapshot.manifest.files.length,
      blocks,
      highWatermark: Object.fromEntries(managed.snapshot.highWatermark),
    };
  }

  private async getCatalog() {
    return await loadStorageCatalogManifest({ db: this.db, groupId: GROUP_ID });
  }

  private async getSnapshotEntries() {
    const entries = await listStorageSnapshotCatalogEntries<CasManifest>({ db: this.db, groupId: GROUP_ID });
    return { entries };
  }

  private async postPullSnapshot(body: unknown) {
    const {
      catalogManifest,
      snapshotEntries,
      sourceCasDir,
      requirement = "bootstrap",
    } = body as {
      catalogManifest: StorageCatalogManifest;
      snapshotEntries: StorageSnapshotCatalogEntry<CasManifest>[];
      sourceCasDir: string;
      requirement?: StoragePullRequirement;
    };

    const storageBinding = catalogManifest.bindings.find((b) => b.partitionId === this.config.partitionId);
    if (!storageBinding) throw new Error(`no binding for partition ${this.config.partitionId}`);

    const wasOnline = this.online;
    if (wasOnline) { await this.syncNode.stop(); this.online = false; }
    await this.db.close();

    const pulled = await pullPartitionCasSnapshot({
      snapshotEntries,
      groupId: GROUP_ID,
      partitionId: this.config.partitionId,
      requirement,
      sourceCasDir,
      targetCasDir: this.config.casDir,
      targetDir: this.config.dataDir,
      storageBinding,
    });

    // PGlite を復元済みデータディレクトリで再起動
    this.db    = new PGlite(this.config.dataDir, { relaxedDurability: false });
    this.inner = new PGlitePartitionStore(this.config.nodeId, this.db);
    await this.inner.migrate();
    this.store = new SensorPartitionStore(this.inner, this.db);

    if (wasOnline) {
      this.syncNode = this.createSyncNode();
      await this.syncNode.start();
      await this.syncNode.subscribe(this.config.partitionId);
      this.online = true;
    }

    return { ok: true, transferredBlocks: pulled.transferredBlocks, snapshotId: pulled.entry.snapshotId };
  }
}

// ─── utility ─────────────────────────────────────────────────────────────────

async function readJson(req: IncomingMessage): Promise<unknown> {
  return new Promise((resolve, reject) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => { try { resolve(data ? JSON.parse(data) : {}); } catch (e) { reject(e); } });
    req.on("error", reject);
  });
}

function returnsRows(sql: string): boolean {
  return /^(select|with|values|show|explain|pragma)\b/i.test(sql.trim());
}

function normalizeRow(row: Record<string, unknown>): Record<string, unknown> {
  return Object.fromEntries(
    Object.entries(row).map(([key, value]) => [key, normalizeValue(value)]),
  );
}

function normalizeValue(value: unknown): unknown {
  if (typeof value === "bigint") {
    return value.toString();
  }
  if (value instanceof Date) {
    return value.toISOString();
  }
  return value;
}

function columnsForRows(rows: Record<string, unknown>[]): string[] {
  const columns = new Set<string>();
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      columns.add(key);
    }
  }
  return [...columns];
}
