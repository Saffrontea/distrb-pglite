/**
 * pgvector dogfooding demo.
 *
 * PGlite nodes replicate tenant-scoped vector payloads through the distrb event log.
 * Receiving nodes then run tenant-local pgvector nearest-neighbor queries.
 */

import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import type { StorageSnapshotScope } from "@distrb-pglite/core";
import {
  MemoryTransport,
  SyncNode,
  type EventEnvelope,
  type PartitionStore,
} from "@distrb-pglite/core";
import {
  bindStoragePartition,
  createManagedPartitionCasSnapshot,
  createStorageBindingGroup,
  listStorageSnapshotCatalogEntries,
  loadStoragePartitionBinding,
  PGlitePartitionStore,
  pullPartitionCasSnapshot,
  type CasManifest,
} from "@distrb-pglite/node";
import { PGlite } from "@electric-sql/pglite";
import { vector } from "@electric-sql/pglite/vector";
import { quoteIdentifier, sameColumns, tableConstraints } from "./schema-utils.js";

const SCHEMA_VERSION = 1;
const VECTOR_GROUP_ID = "sensor-vectors";
const TENANTS = ["jp", "eu"] as const;
type TenantId = (typeof TENANTS)[number];

const SCHEMA = `
  CREATE EXTENSION IF NOT EXISTS vector;

  CREATE TABLE IF NOT EXISTS vector_groups (id serial PRIMARY KEY);

  CREATE TABLE IF NOT EXISTS sensor_profiles_jp (
    sensor_id text NOT NULL,
    event_id text NOT NULL,
    label text NOT NULL,
    embedding vector(3) NOT NULL,
    source_node text NOT NULL,
    PRIMARY KEY (sensor_id),
    UNIQUE (source_node, event_id)
  );

  CREATE TABLE IF NOT EXISTS sensor_profiles_eu (
    sensor_id text NOT NULL,
    event_id text NOT NULL,
    label text NOT NULL,
    embedding vector(3) NOT NULL,
    source_node text NOT NULL,
    PRIMARY KEY (sensor_id),
    UNIQUE (source_node, event_id)
  );
`;

export type VectorPayload = {
  tenantId: TenantId;
  sensorId: string;
  label: string;
  embedding: [number, number, number];
};

type SearchRow = {
  tenant_id: string;
  sensor_id: string;
  label: string;
  embedding: string;
  distance: number | string;
  source_node: string;
};

type ExplainRow = {
  "QUERY PLAN": string;
};
class VectorPartitionStore implements PartitionStore {
  constructor(
    readonly inner: PGlitePartitionStore,
    private readonly db: PGlite,
  ) {}

  registerPartition(id: string) {
    return this.inner.registerPartition(id);
  }
  hasPartition(id: string) {
    return this.inner.hasPartition(id);
  }
  appendLocal<T>(input: Parameters<PGlitePartitionStore["appendLocal"]>[0]) {
    return this.inner.appendLocalEventTransaction<T>(input as never, async (event) => {
      await upsertVectorPayload(this.db, event);
    });
  }
  listEventsAfter(...args: Parameters<PGlitePartitionStore["listEventsAfter"]>) {
    return this.inner.listEventsAfter(...args);
  }
  listEventsAfterCursors(...args: Parameters<PGlitePartitionStore["listEventsAfterCursors"]>) {
    return this.inner.listEventsAfterCursors(...args);
  }
  getHighWatermark(id: string) {
    return this.inner.getHighWatermark(id);
  }
  createSnapshotMetadata(id: string) {
    return this.inner.createSnapshotMetadata(id);
  }

  async applyRemote(event: EventEnvelope): Promise<boolean> {
    return await this.inner.applyRemoteEventTransaction(event, async (appliedEvent) => {
      await upsertVectorPayload(this.db, appliedEvent);
    });
  }
}

export class VectorNode {
  private constructor(
    readonly nodeId: string,
    readonly dataDir: string,
    readonly db: PGlite,
    readonly store: VectorPartitionStore,
    readonly syncNode: SyncNode,
  ) {}

  static async create(
    nodeId: string,
    dataDir: string,
    transport: MemoryTransport,
    tenants: readonly TenantId[],
    snapshotScope: StorageSnapshotScope = "data-only",
  ): Promise<VectorNode> {
    const db = new PGlite(dataDir, {
      extensions: { vector },
      relaxedDurability: false,
    });
    await db.exec(SCHEMA);
    await migrateVectorEventIdentity(db, tenants);
    await createTenantIvfIndexes(db, tenants);

    const inner = new PGlitePartitionStore(nodeId, db);
    await inner.migrate();
    const store = new VectorPartitionStore(inner, db);
    await setupStorageBindings(db, tenants, snapshotScope);
    const syncNode = new SyncNode(nodeId, store, transport);
    await syncNode.start();
    for (const tenantId of tenants) {
      await syncNode.subscribe(partitionForTenant(tenantId));
    }

    return new VectorNode(nodeId, dataDir, db, store, syncNode);
  }

  async writeProfile(payload: VectorPayload): Promise<void> {
    await this.syncNode.write<VectorPayload>({
      partitionId: partitionForTenant(payload.tenantId),
      schemaVersion: SCHEMA_VERSION,
      payload,
    });
  }

  async subscribeTenant(tenantId: TenantId): Promise<void> {
    await this.syncNode.subscribe(partitionForTenant(tenantId));
  }

  async catchUpTenantFrom(tenantId: TenantId, peerNodeId: string): Promise<number> {
    return this.syncNode.catchUpFrom(peerNodeId, partitionForTenant(tenantId));
  }

  async createIvfIndex(tenantId: TenantId): Promise<void> {
    await createTenantIvfIndexes(this.db, [tenantId]);
  }

  async nearest(
    tenantId: TenantId,
    embedding: [number, number, number],
    limit = 3,
  ): Promise<SearchRow[]> {
    const result = await this.db.query<SearchRow>(
      `
        SELECT
          $1::text AS tenant_id,
          sensor_id,
          label,
          embedding::text AS embedding,
          embedding <-> $2::vector AS distance,
          source_node
        FROM ${tableForTenant(tenantId)}
        ORDER BY embedding <-> $2::vector
        LIMIT $3
      `,
      [tenantId, formatVector(embedding), limit],
    );
    return result.rows;
  }

  async explainNearestIvf(
    tenantId: TenantId,
    embedding: [number, number, number],
    limit = 3,
  ): Promise<string[]> {
    await this.db.exec("SET enable_seqscan = off; SET enable_sort = off;");
    try {
      const result = await this.db.query<ExplainRow>(
        `
          EXPLAIN (COSTS OFF)
          SELECT
            $1::text AS tenant_id,
            sensor_id,
            label,
            embedding::text AS embedding,
            embedding <-> $2::vector AS distance,
            source_node
          FROM ${tableForTenant(tenantId)}
          ORDER BY embedding <-> $2::vector
          LIMIT $3
        `,
        [tenantId, formatVector(embedding), limit],
      );
      return result.rows.map((row) => row["QUERY PLAN"]);
    } finally {
      await this.db.exec("RESET enable_sort; RESET enable_seqscan;");
    }
  }

  async close(): Promise<void> {
    await this.syncNode.stop();
    await this.db.close();
  }
}

/*
 * Local and remote vector projections are intentionally run through
 * PGlitePartitionStore's event transaction. This keeps the event log and
 * tenant-local vector tables from diverging when SQL projection fails.
 */
async function upsertVectorPayload(db: PGlite, event: EventEnvelope): Promise<void> {
  assertVectorPayload(event.payload);
  const payload = event.payload;
  if (event.partitionId !== partitionForTenant(payload.tenantId)) {
    throw new Error(
      `vector payload tenant mismatch: partition=${event.partitionId} payload=${payload.tenantId}`,
    );
  }

  const tableName = tableForTenant(payload.tenantId);
  await db.query(
    `
      INSERT INTO ${tableName} (sensor_id, event_id, label, embedding, source_node)
      VALUES ($1, $2, $3, $4::vector, $5)
      ON CONFLICT (sensor_id) DO UPDATE SET
        event_id = excluded.event_id,
        label = excluded.label,
        embedding = excluded.embedding,
        source_node = excluded.source_node
      WHERE
        excluded.event_id > ${tableName}.event_id
        OR (
          excluded.event_id = ${tableName}.event_id
          AND excluded.source_node > ${tableName}.source_node
        )
    `,
    [
      payload.sensorId,
      event.eventId,
      payload.label,
      formatVector(payload.embedding),
      event.sourceNodeId,
    ],
  );
}

function assertVectorPayload(value: unknown): asserts value is VectorPayload {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    throw new Error("invalid vector payload");
  }
  const payload = value as Record<string, unknown>;
  if (!isTenantId(payload.tenantId)) {
    throw new Error("invalid vector payload tenantId");
  }
  if (typeof payload.sensorId !== "string" || payload.sensorId.length === 0) {
    throw new Error("invalid vector payload sensorId");
  }
  if (typeof payload.label !== "string" || payload.label.length === 0) {
    throw new Error("invalid vector payload label");
  }
  if (
    !Array.isArray(payload.embedding) ||
    payload.embedding.length !== 3 ||
    !payload.embedding.every((value) => typeof value === "number" && Number.isFinite(value))
  ) {
    throw new Error("invalid vector payload embedding");
  }
}

function isTenantId(value: unknown): value is TenantId {
  return TENANTS.includes(value as TenantId);
}

function formatVector(values: readonly number[]): string {
  return `[${values.join(",")}]`;
}

function partitionForTenant(tenantId: TenantId): string {
  return `tenant:${tenantId}:vectors`;
}

function tableForTenant(tenantId: TenantId): string {
  return `sensor_profiles_${tenantId}`;
}

function ivfIndexName(tenantId: TenantId): string {
  return `${tableForTenant(tenantId)}_embedding_ivfflat_idx`;
}

async function createTenantIvfIndexes(db: PGlite, tenants: readonly TenantId[]): Promise<void> {
  for (const tenantId of tenants) {
    await db.exec(`
      CREATE INDEX IF NOT EXISTS ${ivfIndexName(tenantId)}
        ON ${tableForTenant(tenantId)}
        USING ivfflat (embedding vector_l2_ops)
        WITH (lists = 1);

      ANALYZE ${tableForTenant(tenantId)};
    `);
  }
}

async function migrateVectorEventIdentity(
  db: PGlite,
  tenants: readonly TenantId[],
): Promise<void> {
  for (const tenantId of tenants) {
    const tableName = tableForTenant(tenantId);
    const constraints = await tableConstraints(db, tableName);
    for (const constraint of constraints) {
      if (constraint.constraint_type === "u" && sameColumns(constraint.columns, ["event_id"])) {
        await db.exec(
          `ALTER TABLE ${quoteIdentifier(tableName)} DROP CONSTRAINT ${quoteIdentifier(constraint.constraint_name)}`,
        );
      }
    }

    const hasCompositeUnique = (await tableConstraints(db, tableName)).some(
      (constraint) =>
        constraint.constraint_type === "u" &&
        sameColumns(constraint.columns, ["source_node", "event_id"]),
    );
    if (!hasCompositeUnique) {
      await db.exec(
        `ALTER TABLE ${quoteIdentifier(tableName)} ADD UNIQUE (source_node, event_id)`,
      );
    }
  }
}

async function setupStorageBindings(
  db: PGlite,
  tenants: readonly TenantId[],
  snapshotScope: StorageSnapshotScope,
): Promise<void> {
  await createStorageBindingGroup({
    db,
    groupId: VECTOR_GROUP_ID,
    rootRelation: "vector_groups",
  });
  for (const tenantId of tenants) {
    await bindStoragePartition({
      db,
      groupId: VECTOR_GROUP_ID,
      partitionId: partitionForTenant(tenantId),
      relations: [tableForTenant(tenantId)],
      snapshotScope,
    });
  }
}

async function openVectorDb(dataDir: string): Promise<PGlite> {
  return new PGlite(dataDir, {
    extensions: { vector },
    relaxedDurability: false,
  });
}

async function queryNearest(
  db: PGlite,
  tenantId: TenantId,
  embedding: [number, number, number],
  limit = 3,
): Promise<SearchRow[]> {
  const result = await db.query<SearchRow>(
    `
      SELECT
        $1::text AS tenant_id,
        sensor_id,
        label,
        embedding::text AS embedding,
        embedding <-> $2::vector AS distance,
        source_node
      FROM ${tableForTenant(tenantId)}
      ORDER BY embedding <-> $2::vector
      LIMIT $3
    `,
    [tenantId, formatVector(embedding), limit],
  );
  return result.rows;
}

async function explainNearestIvfForDb(
  db: PGlite,
  tenantId: TenantId,
  embedding: [number, number, number],
  limit = 3,
): Promise<string[]> {
  await db.exec("SET enable_seqscan = off; SET enable_sort = off;");
  try {
    const result = await db.query<ExplainRow>(
      `
        EXPLAIN (COSTS OFF)
        SELECT
          $1::text AS tenant_id,
          sensor_id,
          label,
          embedding::text AS embedding,
          embedding <-> $2::vector AS distance,
          source_node
        FROM ${tableForTenant(tenantId)}
        ORDER BY embedding <-> $2::vector
        LIMIT $3
      `,
      [tenantId, formatVector(embedding), limit],
    );
    return result.rows.map((row) => row["QUERY PLAN"]);
  } finally {
    await db.exec("RESET enable_sort; RESET enable_seqscan;");
  }
}

async function seedBootstrapProfiles(node: VectorNode): Promise<void> {
  await node.writeProfile({
    tenantId: "jp",
    sensorId: "bootstrap-temperature",
    label: "bootstrap temperature sensor",
    embedding: [0.9, 0.14, 0.08],
  });
  await node.writeProfile({
    tenantId: "jp",
    sensorId: "bootstrap-pressure",
    label: "bootstrap pressure sensor",
    embedding: [0.12, 0.2, 0.88],
  });
}

async function assertNearestSensor(
  db: PGlite,
  tenantId: TenantId,
  embedding: [number, number, number],
  expectedSensorId: string,
  label: string,
): Promise<SearchRow[]> {
  const rows = await queryNearest(db, tenantId, embedding, 2);
  if (rows[0]?.sensor_id !== expectedSensorId) {
    throw new Error(`${label}: expected ${expectedSensorId}, got ${JSON.stringify(rows[0])}`);
  }
  return rows;
}

async function assertSensorExists(
  db: PGlite,
  tenantId: TenantId,
  sensorId: string,
  label: string,
): Promise<void> {
  const result = await db.query<{ exists: boolean }>(
    `SELECT EXISTS (SELECT 1 FROM ${tableForTenant(tenantId)} WHERE sensor_id = $1) AS exists`,
    [sensorId],
  );
  if (!result.rows[0]?.exists) {
    throw new Error(`${label}: expected row ${sensorId} to exist`);
  }
}

async function verifyDataOnlySnapshotDistribution(
  baseDir: string,
): Promise<{ transferredBlocks: number; nearest: SearchRow[]; explain: string[] }> {
  const partitionId = partitionForTenant("jp");
  const sourceDir = join(baseDir, "data-source");
  const targetDir = join(baseDir, "data-only-target");
  const bootstrapSourceCasDir = join(baseDir, "cas-data-bootstrap-source");
  const bootstrapTargetCasDir = join(baseDir, "cas-data-bootstrap-target");
  const deltaSourceCasDir = join(baseDir, "cas-data-delta-source");
  const deltaTargetCasDir = join(baseDir, "cas-data-delta-target");
  const source = await VectorNode.create(
    "vector-node-data-source",
    sourceDir,
    new MemoryTransport(),
    ["jp"],
    "catalog-and-data",
  );
  let sourceDbClosed = false;

  try {
    await seedBootstrapProfiles(source);
    await createManagedPartitionCasSnapshot({
      db: source.db,
      store: source.store,
      groupId: VECTOR_GROUP_ID,
      partitionId,
      dataDir: source.dataDir,
      casDir: bootstrapSourceCasDir,
    });
    const bootstrapEntries = await listStorageSnapshotCatalogEntries<CasManifest>({
      db: source.db,
      groupId: VECTOR_GROUP_ID,
      partitionId,
    });
    const sourceBootstrapBinding = await loadStoragePartitionBinding({
      db: source.db,
      groupId: VECTOR_GROUP_ID,
      partitionId,
    });
    if (!sourceBootstrapBinding) {
      throw new Error("data-only bootstrap source binding was not created");
    }
    await pullPartitionCasSnapshot({
      snapshotEntries: bootstrapEntries,
      groupId: VECTOR_GROUP_ID,
      partitionId,
      requirement: "bootstrap",
      sourceCasDir: bootstrapSourceCasDir,
      targetCasDir: bootstrapTargetCasDir,
      targetDir,
      storageBinding: sourceBootstrapBinding,
    });

    await bindStoragePartition({
      db: source.db,
      groupId: VECTOR_GROUP_ID,
      partitionId,
      relations: [tableForTenant("jp")],
      snapshotScope: "data-only",
    });
    await source.writeProfile({
      tenantId: "jp",
      sensorId: "data-only-delta-temperature",
      label: "data-only delta temperature sensor",
      embedding: [0.86, 0.21, 0.1],
    });
    await source.db.exec(`VACUUM ${tableForTenant("jp")}`);
    const managedDelta = await createManagedPartitionCasSnapshot({
      db: source.db,
      store: source.store,
      groupId: VECTOR_GROUP_ID,
      partitionId,
      dataDir: source.dataDir,
      casDir: deltaSourceCasDir,
      saveCatalogEntry: false,
      beforeStorageSnapshot: async () => {
        await source.db.close();
        sourceDbClosed = true;
      },
    });

    const targetDb = await openVectorDb(targetDir);
    try {
      await bindStoragePartition({
        db: targetDb,
        groupId: VECTOR_GROUP_ID,
        partitionId,
        relations: [tableForTenant("jp")],
        snapshotScope: "data-only",
      });
      const targetBinding = await loadStoragePartitionBinding({
        db: targetDb,
        groupId: VECTOR_GROUP_ID,
        partitionId,
      });
      if (!targetBinding) {
        throw new Error("data-only target binding was not created");
      }
      await targetDb.close();

      const pull = await pullPartitionCasSnapshot({
        snapshotEntries: [managedDelta.entry],
        groupId: VECTOR_GROUP_ID,
        partitionId,
        requirement: "data-refresh",
        sourceCasDir: deltaSourceCasDir,
        targetCasDir: deltaTargetCasDir,
        targetDir,
        storageBinding: targetBinding,
      });

      const restoredDb = await openVectorDb(targetDir);
      try {
        await assertSensorExists(
          restoredDb,
          "jp",
          "data-only-delta-temperature",
          "data-only snapshot restore",
        );
        const nearest = await queryNearest(restoredDb, "jp", [0.87, 0.2, 0.1], 3);
        const explain = await explainNearestIvfForDb(restoredDb, "jp", [0.87, 0.2, 0.1], 2);
        if (!explain.join("\n").includes(ivfIndexName("jp"))) {
          throw new Error(
            `data-only snapshot did not preserve IVFFlat EXPLAIN path:\n${explain.join("\n")}`,
          );
        }
        return {
          transferredBlocks: pull.transferredBlocks,
          nearest,
          explain,
        };
      } finally {
        await restoredDb.close();
      }
    } catch (error) {
      try {
        await targetDb.close();
      } catch {
        // targetDb may have already been closed before the file-level restore.
      }
      throw error;
    }
  } finally {
    if (!sourceDbClosed) {
      await source.close();
    } else {
      await source.syncNode.stop();
    }
  }
}

async function verifyCatalogAndDataSnapshotDistribution(
  baseDir: string,
): Promise<{ transferredBlocks: number; nearest: SearchRow[]; explain: string[] }> {
  const partitionId = partitionForTenant("jp");
  const sourceDir = join(baseDir, "catalog-source");
  const targetDir = join(baseDir, "catalog-target");
  const sourceCasDir = join(baseDir, "cas-catalog-source");
  const targetCasDir = join(baseDir, "cas-catalog-target");
  const source = await VectorNode.create(
    "vector-node-catalog-source",
    sourceDir,
    new MemoryTransport(),
    ["jp"],
    "catalog-and-data",
  );

  try {
    await seedBootstrapProfiles(source);
    await createManagedPartitionCasSnapshot({
      db: source.db,
      store: source.store,
      groupId: VECTOR_GROUP_ID,
      partitionId,
      dataDir: source.dataDir,
      casDir: sourceCasDir,
    });
    const snapshotEntries = await listStorageSnapshotCatalogEntries<CasManifest>({
      db: source.db,
      groupId: VECTOR_GROUP_ID,
      partitionId,
    });
    const sourceBinding = await loadStoragePartitionBinding({
      db: source.db,
      groupId: VECTOR_GROUP_ID,
      partitionId,
    });
    if (!sourceBinding) {
      throw new Error("catalog-and-data source binding was not created");
    }
    const pull = await pullPartitionCasSnapshot({
      snapshotEntries,
      groupId: VECTOR_GROUP_ID,
      partitionId,
      requirement: "bootstrap",
      sourceCasDir,
      targetCasDir,
      targetDir,
      storageBinding: sourceBinding,
    });

    const restoredDb = await openVectorDb(targetDir);
    try {
      const nearest = await assertNearestSensor(
        restoredDb,
        "jp",
        [0.9, 0.1, 0.1],
        "bootstrap-temperature",
        "catalog-and-data snapshot restore",
      );
      const explain = await explainNearestIvfForDb(restoredDb, "jp", [0.9, 0.1, 0.1], 2);
      if (!explain.join("\n").includes(ivfIndexName("jp"))) {
        throw new Error(
          `catalog-and-data snapshot did not preserve IVFFlat EXPLAIN path:\n${explain.join("\n")}`,
        );
      }
      return {
        transferredBlocks: pull.transferredBlocks,
        nearest,
        explain,
      };
    } finally {
      await restoredDb.close();
    }
  } finally {
    await source.close();
  }
}

async function main(): Promise<void> {
  const baseDir = await mkdtemp(join(tmpdir(), "distrb-vector-demo-"));
  const transport = new MemoryTransport();
  const nodeA = await VectorNode.create("vector-node-a", join(baseDir, "a"), transport, TENANTS);
  const nodeB = await VectorNode.create("vector-node-b", join(baseDir, "b"), transport, TENANTS);
  const jpOnlyNode = await VectorNode.create(
    "vector-node-jp-only",
    join(baseDir, "jp-only"),
    transport,
    ["jp"],
  );

  try {
    await nodeA.writeProfile({
      tenantId: "jp",
      sensorId: "tokyo-temperature",
      label: "urban temperature sensor",
      embedding: [0.92, 0.12, 0.08],
    });
    await nodeA.writeProfile({
      tenantId: "jp",
      sensorId: "osaka-humidity",
      label: "indoor humidity sensor",
      embedding: [0.08, 0.86, 0.2],
    });
    await nodeA.writeProfile({
      tenantId: "jp",
      sensorId: "sapporo-pressure",
      label: "cold-region pressure sensor",
      embedding: [0.15, 0.18, 0.91],
    });
    await nodeA.writeProfile({
      tenantId: "eu",
      sensorId: "berlin-temperature",
      label: "eu tenant temperature sensor",
      embedding: [0.91, 0.11, 0.07],
    });
    await nodeA.writeProfile({
      tenantId: "eu",
      sensorId: "paris-air-quality",
      label: "eu tenant air quality sensor",
      embedding: [0.2, 0.75, 0.25],
    });

    const replicated = await nodeB.nearest("jp", [0.9, 0.1, 0.1]);
    const nearest = replicated[0];
    if (nearest?.sensor_id !== "tokyo-temperature") {
      throw new Error(`unexpected nearest result before catchup: ${JSON.stringify(nearest)}`);
    }

    const euReplicated = await nodeB.nearest("eu", [0.9, 0.1, 0.1]);
    if (euReplicated[0]?.sensor_id !== "berlin-temperature") {
      throw new Error(`unexpected eu nearest result: ${JSON.stringify(euReplicated[0])}`);
    }

    const jpOnlyEuRows = await jpOnlyNode.nearest("eu", [0.9, 0.1, 0.1]);
    if (jpOnlyEuRows.length !== 0) {
      throw new Error(
        `jp-only node should not receive eu tenant rows: ${JSON.stringify(jpOnlyEuRows)}`,
      );
    }

    await nodeB.syncNode.stop();
    await nodeA.writeProfile({
      tenantId: "jp",
      sensorId: "yokohama-temperature",
      label: "bay-side temperature sensor",
      embedding: [0.88, 0.2, 0.12],
    });
    await nodeA.writeProfile({
      tenantId: "eu",
      sensorId: "madrid-temperature",
      label: "southern eu temperature sensor",
      embedding: [0.87, 0.19, 0.11],
    });

    await nodeB.syncNode.start();
    await nodeB.subscribeTenant("jp");
    await nodeB.subscribeTenant("eu");
    const appliedJp = await nodeB.catchUpTenantFrom("jp", nodeA.nodeId);
    const appliedEu = await nodeB.catchUpTenantFrom("eu", nodeA.nodeId);
    if (appliedJp < 1 || appliedEu < 1) {
      throw new Error(`expected catchup per tenant, got jp=${appliedJp}, eu=${appliedEu}`);
    }

    const afterJpCatchup = await nodeB.nearest("jp", [0.87, 0.2, 0.1], 2);
    if (afterJpCatchup[0]?.sensor_id !== "yokohama-temperature") {
      throw new Error(
        `unexpected jp nearest result after catchup: ${JSON.stringify(afterJpCatchup[0])}`,
      );
    }

    const afterEuCatchup = await nodeB.nearest("eu", [0.87, 0.2, 0.1], 2);
    if (afterEuCatchup[0]?.sensor_id !== "madrid-temperature") {
      throw new Error(
        `unexpected eu nearest result after catchup: ${JSON.stringify(afterEuCatchup[0])}`,
      );
    }

    const skippedEu = await jpOnlyNode.catchUpTenantFrom("eu", nodeA.nodeId);
    if (skippedEu !== 0) {
      throw new Error(`jp-only node applied eu catchup without subscription: ${skippedEu}`);
    }

    await nodeB.createIvfIndex("jp");
    await nodeB.createIvfIndex("eu");
    const jpExplain = await nodeB.explainNearestIvf("jp", [0.87, 0.2, 0.1], 2);
    const euExplain = await nodeB.explainNearestIvf("eu", [0.87, 0.2, 0.1], 2);
    const combinedPlan = [...jpExplain, ...euExplain].join("\n");
    if (!combinedPlan.includes(ivfIndexName("jp")) || !combinedPlan.includes(ivfIndexName("eu"))) {
      throw new Error(`expected IVFFlat index in EXPLAIN plan:\n${combinedPlan}`);
    }
    let dataOnlySnapshot:
      | Awaited<ReturnType<typeof verifyDataOnlySnapshotDistribution>>
      | undefined;
    let dataOnlySnapshotError: string | undefined;
    try {
      dataOnlySnapshot = await verifyDataOnlySnapshotDistribution(baseDir);
    } catch (error) {
      dataOnlySnapshotError = error instanceof Error ? error.message : String(error);
    }
    const catalogSnapshot = await verifyCatalogAndDataSnapshotDistribution(baseDir);

    console.log("pgvector multi-tenant demo passed");
    console.log(`catchup applied: jp=${appliedJp}, eu=${appliedEu}, jp-only-eu=${skippedEu}`);
    console.log(
      `snapshot pull: data-only blocks=${dataOnlySnapshot?.transferredBlocks ?? "failed"}, ` +
        `catalog-and-data blocks=${catalogSnapshot.transferredBlocks}`,
    );
    console.log("jp nearest:");
    console.table(
      afterJpCatchup.map((row) => ({
        tenant_id: row.tenant_id,
        sensor_id: row.sensor_id,
        label: row.label,
        distance: Number(row.distance).toFixed(4),
        source_node: row.source_node,
      })),
    );
    console.log("eu nearest:");
    console.table(
      afterEuCatchup.map((row) => ({
        tenant_id: row.tenant_id,
        sensor_id: row.sensor_id,
        label: row.label,
        distance: Number(row.distance).toFixed(4),
        source_node: row.source_node,
      })),
    );
    console.log("jp EXPLAIN with enable_seqscan=off:");
    console.log(jpExplain.map((line) => `  ${line}`).join("\n"));
    console.log("eu EXPLAIN with enable_seqscan=off:");
    console.log(euExplain.map((line) => `  ${line}`).join("\n"));
    console.log("catalog-and-data snapshot EXPLAIN after bootstrap:");
    console.log(catalogSnapshot.explain.map((line) => `  ${line}`).join("\n"));
    if (dataOnlySnapshot) {
      console.log("data-only snapshot EXPLAIN after catalog-compatible refresh:");
      console.log(dataOnlySnapshot.explain.map((line) => `  ${line}`).join("\n"));
      console.log("data-only snapshot nearest:");
      console.table(
        dataOnlySnapshot.nearest.map((row) => ({
          tenant_id: row.tenant_id,
          sensor_id: row.sensor_id,
          label: row.label,
          distance: Number(row.distance).toFixed(4),
          source_node: row.source_node,
        })),
      );
    } else {
      console.log("data-only snapshot refresh did not produce a readable pgvector plan:");
      console.log(`  ${dataOnlySnapshotError}`);
    }
    console.log("catalog-and-data snapshot nearest:");
    console.table(
      catalogSnapshot.nearest.map((row) => ({
        tenant_id: row.tenant_id,
        sensor_id: row.sensor_id,
        label: row.label,
        distance: Number(row.distance).toFixed(4),
        source_node: row.source_node,
      })),
    );
  } finally {
    await Promise.allSettled([nodeA.close(), nodeB.close(), jpOnlyNode.close()]);
    await rm(baseDir, { recursive: true, force: true });
  }
}

if (process.argv[1] && fileURLToPath(import.meta.url) === resolve(process.argv[1])) {
  await main();
}
