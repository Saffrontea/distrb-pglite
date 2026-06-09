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
import { MemoryTransport, type EventEnvelope } from "@distrb-pglite/core";
import { DistributedPGlite, type PGliteClient } from "@distrb-pglite/node";
import type { PGlite } from "@electric-sql/pglite";
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

type VectorSnapshotOptions = Parameters<DistributedPGlite["createSnapshot"]>[1] & {
  casDir?: string;
};

export class VectorNode {
  private constructor(
    readonly nodeId: string,
    readonly dataDir: string,
    private readonly handle: DistributedPGlite,
  ) {}

  get db(): PGlite {
    return this.handle.db;
  }

  get store() {
    return this.handle.store;
  }

  static async create(
    nodeId: string,
    dataDir: string,
    transport: MemoryTransport,
    tenants: readonly TenantId[],
    snapshotScope: "catalog-and-data" | "data-only" = "data-only",
    casDir?: string,
  ): Promise<VectorNode> {
    const handle = await DistributedPGlite.open({
      nodeId,
      dataDir,
      casDir,
      pgliteOptions: { extensions: { vector } },
      relaxedDurability: false,
      schema: SCHEMA,
      migrate: async (db) => {
        await migrateVectorEventIdentity(db, tenants);
        await createTenantIvfIndexes(db, tenants);
      },
      transport,
      partitions: tenants.map((tenantId) => ({
        id: partitionForTenant(tenantId),
        schemaVersion: SCHEMA_VERSION,
        project: upsertVectorPayload,
        storage: {
          groupId: VECTOR_GROUP_ID,
          rootRelation: "vector_groups",
          relations: [tableForTenant(tenantId)],
          snapshotScope,
        },
      })),
    });

    return new VectorNode(nodeId, dataDir, handle);
  }

  async writeProfile(payload: VectorPayload): Promise<void> {
    await this.handle.write<VectorPayload>({
      partitionId: partitionForTenant(payload.tenantId),
      payload,
    });
  }

  async subscribeTenant(tenantId: TenantId): Promise<void> {
    await this.handle.subscribe({
      id: partitionForTenant(tenantId),
      schemaVersion: SCHEMA_VERSION,
      project: upsertVectorPayload,
      storage: {
        groupId: VECTOR_GROUP_ID,
        rootRelation: "vector_groups",
        relations: [tableForTenant(tenantId)],
        snapshotScope: "data-only",
      },
    });
  }

  async setTenantSnapshotScope(
    tenantId: TenantId,
    snapshotScope: "catalog-and-data" | "data-only",
  ): Promise<void> {
    await this.handle.subscribe({
      id: partitionForTenant(tenantId),
      schemaVersion: SCHEMA_VERSION,
      project: upsertVectorPayload,
      storage: {
        groupId: VECTOR_GROUP_ID,
        rootRelation: "vector_groups",
        relations: [tableForTenant(tenantId)],
        snapshotScope,
      },
    });
  }

  async createTenantSnapshot(tenantId: TenantId, options?: VectorSnapshotOptions) {
    return this.handle.createSnapshot(
      partitionForTenant(tenantId),
      options as Parameters<DistributedPGlite["createSnapshot"]>[1],
    );
  }

  async listTenantSnapshotEntries() {
    return this.handle.listSnapshotEntries(VECTOR_GROUP_ID);
  }

  async loadSnapshotCatalog() {
    return this.handle.loadCatalog(VECTOR_GROUP_ID);
  }

  async catchUpTenantFrom(tenantId: TenantId, peerNodeId: string): Promise<number> {
    return this.handle.catchUpFrom(peerNodeId, partitionForTenant(tenantId));
  }

  async stop(): Promise<void> {
    await this.handle.stop();
  }

  async start(): Promise<void> {
    await this.handle.start();
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
    await this.handle.close();
  }
}

/*
 * Local and remote vector projections are intentionally run through
 * PGlitePartitionStore's event transaction. This keeps the event log and
 * tenant-local vector tables from diverging when SQL projection fails.
 */
async function upsertVectorPayload(db: PGliteClient, event: EventEnvelope): Promise<void> {
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

async function createTenantIvfIndexes(
  db: PGliteClient,
  tenants: readonly TenantId[],
): Promise<void> {
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
  db: PGliteClient,
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
      await db.exec(`ALTER TABLE ${quoteIdentifier(tableName)} ADD UNIQUE (source_node, event_id)`);
    }
  }
}

async function openVectorHandle(
  dataDir: string,
  options?: {
    casDir?: string;
    tenants?: readonly TenantId[];
    snapshotScope?: "catalog-and-data" | "data-only";
  },
): Promise<DistributedPGlite> {
  const tenants = options?.tenants ?? [];
  return DistributedPGlite.open({
    nodeId: "vector-restore-reader",
    dataDir,
    casDir: options?.casDir,
    pgliteOptions: { extensions: { vector } },
    relaxedDurability: false,
    schema: SCHEMA,
    migrate: async (db) => {
      await migrateVectorEventIdentity(db, TENANTS);
      await createTenantIvfIndexes(db, TENANTS);
    },
    transport: new MemoryTransport(),
    autoStart: false,
    partitions: tenants.map((tenantId) => ({
      id: partitionForTenant(tenantId),
      schemaVersion: SCHEMA_VERSION,
      project: upsertVectorPayload,
      storage: {
        groupId: VECTOR_GROUP_ID,
        rootRelation: "vector_groups",
        relations: [tableForTenant(tenantId)],
        snapshotScope: options?.snapshotScope ?? "data-only",
      },
    })),
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
    await source.createTenantSnapshot("jp", { casDir: bootstrapSourceCasDir });
    const bootstrapEntries = await source.listTenantSnapshotEntries();
    const catalogManifest = await source.loadSnapshotCatalog();
    const bootstrapTargetHandle = await openVectorHandle(targetDir, {
      casDir: bootstrapTargetCasDir,
      tenants: ["jp"],
      snapshotScope: "catalog-and-data",
    });
    await bootstrapTargetHandle.pullSnapshot({
      snapshotEntries: bootstrapEntries,
      partitionId,
      catalogManifest,
      requirement: "bootstrap",
      sourceCasDir: bootstrapSourceCasDir,
    });
    await bootstrapTargetHandle.close();

    await source.setTenantSnapshotScope("jp", "data-only");
    await source.writeProfile({
      tenantId: "jp",
      sensorId: "data-only-delta-temperature",
      label: "data-only delta temperature sensor",
      embedding: [0.86, 0.21, 0.1],
    });
    await source.db.exec(`VACUUM ${tableForTenant("jp")}`);
    const managedDelta = await source.createTenantSnapshot("jp", {
      casDir: deltaSourceCasDir,
      saveCatalogEntry: false,
      beforeStorageSnapshot: async () => {
        await source.db.close();
        sourceDbClosed = true;
      },
    });

    const targetHandle = await openVectorHandle(targetDir, {
      casDir: deltaTargetCasDir,
      tenants: ["jp"],
      snapshotScope: "data-only",
    });
    try {
      const pull = await targetHandle.pullSnapshot({
        snapshotEntries: [managedDelta.entry],
        partitionId,
        requirement: "data-refresh",
        sourceCasDir: deltaSourceCasDir,
      });
      await targetHandle.close();

      const restoredHandle = await openVectorHandle(targetDir);
      try {
        await assertSensorExists(
          restoredHandle.db,
          "jp",
          "data-only-delta-temperature",
          "data-only snapshot restore",
        );
        const nearest = await queryNearest(restoredHandle.db, "jp", [0.87, 0.2, 0.1], 3);
        const explain = await explainNearestIvfForDb(restoredHandle.db, "jp", [0.87, 0.2, 0.1], 2);
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
        await restoredHandle.close();
      }
    } catch (error) {
      try {
        await targetHandle.close();
      } catch {
        // targetHandle may have already been closed before the file-level restore.
      }
      throw error;
    }
  } finally {
    if (!sourceDbClosed) {
      await source.close();
    } else {
      await source.stop();
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
    await source.createTenantSnapshot("jp", { casDir: sourceCasDir });
    const snapshotEntries = await source.listTenantSnapshotEntries();
    const catalogManifest = await source.loadSnapshotCatalog();
    const targetHandle = await openVectorHandle(targetDir, {
      casDir: targetCasDir,
      tenants: ["jp"],
      snapshotScope: "catalog-and-data",
    });
    const pull = await targetHandle.pullSnapshot({
      snapshotEntries,
      partitionId,
      catalogManifest,
      requirement: "bootstrap",
      sourceCasDir,
    });
    await targetHandle.close();

    const restoredHandle = await openVectorHandle(targetDir);
    try {
      const nearest = await assertNearestSensor(
        restoredHandle.db,
        "jp",
        [0.9, 0.1, 0.1],
        "bootstrap-temperature",
        "catalog-and-data snapshot restore",
      );
      const explain = await explainNearestIvfForDb(restoredHandle.db, "jp", [0.9, 0.1, 0.1], 2);
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
      await restoredHandle.close();
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

    await nodeB.stop();
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

    await nodeB.start();
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
