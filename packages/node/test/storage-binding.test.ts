import { PGlite } from "@electric-sql/pglite";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  applyStorageCatalogManifest,
  bindStoragePartition,
  clusterStoragePartition,
  createStorageBindingGroup,
  createPartitionCasSnapshot,
  detectStorageBindingDrift,
  discoverStoragePartitionBinding,
  loadStorageBindingGroup,
  loadStorageCatalogManifest,
  loadStoragePartitionBinding,
  loadStorageSnapshotCatalogEntry,
  loadStorageSnapshotCatalogEntryForPull,
  listStorageSnapshotCatalogEntries,
  migrateStorageBindingCatalog,
  PGlitePartitionStore,
  planStorageGcFromCatalog,
  planStorageSnapshotFence,
  prepareStorageSnapshotFence,
  reindexStoragePartition,
  refreshStorageBindingGeneration,
  runStorageCasAutoGcFromCatalog,
  runStorageCasGcFromCatalog,
  savePartitionCasSnapshotCatalogEntry,
  saveStorageBindingGroup,
  saveStorageSnapshotCatalogEntry,
  truncateStoragePartition,
  vacuumStoragePartition,
  vacuumFullStoragePartition,
} from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("discoverStoragePartitionBinding", () => {
  it("binds a partition to PGlite catalog relation identity and matching files", async () => {
    const root = await makeTempDir("storage-binding");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");

    const db = new PGlite(dataDir);
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await db.query(
      "INSERT INTO sensor_readings (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    await store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });

    const binding = await discoverStoragePartitionBinding({
      db,
      partitionId: "tenant:jp",
      rootRelation: "public.sensor_readings",
      snapshotScope: "data-only",
    });

    expect(binding.partitionId).toBe("tenant:jp");
    expect(binding.rootRelationOid).toBeGreaterThan(0);
    expect(binding.relations).toHaveLength(1);
    expect(binding.relations[0]).toMatchObject({
      databaseOid: expect.any(Number),
      tablespaceOid: expect.any(Number),
      relationOid: binding.rootRelationOid,
      relfilenode: expect.any(Number),
      storagePath: expect.stringMatching(/^base\/\d+\/\d+/),
    });

    const snapshot = await createPartitionCasSnapshot({
      store,
      partitionId: "tenant:jp",
      storageBinding: binding,
      dataDir,
      casDir,
    });
    await db.close();

    expect(snapshot.manifest.files.length).toBeGreaterThan(0);
    expect(
      snapshot.manifest.files.some((file) =>
        file.path.startsWith(binding.relations[0]?.storagePath ?? "missing"),
      ),
    ).toBe(true);
    expect(snapshot.manifest.files.some((file) => file.path.startsWith("pg_xact/"))).toBe(true);
  }, 120_000);

  it("rejects unknown relations", async () => {
    const root = await makeTempDir("storage-binding-missing");
    const db = new PGlite(join(root, "data"));

    await expect(
      discoverStoragePartitionBinding({
        db,
        partitionId: "tenant:jp",
        rootRelation: "public.missing_relation",
        snapshotScope: "data-only",
      }),
    ).rejects.toThrow("relation not found");
    await db.close();
  }, 120_000);

  it("persists binding groups and reloads bindings across PGlite connections", async () => {
    const root = await makeTempDir("storage-binding-catalog");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");

    let db = new PGlite(dataDir);
    let store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await db.query(
      "INSERT INTO sensor_readings (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    await store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });

    const binding = await discoverStoragePartitionBinding({
      db,
      partitionId: "tenant:jp",
      rootRelation: "public.sensor_readings",
      snapshotScope: "data-only",
    });
    await saveStorageBindingGroup(db, {
      groupId: "sensor_readings",
      rootRelationOid: binding.rootRelationOid,
      ddlGeneration: 1,
      vacuumPolicy: {
        retainSnapshotGenerations: 2,
        gcAfterGeneration: 5,
      },
      bindings: [
        {
          ...binding,
          ddlGeneration: 1,
          snapshotComponents: ["data"],
          pullRequires: ["data"],
        },
      ],
    });
    await db.close();

    db = new PGlite(dataDir);
    store = new PGlitePartitionStore("node-a", db);
    await store.migrate();

    const group = await loadStorageBindingGroup(db, "sensor_readings");
    expect(group).toMatchObject({
      groupId: "sensor_readings",
      rootRelationOid: binding.rootRelationOid,
      ddlGeneration: 1,
      vacuumPolicy: {
        retainSnapshotGenerations: 2,
        gcAfterGeneration: 5,
      },
    });
    expect(group?.bindings).toHaveLength(1);

    const loadedBinding = await loadStoragePartitionBinding({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    expect(loadedBinding).toMatchObject({
      partitionId: "tenant:jp",
      rootRelationOid: binding.rootRelationOid,
      ddlGeneration: 1,
      snapshotComponents: ["data"],
      pullRequires: ["data"],
    });

    const snapshot = await createPartitionCasSnapshot({
      store,
      partitionId: "tenant:jp",
      storageBinding: loadedBinding!,
      dataDir,
      casDir,
    });
    await savePartitionCasSnapshotCatalogEntry({
      db,
      groupId: "sensor_readings",
      snapshot,
    });
    const persistedSnapshot = await loadStorageSnapshotCatalogEntry({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: snapshot.snapshotId,
    });
    await db.close();

    expect(snapshot.storageBinding).toEqual(loadedBinding);
    expect(snapshot.manifest.files.length).toBeGreaterThan(0);
    expect(persistedSnapshot).toMatchObject({
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: snapshot.snapshotId,
      components: ["data"],
      ddlGeneration: 1,
      manifestKind: "cas",
      manifest: snapshot.manifest,
    });
    expect(persistedSnapshot?.highWatermark).toEqual(snapshot.highWatermark);
  }, 120_000);

  it("simulates extension DDL by grouping child relations under one root relation", async () => {
    const root = await makeTempDir("storage-binding-ddl");
    const dataDir = join(root, "data");
    const db = new PGlite(dataDir);

    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_us (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);

    const created = await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
      vacuumPolicy: {
        retainSnapshotGenerations: 3,
        gcAfterGeneration: 8,
      },
    });
    expect(created.ddlGeneration).toBe(1);
    expect(created.bindings).toEqual([]);

    const withJp = await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    expect(withJp.ddlGeneration).toBe(2);
    expect(withJp.bindings).toHaveLength(1);
    expect(withJp.bindings[0]).toMatchObject({
      partitionId: "tenant:jp",
      rootRelationOid: created.rootRelationOid,
      ddlGeneration: 2,
      snapshotComponents: ["data"],
      pullRequires: ["data"],
    });
    expect(withJp.bindings[0]?.boundChildRelationOids).toEqual([
      withJp.bindings[0]?.relations[0]?.relationOid,
    ]);
    expect(withJp.bindings[0]?.relations[0]).toMatchObject({
      role: "child",
      parentRelationOid: created.rootRelationOid,
      storagePath: expect.stringMatching(/^base\/\d+\/\d+/),
    });

    const withUs = await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:us",
      relations: ["public.sensor_readings_tenant_us"],
      snapshotScope: "catalog-and-data",
      snapshotComponents: ["catalog", "data"],
      pullRequires: ["catalog", "data"],
    });
    expect(withUs.ddlGeneration).toBe(3);
    expect(withUs.bindings.map((binding) => binding.partitionId)).toEqual([
      "tenant:jp",
      "tenant:us",
    ]);

    await db.close();

    const reopened = new PGlite(dataDir);
    const reloaded = await loadStorageBindingGroup(reopened, "sensor_readings");
    await reopened.close();

    expect(reloaded?.ddlGeneration).toBe(3);
    expect(reloaded?.vacuumPolicy).toEqual({
      retainSnapshotGenerations: 3,
      gcAfterGeneration: 8,
    });
    expect(reloaded?.bindings).toHaveLength(2);
    expect(
      reloaded?.bindings.every((binding) => binding.boundChildRelationOids?.length === 1),
    ).toBe(true);
    expect(
      reloaded?.bindings.every((binding) =>
        binding.relations.some(
          (relation) =>
            relation.role === "child" && relation.parentRelationOid === created.rootRelationOid,
        ),
      ),
    ).toBe(true);
  }, 120_000);

  it("exposes SQL DDL and DML for binding relation groups", async () => {
    const root = await makeTempDir("storage-binding-sql");
    const dataDir = join(root, "data");
    const db = new PGlite(dataDir);

    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE INDEX sensor_readings_tenant_jp_sensor_id
        ON sensor_readings_tenant_jp (sensor_id);
    `);
    await migrateStorageBindingCatalog(db);

    await db.query(
      `
        SELECT distrb_create_binding_group(
          $1,
          $2::regclass,
          $3::jsonb
        )
      `,
      [
        "sensor_readings",
        "public.sensor_readings",
        JSON.stringify({ retainSnapshotGenerations: 2, gcAfterGeneration: 7 }),
      ],
    );
    await db.query(
      `
        SELECT distrb_bind_partition(
          $1,
          $2,
          $3::regclass[],
          $4,
          $5::text[],
          $6::text[]
        )
      `,
      [
        "sensor_readings",
        "tenant:jp",
        ["public.sensor_readings_tenant_jp"],
        "catalog-and-data",
        ["catalog", "data"],
        ["data"],
      ],
    );
    await db.query(
      `
        UPDATE distrb_storage_bindings
        SET binding = jsonb_set(binding, '{relations}', '[]'::jsonb)
        WHERE group_id = $1
          AND partition_id = $2
      `,
      ["sensor_readings", "tenant:jp"],
    );

    const group = await loadStorageBindingGroup(db, "sensor_readings");
    const relationRows = await db.query<{
      partition_id: string;
      relation_ordinal: number;
      relation_role: string;
      parent_relation_oid: number;
      storage_path: string;
      relation_oid: number;
    }>(
      `
        SELECT
          partition_id,
          relation_ordinal,
          relation_role,
          parent_relation_oid,
          storage_path,
          relation_oid
        FROM distrb_storage_binding_relations
        WHERE group_id = $1
        ORDER BY relation_ordinal
      `,
      ["sensor_readings"],
    );
    const lineageRows = await db.query<{ relation_oid: number; ddl_generation: number }>(
      `
        SELECT relation_oid, ddl_generation
        FROM distrb_storage_relation_lineage
        WHERE group_id = $1
          AND partition_id = $2
        ORDER BY relation_oid
      `,
      ["sensor_readings", "tenant:jp"],
    );
    await db.close();

    expect(group).toMatchObject({
      groupId: "sensor_readings",
      ddlGeneration: 2,
      vacuumPolicy: {
        retainSnapshotGenerations: 2,
        gcAfterGeneration: 7,
      },
    });
    expect(group?.bindings).toHaveLength(1);
    expect(group?.bindings[0]).toMatchObject({
      partitionId: "tenant:jp",
      snapshotScope: "catalog-and-data",
      snapshotComponents: ["catalog", "data"],
      pullRequires: ["data"],
      ddlGeneration: 2,
    });
    expect(group?.bindings[0]?.relations.length).toBeGreaterThanOrEqual(2);
    expect(relationRows.rows.length).toBeGreaterThanOrEqual(2);
    expect(relationRows.rows.map((row) => row.relation_ordinal)).toEqual(
      relationRows.rows.map((_, index) => index),
    );
    expect(relationRows.rows.map((row) => row.relation_role)).toEqual(
      expect.arrayContaining(["child", "index"]),
    );
    expect(relationRows.rows[0]).toMatchObject({
      parent_relation_oid: group?.rootRelationOid,
      storage_path: expect.stringMatching(/^base\//),
    });
    expect(relationRows.rows[1]).toMatchObject({
      parent_relation_oid: relationRows.rows[0]?.relation_oid,
      storage_path: expect.stringMatching(/^base\//),
    });
    expect(lineageRows.rows).toHaveLength(relationRows.rows.length);
    expect(lineageRows.rows.every((row) => row.ddl_generation === 2)).toBe(true);
  }, 120_000);

  it("creates root and bound tables through the PGlite SQL interface", async () => {
    const root = await makeTempDir("storage-binding-sql-create");
    const dataDir = join(root, "data");
    const db = new PGlite(dataDir);
    await migrateStorageBindingCatalog(db);

    await db.query(
      `
        SELECT distrb_create_root_table(
          $1,
          $2,
          $3,
          $4::jsonb
        )
      `,
      [
        "sensor_readings",
        "public.sensor_readings",
        "tenant_id text NOT NULL, sensor_id text NOT NULL, reading integer NOT NULL",
        JSON.stringify({ retainSnapshotGenerations: 2, gcAfterGeneration: 7 }),
      ],
    );
    await db.query(
      `
        SELECT distrb_create_bound_table(
          $1,
          $2,
          $3,
          $4,
          $5,
          $6::text[],
          $7::text[]
        )
      `,
      [
        "sensor_readings",
        "tenant:jp",
        "public.sensor_readings_tenant_jp",
        "LIKE public.sensor_readings INCLUDING ALL",
        "catalog-and-data",
        ["catalog", "data"],
        ["data"],
      ],
    );
    await db.query(
      "INSERT INTO sensor_readings_tenant_jp (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    await db.query(
      `
        SELECT distrb_snapshot_policy(
          $1,
          $2,
          $3,
          $4::text[],
          $5::text[],
          $6::jsonb
        )
      `,
      [
        "sensor_readings",
        "tenant:jp",
        "data-only",
        ["data"],
        ["data"],
        JSON.stringify({ retainSnapshotGenerations: 4, gcAfterGeneration: 12 }),
      ],
    );

    const tables = await db.query<{ root_exists: string; child_exists: string }>(
      `
        SELECT
          to_regclass('public.sensor_readings')::text AS root_exists,
          to_regclass('public.sensor_readings_tenant_jp')::text AS child_exists
      `,
    );
    const group = await loadStorageBindingGroup(db, "sensor_readings");
    const row = await db.query<{ reading: number }>(
      "SELECT reading FROM sensor_readings_tenant_jp WHERE tenant_id = $1",
      ["tenant:jp"],
    );
    await db.close();

    expect(tables.rows[0]).toEqual({
      root_exists: "sensor_readings",
      child_exists: "sensor_readings_tenant_jp",
    });
    expect(row.rows[0]?.reading).toBe(42);
    expect(group).toMatchObject({
      groupId: "sensor_readings",
      ddlGeneration: 3,
      vacuumPolicy: {
        retainSnapshotGenerations: 4,
        gcAfterGeneration: 12,
      },
    });
    expect(group?.bindings[0]).toMatchObject({
      partitionId: "tenant:jp",
      snapshotScope: "data-only",
      snapshotComponents: ["data"],
      pullRequires: ["data"],
      ddlGeneration: 3,
    });
    expect(group?.bindings[0]?.relations[0]).toMatchObject({
      parentRelationOid: group.rootRelationOid,
      storagePath: expect.stringMatching(/^base\/\d+\/\d+/),
    });
  }, 120_000);

  it("plans and applies SQL-backed snapshot fences for data-only bindings", async () => {
    const root = await makeTempDir("storage-binding-fence");
    const dataDir = join(root, "data");
    const db = new PGlite(dataDir);
    await migrateStorageBindingCatalog(db);

    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE INDEX sensor_readings_tenant_jp_sensor_id
        ON sensor_readings_tenant_jp (sensor_id);
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    await db.query(
      "INSERT INTO sensor_readings_tenant_jp (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );

    const planned = await planStorageSnapshotFence({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const applied = await prepareStorageSnapshotFence({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    await db.close();

    expect(planned).toEqual([
      expect.objectContaining({
        partitionId: "tenant:jp",
        relationOrdinal: 0,
        relationName: "sensor_readings_tenant_jp",
        relationRole: "child",
      }),
    ]);
    expect(applied).toEqual(planned);
  }, 120_000);

  it("detects closure drift from bound child relation metadata and refreshes generation", async () => {
    const root = await makeTempDir("storage-binding-drift");
    const dataDir = join(root, "data");
    const db = new PGlite(dataDir);
    await migrateStorageBindingCatalog(db);

    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    const before = await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    expect(before.bindings[0]?.relations.map((relation) => relation.role)).toEqual(
      expect.arrayContaining(["child", "toast", "data-catalog", "index"]),
    );

    await db.exec(
      "CREATE INDEX sensor_readings_tenant_jp_sensor_id ON sensor_readings_tenant_jp (sensor_id)",
    );
    const drift = await detectStorageBindingDrift({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const refreshed = await refreshStorageBindingGeneration({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const afterDrift = await detectStorageBindingDrift({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const lineageRows = await db.query<{ relation_role: string; ddl_generation: number }>(
      `
        SELECT relation_role, ddl_generation
        FROM distrb_storage_relation_lineage
        WHERE group_id = $1
          AND partition_id = $2
        ORDER BY ddl_generation, relation_role
      `,
      ["sensor_readings", "tenant:jp"],
    );
    await db.close();

    expect(drift).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          partitionId: "tenant:jp",
          currentRelation: expect.objectContaining({ role: "index" }),
        }),
      ]),
    );
    expect(refreshed.ddlGeneration).toBe(3);
    expect(refreshed.bindings[0]).toMatchObject({
      ddlGeneration: 3,
      boundChildRelationOids: before.bindings[0]?.boundChildRelationOids,
    });
    expect(refreshed.bindings[0]?.relations.map((relation) => relation.role)).toEqual(
      expect.arrayContaining(["child", "index", "toast", "data-catalog"]),
    );
    expect(afterDrift).toEqual([]);
    expect(lineageRows.rows).toEqual(
      expect.arrayContaining([
        { relation_role: "child", ddl_generation: 2 },
        { relation_role: "child", ddl_generation: 3 },
        { relation_role: "data-catalog", ddl_generation: 2 },
        { relation_role: "data-catalog", ddl_generation: 3 },
        { relation_role: "index", ddl_generation: 3 },
      ]),
    );
  }, 120_000);

  it("detects ALTER TABLE schema drift even when relation storage is unchanged", async () => {
    const root = await makeTempDir("storage-binding-alter-drift");
    const db = new PGlite(join(root, "data"));
    await migrateStorageBindingCatalog(db);

    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });

    await db.exec("ALTER TABLE sensor_readings_tenant_jp ADD COLUMN battery_level integer");
    const drift = await detectStorageBindingDrift({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const refreshed = await refreshStorageBindingGeneration({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const afterDrift = await detectStorageBindingDrift({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    await db.close();

    expect(drift).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          driftKind: "changed",
          currentRelation: expect.objectContaining({ role: "child" }),
        }),
      ]),
    );
    expect(refreshed.ddlGeneration).toBe(3);
    expect(refreshed.storageGeneration).toBe(3);
    expect(refreshed.bindings[0]).toMatchObject({ ddlGeneration: 3, storageGeneration: 3 });
    expect(afterDrift).toEqual([]);
  }, 120_000);

  it("treats TRUNCATE control API rewrites as storage drift without advancing DDL generation", async () => {
    const root = await makeTempDir("storage-binding-truncate-drift");
    const db = new PGlite(join(root, "data"));
    await migrateStorageBindingCatalog(db);

    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      INSERT INTO sensor_readings_tenant_jp (tenant_id, sensor_id, reading)
      VALUES ('tenant:jp', 'tokyo-1', 42);
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    const before = await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    const beforeChild = before.bindings[0]?.relations.find((relation) => relation.role === "child");

    const control = await truncateStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const drift = await detectStorageBindingDrift({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const refreshed = control.group;
    const afterChild = refreshed.bindings[0]?.relations.find(
      (relation) => relation.role === "child",
    );
    const afterDrift = await detectStorageBindingDrift({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    await db.close();

    expect(control).toMatchObject({
      operation: "truncate",
      relationNames: ["public.sensor_readings_tenant_jp"],
    });
    expect(drift).toEqual([]);
    expect(afterChild?.relfilenode).not.toBe(beforeChild?.relfilenode);
    expect(refreshed.ddlGeneration).toBe(2);
    expect(refreshed.storageGeneration).toBe(3);
    expect(refreshed.bindings[0]).toMatchObject({ ddlGeneration: 2, storageGeneration: 3 });
    expect(afterDrift).toEqual([]);
  }, 120_000);

  it("treats storage rewrite control APIs as storage generation changes", async () => {
    const root = await makeTempDir("storage-binding-rewrite-controls");
    const db = new PGlite(join(root, "data"));
    await migrateStorageBindingCatalog(db);

    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      INSERT INTO sensor_readings_tenant_jp (tenant_id, sensor_id, reading)
      VALUES ('tenant:jp', 'tokyo-1', 42);
      CREATE INDEX sensor_readings_tenant_jp_sensor_id
      ON sensor_readings_tenant_jp (sensor_id);
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });

    const vacuumedPlain = await vacuumStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const reindexed = await reindexStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const vacuumed = await vacuumFullStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const clustered = await clusterStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      clusterIndexByRelation: {
        "public.sensor_readings_tenant_jp": "sensor_readings_tenant_jp_sensor_id",
      },
    });
    await db.close();

    expect(vacuumedPlain.group).toMatchObject({ ddlGeneration: 2, storageGeneration: 2 });
    expect(reindexed.group).toMatchObject({ ddlGeneration: 2, storageGeneration: 3 });
    expect(vacuumed.group).toMatchObject({ ddlGeneration: 2, storageGeneration: 4 });
    expect(clustered.group).toMatchObject({ ddlGeneration: 2, storageGeneration: 5 });
    expect(clustered.group.bindings[0]).toMatchObject({ ddlGeneration: 2, storageGeneration: 5 });
  }, 120_000);

  it("lists persisted snapshot catalog entries by group and partition", async () => {
    const root = await makeTempDir("storage-snapshot-catalog");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    const db = new PGlite(dataDir);
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_us (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
      vacuumPolicy: {
        retainSnapshotGenerations: 1,
        gcAfterGeneration: 9,
      },
    });

    const withJp = await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    const withUs = await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:us",
      relations: ["public.sensor_readings_tenant_us"],
      snapshotScope: "data-only",
    });

    const jpBinding = withJp.bindings.find((binding) => binding.partitionId === "tenant:jp")!;
    const usBinding = withUs.bindings.find((binding) => binding.partitionId === "tenant:us")!;
    for (const partitionId of ["tenant:jp", "tenant:us"]) {
      await store.appendLocal({
        partitionId,
        schemaVersion: 1,
        payload: { sensorId: partitionId, reading: 42 },
      });
    }

    const jpSnapshot = await createPartitionCasSnapshot({
      store,
      partitionId: "tenant:jp",
      storageBinding: jpBinding,
      dataDir,
      casDir,
    });
    const usSnapshot = await createPartitionCasSnapshot({
      store,
      partitionId: "tenant:us",
      storageBinding: usBinding,
      dataDir,
      casDir,
    });
    await savePartitionCasSnapshotCatalogEntry({
      db,
      groupId: "sensor_readings",
      snapshot: jpSnapshot,
    });
    await savePartitionCasSnapshotCatalogEntry({
      db,
      groupId: "sensor_readings",
      snapshot: usSnapshot,
    });

    const allEntries = await listStorageSnapshotCatalogEntries({
      db,
      groupId: "sensor_readings",
    });
    const jpEntries = await listStorageSnapshotCatalogEntries({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const dataPullEntry = await loadStorageSnapshotCatalogEntryForPull({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      requirement: "data-refresh",
    });
    const bootstrapEntry = await loadStorageSnapshotCatalogEntryForPull({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      requirement: "bootstrap",
    });
    const retainedGenerations = await db.query<{ ddl_generation: number }>(
      `
        SELECT ddl_generation
        FROM distrb_retained_snapshot_generations($1)
      `,
      ["sensor_readings"],
    );
    const gcPlan = await planStorageGcFromCatalog({
      db,
      groupId: "sensor_readings",
      activePullGenerations: [2],
    });
    const catalogManifest = await loadStorageCatalogManifest({
      db,
      groupId: "sensor_readings",
    });
    await db.close();

    expect(allEntries.map((entry) => entry.partitionId)).toEqual(["tenant:jp", "tenant:us"]);
    expect(jpEntries).toHaveLength(1);
    expect(jpEntries[0]).toMatchObject({
      partitionId: "tenant:jp",
      snapshotId: jpSnapshot.snapshotId,
      ddlGeneration: jpBinding.ddlGeneration,
      components: ["data"],
    });
    expect(dataPullEntry?.snapshotId).toBe(jpSnapshot.snapshotId);
    expect(bootstrapEntry).toBeUndefined();
    expect(retainedGenerations.rows.map((row) => row.ddl_generation)).toEqual([2, 3]);
    expect(gcPlan).toMatchObject({
      ddlGeneration: 3,
      retainFromGeneration: 3,
      protectedGenerations: [2, 3],
      gcAllowedThroughGeneration: 1,
    });
    expect(catalogManifest).toMatchObject({
      version: 1,
      groupId: "sensor_readings",
      ddlGeneration: 3,
      snapshots: [
        expect.objectContaining({ partitionId: "tenant:jp", manifestKind: "cas" }),
        expect.objectContaining({ partitionId: "tenant:us", manifestKind: "cas" }),
      ],
    });
    expect(catalogManifest?.bindings.map((binding) => binding.partitionId)).toEqual([
      "tenant:jp",
      "tenant:us",
    ]);
    expect(
      catalogManifest?.bindings.every(
        (binding) =>
          binding.relations.some((relation) => relation.role === "child") &&
          binding.relations.some((relation) => relation.role === "toast"),
      ),
    ).toBe(true);
  }, 120_000);

  it("applies pulled storage catalog manifests to a local catalog", async () => {
    const sourceRoot = await makeTempDir("storage-catalog-source");
    const targetRoot = await makeTempDir("storage-catalog-target");
    const sourceDb = new PGlite(join(sourceRoot, "data"));
    const targetDb = new PGlite(join(targetRoot, "data"));

    await sourceDb.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await createStorageBindingGroup({
      db: sourceDb,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
      vacuumPolicy: { retainSnapshotGenerations: 2, gcAfterGeneration: 9 },
    });
    await bindStoragePartition({
      db: sourceDb,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "catalog-and-data",
      snapshotComponents: ["catalog", "data"],
      pullRequires: ["catalog", "data"],
    });

    const manifest = await loadStorageCatalogManifest({
      db: sourceDb,
      groupId: "sensor_readings",
    });
    const applied = await applyStorageCatalogManifest({
      db: targetDb,
      manifest: manifest!,
    });
    const loaded = await loadStorageBindingGroup(targetDb, "sensor_readings");
    await sourceDb.close();
    await targetDb.close();

    expect(applied).toEqual(loaded);
    expect(loaded).toMatchObject({
      groupId: "sensor_readings",
      ddlGeneration: 2,
      vacuumPolicy: { retainSnapshotGenerations: 2, gcAfterGeneration: 9 },
    });
    expect(loaded?.bindings).toHaveLength(1);
    expect(loaded?.bindings[0]).toMatchObject({
      partitionId: "tenant:jp",
      boundChildRelationOids: manifest?.bindings[0]?.boundChildRelationOids,
      snapshotScope: "catalog-and-data",
      snapshotComponents: ["catalog", "data"],
      pullRequires: ["catalog", "data"],
    });
  }, 120_000);

  it("rejects pulled storage catalog manifests with invalid snapshot summaries", async () => {
    const root = await makeTempDir("storage-catalog-invalid-snapshot-summary");
    const db = new PGlite(join(root, "data"));
    const binding = {
      partitionId: "tenant:jp",
      rootRelationOid: 1,
      relations: [storageRelation(1, "jp.bin")],
      snapshotScope: "data-only" as const,
    };
    const manifest = {
      version: 1 as const,
      groupId: "sensor_readings",
      rootRelationOid: 1,
      ddlGeneration: 1,
      bindings: [binding],
      snapshots: [
        {
          partitionId: "tenant:jp",
          snapshotId: "snapshot-1",
          components: ["data" as const],
          ddlGeneration: 1,
          manifestKind: "cas" as const,
        },
      ],
    };

    await expect(
      applyStorageCatalogManifest({
        db,
        manifest: {
          ...manifest,
          snapshots: [
            {
              ...manifest.snapshots[0]!,
              components: ["indexes" as never],
            },
          ],
        },
      }),
    ).rejects.toThrow("invalid snapshot catalog component: indexes");
    await expect(
      applyStorageCatalogManifest({
        db,
        manifest: {
          ...manifest,
          snapshots: [
            {
              ...manifest.snapshots[0]!,
              partitionId: "tenant:missing",
            },
          ],
        },
      }),
    ).rejects.toThrow("storage catalog snapshot references unknown partition: tenant:missing");
    await expect(
      applyStorageCatalogManifest({
        db,
        manifest: {
          ...manifest,
          snapshots: [
            {
              ...manifest.snapshots[0]!,
              manifestKind: "tarball" as never,
            },
          ],
        },
      }),
    ).rejects.toThrow("invalid storage catalog snapshot manifest kind: tarball");
    await expect(
      applyStorageCatalogManifest({
        db,
        manifest: {
          ...manifest,
          snapshots: [
            {
              ...manifest.snapshots[0]!,
              ddlGeneration: -1,
            },
          ],
        },
      }),
    ).rejects.toThrow("storage catalog snapshot ddlGeneration must be a non-negative integer");

    await db.close();
  });

  it("rejects invalid storage binding groups before saving", async () => {
    const root = await makeTempDir("storage-binding-invalid-save");
    const db = new PGlite(join(root, "data"));
    const validBinding = {
      partitionId: "tenant:jp",
      rootRelationOid: 1,
      relations: [storageRelation(1, "jp.bin")],
      snapshotScope: "data-only" as const,
    };

    await expect(
      saveStorageBindingGroup(db, {
        groupId: "sensor_readings",
        rootRelationOid: 1,
        ddlGeneration: 1,
        bindings: [validBinding, { ...validBinding }],
      }),
    ).rejects.toThrow("duplicate storage binding partitionId: tenant:jp");
    await expect(
      saveStorageBindingGroup(db, {
        groupId: "sensor_readings",
        rootRelationOid: 1,
        ddlGeneration: 1,
        bindings: [
          {
            ...validBinding,
            snapshotScope: "delta" as never,
          },
        ],
      }),
    ).rejects.toThrow("invalid storage binding snapshot scope: delta");
    await expect(
      saveStorageBindingGroup(db, {
        groupId: "sensor_readings",
        rootRelationOid: 1,
        ddlGeneration: 1,
        bindings: [
          {
            ...validBinding,
            relations: [],
          },
        ],
      }),
    ).rejects.toThrow("storage binding relations must be a non-empty array");
    await expect(
      saveStorageBindingGroup(db, {
        groupId: "sensor_readings",
        rootRelationOid: 1,
        ddlGeneration: -1,
        bindings: [validBinding],
      }),
    ).rejects.toThrow("storage binding ddlGeneration must be a non-negative integer");

    await db.close();
  });

  it("rejects invalid storage bindings when loading from the catalog", async () => {
    const root = await makeTempDir("storage-binding-invalid-load");
    const db = new PGlite(join(root, "data"));

    await saveStorageBindingGroup(db, {
      groupId: "sensor_readings",
      rootRelationOid: 1,
      ddlGeneration: 1,
      bindings: [
        {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          relations: [storageRelation(1, "jp.bin")],
          snapshotScope: "data-only",
        },
      ],
    });
    await db.query(
      `
        UPDATE distrb_storage_bindings
        SET binding = jsonb_set(binding, '{snapshotScope}', '"delta"'::jsonb)
        WHERE group_id = $1
          AND partition_id = $2
      `,
      ["sensor_readings", "tenant:jp"],
    );

    await expect(loadStorageBindingGroup(db, "sensor_readings")).rejects.toThrow(
      "invalid storage binding snapshot scope: delta",
    );

    await db.close();
  });

  it("preserves local snapshot catalog entries when applying a pulled storage catalog manifest", async () => {
    const root = await makeTempDir("storage-catalog-preserve-snapshots");
    const db = new PGlite(join(root, "data"));
    const oldHash = "a".repeat(64);

    await saveStorageBindingGroup(db, {
      groupId: "sensor_readings",
      rootRelationOid: 1,
      ddlGeneration: 1,
      bindings: [
        {
          partitionId: "tenant:old",
          rootRelationOid: 1,
          relations: [storageRelation(1, "old.bin")],
          snapshotScope: "data-only",
        },
      ],
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:old",
      snapshotId: "local-snapshot",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: 1,
      manifestKind: "cas",
      manifest: casManifest("local-snapshot", oldHash, 1),
    });

    await applyStorageCatalogManifest({
      db,
      manifest: {
        version: 1,
        groupId: "sensor_readings",
        rootRelationOid: 2,
        ddlGeneration: 2,
        bindings: [
          {
            partitionId: "tenant:new",
            rootRelationOid: 2,
            relations: [storageRelation(2, "new.bin")],
            snapshotScope: "data-only",
          },
        ],
        snapshots: [],
      },
    });

    const snapshot = await loadStorageSnapshotCatalogEntry({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:old",
      snapshotId: "local-snapshot",
    });
    const group = await loadStorageBindingGroup(db, "sensor_readings");
    await db.close();

    expect(snapshot?.snapshotId).toBe("local-snapshot");
    expect(group?.bindings.map((binding) => binding.partitionId)).toEqual(["tenant:new"]);
  });

  it("rejects snapshot catalog entries with duplicate high-watermark source cursors", async () => {
    const root = await makeTempDir("storage-catalog-duplicate-hwm");
    const db = new PGlite(join(root, "data"));

    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        groupId: "sensor_readings",
        partitionId: "tenant:jp",
        snapshotId: "bad-snapshot",
        components: ["data"],
        highWatermark: [
          ["node-a", "018bcfe5-6800-7001-8000-000000000001"],
          ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
        ],
        ddlGeneration: 1,
        manifestKind: "cas",
        manifest: casManifest("bad-snapshot", "a".repeat(64), 1),
      }),
    ).rejects.toThrow("duplicate high-watermark source node: node-a");

    await db.close();
  });

  it("rejects snapshot catalog entries with non-UUIDv7 high-watermark event ids", async () => {
    const root = await makeTempDir("storage-catalog-invalid-hwm-event-id");
    const db = new PGlite(join(root, "data"));

    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        groupId: "sensor_readings",
        partitionId: "tenant:jp",
        snapshotId: "bad-snapshot",
        components: ["data"],
        highWatermark: [["node-a", "not-a-uuid"]],
        ddlGeneration: 1,
        manifestKind: "cas",
        manifest: casManifest("bad-snapshot", "a".repeat(64), 1),
      }),
    ).rejects.toThrow("invalid high-watermark event id at index 0");

    await db.close();
  });

  it("rejects snapshot catalog entries with invalid control fields", async () => {
    const root = await makeTempDir("storage-catalog-invalid-control-fields");
    const db = new PGlite(join(root, "data"));

    const baseEntry = {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "bad-snapshot",
      components: ["data"] as const,
      highWatermark: [],
      ddlGeneration: 1,
      manifestKind: "cas" as const,
      manifest: casManifest("bad-snapshot", "a".repeat(64), 1),
    };

    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        ...baseEntry,
        components: [] as never,
      }),
    ).rejects.toThrow("snapshot catalog components must be a non-empty array");
    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        ...baseEntry,
        components: ["data", "data"],
      }),
    ).rejects.toThrow("duplicate snapshot catalog component: data");
    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        ...baseEntry,
        components: ["indexes"] as never,
      }),
    ).rejects.toThrow("invalid snapshot catalog component: indexes");
    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        ...baseEntry,
        manifestKind: "tarball" as never,
      }),
    ).rejects.toThrow("invalid snapshot catalog manifest kind: tarball");
    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        ...baseEntry,
        ddlGeneration: -1,
      }),
    ).rejects.toThrow("snapshot catalog ddlGeneration must be a non-negative integer");

    await db.close();
  });

  it("rejects snapshot catalog entries with invalid CAS manifests", async () => {
    const root = await makeTempDir("storage-catalog-invalid-cas-manifest");
    const db = new PGlite(join(root, "data"));

    const baseEntry = {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "bad-snapshot",
      components: ["data"] as const,
      highWatermark: [],
      ddlGeneration: 1,
      manifestKind: "cas" as const,
      manifest: casManifest("bad-snapshot", "a".repeat(64), 1),
    };

    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        ...baseEntry,
        manifest: casManifest("other-snapshot", "a".repeat(64), 1),
      }),
    ).rejects.toThrow("CAS manifest snapshotId mismatch: other-snapshot !== bad-snapshot");
    await expect(
      saveStorageSnapshotCatalogEntry(db, {
        ...baseEntry,
        manifest: {
          ...baseEntry.manifest,
          files: [
            {
              ...baseEntry.manifest.files[0],
              blocks: [{ hash: "not-a-hash", size: 1 }],
            },
          ],
        },
      }),
    ).rejects.toThrow(
      "CAS manifest files[0].blocks[0].hash must be a 64-character lowercase sha256 hash",
    );

    await db.close();
  });

  it("rejects invalid CAS manifests when loading snapshot catalog entries", async () => {
    const root = await makeTempDir("storage-catalog-invalid-cas-manifest-load");
    const db = new PGlite(join(root, "data"));

    await saveStorageBindingGroup(db, {
      groupId: "sensor_readings",
      rootRelationOid: 1,
      ddlGeneration: 1,
      bindings: [
        {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          relations: [storageRelation(1, "jp.bin")],
          snapshotScope: "data-only",
        },
      ],
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "bad-snapshot",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: 1,
      manifestKind: "cas",
      manifest: casManifest("bad-snapshot", "a".repeat(64), 1),
    });
    await db.query(
      `
        UPDATE distrb_storage_snapshots
        SET manifest = jsonb_set(manifest, '{snapshotId}', '"other-snapshot"'::jsonb)
        WHERE group_id = $1
          AND partition_id = $2
          AND snapshot_id = $3
      `,
      ["sensor_readings", "tenant:jp", "bad-snapshot"],
    );

    await expect(
      loadStorageSnapshotCatalogEntry({
        db,
        groupId: "sensor_readings",
        partitionId: "tenant:jp",
        snapshotId: "bad-snapshot",
      }),
    ).rejects.toThrow("CAS manifest snapshotId mismatch: other-snapshot !== bad-snapshot");

    await db.close();
  });

  it("executes CAS block GC from snapshot catalog policy", async () => {
    const root = await makeTempDir("storage-cas-gc");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(casDir, { recursive: true });
    const oldHash = "a".repeat(64);
    const newHash = "b".repeat(64);
    const orphanHash = "9".repeat(64);
    await writeFile(join(casDir, oldHash), "old");
    await writeFile(join(casDir, newHash), "new");
    await writeFile(join(casDir, orphanHash), "orphan");

    const db = new PGlite(dataDir);
    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
      vacuumPolicy: { retainSnapshotGenerations: 1, gcAfterGeneration: 9 },
    });
    await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "old",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: 1,
      manifestKind: "cas",
      manifest: casManifest("old", oldHash, 3),
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "new",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: 2,
      manifestKind: "cas",
      manifest: casManifest("new", newHash, 3),
    });

    const result = await runStorageCasGcFromCatalog({
      db,
      groupId: "sensor_readings",
      casDir,
    });
    await db.close();

    expect(result).toMatchObject({
      retainedSnapshotIds: ["new", "old"],
      gcEligibleSnapshotIds: [],
      gc: {
        retained: [oldHash, newHash],
        deleted: [orphanHash],
      },
    });
  }, 120_000);

  it("uses storage generation, not only DDL generation, for CAS block GC", async () => {
    const root = await makeTempDir("storage-cas-gc-storage-generation");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(casDir, { recursive: true });
    const oldHash = "e".repeat(64);
    const newHash = "f".repeat(64);
    const orphanHash = "8".repeat(64);
    await writeFile(join(casDir, oldHash), "old");
    await writeFile(join(casDir, newHash), "new");
    await writeFile(join(casDir, orphanHash), "orphan");

    const db = new PGlite(dataDir);
    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      INSERT INTO sensor_readings_tenant_jp (tenant_id, sensor_id, reading)
      VALUES ('tenant:jp', 'tokyo-1', 42);
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
      vacuumPolicy: { retainSnapshotGenerations: 1, gcAfterGeneration: 9 },
    });
    const bound = await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "old-storage",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: bound.ddlGeneration,
      storageGeneration: bound.storageGeneration,
      manifestKind: "cas",
      manifest: casManifest("old-storage", oldHash, 3),
    });
    const truncated = await truncateStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "new-storage",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: truncated.group.ddlGeneration,
      storageGeneration: truncated.group.storageGeneration,
      manifestKind: "cas",
      manifest: casManifest("new-storage", newHash, 3),
    });

    const result = await runStorageCasGcFromCatalog({
      db,
      groupId: "sensor_readings",
      casDir,
    });
    await db.close();

    expect(result).toMatchObject({
      plan: {
        ddlGeneration: 2,
        storageGeneration: 3,
        protectedStorageGenerations: [2, 3],
        storageGcAllowedThroughGeneration: 1,
      },
      retainedSnapshotIds: ["new-storage", "old-storage"],
      gcEligibleSnapshotIds: [],
      gc: {
        retained: [oldHash, newHash],
        deleted: [orphanHash],
      },
    });
  }, 120_000);

  it("runs catalog CAS GC from an auto-GC policy and can be disabled", async () => {
    const root = await makeTempDir("storage-cas-auto-gc");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(casDir, { recursive: true });
    const oldHash = "c".repeat(64);
    const newHash = "d".repeat(64);
    const orphanHash = "7".repeat(64);
    await writeFile(join(casDir, oldHash), "old");
    await writeFile(join(casDir, newHash), "new");
    await writeFile(join(casDir, orphanHash), "orphan");

    const db = new PGlite(dataDir);
    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
      vacuumPolicy: { retainSnapshotGenerations: 1, gcAfterGeneration: 9 },
    });
    await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "old",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: 1,
      manifestKind: "cas",
      manifest: casManifest("old", oldHash, 3),
    });
    await saveStorageSnapshotCatalogEntry(db, {
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      snapshotId: "new",
      components: ["data"],
      highWatermark: [],
      ddlGeneration: 2,
      manifestKind: "cas",
      manifest: casManifest("new", newHash, 3),
    });

    await expect(
      runStorageCasAutoGcFromCatalog({
        db,
        groupId: "sensor_readings",
        casDir,
        policy: { enabled: false },
      }),
    ).resolves.toEqual({ skipped: true, reason: "disabled" });

    const result = await runStorageCasAutoGcFromCatalog({
      db,
      groupId: "sensor_readings",
      casDir,
      policy: { enabled: true },
    });
    await db.close();

    expect(result).toMatchObject({
      skipped: false,
      retainedSnapshotIds: ["new", "old"],
      gcEligibleSnapshotIds: [],
      gc: {
        retained: [oldHash, newHash],
        deleted: [orphanHash],
      },
    });
  }, 120_000);
});

function casManifest(snapshotId: string, hash: string, size: number) {
  return {
    version: 1 as const,
    snapshotId,
    parentSnapshotIds: [],
    chunkSize: 64 * 1024,
    directories: [],
    files: [
      {
        path: `${snapshotId}.bin`,
        size,
        sha256: hash,
        components: ["data" as const],
        blocks: [{ hash, size }],
      },
    ],
  };
}

function storageRelation(relationOid: number, storagePath: string) {
  return {
    databaseOid: 1,
    tablespaceOid: 1,
    relationOid,
    relfilenode: relationOid,
    storagePath,
    role: "child" as const,
  };
}
