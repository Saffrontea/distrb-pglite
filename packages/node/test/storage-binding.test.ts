import { PGlite } from "@electric-sql/pglite";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  bindStoragePartition,
  createStorageBindingGroup,
  createPartitionCasSnapshot,
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
  savePartitionCasSnapshotCatalogEntry,
  saveStorageBindingGroup,
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
    expect(snapshot.manifest.files.every((file) =>
      file.path.startsWith(binding.relations[0]?.storagePath ?? "missing"),
    )).toBe(true);
  }, 120_000);

  it("rejects unknown relations", async () => {
    const root = await makeTempDir("storage-binding-missing");
    const db = new PGlite(join(root, "data"));

    await expect(discoverStoragePartitionBinding({
      db,
      partitionId: "tenant:jp",
      rootRelation: "public.missing_relation",
      snapshotScope: "data-only",
    })).rejects.toThrow("relation not found");
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
    expect(reloaded?.bindings.every((binding) =>
      binding.relations.some((relation) =>
        relation.role === "child" && relation.parentRelationOid === created.rootRelationOid
      ),
    )).toBe(true);
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
    expect(catalogManifest?.bindings.every((binding) =>
      binding.relations.some((relation) => relation.role === "child")
      && binding.relations.some((relation) => relation.role === "toast")
    )).toBe(true);
  }, 120_000);
});
