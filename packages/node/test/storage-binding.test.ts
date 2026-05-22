import { PGlite } from "@electric-sql/pglite";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  createPartitionCasSnapshot,
  discoverStoragePartitionBinding,
  loadStorageBindingGroup,
  loadStoragePartitionBinding,
  PGlitePartitionStore,
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
    await db.close();

    expect(snapshot.storageBinding).toEqual(loadedBinding);
    expect(snapshot.manifest.files.length).toBeGreaterThan(0);
  }, 120_000);
});
