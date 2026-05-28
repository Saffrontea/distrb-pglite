import { InMemoryPartitionStore } from "@distrb-pglite/core";
import { PGlite } from "@electric-sql/pglite";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  bindStoragePartition,
  createStorageBindingGroup,
  createPartitionCasSnapshot,
  discoverStoragePartitionBinding,
  highWatermarkMap,
  listEventsAfterPartitionSnapshot,
  loadStoragePartitionBinding,
  PGlitePartitionStore,
  prepareStorageSnapshotFence,
  replayEventsAfterPartitionSnapshot,
  restorePartitionCasSnapshot,
  restorePartitionDataOnlyCasSnapshot,
} from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("partition CAS snapshots", () => {
  it("binds a CAS manifest to a partition high-watermark vector", async () => {
    const root = await makeTempDir("partition-snapshot");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "db-file"), "snapshot-state");

    const store = new InMemoryPartitionStore("node-c");
    await store.registerPartition("tenant:jp");
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a2" },
    });
    await store.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });

    const snapshot = await createPartitionCasSnapshot({
      store,
      partitionId: "tenant:jp",
      storageBinding: storageBinding("tenant:jp"),
      dataDir,
      casDir,
      chunkSize: 8,
    });

    expect(snapshot.version).toBe(1);
    expect(snapshot.partitionId).toBe("tenant:jp");
    expect(snapshot.storageBinding).toEqual(storageBinding("tenant:jp"));
    expect(snapshot.highWatermark).toEqual([
      ["node-a", "018bcfe5-6800-7002-8000-000000000002"],
      ["node-b", "018bcfe5-6800-7001-8000-000000000003"],
    ]);
    expect(snapshot.manifest.files).toHaveLength(1);
    expect(highWatermarkMap(snapshot)).toEqual(new Map(snapshot.highWatermark));
  });

  it("uses snapshot high-watermarks as replay cursors for the same partition", async () => {
    const root = await makeTempDir("partition-replay");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "db-file"), "snapshot-state");

    const snapshotStore = new InMemoryPartitionStore("node-snapshot");
    await snapshotStore.registerPartition("tenant:jp");
    await snapshotStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });

    const snapshot = await createPartitionCasSnapshot({
      store: snapshotStore,
      partitionId: "tenant:jp",
      storageBinding: storageBinding("tenant:jp"),
      dataDir,
      casDir,
    });

    const peerStore = new InMemoryPartitionStore("node-peer");
    await peerStore.registerPartition("tenant:jp");
    await peerStore.registerPartition("tenant:us");
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000001",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a1" },
    });
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7002-8000-000000000002",
      sourceNodeId: "node-a",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "a2" },
    });
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000003",
      sourceNodeId: "node-b",
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { value: "b1" },
    });
    await peerStore.applyRemote({
      eventId: "018bcfe5-6800-7001-8000-000000000004",
      sourceNodeId: "node-a",
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { value: "us" },
    });

    const replay = await listEventsAfterPartitionSnapshot(peerStore, snapshot);

    expect(replay.map((event) => event.payload)).toEqual([
      { value: "a2" },
      { value: "b1" },
    ]);
  });

  it("uses data-only storage bindings to snapshot matching relation files", async () => {
    const root = await makeTempDir("partition-data-only");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(join(dataDir, "base", "5"), { recursive: true });
    await mkdir(join(dataDir, "global"), { recursive: true });
    await writeFile(join(dataDir, "base", "5", "16384"), "main");
    await writeFile(join(dataDir, "base", "5", "16384.1"), "segment");
    await writeFile(join(dataDir, "base", "5", "16384_fsm"), "fsm");
    await writeFile(join(dataDir, "base", "5", "16384_vm.2"), "vm-segment");
    await writeFile(join(dataDir, "base", "5", "99999"), "other-relation");
    await writeFile(join(dataDir, "global", "pg_control"), "catalog-control");

    const store = new InMemoryPartitionStore("node-c");
    await store.registerPartition("tenant:jp");

    const snapshot = await createPartitionCasSnapshot({
      store,
      partitionId: "tenant:jp",
      storageBinding: {
        ...storageBinding("tenant:jp"),
        snapshotScope: "data-only",
      },
      dataDir,
      casDir,
      chunkSize: 8,
    });

    expect(snapshot.manifest.directories).toEqual(["base", "base/5"]);
    expect(snapshot.manifest.files.map((file) => file.path)).toEqual([
      "base/5/16384",
      "base/5/16384_fsm",
      "base/5/16384_vm.2",
      "base/5/16384.1",
    ]);
  });

  it("rejects a storage binding for a different partition", async () => {
    const root = await makeTempDir("partition-mismatch");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "db-file"), "snapshot-state");

    const store = new InMemoryPartitionStore("node-c");
    await store.registerPartition("tenant:jp");

    await expect(createPartitionCasSnapshot({
      store,
      partitionId: "tenant:jp",
      storageBinding: storageBinding("tenant:us"),
      dataDir,
      casDir,
    })).rejects.toThrow("storage binding partition mismatch");
  });

  it("rejects data-only restore remapping with a target binding for another partition", async () => {
    const root = await makeTempDir("partition-data-only-target-mismatch");
    const casDir = join(root, "cas");
    const targetDir = join(root, "target");
    const sourceBinding = {
      ...storageBinding("tenant:jp"),
      snapshotScope: "data-only" as const,
    };
    const targetBinding = {
      ...storageBinding("tenant:us"),
      snapshotScope: "data-only" as const,
    };

    await expect(restorePartitionDataOnlyCasSnapshot({
      snapshot: {
        version: 1,
        snapshotId: "partition-snapshot:tenant:jp:fixed",
        partitionId: "tenant:jp",
        storageBinding: sourceBinding,
        highWatermark: [],
        manifest: {
          version: 1,
          snapshotId: "partition-snapshot:tenant:jp:fixed",
          parentSnapshotIds: [],
          chunkSize: 64 * 1024,
          directories: ["base", "base/5"],
          files: [{
            path: "base/5/16384",
            size: 0,
            sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
            components: ["data"],
            blocks: [],
          }],
        },
      },
      casDir,
      targetDir,
      targetStorageBinding: targetBinding,
    })).rejects.toThrow("target storage binding partition mismatch");
  });

  it("rejects data-only restore remapping with a non-data-only target binding", async () => {
    const root = await makeTempDir("partition-data-only-target-scope-mismatch");
    const sourceBinding = {
      ...storageBinding("tenant:jp"),
      snapshotScope: "data-only" as const,
    };

    await expect(restorePartitionDataOnlyCasSnapshot({
      snapshot: dataOnlySnapshot(sourceBinding),
      casDir: join(root, "cas"),
      targetDir: join(root, "target"),
      targetStorageBinding: storageBinding("tenant:jp"),
    })).rejects.toThrow("target storage binding must use data-only snapshot scope");
  });

  it("rejects data-only restore remapping when target binding DDL generation differs", async () => {
    const root = await makeTempDir("partition-data-only-target-ddl-mismatch");
    const sourceBinding = {
      ...storageBinding("tenant:jp"),
      snapshotScope: "data-only" as const,
      ddlGeneration: 3,
    };
    const targetBinding = {
      ...storageBinding("tenant:jp"),
      snapshotScope: "data-only" as const,
      ddlGeneration: 4,
    };

    await expect(restorePartitionDataOnlyCasSnapshot({
      snapshot: dataOnlySnapshot(sourceBinding),
      casDir: join(root, "cas"),
      targetDir: join(root, "target"),
      targetStorageBinding: targetBinding,
    })).rejects.toThrow("target storage binding DDL generation mismatch");
  });

  it("rejects data-only restore remapping when target binding relation shape differs", async () => {
    const root = await makeTempDir("partition-data-only-target-relation-mismatch");
    const sourceBinding = {
      ...storageBinding("tenant:jp"),
      snapshotScope: "data-only" as const,
      relations: [
        ...storageBinding("tenant:jp").relations,
        {
          databaseOid: 5,
          tablespaceOid: 1_668,
          relationOid: 16_385,
          relfilenode: 16_385,
          role: "index" as const,
          parentRelationOid: 16_384,
        },
      ],
    };
    const targetBinding = {
      ...storageBinding("tenant:jp"),
      snapshotScope: "data-only" as const,
    };

    await expect(restorePartitionDataOnlyCasSnapshot({
      snapshot: dataOnlySnapshot(sourceBinding),
      casDir: join(root, "cas"),
      targetDir: join(root, "target"),
      targetStorageBinding: targetBinding,
    })).rejects.toThrow("target storage binding relation count mismatch");
  });

  it("restores a catalog-and-data snapshot and replays only events after its high-watermark", async () => {
    const root = await makeTempDir("partition-pull");
    const sourceDir = join(root, "source");
    const casDir = join(root, "cas");
    const targetDir = join(root, "target");

    let sourceDb = new PGlite(sourceDir, { relaxedDurability: false });
    let sourceStore = new PGlitePartitionStore("node-a", sourceDb);
    await sourceStore.migrate();
    await sourceDb.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await sourceDb.query(
      "INSERT INTO sensor_readings (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    const snapshottedEvent = await sourceStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });

    const binding = await discoverStoragePartitionBinding({
      db: sourceDb,
      partitionId: "tenant:jp",
      rootRelation: "public.sensor_readings",
      snapshotScope: "catalog-and-data",
    });
    const snapshot = await createPartitionCasSnapshot({
      store: sourceStore,
      partitionId: "tenant:jp",
      storageBinding: binding,
      dataDir: sourceDir,
      casDir,
      beforeStorageSnapshot: async () => {
        await sourceDb.close();
      },
    });
    expect(snapshot.manifest.files.some((file) => file.components?.includes("data"))).toBe(true);
    expect(snapshot.manifest.files.some((file) => file.components?.includes("catalog"))).toBe(true);

    sourceDb = new PGlite(sourceDir);
    sourceStore = new PGlitePartitionStore("node-a", sourceDb);
    await sourceStore.migrate();
    const replayedEvent = await sourceStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-2", reading: 43 },
    });
    await sourceStore.appendLocal({
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { sensorId: "sf-2", reading: 11 },
    });
    await sourceDb.close();

    await restorePartitionCasSnapshot({ snapshot, casDir, targetDir });

    const targetDb = new PGlite(targetDir, { relaxedDurability: false });
    const targetStore = new PGlitePartitionStore("node-target", targetDb);
    await targetStore.migrate();
    await expect(
      targetStore.listEventsAfterCursors("tenant:jp", new Map()),
    ).resolves.toMatchObject([
      {
        eventId: snapshottedEvent.eventId,
        partitionId: "tenant:jp",
        payload: { sensorId: "tokyo-1", reading: 42 },
      },
    ]);

    sourceDb = new PGlite(sourceDir);
    sourceStore = new PGlitePartitionStore("node-a", sourceDb);
    await sourceStore.migrate();
    await expect(replayEventsAfterPartitionSnapshot({
      sourceStore,
      targetStore,
      snapshot,
    })).resolves.toBe(1);

    await expect(
      targetStore.listEventsAfterCursors("tenant:jp", new Map()),
    ).resolves.toMatchObject([
      {
        eventId: snapshottedEvent.eventId,
        partitionId: "tenant:jp",
        payload: { sensorId: "tokyo-1", reading: 42 },
      },
      {
        eventId: replayedEvent.eventId,
        partitionId: "tenant:jp",
        payload: { sensorId: "tokyo-2", reading: 43 },
      },
    ]);
    await expect(
      targetStore.listEventsAfterCursors("tenant:us", new Map()),
    ).resolves.toHaveLength(0);

    await targetDb.close();
    await sourceDb.close();
  }, 120_000);

  it("applies a data-only snapshot to a catalog-compatible PGlite data directory", async () => {
    const root = await makeTempDir("partition-data-only-pull");
    const sourceDir = join(root, "source");
    const targetDir = join(root, "target");
    const casDir = join(root, "cas");

    let sourceDb = new PGlite(sourceDir, { relaxedDurability: false });
    let sourceStore = new PGlitePartitionStore("node-source", sourceDb);
    await sourceStore.migrate();
    await sourceDb.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);

    const bootstrapBinding = await discoverStoragePartitionBinding({
      db: sourceDb,
      partitionId: "tenant:jp",
      rootRelation: "public.sensor_readings",
      snapshotScope: "catalog-and-data",
    });
    const bootstrapSnapshot = await createPartitionCasSnapshot({
      store: sourceStore,
      partitionId: "tenant:jp",
      storageBinding: bootstrapBinding,
      dataDir: sourceDir,
      casDir,
      beforeStorageSnapshot: async () => {
        await sourceDb.close();
      },
    });
    await restorePartitionCasSnapshot({
      snapshot: bootstrapSnapshot,
      casDir,
      targetDir,
    });

    sourceDb = new PGlite(sourceDir, { relaxedDurability: false });
    sourceStore = new PGlitePartitionStore("node-source", sourceDb);
    await sourceStore.migrate();
    await createStorageBindingGroup({
      db: sourceDb,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    await bindStoragePartition({
      db: sourceDb,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings"],
      snapshotScope: "data-only",
    });
    await sourceDb.query(
      "INSERT INTO sensor_readings (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    await sourceStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });
    await expect(prepareStorageSnapshotFence({
      db: sourceDb,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    })).resolves.toEqual([
      expect.objectContaining({
        partitionId: "tenant:jp",
        relationName: "sensor_readings",
        relationRole: "child",
      }),
    ]);

    const targetDb = new PGlite(targetDir, { relaxedDurability: false });
    await createStorageBindingGroup({
      db: targetDb,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    await bindStoragePartition({
      db: targetDb,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings"],
      snapshotScope: "data-only",
    });
    const targetBinding = await loadStoragePartitionBinding({
      db: targetDb,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    await targetDb.close();

    const binding = await loadStoragePartitionBinding({
      db: sourceDb,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    const snapshot = await createPartitionCasSnapshot({
      store: sourceStore,
      partitionId: "tenant:jp",
      storageBinding: binding!,
      dataDir: sourceDir,
      casDir,
      chunking: { strategy: "page-aligned", pageSize: 4096, maxChunkSize: 16 * 1024 },
      beforeStorageSnapshot: async () => {
        await sourceDb.close();
      },
    });

    expect(snapshot.manifest.files.some((file) => file.path.startsWith("base/"))).toBe(true);
    expect(snapshot.manifest.files.some((file) => file.path.startsWith("pg_xact/"))).toBe(true);
    await restorePartitionDataOnlyCasSnapshot({
      snapshot,
      casDir,
      targetDir,
      targetStorageBinding: targetBinding!,
    });

    const pulledDb = new PGlite(targetDir, { relaxedDurability: false });
    const readings = await pulledDb.query<{ tenant_id: string; sensor_id: string; reading: number }>(
      "SELECT tenant_id, sensor_id, reading FROM sensor_readings",
    );
    await pulledDb.close();

    expect(readings.rows).toEqual([
      { tenant_id: "tenant:jp", sensor_id: "tokyo-1", reading: 42 },
    ]);
  }, 120_000);
});

function storageBinding(partitionId: string) {
  return {
    partitionId,
    rootRelationOid: 16_384,
    snapshotScope: "catalog-and-data" as const,
    relations: [
      {
        databaseOid: 5,
        tablespaceOid: 1_668,
        relationOid: 16_384,
        relfilenode: 16_384,
      },
    ],
  };
}

function dataOnlySnapshot(storageBinding: ReturnType<typeof storageBinding>) {
  return {
    version: 1 as const,
    snapshotId: `partition-snapshot:${storageBinding.partitionId}:fixed`,
    partitionId: storageBinding.partitionId,
    storageBinding,
    highWatermark: [],
    manifest: {
      version: 1 as const,
      snapshotId: `partition-snapshot:${storageBinding.partitionId}:fixed`,
      parentSnapshotIds: [],
      chunkSize: 64 * 1024,
      directories: ["base", "base/5"],
      files: [{
        path: "base/5/16384",
        size: 0,
        sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        components: ["data" as const],
        blocks: [],
      }],
    },
  };
}
