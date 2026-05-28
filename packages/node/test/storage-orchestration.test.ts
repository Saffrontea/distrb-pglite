import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import {
  bindStoragePartition,
  createManagedPartitionCasSnapshot,
  createStorageBindingGroup,
  listStorageSnapshotCatalogEntries,
  PGlitePartitionStore,
  pullPartitionCasSnapshot,
  type CasManifest,
} from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("storage orchestration", () => {
  it("refreshes storage drift and saves a snapshot catalog entry before CAS snapshot", async () => {
    const root = await makeTempDir("managed-snapshot");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    const db = new PGlite(dataDir, { relaxedDurability: false });
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
    await db.exec("CREATE INDEX sensor_readings_tenant_jp_sensor_id ON sensor_readings_tenant_jp (sensor_id)");
    await db.query(
      "INSERT INTO sensor_readings_tenant_jp (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    await store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });

    const managed = await createManagedPartitionCasSnapshot({
      db,
      store,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      dataDir,
      casDir,
    });
    const entries = await listStorageSnapshotCatalogEntries<CasManifest>({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    await db.close();

    expect(managed.drift).toEqual(expect.arrayContaining([
      expect.objectContaining({
        currentRelation: expect.objectContaining({ role: "index" }),
      }),
    ]));
    expect(managed.fence).toEqual([
      expect.objectContaining({ relationName: "sensor_readings_tenant_jp" }),
    ]);
    expect(managed.snapshot.storageBinding).toMatchObject({
      ddlGeneration: 3,
      relations: expect.arrayContaining([
        expect.objectContaining({ role: "index" }),
      ]),
    });
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      snapshotId: managed.snapshot.snapshotId,
      ddlGeneration: 3,
      manifestKind: "cas",
    });
  }, 120_000);

  it("applies catalog manifests, selects pull snapshots, transfers blocks, and restores CAS data", async () => {
    const root = await makeTempDir("managed-pull");
    const sourceCasDir = join(root, "source-cas");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    await mkdir(sourceCasDir, { recursive: true });
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);
    await writeFile(join(sourceCasDir, hash), content);

    const targetDb = new PGlite(join(root, "catalog"));
    const catalogManifest = {
      version: 1 as const,
      groupId: "files",
      rootRelationOid: 1,
      ddlGeneration: 2,
      bindings: [{
        partitionId: "tenant:jp",
        rootRelationOid: 1,
        boundChildRelationOids: [1],
        relations: [{
          databaseOid: 1,
          tablespaceOid: 1,
          relationOid: 1,
          relfilenode: 1,
          storagePath: "payload.txt",
          role: "child" as const,
          parentRelationOid: 1,
        }],
        snapshotScope: "catalog-and-data" as const,
        snapshotComponents: ["catalog" as const, "data" as const],
        pullRequires: ["catalog" as const, "data" as const],
        ddlGeneration: 2,
      }],
      snapshots: [{
        partitionId: "tenant:jp",
        snapshotId: "newer",
        components: ["catalog" as const, "data" as const],
        ddlGeneration: 2,
        manifestKind: "cas" as const,
      }],
    };
    const manifest: CasManifest = {
      version: 1,
      snapshotId: "newer",
      parentSnapshotIds: [],
      chunkSize: 64 * 1024,
      directories: [],
      files: [{
        path: "payload.txt",
        size: content.length,
        sha256: hash,
        components: ["data"],
        blocks: [{ hash, size: content.length }],
      }],
    };

    const pulled = await pullPartitionCasSnapshot({
      db: targetDb,
      catalogManifest,
      snapshotEntries: [{
        groupId: "files",
        partitionId: "tenant:jp",
        snapshotId: "newer",
        components: ["catalog", "data"],
        highWatermark: [],
        ddlGeneration: 2,
        manifestKind: "cas",
        manifest,
      }],
      groupId: "files",
      partitionId: "tenant:jp",
      requirement: "bootstrap",
      sourceCasDir,
      targetCasDir,
      targetDir,
    });
    const restored = await readFile(join(targetDir, "payload.txt"), "utf8");
    await targetDb.close();

    expect(pulled).toMatchObject({
      transferredBlocks: 1,
      entry: { snapshotId: "newer" },
    });
    expect(restored).toBe("snapshot payload");
  }, 120_000);

  it("rejects data-only pulls when the target storage binding is on a different DDL generation", async () => {
    const root = await makeTempDir("managed-pull-ddl-mismatch");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);
    const manifest: CasManifest = {
      version: 1,
      snapshotId: "newer",
      parentSnapshotIds: [],
      chunkSize: 64 * 1024,
      directories: [],
      files: [{
        path: "payload.txt",
        size: content.length,
        sha256: hash,
        components: ["data"],
        blocks: [{ hash, size: content.length }],
      }],
    };

    await expect(pullPartitionCasSnapshot({
      snapshotEntries: [{
        groupId: "files",
        partitionId: "tenant:jp",
        snapshotId: "newer",
        components: ["data"],
        highWatermark: [],
        ddlGeneration: 3,
        manifestKind: "cas",
        manifest,
      }],
      groupId: "files",
      partitionId: "tenant:jp",
      requirement: "data-refresh",
      targetCasDir,
      targetDir,
      storageBinding: {
        partitionId: "tenant:jp",
        rootRelationOid: 1,
        relations: [{
          databaseOid: 1,
          tablespaceOid: 1,
          relationOid: 1,
          relfilenode: 1,
          storagePath: "payload.txt",
          role: "child",
        }],
        snapshotScope: "data-only",
        ddlGeneration: 2,
      },
    })).rejects.toThrow("storage binding DDL generation mismatch");
  });

  it("rejects pulls when an explicit target storage binding belongs to another partition", async () => {
    const root = await makeTempDir("managed-pull-partition-mismatch");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);

    await expect(pullPartitionCasSnapshot({
      snapshotEntries: [{
        groupId: "files",
        partitionId: "tenant:jp",
        snapshotId: "newer",
        components: ["catalog", "data"],
        highWatermark: [],
        ddlGeneration: 2,
        manifestKind: "cas",
        manifest: {
          version: 1,
          snapshotId: "newer",
          parentSnapshotIds: [],
          chunkSize: 64 * 1024,
          directories: [],
          files: [{
            path: "payload.txt",
            size: content.length,
            sha256: hash,
            components: ["data"],
            blocks: [{ hash, size: content.length }],
          }],
        },
      }],
      groupId: "files",
      partitionId: "tenant:jp",
      requirement: "bootstrap",
      targetCasDir,
      targetDir,
      storageBinding: {
        partitionId: "tenant:us",
        rootRelationOid: 1,
        relations: [{
          databaseOid: 1,
          tablespaceOid: 1,
          relationOid: 1,
          relfilenode: 1,
          storagePath: "payload.txt",
          role: "child",
        }],
        snapshotScope: "catalog-and-data",
        ddlGeneration: 2,
      },
    })).rejects.toThrow("storage binding partition mismatch");
  });
});

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}
