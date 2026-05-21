import { PGlite } from "@electric-sql/pglite";
import { mkdtemp } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  createPartitionCasSnapshot,
  discoverStoragePartitionBinding,
  PGlitePartitionStore,
} from "../src/index.js";

describe("discoverStoragePartitionBinding", () => {
  it("binds a partition to PGlite catalog relation identity and matching files", async () => {
    const root = await mkdtemp(join(tmpdir(), "distrb-pglite-storage-binding-"));
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
      hypertable: "public.sensor_readings",
      snapshotScope: "data-only",
    });

    expect(binding.partitionId).toBe("tenant:jp");
    expect(binding.hypertableOid).toBeGreaterThan(0);
    expect(binding.relations).toHaveLength(1);
    expect(binding.relations[0]).toMatchObject({
      databaseOid: expect.any(Number),
      tablespaceOid: expect.any(Number),
      relationOid: binding.hypertableOid,
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
    const root = await mkdtemp(join(tmpdir(), "distrb-pglite-storage-binding-missing-"));
    const db = new PGlite(join(root, "data"));

    await expect(discoverStoragePartitionBinding({
      db,
      partitionId: "tenant:jp",
      hypertable: "public.missing_relation",
      snapshotScope: "data-only",
    })).rejects.toThrow("relation not found");
    await db.close();
  }, 120_000);
});
