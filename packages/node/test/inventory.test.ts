import { PGlite } from "@electric-sql/pglite";
import { mkdtemp } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import {
  copyDataDirSnapshot,
  inventoryDataDir,
  PGlitePartitionStore,
} from "../src/index.js";

describe("inventoryDataDir", () => {
  it("lists PGlite data directory files with deterministic hashes", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), "distrb-pglite-inventory-"));
    const db = new PGlite(dataDir);
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "inventory target" },
    });
    await db.close();

    const first = await inventoryDataDir(dataDir);
    const second = await inventoryDataDir(dataDir);

    expect(first.root).toBe(dataDir);
    expect(first.files.length).toBeGreaterThan(0);
    expect(first.files).toEqual(second.files);
    expect(first.files).toEqual(
      [...first.files].sort((left, right) => left.path.localeCompare(right.path)),
    );
    expect(first.files.every((file) => file.size >= 0)).toBe(true);
    expect(first.files.some((file) => file.size > 0)).toBe(true);
    expect(first.files.every((file) => /^[0-9a-f]{64}$/.test(file.sha256))).toBe(true);
  }, 120_000);

  it("copies a closed PGlite data directory and imports application data plus event metadata", async () => {
    const root = await mkdtemp(join(tmpdir(), "distrb-pglite-import-"));
    const sourceDir = join(root, "source");
    const targetDir = join(root, "target");

    const sourceDb = new PGlite(sourceDir);
    const sourceStore = new PGlitePartitionStore("node-a", sourceDb);
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
    const event = await sourceStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });
    await sourceDb.close();

    const copied = await copyDataDirSnapshot({ sourceDir, targetDir });
    expect(copied.target.files).toEqual(copied.source.files);

    const importedDb = new PGlite(targetDir);
    const importedStore = new PGlitePartitionStore("node-a", importedDb);
    await importedStore.migrate();

    const readings = await importedDb.query<{
      tenant_id: string;
      sensor_id: string;
      reading: number;
    }>(
      "SELECT tenant_id, sensor_id, reading FROM sensor_readings ORDER BY sensor_id",
    );
    expect(readings.rows).toEqual([
      { tenant_id: "tenant:jp", sensor_id: "tokyo-1", reading: 42 },
    ]);

    await expect(
      importedStore.listEventsAfterCursors("tenant:jp", new Map()),
    ).resolves.toMatchObject([
      {
        eventId: event.eventId,
        sourceNodeId: "node-a",
        partitionId: "tenant:jp",
        payload: { sensorId: "tokyo-1", reading: 42 },
      },
    ]);
    await expect(importedStore.getHighWatermark("tenant:jp")).resolves.toEqual(
      new Map([["node-a", event.eventId]]),
    );
    await importedDb.close();
  }, 120_000);
});
