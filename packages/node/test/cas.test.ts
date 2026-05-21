import { PGlite } from "@electric-sql/pglite";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { mkdtemp } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import {
  diffMissingBlocks,
  inventoryDataDir,
  listCasBlockHashes,
  manifestFilesEqualInventory,
  PGlitePartitionStore,
  restoreFromCas,
  snapshotToCas,
  transferMissingBlocks,
} from "../src/index.js";

describe("CAS snapshots", () => {
  it("chunks a data directory, deduplicates blocks, and restores it", async () => {
    const root = await mkdtemp(join(tmpdir(), "distrb-pglite-cas-"));
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    const restoreDir = join(root, "restore");
    await mkdir(join(dataDir, "nested"), { recursive: true });
    await writeFile(join(dataDir, "a.txt"), "abcdefghijabcdefghij");
    await writeFile(join(dataDir, "nested", "b.txt"), "abcdefghijXYZ");

    const manifest = await snapshotToCas({ dataDir, casDir, chunkSize: 10 });
    const inventory = await inventoryDataDir(dataDir);
    await restoreFromCas({ manifest, casDir, targetDir: restoreDir });
    const restoredInventory = await inventoryDataDir(restoreDir);

    expect(manifestFilesEqualInventory(manifest, inventory.files)).toBe(true);
    expect(restoredInventory.files).toEqual(inventory.files);

    const allBlockHashes = new Set(
      manifest.files.flatMap((file) => file.blocks.map((block) => block.hash)),
    );
    const repeatedBlockCount = manifest.files
      .flatMap((file) => file.blocks)
      .filter((block) => block.hash === manifest.files[0]?.blocks[0]?.hash).length;

    expect(allBlockHashes.size).toBeLessThan(
      manifest.files.reduce((count, file) => count + file.blocks.length, 0),
    );
    expect(repeatedBlockCount).toBeGreaterThan(1);
    expect(diffMissingBlocks(manifest, allBlockHashes)).toEqual([]);
    expect(diffMissingBlocks(manifest, new Set()).map((block) => block.hash)).toEqual(
      [...allBlockHashes].sort(),
    );
  });

  it("transfers only missing blocks between parented snapshots and restores a readable PGlite data directory", async () => {
    const root = await mkdtemp(join(tmpdir(), "distrb-pglite-cas-diff-"));
    const dataDir = join(root, "data");
    const sourceCasDir = join(root, "source-cas");
    const targetCasDir = join(root, "target-cas");
    const restoreDir = join(root, "restore");

    const firstDb = new PGlite(dataDir);
    const firstStore = new PGlitePartitionStore("node-a", firstDb);
    await firstStore.migrate();
    await firstDb.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await firstDb.query(
      "INSERT INTO sensor_readings (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    await firstStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });
    await firstDb.close();

    const firstManifest = await snapshotToCas({
      dataDir,
      casDir: sourceCasDir,
      chunkSize: 4096,
      snapshotId: "snapshot-1",
    });
    expect(firstManifest.parentSnapshotIds).toEqual([]);

    const firstTransfer = await transferMissingBlocks({
      manifest: firstManifest,
      sourceCasDir,
      targetCasDir,
    });
    expect(firstTransfer.length).toBeGreaterThan(0);

    const secondDb = new PGlite(dataDir);
    const secondStore = new PGlitePartitionStore("node-a", secondDb);
    await secondStore.migrate();
    await secondDb.query(
      "INSERT INTO sensor_readings (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-2", 43],
    );
    await secondStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-2", reading: 43 },
    });
    await secondDb.close();

    const secondManifest = await snapshotToCas({
      dataDir,
      casDir: sourceCasDir,
      chunkSize: 4096,
      snapshotId: "snapshot-2",
      parentSnapshotIds: [firstManifest.snapshotId],
    });
    expect(secondManifest.parentSnapshotIds).toEqual(["snapshot-1"]);

    const secondUniqueBlocks = new Set(
      secondManifest.files.flatMap((file) => file.blocks.map((block) => block.hash)),
    );
    const targetBlocksBefore = await listCasBlockHashes(targetCasDir);
    const missingBeforeTransfer = diffMissingBlocks(secondManifest, targetBlocksBefore);
    expect(missingBeforeTransfer.length).toBeGreaterThan(0);
    expect(missingBeforeTransfer.length).toBeLessThan(secondUniqueBlocks.size);

    const secondTransfer = await transferMissingBlocks({
      manifest: secondManifest,
      sourceCasDir,
      targetCasDir,
      availableBlockHashes: targetBlocksBefore,
    });
    expect(secondTransfer).toEqual(missingBeforeTransfer);

    await restoreFromCas({
      manifest: secondManifest,
      casDir: targetCasDir,
      targetDir: restoreDir,
    });

    const restoredDb = new PGlite(restoreDir);
    const restoredStore = new PGlitePartitionStore("node-a", restoredDb);
    await restoredStore.migrate();
    const readings = await restoredDb.query<{
      sensor_id: string;
      reading: number;
    }>(
      "SELECT sensor_id, reading FROM sensor_readings ORDER BY sensor_id",
    );
    expect(readings.rows).toEqual([
      { sensor_id: "tokyo-1", reading: 42 },
      { sensor_id: "tokyo-2", reading: 43 },
    ]);
    await expect(
      restoredStore.listEventsAfterCursors("tenant:jp", new Map()),
    ).resolves.toHaveLength(2);
    await restoredDb.close();
  }, 120_000);
});
