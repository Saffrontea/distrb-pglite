import { PGlite } from "@electric-sql/pglite";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  diffMissingBlocks,
  gcCasBlocks,
  inventoryDataDir,
  listCasBlockHashes,
  manifestFilesEqualInventory,
  PGlitePartitionStore,
  restoreFromCas,
  snapshotToCas,
  transferMissingBlocks,
} from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("CAS snapshots", () => {
  it("chunks a data directory, deduplicates blocks, and restores it", async () => {
    const root = await makeTempDir("cas");
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

  it("compresses blocks with zstd and restores the original files", async () => {
    const root = await makeTempDir("cas-zstd");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    const restoreDir = join(root, "restore");
    await mkdir(join(dataDir, "nested"), { recursive: true });
    await writeFile(join(dataDir, "a.txt"), "abcdefghijabcdefghij");
    await writeFile(join(dataDir, "nested", "b.txt"), "abcdefghijXYZ");

    const manifest = await snapshotToCas({ dataDir, casDir, chunkSize: 10, compress: true });
    const inventory = await inventoryDataDir(dataDir);
    await restoreFromCas({ manifest, casDir, targetDir: restoreDir });
    const restoredInventory = await inventoryDataDir(restoreDir);

    expect(manifest.compression).toBe("zstd");
    expect(manifestFilesEqualInventory(manifest, inventory.files)).toBe(true);
    expect(restoredInventory.files).toEqual(inventory.files);
  });

  it("chunks files on page-aligned boundaries", async () => {
    const root = await makeTempDir("cas-page-aligned");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "pages.bin"), Buffer.alloc(10 * 1024, 7));

    const manifest = await snapshotToCas({
      dataDir,
      casDir,
      chunking: { strategy: "page-aligned", pageSize: 4096, maxChunkSize: 6144 },
    });

    expect(manifest.chunkSize).toBe(4096);
    expect(manifest.chunking).toEqual({
      strategy: "page-aligned",
      pageSize: 4096,
      maxChunkSize: 6144,
    });
    expect(manifest.files[0]?.blocks).toHaveLength(3);
  });

  it("garbage collects CAS blocks not referenced by retained manifests", async () => {
    const root = await makeTempDir("cas-gc");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "a.txt"), "cccccddddd");

    const first = await snapshotToCas({
      dataDir,
      casDir,
      chunkSize: 5,
      snapshotId: "snapshot-1",
    });
    await writeFile(join(dataDir, "a.txt"), "aaaaabbbbb");
    const second = await snapshotToCas({
      dataDir,
      casDir,
      chunkSize: 5,
      snapshotId: "snapshot-2",
      parentSnapshotIds: [first.snapshotId],
    });

    const before = await listCasBlockHashes(casDir);
    const gc = await gcCasBlocks({ casDir, retainedManifests: [second] });
    const after = await listCasBlockHashes(casDir);
    const secondHashes = new Set(second.files.flatMap((file) => file.blocks.map((block) => block.hash)));

    expect(before.size).toBeGreaterThan(after.size);
    expect(gc.deleted.length).toBeGreaterThan(0);
    expect(after).toEqual(secondHashes);
  });

  it("transfers only missing blocks between parented snapshots and restores a readable PGlite data directory", async () => {
    const root = await makeTempDir("cas-diff");
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
