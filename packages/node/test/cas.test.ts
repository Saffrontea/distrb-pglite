import { createHash } from "node:crypto";
import { PGlite } from "@electric-sql/pglite";
import { mkdir, readFile, writeFile } from "node:fs/promises";
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

  it("rejects invalid fixed chunk sizes before snapshotting", async () => {
    const root = await makeTempDir("cas-invalid-fixed-chunking");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "payload.txt"), "payload");

    await expect(snapshotToCas({
      dataDir,
      casDir,
      chunkSize: 0,
    })).rejects.toThrow("chunkSize must be a positive integer");
  });

  it("rejects invalid page-aligned chunking parameters before snapshotting", async () => {
    const root = await makeTempDir("cas-invalid-page-chunking");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "payload.txt"), "payload");

    await expect(snapshotToCas({
      dataDir,
      casDir,
      chunking: { strategy: "page-aligned", pageSize: 0, maxChunkSize: 4096 },
    })).rejects.toThrow("pageSize must be a positive integer");
    await expect(snapshotToCas({
      dataDir,
      casDir,
      chunking: { strategy: "page-aligned", pageSize: 4096, maxChunkSize: 2048 },
    })).rejects.toThrow("maxChunkSize must be greater than or equal to pageSize");
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

  it("rejects restore manifests whose file paths escape the target directory", async () => {
    const root = await makeTempDir("cas-path-traversal");
    const casDir = join(root, "cas");
    const targetDir = join(root, "restore");
    const outside = join(root, "outside.txt");
    const content = Buffer.from("escape");
    const hash = sha256(content);
    await mkdir(casDir, { recursive: true });
    await writeFile(join(casDir, hash), content);

    await expect(restoreFromCas({
      manifest: {
        version: 1,
        snapshotId: "bad-path",
        parentSnapshotIds: [],
        chunkSize: 64 * 1024,
        directories: [],
        files: [{
          path: "../outside.txt",
          size: content.length,
          sha256: hash,
          blocks: [{ hash, size: content.length }],
        }],
      },
      casDir,
      targetDir,
    })).rejects.toThrow("logical path escapes filesystem root");
    await expect(readFile(outside)).rejects.toMatchObject({ code: "ENOENT" });
  });

  it("rejects CAS block hashes that are not canonical hex digests", async () => {
    const root = await makeTempDir("cas-invalid-hash");
    const targetDir = join(root, "restore");

    await expect(restoreFromCas({
      manifest: {
        version: 1,
        snapshotId: "bad-hash",
        parentSnapshotIds: [],
        chunkSize: 64 * 1024,
        directories: [],
        files: [{
          path: "payload.txt",
          size: 0,
          sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          blocks: [{ hash: "../escape", size: 0 }],
        }],
      },
      casDir: join(root, "cas"),
      targetDir,
    })).rejects.toThrow("invalid CAS block hash: ../escape");
  });

  it("rejects restore manifests with invalid chunk sizes", async () => {
    const root = await makeTempDir("cas-invalid-restore-chunk");

    await expect(restoreFromCas({
      manifest: {
        version: 1,
        snapshotId: "bad-chunk-size",
        parentSnapshotIds: [],
        chunkSize: 0,
        directories: [],
        files: [],
      },
      casDir: join(root, "cas"),
      targetDir: join(root, "restore"),
    })).rejects.toThrow("manifest chunkSize must be a positive integer");
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

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}
