import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { mkdtemp } from "node:fs/promises";
import { describe, expect, it } from "vitest";
import {
  diffMissingBlocks,
  inventoryDataDir,
  manifestFilesEqualInventory,
  restoreFromCas,
  snapshotToCas,
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
});
