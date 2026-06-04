import { MemoryBlobStore } from "@distrb-pglite/core";
import { createHash } from "node:crypto";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  gcCasBlockStore,
  NodeDataDirFilesystem,
  PGliteCasFilesystem,
  inventoryFilesystem,
  restoreFilesystemFromCas,
  snapshotFilesystemToCas,
  transferMissingBlocksBetweenStores,
  type CasBlockStore,
} from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("CAS logical filesystem snapshots", () => {
  it("snapshots a Node data directory through the logical filesystem interface", async () => {
    const root = await makeTempDir("cas-logical-node");
    const dataDir = join(root, "data");
    const restoreDir = join(root, "restore");
    const casStore = new MemoryCasBlockStore();
    await mkdir(join(dataDir, "base", "5"), { recursive: true });
    await writeFile(join(dataDir, "base", "5", "16384"), "aaaabbbb");
    await writeFile(join(dataDir, "PG_VERSION"), "17");

    const manifest = await snapshotFilesystemToCas({
      fs: new NodeDataDirFilesystem(dataDir),
      casStore,
      chunkSize: 4,
    });
    await restoreFilesystemFromCas({
      fs: new NodeDataDirFilesystem(restoreDir),
      casStore,
      manifest,
    });

    await expect(
      inventoryFilesystem({
        fs: new NodeDataDirFilesystem(restoreDir),
      }),
    ).resolves.toMatchObject({
      directories: ["base", "base/5"],
      files: [
        expect.objectContaining({ path: "base/5/16384", size: 8 }),
        expect.objectContaining({ path: "PG_VERSION", size: 2 }),
      ],
    });
    expect(manifest.files.map((file) => file.path)).toEqual(["base/5/16384", "PG_VERSION"]);
  });

  it("snapshots PGlite VFS logical paths without depending on host data-dir layout", async () => {
    const casStore = new MemoryCasBlockStore();
    const vfs = new PGliteCasFilesystem(createBlobStore(), 4);
    vfs.writeFile("/base/5/16384", "aaaabbbb");
    vfs.writeFile("/global/pg_control", "catalog");

    const manifest = await snapshotFilesystemToCas({
      fs: vfs.asSnapshotDataFilesystem(),
      casStore,
      chunkSize: 4,
    });
    const restored = new PGliteCasFilesystem(createBlobStore(), 4);
    await restoreFilesystemFromCas({
      fs: restored.asSnapshotDataFilesystem(),
      casStore,
      manifest,
    });

    await expect(
      inventoryFilesystem({
        fs: restored.asSnapshotDataFilesystem(),
      }),
    ).resolves.toMatchObject({
      directories: ["base", "base/5", "global"],
      files: [
        expect.objectContaining({ path: "base/5/16384", size: 8 }),
        expect.objectContaining({ path: "global/pg_control", size: 7 }),
      ],
    });
  });

  it("transfers and garbage-collects blocks through the CAS block store interface", async () => {
    const root = await makeTempDir("cas-block-store");
    const dataDir = join(root, "data");
    const sourceStore = new MemoryCasBlockStore();
    const targetStore = new MemoryCasBlockStore();
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "a.txt"), "aaaabbbbcccc");

    const manifest = await snapshotFilesystemToCas({
      fs: new NodeDataDirFilesystem(dataDir),
      casStore: sourceStore,
      chunkSize: 4,
      snapshotId: "snapshot-1",
    });

    const copied = await transferMissingBlocksBetweenStores({
      manifest,
      sourceCasStore: sourceStore,
      targetCasStore: targetStore,
    });
    expect(copied).toHaveLength(3);
    expect(await targetStore.listBlockHashes()).toEqual(await sourceStore.listBlockHashes());

    await targetStore.writeBlock("0".repeat(64), new Uint8Array([1]));
    const gc = await gcCasBlockStore({
      casStore: targetStore,
      retainedManifests: [manifest],
    });

    expect(gc.deleted).toEqual(["0".repeat(64)]);
    expect(await targetStore.listBlockHashes()).toEqual(await sourceStore.listBlockHashes());
  });
});

function createBlobStore(): MemoryBlobStore {
  return new MemoryBlobStore((content) => createHash("sha256").update(content).digest("hex"));
}

class MemoryCasBlockStore implements CasBlockStore {
  private blocks = new Map<string, Uint8Array>();

  async readBlock(hash: string): Promise<Uint8Array> {
    const block = this.blocks.get(hash);
    if (!block) {
      throw new Error(`missing block: ${hash}`);
    }
    return new Uint8Array(block);
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    this.blocks.set(hash, new Uint8Array(content));
  }

  async listBlockHashes(): Promise<Set<string>> {
    return new Set(this.blocks.keys());
  }

  async deleteBlock(hash: string): Promise<void> {
    this.blocks.delete(hash);
  }
}
