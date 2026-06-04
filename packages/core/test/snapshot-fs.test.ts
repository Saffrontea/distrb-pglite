import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { CasSnapshotFilesystem, MemoryBlobStore } from "../src/index.js";

describe("CasSnapshotFilesystem", () => {
  it("snapshots and restores files through a blob store contract", async () => {
    const blobs = new MemoryBlobStore((content) =>
      createHash("sha256").update(content).digest("hex"),
    );
    const fs = new CasSnapshotFilesystem(blobs, 4);

    await fs.writeFile("base/one", new TextEncoder().encode("aaaabbbb"));
    await fs.writeFile("base/two", new TextEncoder().encode("aaaacccc"));

    const manifest = await fs.snapshot();
    const restored = new CasSnapshotFilesystem(blobs, 4);
    await restored.restore(manifest);

    expect(new TextDecoder().decode(await restored.readFile("base/one"))).toBe("aaaabbbb");
    expect(new TextDecoder().decode(await restored.readFile("base/two"))).toBe("aaaacccc");

    const blockHashes = manifest.files.flatMap((file) => file.blocks.map((block) => block.hash));
    expect(new Set(blockHashes).size).toBeLessThan(blockHashes.length);
  });

  it("fails restore when a referenced blob is unavailable", async () => {
    const hash = (content: Uint8Array) => createHash("sha256").update(content).digest("hex");
    const sourceBlobs = new MemoryBlobStore(hash);
    const targetBlobs = new MemoryBlobStore(hash);
    const source = new CasSnapshotFilesystem(sourceBlobs, 4);
    const target = new CasSnapshotFilesystem(targetBlobs, 4);

    await source.writeFile("base/one", new TextEncoder().encode("data"));
    const manifest = await source.snapshot();

    await expect(target.restore(manifest)).rejects.toThrow("blob not found");
  });

  it("keeps the existing filesystem when restore fails", async () => {
    const hash = (content: Uint8Array) => createHash("sha256").update(content).digest("hex");
    const sourceBlobs = new MemoryBlobStore(hash);
    const targetBlobs = new MemoryBlobStore(hash);
    const source = new CasSnapshotFilesystem(sourceBlobs, 4);
    const target = new CasSnapshotFilesystem(targetBlobs, 4);

    await source.writeFile("base/new", new TextEncoder().encode("new-data"));
    await target.writeFile("base/existing", new TextEncoder().encode("keep-me"));
    const manifest = await source.snapshot();

    await expect(target.restore(manifest)).rejects.toThrow("blob not found");
    await expect(target.readFile("base/existing")).resolves.toEqual(
      new TextEncoder().encode("keep-me"),
    );
  });

  it("rejects invalid chunk sizes before snapshotting", () => {
    const blobs = new MemoryBlobStore((content) =>
      createHash("sha256").update(content).digest("hex"),
    );

    expect(() => new CasSnapshotFilesystem(blobs, 0)).toThrow(
      "chunkSize must be a positive integer",
    );
    expect(() => new CasSnapshotFilesystem(blobs, -1)).toThrow(
      "chunkSize must be a positive integer",
    );
    expect(() => new CasSnapshotFilesystem(blobs, 1.5)).toThrow(
      "chunkSize must be a positive integer",
    );
  });

  it("rejects restore manifests with invalid chunk sizes", async () => {
    const blobs = new MemoryBlobStore((content) =>
      createHash("sha256").update(content).digest("hex"),
    );
    const fs = new CasSnapshotFilesystem(blobs, 4);

    await expect(
      fs.restore({
        version: 1,
        chunkSize: 0,
        directories: [],
        files: [],
      }),
    ).rejects.toThrow("manifest chunkSize must be a positive integer");
  });
});
