import { MemoryBlobStore } from "@distrb-pglite/core";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { PGliteCasFilesystem } from "../src/index.js";

describe("PGliteCasFilesystem", () => {
  it("supports Node-like file operations needed by PGlite BaseFilesystem", () => {
    const fs = createFs();

    fs.mkdir("/base/pgdata", { recursive: true });
    const fd = fs.open("/base/pgdata/file", "w+");
    const input = new TextEncoder().encode("hello world");

    expect(fs.write(fd, input, 0, input.length, 0)).toBe(input.length);
    expect(fs.fstat(fd).size).toBe(input.length);

    const output = new Uint8Array(input.length);
    expect(fs.read(fd, output, 0, output.length, 0)).toBe(input.length);
    expect(new TextDecoder().decode(output)).toBe("hello world");

    fs.truncate("/base/pgdata/file", 5);
    expect(fs.lstat("/base/pgdata/file").size).toBe(5);
    expect(fs.readdir("/base/pgdata")).toEqual(["file"]);

    fs.rename("/base/pgdata/file", "/base/pgdata/renamed");
    expect(fs.readdir("/base/pgdata")).toEqual(["renamed"]);

    fs.unlink("/base/pgdata/renamed");
    expect(fs.readdir("/base/pgdata")).toEqual([]);
    fs.close(fd);
  });

  it("snapshots and restores adapter state through CAS blobs", async () => {
    const blobs = createBlobStore();
    const source = new PGliteCasFilesystem(blobs, 4);
    source.writeFile("/base/a", "aaaabbbb");
    source.writeFile("/base/b", "aaaacccc");

    const manifest = await source.snapshot();
    const restored = new PGliteCasFilesystem(blobs, 4);
    await restored.restore(manifest);

    const fd = restored.open("/base/a", "r");
    const output = new Uint8Array(8);
    restored.read(fd, output, 0, output.length, 0);

    expect(new TextDecoder().decode(output)).toBe("aaaabbbb");
    expect(manifest.files.map((file) => file.path)).toEqual(["/base/a", "/base/b"]);
    expect(
      new Set(manifest.files.flatMap((file) => file.blocks.map((block) => block.hash))).size,
    ).toBeLessThan(4);
  });

  it("fails restore when a required CAS block is unavailable", async () => {
    const source = new PGliteCasFilesystem(createBlobStore(), 4);
    source.writeFile("/base/a", "aaaabbbb");
    const manifest = await source.snapshot();

    const restored = new PGliteCasFilesystem(createBlobStore(), 4);

    await expect(restored.restore(manifest)).rejects.toThrow("blob not found");
  });

  it("rejects corrupted filesystem snapshot manifests", async () => {
    const blobs = createBlobStore();
    const source = new PGliteCasFilesystem(blobs, 4);
    source.writeFile("/base/a", "aaaabbbb");
    const manifest = await source.snapshot();
    const corrupted = {
      ...manifest,
      files: [
        {
          ...manifest.files[0]!,
          size: manifest.files[0]!.size + 1,
        },
      ],
    };
    const restored = new PGliteCasFilesystem(blobs, 4);

    await expect(restored.restore(corrupted)).rejects.toThrow("file size mismatch");
  });
});

function createFs(): PGliteCasFilesystem {
  return new PGliteCasFilesystem(createBlobStore(), 4);
}

function createBlobStore(): MemoryBlobStore {
  return new MemoryBlobStore((content) => createHash("sha256").update(content).digest("hex"));
}
