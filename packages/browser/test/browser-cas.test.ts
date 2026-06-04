import { describe, expect, it } from "vitest";
import { PGlite } from "@electric-sql/pglite";
import {
  BrowserPGliteCasFilesystem,
  BrowserSha256BlobStore,
  diffMissingBrowserCasBlocks,
  gcBrowserCasBlocks,
  restoreBrowserFilesystemFromCas,
  snapshotBrowserFilesystemToCas,
  transferMissingBrowserCasBlocks,
  validateBrowserCasManifest,
  type BrowserCasBlockStore,
  type BrowserCasManifest,
  type BrowserSnapshotDataFilesystem,
} from "../src/index.js";

describe("browser CAS helpers", () => {
  it("snapshots, transfers, and restores a browser filesystem without Node APIs", async () => {
    const sourceFs = new MemoryDataFilesystem();
    await sourceFs.mkdir("base");
    await sourceFs.writeFile("base/one", new TextEncoder().encode("aaaabbbb"));
    await sourceFs.writeFile("base/two", new TextEncoder().encode("aaaacccc"));

    const sourceCas = new MemoryCasBlockStore();
    const targetCas = new MemoryCasBlockStore();
    const manifest = await snapshotBrowserFilesystemToCas({
      fs: sourceFs,
      casStore: sourceCas,
      chunkSize: 4,
      snapshotId: "snapshot-1",
    });

    expect(manifest.files.map((file) => file.path)).toEqual(["base/one", "base/two"]);
    expect(manifest.directories).toEqual(["base"]);

    const missing = diffMissingBrowserCasBlocks(manifest, await targetCas.listBlockHashes());
    expect(missing.length).toBeGreaterThan(0);
    await transferMissingBrowserCasBlocks({
      manifest,
      sourceCasStore: sourceCas,
      targetCasStore: targetCas,
    });

    const restoredFs = new MemoryDataFilesystem();
    await restoreBrowserFilesystemFromCas({
      manifest,
      casStore: targetCas,
      fs: restoredFs,
    });

    expect(new TextDecoder().decode(await restoredFs.readFile("base/one"))).toBe("aaaabbbb");
    expect(new TextDecoder().decode(await restoredFs.readFile("base/two"))).toBe("aaaacccc");
  });

  it("restores through chunked filesystem writes without concatenating whole files", async () => {
    const sourceFs = new MemoryDataFilesystem();
    await sourceFs.writeFile("large", new TextEncoder().encode("aaaabbbbcccc"));
    const sourceCas = new MemoryCasBlockStore();
    const manifest = await snapshotBrowserFilesystemToCas({
      fs: sourceFs,
      casStore: sourceCas,
      chunkSize: 4,
      snapshotId: "snapshot-1",
    });
    const restoredFs = new ChunkedMemoryDataFilesystem();

    await restoreBrowserFilesystemFromCas({
      manifest,
      casStore: sourceCas,
      fs: restoredFs,
    });

    expect(restoredFs.writeFileCalls).toBe(0);
    expect(restoredFs.chunkWrites).toBe(3);
    expect(new TextDecoder().decode(await restoredFs.readFile("large"))).toBe("aaaabbbbcccc");
  });

  it("rejects chunked restores when the file hash does not match verified blocks", async () => {
    const sourceFs = new MemoryDataFilesystem();
    await sourceFs.writeFile("large", new TextEncoder().encode("aaaabbbbcccc"));
    const sourceCas = new MemoryCasBlockStore();
    const manifest = await snapshotBrowserFilesystemToCas({
      fs: sourceFs,
      casStore: sourceCas,
      chunkSize: 4,
      snapshotId: "snapshot-1",
    });
    const restoredFs = new ChunkedMemoryDataFilesystem();

    await expect(restoreBrowserFilesystemFromCas({
      manifest: {
        ...manifest,
        files: [{
          ...manifest.files[0]!,
          sha256: await digest(new TextEncoder().encode("different")),
        }],
      },
      casStore: sourceCas,
      fs: restoredFs,
    })).rejects.toThrow("CAS file integrity check failed: large");
  });

  it("rejects invalid chunking before browser snapshotting", async () => {
    const sourceFs = new MemoryDataFilesystem();
    await sourceFs.writeFile("payload", new TextEncoder().encode("payload"));
    const sourceCas = new MemoryCasBlockStore();

    await expect(snapshotBrowserFilesystemToCas({
      fs: sourceFs,
      casStore: sourceCas,
      chunkSize: 0,
    })).rejects.toThrow("chunkSize must be a positive integer");
    await expect(snapshotBrowserFilesystemToCas({
      fs: sourceFs,
      casStore: sourceCas,
      chunking: { strategy: "page-aligned", pageSize: 0, maxChunkSize: 4096 },
    })).rejects.toThrow("pageSize must be a positive integer");
    await expect(snapshotBrowserFilesystemToCas({
      fs: sourceFs,
      casStore: sourceCas,
      chunking: { strategy: "page-aligned", pageSize: 4096, maxChunkSize: 2048 },
    })).rejects.toThrow("maxChunkSize must be greater than or equal to pageSize");
  });

  it("rejects restore manifests whose browser file paths escape the target root", async () => {
    const content = new TextEncoder().encode("escape");
    const hash = await digest(content);
    const sourceCas = new MemoryCasBlockStore();
    await sourceCas.writeBlock(hash, content);
    const restoredFs = new MemoryDataFilesystem();

    await expect(restoreBrowserFilesystemFromCas({
      manifest: {
        version: 1,
        snapshotId: "bad-path",
        parentSnapshotIds: [],
        chunkSize: 64 * 1024,
        directories: [],
        files: [{
          path: "../outside",
          size: content.length,
          sha256: hash,
          blocks: [{ hash, size: content.length }],
        }],
      },
      casStore: sourceCas,
      fs: restoredFs,
    })).rejects.toThrow("logical path escapes filesystem root");
  });

  it("rejects restore manifests with non-canonical browser CAS block hashes", async () => {
    const restoredFs = new MemoryDataFilesystem();

    await expect(restoreBrowserFilesystemFromCas({
      manifest: {
        version: 1,
        snapshotId: "bad-hash",
        parentSnapshotIds: [],
        chunkSize: 64 * 1024,
        directories: [],
        files: [{
          path: "payload",
          size: 0,
          sha256: "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
          blocks: [{ hash: "../escape", size: 0 }],
        }],
      },
      casStore: new MemoryCasBlockStore(),
      fs: restoredFs,
    })).rejects.toThrow("browser CAS manifest files[0].blocks[0].hash must be a 64-character lowercase sha256 hash");
  });

  it("rejects restore manifests with invalid browser chunk sizes", async () => {
    await expect(restoreBrowserFilesystemFromCas({
      manifest: {
        version: 1,
        snapshotId: "bad-chunk-size",
        parentSnapshotIds: [],
        chunkSize: 0,
        directories: [],
        files: [],
      },
      casStore: new MemoryCasBlockStore(),
      fs: new MemoryDataFilesystem(),
    })).rejects.toThrow("browser CAS manifest chunkSize must be a positive integer");
  });

  it("rejects invalid browser CAS manifests before diff, transfer, and GC", async () => {
    const valid = browserCasManifest("snapshot-1", "a".repeat(64), 1);
    const invalidHash = {
      ...valid,
      files: [{
        ...valid.files[0]!,
        blocks: [{ hash: "not-a-hash", size: 1 }],
      }],
    };
    const invalidSnapshotId = {
      ...valid,
      snapshotId: "snapshot-2",
    };
    const duplicatePath = {
      ...valid,
      files: [
        valid.files[0]!,
        {
          ...valid.files[0]!,
          sha256: "b".repeat(64),
          blocks: [{ hash: "b".repeat(64), size: 1 }],
        },
      ],
    };
    const duplicateDirectory = {
      ...valid,
      directories: ["base", "base"],
    };
    const fileDirectoryCollision = {
      ...valid,
      directories: [valid.files[0]!.path],
    };

    expect(() => validateBrowserCasManifest(invalidSnapshotId, "snapshot-1"))
      .toThrow("browser CAS manifest snapshotId mismatch: snapshot-2 !== snapshot-1");
    expect(() => validateBrowserCasManifest(duplicatePath))
      .toThrow("duplicate browser CAS manifest file path");
    expect(() => validateBrowserCasManifest(duplicateDirectory))
      .toThrow("duplicate browser CAS manifest directory path");
    expect(() => validateBrowserCasManifest(fileDirectoryCollision))
      .toThrow("browser CAS manifest path is both a file and directory");
    expect(() => diffMissingBrowserCasBlocks(invalidHash, new Set()))
      .toThrow("browser CAS manifest files[0].blocks[0].hash must be a 64-character lowercase sha256 hash");
    expect(() => diffMissingBrowserCasBlocks(duplicatePath, new Set()))
      .toThrow("duplicate browser CAS manifest file path");
    expect(() => diffMissingBrowserCasBlocks(duplicateDirectory, new Set()))
      .toThrow("duplicate browser CAS manifest directory path");
    expect(() => diffMissingBrowserCasBlocks(fileDirectoryCollision, new Set()))
      .toThrow("browser CAS manifest path is both a file and directory");
    await expect(transferMissingBrowserCasBlocks({
      manifest: invalidHash,
      sourceCasStore: new MemoryCasBlockStore(),
      targetCasStore: new MemoryCasBlockStore(),
    })).rejects.toThrow("browser CAS manifest files[0].blocks[0].hash must be a 64-character lowercase sha256 hash");
    await expect(transferMissingBrowserCasBlocks({
      manifest: duplicatePath,
      sourceCasStore: new MemoryCasBlockStore(),
      targetCasStore: new MemoryCasBlockStore(),
    })).rejects.toThrow("duplicate browser CAS manifest file path");
    await expect(gcBrowserCasBlocks({
      casStore: new MemoryCasBlockStore(),
      retainedManifests: [invalidHash],
    })).rejects.toThrow("browser CAS manifest files[0].blocks[0].hash must be a 64-character lowercase sha256 hash");
    await expect(gcBrowserCasBlocks({
      casStore: new MemoryCasBlockStore(),
      retainedManifests: [duplicatePath],
    })).rejects.toThrow("duplicate browser CAS manifest file path");
  });
});

describe("BrowserPGliteCasFilesystem", () => {
  it("supports file operations and CAS restore with Web Crypto hashes", async () => {
    const blobs = new BrowserSha256BlobStore();
    const source = new BrowserPGliteCasFilesystem(blobs, 4);
    source.mkdir("/base/pgdata", { recursive: true });
    const fd = source.open("/base/pgdata/file", "w+");
    const input = new TextEncoder().encode("hello browser");

    expect(source.write(fd, input, 0, input.length, 0)).toBe(input.length);
    expect(source.fstat(fd).size).toBe(input.length);

    const output = new Uint8Array(input.length);
    expect(source.read(fd, output, 0, output.length, 0)).toBe(input.length);
    expect(new TextDecoder().decode(output)).toBe("hello browser");
    source.close(fd);

    const manifest = await source.snapshot();
    const restored = new BrowserPGliteCasFilesystem(blobs, 4);
    await restored.restore(manifest);

    expect(restored.readdir("/base/pgdata")).toEqual(["file"]);
    expect(restored.lstat("/base/pgdata/file").size).toBe(input.length);
  });

  it("runs PGlite and restores a readable database from a browser CAS filesystem snapshot", async () => {
    const blobs = new BrowserSha256BlobStore();
    const sourceFs = new BrowserPGliteCasFilesystem(blobs, 16 * 1024);
    const sourceDb = new PGlite({ fs: sourceFs, relaxedDurability: false });
    await sourceDb.waitReady;
    await sourceDb.exec(`
      CREATE TABLE readings (id text PRIMARY KEY, value int NOT NULL);
      INSERT INTO readings (id, value) VALUES ('a', 1), ('b', 2);
    `);
    await sourceDb.close();

    const manifest = await sourceFs.snapshot();
    const restoredFs = new BrowserPGliteCasFilesystem(blobs, 16 * 1024);
    await restoredFs.restore(manifest);
    const restoredDb = new PGlite({ fs: restoredFs, relaxedDurability: false });
    await restoredDb.waitReady;
    const result = await restoredDb.query<{ total: string }>(
      "SELECT sum(value)::int AS total FROM readings",
    );

    expect(result.rows).toEqual([{ total: 3 }]);
    await restoredDb.close();
  });
});

class MemoryCasBlockStore implements BrowserCasBlockStore {
  private readonly blocks = new Map<string, Uint8Array>();

  async readBlock(hash: string): Promise<Uint8Array> {
    const content = this.blocks.get(hash);
    if (!content) {
      throw new Error(`block not found: ${hash}`);
    }
    return new Uint8Array(content);
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

class MemoryDataFilesystem implements BrowserSnapshotDataFilesystem {
  private readonly files = new Map<string, Uint8Array>();
  private readonly directories = new Set<string>([""]);

  async listDir(path: string): Promise<Array<{ name: string; type: "file" | "directory" }>> {
    const normalized = normalize(path);
    const prefix = normalized ? `${normalized}/` : "";
    const entries = new Map<string, "file" | "directory">();

    for (const dir of this.directories) {
      if (!dir.startsWith(prefix) || dir === normalized) {
        continue;
      }
      const rest = dir.slice(prefix.length);
      if (rest && !rest.includes("/")) {
        entries.set(rest, "directory");
      }
    }

    for (const file of this.files.keys()) {
      if (!file.startsWith(prefix)) {
        continue;
      }
      const rest = file.slice(prefix.length);
      if (rest && !rest.includes("/")) {
        entries.set(rest, "file");
      }
    }

    return [...entries]
      .map(([name, type]) => ({ name, type }))
      .sort((left, right) => left.name.localeCompare(right.name));
  }

  async readFile(path: string): Promise<Uint8Array> {
    const content = this.files.get(normalize(path));
    if (!content) {
      throw new Error(`file not found: ${path}`);
    }
    return new Uint8Array(content);
  }

  async writeFile(path: string, data: Uint8Array): Promise<void> {
    const normalized = normalize(path);
    await this.mkdir(dirname(normalized));
    this.files.set(normalized, new Uint8Array(data));
  }

  async mkdir(path: string): Promise<void> {
    const normalized = normalize(path);
    const parts = normalized ? normalized.split("/") : [];
    let current = "";
    this.directories.add(current);
    for (const part of parts) {
      current = current ? `${current}/${part}` : part;
      this.directories.add(current);
    }
  }
}

class ChunkedMemoryDataFilesystem extends MemoryDataFilesystem {
  writeFileCalls = 0;
  chunkWrites = 0;

  override async writeFile(path: string, data: Uint8Array): Promise<void> {
    this.writeFileCalls += 1;
    await super.writeFile(path, data);
  }

  override async writeFileChunks(
    path: string,
    chunks: AsyncIterable<Uint8Array>,
    expectedSize: number,
  ): Promise<void> {
    const restored: Uint8Array[] = [];
    let size = 0;
    for await (const chunk of chunks) {
      this.chunkWrites += 1;
      restored.push(new Uint8Array(chunk));
      size += chunk.length;
    }
    if (size !== expectedSize) {
      throw new Error("size mismatch");
    }
    await super.writeFile(path, concat(restored));
  }
}

function concat(chunks: Uint8Array[]): Uint8Array {
  const total = chunks.reduce((size, chunk) => size + chunk.length, 0);
  const result = new Uint8Array(total);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result;
}

function normalize(path: string): string {
  return path
    .replaceAll("\\", "/")
    .replace(/^\/+/, "")
    .replace(/\/+/g, "/")
    .replace(/\/$/, "");
}

function dirname(path: string): string {
  const index = path.lastIndexOf("/");
  return index <= 0 ? "" : path.slice(0, index);
}

async function digest(content: Uint8Array): Promise<string> {
  const hash = await crypto.subtle.digest("SHA-256", content);
  return [...new Uint8Array(hash)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function browserCasManifest(snapshotId: string, hash: string, size: number): BrowserCasManifest {
  return {
    version: 1,
    snapshotId,
    parentSnapshotIds: [],
    chunkSize: 64 * 1024,
    directories: [],
    files: [{
      path: `${snapshotId}.bin`,
      size,
      sha256: hash,
      components: ["data"],
      blocks: [{ hash, size }],
    }],
  };
}
