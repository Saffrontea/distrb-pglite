import type { BlobRef, BlobStore } from "./blob-store.js";

// Generic content-addressed filesystem shape. It does not know about
// partitioning, parent snapshots, or host data-directory integrity checks.
export type SnapshotFile<TBlock extends BlobRef = BlobRef> = {
  path: string;
  size: number;
  blocks: TBlock[];
};

export type SnapshotFilesystemManifest<TFile extends SnapshotFile = SnapshotFile> = {
  version: 1;
  chunkSize: number;
  directories: string[];
  files: TFile[];
};

export type SnapshotFilesystem = {
  writeFile(path: string, content: Uint8Array): Promise<void>;
  readFile(path: string): Promise<Uint8Array>;
  snapshot(): Promise<SnapshotFilesystemManifest>;
  restore(manifest: SnapshotFilesystemManifest): Promise<void>;
};

export class CasSnapshotFilesystem implements SnapshotFilesystem {
  private readonly files = new Map<string, Uint8Array>();

  constructor(
    private readonly blobs: BlobStore,
    private readonly chunkSize = 64 * 1024,
  ) {
    assertPositiveInteger(chunkSize, "chunkSize");
  }

  async writeFile(path: string, content: Uint8Array): Promise<void> {
    this.files.set(path, new Uint8Array(content));
  }

  async readFile(path: string): Promise<Uint8Array> {
    const content = this.files.get(path);
    if (!content) {
      throw new Error(`file not found: ${path}`);
    }

    return new Uint8Array(content);
  }

  async snapshot(): Promise<SnapshotFilesystemManifest> {
    const files: SnapshotFile[] = [];
    for (const [path, content] of [...this.files].sort(([left], [right]) => left.localeCompare(right))) {
      const blocks: BlobRef[] = [];
      for (let offset = 0; offset < content.length || (content.length === 0 && offset === 0); offset += this.chunkSize) {
        const chunk = content.subarray(offset, Math.min(offset + this.chunkSize, content.length));
        blocks.push(await this.blobs.put(chunk));
        if (content.length === 0) {
          break;
        }
      }
      files.push({ path, size: content.length, blocks });
    }

    return {
      version: 1,
      chunkSize: this.chunkSize,
      directories: [],
      files,
    };
  }

  async restore(manifest: SnapshotFilesystemManifest): Promise<void> {
    assertPositiveInteger(manifest.chunkSize, "manifest chunkSize");
    const restoredFiles = new Map<string, Uint8Array>();
    for (const file of manifest.files) {
      const chunks: Uint8Array[] = [];
      for (const block of file.blocks) {
        const content = await this.blobs.get(block.hash);
        if (content.length !== block.size) {
          throw new Error(`blob size mismatch: ${block.hash}`);
        }
        chunks.push(content);
      }
      const restored = concat(chunks);
      if (restored.length !== file.size) {
        throw new Error(`file size mismatch: ${file.path}`);
      }
      restoredFiles.set(file.path, restored);
    }
    this.files.clear();
    for (const [path, content] of restoredFiles) {
      this.files.set(path, content);
    }
  }
}

function assertPositiveInteger(value: number, name: string): void {
  if (!Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
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
