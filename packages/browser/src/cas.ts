import type {
  BlobRef,
  SnapshotFile,
  SnapshotFilesystemManifest,
  StorageSnapshotComponent,
} from "@distrb-pglite/core";
import {
  inventoryBrowserFilesystem,
  normalizeBrowserLogicalPath,
  type BrowserDataDirInventory,
  type BrowserFileInventoryEntry,
  type BrowserSnapshotDataFilesystem,
} from "./filesystem.js";
import { sha256 } from "./blob-store.js";

export type BrowserCasBlock = BlobRef;

export type BrowserCasBlockStore = {
  readBlock(hash: string): Promise<Uint8Array>;
  writeBlock(hash: string, content: Uint8Array): Promise<void>;
  listBlockHashes(): Promise<Set<string>>;
  deleteBlock(hash: string): Promise<void>;
};

export class OpfsCasBlockStore implements BrowserCasBlockStore {
  constructor(private readonly root: FileSystemDirectoryHandle) {}

  static async open(rootName = "distrb-pglite-cas"): Promise<OpfsCasBlockStore> {
    if (!navigator.storage?.getDirectory) {
      throw new Error("OPFS is not available in this browser context");
    }
    const opfsRoot = await navigator.storage.getDirectory();
    const root = await opfsRoot.getDirectoryHandle(rootName, { create: true });
    return new OpfsCasBlockStore(root);
  }

  async readBlock(hash: string): Promise<Uint8Array> {
    const file = await this.root.getFileHandle(hash);
    return new Uint8Array(await (await file.getFile()).arrayBuffer());
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    const file = await this.root.getFileHandle(hash, { create: true });
    const writable = await file.createWritable();
    await writable.write(new Uint8Array(content));
    await writable.close();
  }

  async listBlockHashes(): Promise<Set<string>> {
    const hashes = new Set<string>();
    for await (const [name, handle] of this.root.entries()) {
      if (handle.kind === "file" && /^[0-9a-f]{64}$/.test(name)) {
        hashes.add(name);
      }
    }
    return hashes;
  }

  async deleteBlock(hash: string): Promise<void> {
    await this.root.removeEntry(hash).catch((error: unknown) => {
      if (!isNotFoundError(error)) {
        throw error;
      }
    });
  }
}

export type BrowserCasFileManifest = SnapshotFile<BrowserCasBlock> & {
  sha256: string;
  components?: StorageSnapshotComponent[];
};

export type BrowserCasManifest = SnapshotFilesystemManifest<BrowserCasFileManifest> & {
  snapshotId: string;
  parentSnapshotIds: string[];
  chunking?: BrowserCasChunking;
};

export type BrowserCasSnapshotFilter = {
  includeFile(file: BrowserFileInventoryEntry): boolean;
  includeDirectory?(directory: string): boolean;
  componentsForFile?(file: BrowserFileInventoryEntry): StorageSnapshotComponent[];
};

export type BrowserCasChunking =
  | { strategy: "fixed"; chunkSize: number }
  | { strategy: "page-aligned"; pageSize: number; maxChunkSize: number };

export type BrowserCasGcResult = {
  referenced: string[];
  deleted: string[];
  retained: string[];
};

export async function snapshotBrowserFilesystemToCas(options: {
  fs: BrowserSnapshotDataFilesystem;
  casStore: BrowserCasBlockStore;
  chunkSize?: number;
  chunking?: BrowserCasChunking;
  snapshotId?: string;
  parentSnapshotIds?: string[];
  filter?: BrowserCasSnapshotFilter;
}): Promise<BrowserCasManifest> {
  const chunking = options.chunking ?? fixedChunking(options.chunkSize ?? 64 * 1024);
  const chunkSize = manifestChunkSize(chunking);
  const inventory = await inventoryBrowserFilesystem({ fs: options.fs });
  const selectedFiles = options.filter
    ? inventory.files.filter((file) => options.filter?.includeFile(file) ?? false)
    : inventory.files;
  const files: BrowserCasFileManifest[] = [];

  for (const file of selectedFiles) {
    const content = await options.fs.readFile(file.path);
    const blocks = await writeBlocks(options.casStore, content, chunking);
    const components = options.filter?.componentsForFile?.(file);
    files.push({
      ...file,
      ...(components ? { components } : {}),
      blocks,
    });
  }

  return {
    version: 1,
    snapshotId: options.snapshotId ?? crypto.randomUUID(),
    parentSnapshotIds: options.parentSnapshotIds ?? [],
    chunkSize,
    chunking,
    directories: options.filter
      ? selectedDirectories(inventory.directories, selectedFiles, options.filter)
      : inventory.directories,
    files,
  };
}

export async function restoreBrowserFilesystemFromCas(options: {
  manifest: BrowserCasManifest;
  casStore: BrowserCasBlockStore;
  fs: BrowserSnapshotDataFilesystem;
}): Promise<void> {
  await options.fs.mkdir("");
  for (const directory of options.manifest.directories) {
    await options.fs.mkdir(directory);
  }

  for (const file of options.manifest.files) {
    const chunks: Uint8Array[] = [];
    for (const block of file.blocks) {
      const stored = await options.casStore.readBlock(block.hash);
      if (stored.length !== block.size || await sha256(stored) !== block.hash) {
        throw new Error(`CAS block integrity check failed: ${block.hash}`);
      }
      chunks.push(stored);
    }

    const restored = concat(chunks);
    if (restored.length !== file.size || await sha256(restored) !== file.sha256) {
      throw new Error(`CAS file integrity check failed: ${file.path}`);
    }

    await options.fs.writeFile(file.path, restored);
  }
}

export function diffMissingBrowserCasBlocks(
  manifest: BrowserCasManifest,
  availableBlockHashes: ReadonlySet<string>,
): BrowserCasBlock[] {
  const missing = new Map<string, BrowserCasBlock>();
  for (const file of manifest.files) {
    for (const block of file.blocks) {
      if (!availableBlockHashes.has(block.hash)) {
        missing.set(block.hash, block);
      }
    }
  }
  return [...missing.values()].sort((left, right) => left.hash.localeCompare(right.hash));
}

export async function transferMissingBrowserCasBlocks(options: {
  manifest: BrowserCasManifest;
  sourceCasStore: BrowserCasBlockStore;
  targetCasStore: BrowserCasBlockStore;
  availableBlockHashes?: ReadonlySet<string>;
}): Promise<BrowserCasBlock[]> {
  const available = options.availableBlockHashes ?? await options.targetCasStore.listBlockHashes();
  const missing = diffMissingBrowserCasBlocks(options.manifest, available);

  for (const block of missing) {
    const content = await options.sourceCasStore.readBlock(block.hash);
    if (content.length !== block.size || await sha256(content) !== block.hash) {
      throw new Error(`CAS source block integrity check failed: ${block.hash}`);
    }
    await options.targetCasStore.writeBlock(block.hash, content);
  }

  return missing;
}

export async function gcBrowserCasBlocks(options: {
  casStore: BrowserCasBlockStore;
  retainedManifests: readonly BrowserCasManifest[];
  dryRun?: boolean;
}): Promise<BrowserCasGcResult> {
  const referenced = new Set<string>();
  for (const manifest of options.retainedManifests) {
    for (const file of manifest.files) {
      for (const block of file.blocks) {
        referenced.add(block.hash);
      }
    }
  }

  const available = await options.casStore.listBlockHashes();
  const deleted: string[] = [];
  const retained: string[] = [];
  for (const hash of [...available].sort()) {
    if (referenced.has(hash)) {
      retained.push(hash);
      continue;
    }

    deleted.push(hash);
    if (!options.dryRun) {
      await options.casStore.deleteBlock(hash);
    }
  }

  return {
    referenced: [...referenced].sort(),
    deleted,
    retained,
  };
}

export function browserManifestFilesEqualInventory(
  manifest: BrowserCasManifest,
  inventory: Pick<BrowserDataDirInventory, "directories" | "files"> | readonly BrowserFileInventoryEntry[],
): boolean {
  if (isFileInventoryEntries(inventory)) {
    return JSON.stringify(manifest.files.map(stripBlocks)) === JSON.stringify(inventory);
  }

  return JSON.stringify({
    directories: manifest.directories,
    files: manifest.files.map(stripBlocks),
  }) === JSON.stringify({
    directories: inventory.directories,
    files: inventory.files,
  });
}

async function writeBlocks(
  casStore: BrowserCasBlockStore,
  content: Uint8Array,
  chunking: BrowserCasChunking,
): Promise<BrowserCasBlock[]> {
  const blocks: BrowserCasBlock[] = [];
  for (const [start, end] of chunkRanges(content.length, chunking)) {
    const chunk = content.subarray(start, end);
    const hash = await sha256(chunk);
    await casStore.writeBlock(hash, chunk);
    blocks.push({ hash, size: chunk.length });
  }
  return blocks;
}

function chunkRanges(size: number, chunking: BrowserCasChunking): Array<[number, number]> {
  if (size === 0) {
    return [[0, 0]];
  }

  const chunkSize = manifestChunkSize(chunking);
  const ranges: Array<[number, number]> = [];
  for (let offset = 0; offset < size; offset += chunkSize) {
    ranges.push([offset, Math.min(offset + chunkSize, size)]);
  }
  return ranges;
}

function fixedChunking(chunkSize: number): BrowserCasChunking {
  return { strategy: "fixed", chunkSize };
}

function manifestChunkSize(chunking: BrowserCasChunking): number {
  if (chunking.strategy === "fixed") {
    return chunking.chunkSize;
  }
  return Math.max(chunking.pageSize, Math.floor(chunking.maxChunkSize / chunking.pageSize) * chunking.pageSize);
}

function selectedDirectories(
  directories: readonly string[],
  files: readonly BrowserFileInventoryEntry[],
  filter: BrowserCasSnapshotFilter,
): string[] {
  if (filter.includeDirectory) {
    return directories.filter((directory) => filter.includeDirectory?.(directory) ?? false);
  }

  const required = new Set<string>();
  for (const file of files) {
    let path = dirname(file.path);
    while (path) {
      required.add(path);
      path = dirname(path);
    }
  }

  return directories.filter((directory) => required.has(directory));
}

function dirname(path: string): string {
  const normalized = normalizeBrowserLogicalPath(path);
  const index = normalized.lastIndexOf("/");
  return index <= 0 ? "" : normalized.slice(0, index);
}

function stripBlocks(file: BrowserCasFileManifest): BrowserFileInventoryEntry {
  return {
    path: file.path,
    size: file.size,
    sha256: file.sha256,
  };
}

function isFileInventoryEntries(
  inventory: Pick<BrowserDataDirInventory, "directories" | "files"> | readonly BrowserFileInventoryEntry[],
): inventory is readonly BrowserFileInventoryEntry[] {
  return Array.isArray(inventory);
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

function isNotFoundError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "NotFoundError";
}
