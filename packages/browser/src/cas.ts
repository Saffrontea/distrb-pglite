import {
  uuidV7,
  type BlobRef,
  type SnapshotFile,
  type SnapshotFilesystemManifest,
  type StorageSnapshotComponent,
} from "@distrb-pglite/core";
import {
  inventoryBrowserFilesystem,
  normalizeBrowserLogicalPath,
  validateBrowserLogicalPath,
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
    assertCasBlockHash(hash);
    const file = await this.root.getFileHandle(hash);
    return new Uint8Array(await (await file.getFile()).arrayBuffer());
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    assertCasBlockHash(hash);
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
    assertCasBlockHash(hash);
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
    snapshotId: options.snapshotId ?? uuidV7(),
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
  const manifest = validateBrowserCasManifest(options.manifest);
  await options.fs.mkdir("");
  for (const directory of manifest.directories) {
    validateBrowserLogicalPath(directory);
    await options.fs.mkdir(directory);
  }

  for (const file of manifest.files) {
    validateBrowserLogicalPath(file.path);
    if (options.fs.writeFileChunks) {
      const chunks: Uint8Array[] = [];
      await options.fs.writeFileChunks(
        file.path,
        collectVerifiedBlockChunks(options.casStore, file.blocks, chunks),
        file.size,
      );
      const restored = concat(chunks);
      if (restored.length !== file.size || (await sha256(restored)) !== file.sha256) {
        throw new Error(`CAS file integrity check failed: ${file.path}`);
      }
      continue;
    }

    const chunks: Uint8Array[] = [];
    for (const block of file.blocks) {
      chunks.push(await readVerifiedBlock(options.casStore, block));
    }
    const restored = concat(chunks);
    if (restored.length !== file.size || (await sha256(restored)) !== file.sha256) {
      throw new Error(`CAS file integrity check failed: ${file.path}`);
    }
    await options.fs.writeFile(file.path, restored);
  }
}

export function diffMissingBrowserCasBlocks(
  manifest: BrowserCasManifest,
  availableBlockHashes: ReadonlySet<string>,
): BrowserCasBlock[] {
  const validated = validateBrowserCasManifest(manifest);
  const missing = new Map<string, BrowserCasBlock>();
  for (const file of validated.files) {
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
  const manifest = validateBrowserCasManifest(options.manifest);
  const available =
    options.availableBlockHashes ?? (await options.targetCasStore.listBlockHashes());
  const missing = diffMissingBrowserCasBlocks(manifest, available);

  for (const block of missing) {
    const content = await options.sourceCasStore.readBlock(block.hash);
    if (content.length !== block.size || (await sha256(content)) !== block.hash) {
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
    for (const file of validateBrowserCasManifest(manifest).files) {
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

export function validateBrowserCasManifest(
  manifest: unknown,
  expectedSnapshotId?: string,
): BrowserCasManifest {
  if (!isRecord(manifest)) {
    throw new Error("browser CAS manifest must be an object");
  }
  if (manifest.version !== 1) {
    throw new Error(`invalid browser CAS manifest version: ${String(manifest.version)}`);
  }
  assertNonEmptyString(manifest.snapshotId, "browser CAS manifest snapshotId");
  if (expectedSnapshotId !== undefined && manifest.snapshotId !== expectedSnapshotId) {
    throw new Error(
      `browser CAS manifest snapshotId mismatch: ${manifest.snapshotId} !== ${expectedSnapshotId}`,
    );
  }
  assertPositiveInteger(manifest.chunkSize, "browser CAS manifest chunkSize");

  if (!Array.isArray(manifest.parentSnapshotIds)) {
    throw new Error("browser CAS manifest parentSnapshotIds must be an array");
  }
  for (const [index, parentSnapshotId] of manifest.parentSnapshotIds.entries()) {
    assertNonEmptyString(parentSnapshotId, `browser CAS manifest parentSnapshotIds[${index}]`);
  }

  if (!Array.isArray(manifest.directories)) {
    throw new Error("browser CAS manifest directories must be an array");
  }
  const directoryPaths = new Set<string>();
  for (const [index, directory] of manifest.directories.entries()) {
    assertNonEmptyString(directory, `browser CAS manifest directories[${index}]`);
    const path = validateBrowserLogicalPath(directory);
    if (directoryPaths.has(path)) {
      throw new Error(`duplicate browser CAS manifest directory path: ${path}`);
    }
    directoryPaths.add(path);
  }

  if (!Array.isArray(manifest.files)) {
    throw new Error("browser CAS manifest files must be an array");
  }
  const filePaths = new Set<string>();
  for (const [index, file] of manifest.files.entries()) {
    validateBrowserCasFileManifest(file, index);
    const path = validateBrowserLogicalPath((file as { path: string }).path);
    if (filePaths.has(path)) {
      throw new Error(`duplicate browser CAS manifest file path: ${path}`);
    }
    if (directoryPaths.has(path)) {
      throw new Error(`browser CAS manifest path is both a file and directory: ${path}`);
    }
    filePaths.add(path);
  }

  if (manifest.chunking !== undefined) {
    validateBrowserCasChunking(manifest.chunking);
  }

  return manifest as BrowserCasManifest;
}

export function browserManifestFilesEqualInventory(
  manifest: BrowserCasManifest,
  inventory:
    | Pick<BrowserDataDirInventory, "directories" | "files">
    | readonly BrowserFileInventoryEntry[],
): boolean {
  if (isFileInventoryEntries(inventory)) {
    return JSON.stringify(manifest.files.map(stripBlocks)) === JSON.stringify(inventory);
  }

  return (
    JSON.stringify({
      directories: manifest.directories,
      files: manifest.files.map(stripBlocks),
    }) ===
    JSON.stringify({
      directories: inventory.directories,
      files: inventory.files,
    })
  );
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
  assertPositiveInteger(chunkSize, "chunkSize");
  return { strategy: "fixed", chunkSize };
}

function manifestChunkSize(chunking: BrowserCasChunking): number {
  if (chunking.strategy === "fixed") {
    assertPositiveInteger(chunking.chunkSize, "chunkSize");
    return chunking.chunkSize;
  }
  assertPositiveInteger(chunking.pageSize, "pageSize");
  assertPositiveInteger(chunking.maxChunkSize, "maxChunkSize");
  if (chunking.maxChunkSize < chunking.pageSize) {
    throw new Error(
      `maxChunkSize must be greater than or equal to pageSize: ${chunking.maxChunkSize} < ${chunking.pageSize}`,
    );
  }
  return Math.max(
    chunking.pageSize,
    Math.floor(chunking.maxChunkSize / chunking.pageSize) * chunking.pageSize,
  );
}

function validateBrowserCasFileManifest(file: unknown, index: number): void {
  if (!isRecord(file)) {
    throw new Error(`browser CAS manifest files[${index}] must be an object`);
  }
  assertNonEmptyString(file.path, `browser CAS manifest files[${index}].path`);
  assertNonNegativeInteger(file.size, `browser CAS manifest files[${index}].size`);
  assertCasBlockHash(file.sha256, `browser CAS manifest files[${index}].sha256`);
  if (!Array.isArray(file.blocks)) {
    throw new Error(`browser CAS manifest files[${index}].blocks must be an array`);
  }
  for (const [blockIndex, block] of file.blocks.entries()) {
    validateBrowserCasBlock(block, `browser CAS manifest files[${index}].blocks[${blockIndex}]`);
  }
  if (file.components !== undefined) {
    if (!Array.isArray(file.components) || file.components.length === 0) {
      throw new Error(`browser CAS manifest files[${index}].components must be a non-empty array`);
    }
    const seen = new Set<StorageSnapshotComponent>();
    for (const component of file.components) {
      if (component !== "catalog" && component !== "data") {
        throw new Error(`invalid browser CAS manifest file component: ${String(component)}`);
      }
      if (seen.has(component)) {
        throw new Error(`duplicate browser CAS manifest file component: ${component}`);
      }
      seen.add(component);
    }
  }
}

function validateBrowserCasBlock(block: unknown, name: string): void {
  if (!isRecord(block)) {
    throw new Error(`${name} must be an object`);
  }
  assertCasBlockHash(block.hash, `${name}.hash`);
  assertNonNegativeInteger(block.size, `${name}.size`);
}

function validateBrowserCasChunking(chunking: unknown): void {
  if (!isRecord(chunking)) {
    throw new Error("browser CAS manifest chunking must be an object");
  }
  if (chunking.strategy === "fixed") {
    assertPositiveInteger(chunking.chunkSize, "browser CAS manifest chunking.chunkSize");
    return;
  }
  if (chunking.strategy === "page-aligned") {
    assertPositiveInteger(chunking.pageSize, "browser CAS manifest chunking.pageSize");
    assertPositiveInteger(chunking.maxChunkSize, "browser CAS manifest chunking.maxChunkSize");
    if (chunking.maxChunkSize < chunking.pageSize) {
      throw new Error(
        `browser CAS manifest chunking maxChunkSize must be greater than or equal to pageSize: ${chunking.maxChunkSize} < ${chunking.pageSize}`,
      );
    }
    return;
  }
  throw new Error(`invalid browser CAS manifest chunking strategy: ${String(chunking.strategy)}`);
}

function assertPositiveInteger(value: unknown, name: string): asserts value is number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function assertNonNegativeInteger(value: unknown, name: string): asserts value is number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
}

function assertNonEmptyString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function assertCasBlockHash(hash: unknown, name = "CAS block hash"): asserts hash is string {
  if (typeof hash !== "string" || !/^[0-9a-f]{64}$/.test(hash)) {
    throw new Error(`${name} must be a 64-character lowercase sha256 hash`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
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
  inventory:
    | Pick<BrowserDataDirInventory, "directories" | "files">
    | readonly BrowserFileInventoryEntry[],
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

async function* verifiedBlockChunks(
  casStore: BrowserCasBlockStore,
  blocks: readonly BrowserCasBlock[],
): AsyncIterable<Uint8Array> {
  for (const block of blocks) {
    const content = await readVerifiedBlock(casStore, block);
    yield content;
  }
}

async function* collectVerifiedBlockChunks(
  casStore: BrowserCasBlockStore,
  blocks: readonly BrowserCasBlock[],
  collected: Uint8Array[],
): AsyncIterable<Uint8Array> {
  for await (const chunk of verifiedBlockChunks(casStore, blocks)) {
    collected.push(chunk);
    yield chunk;
  }
}

async function readVerifiedBlock(
  casStore: BrowserCasBlockStore,
  block: BrowserCasBlock,
): Promise<Uint8Array> {
  assertCasBlockHash(block.hash);
  const stored = await casStore.readBlock(block.hash);
  if (stored.length !== block.size || (await sha256(stored)) !== block.hash) {
    throw new Error(`CAS block integrity check failed: ${block.hash}`);
  }
  return stored;
}

function isNotFoundError(error: unknown): boolean {
  return error instanceof DOMException && error.name === "NotFoundError";
}
