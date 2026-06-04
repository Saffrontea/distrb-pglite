import { createHash } from "node:crypto";
import { mkdir, readFile, readdir, rm, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import {
  type BlobRef,
  type SnapshotFile,
  type SnapshotFilesystemManifest,
  type StorageSnapshotComponent,
  uuidV7,
} from "@distrb-pglite/core";
import {
  NodeDataDirFilesystem,
  inventoryFilesystem,
  normalizeLogicalPath,
  type DataDirInventory,
  type FileInventoryEntry,
  type SnapshotDataFilesystem,
} from "./inventory.js";
import { loadZstd } from "./zstd.js";

export type CasBlock = BlobRef;

export type CasBlockStore = {
  readBlock(hash: string): Promise<Uint8Array>;
  writeBlock(hash: string, content: Uint8Array): Promise<void>;
  listBlockHashes(): Promise<Set<string>>;
  deleteBlock(hash: string): Promise<void>;
};

export class NodeCasBlockStore implements CasBlockStore {
  constructor(readonly root: string) {}

  async readBlock(hash: string): Promise<Uint8Array> {
    return readFile(this.blockPath(hash));
  }

  async writeBlock(hash: string, content: Uint8Array): Promise<void> {
    await mkdir(this.root, { recursive: true });
    await writeFile(this.blockPath(hash), content);
  }

  async listBlockHashes(): Promise<Set<string>> {
    const hashes = new Set<string>();
    let entries: string[];
    try {
      entries = await readdir(this.root);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return hashes;
      }
      throw error;
    }

    for (const entry of entries) {
      if (/^[0-9a-f]{64}$/.test(entry)) {
        hashes.add(entry);
      }
    }
    return hashes;
  }

  async deleteBlock(hash: string): Promise<void> {
    await rm(this.blockPath(hash), { force: true });
  }

  private blockPath(hash: string): string {
    if (!/^[0-9a-f]{64}$/.test(hash)) {
      throw new Error(`invalid CAS block hash: ${hash}`);
    }
    return join(this.root, hash);
  }
}

// Node data-directory CAS snapshots use the core filesystem manifest shape,
// then add per-file hashes so copied/restored host files can be verified.
export type CasFileManifest = SnapshotFile<CasBlock> & {
  sha256: string;
  components?: StorageSnapshotComponent[];
};

// External data-directory snapshot graph. This is not the in-memory filesystem
// adapter manifest; it tracks snapshot lineage for diff transfer.
export type CasManifest = SnapshotFilesystemManifest<CasFileManifest> & {
  snapshotId: string;
  parentSnapshotIds: string[];
  compression?: "zstd";
  chunking?: CasChunking;
};

export type CasSnapshotFilter = {
  includeFile(file: FileInventoryEntry): boolean;
  includeDirectory?(directory: string): boolean;
  componentsForFile?(file: FileInventoryEntry): StorageSnapshotComponent[];
};

export type CasChunking =
  | { strategy: "fixed"; chunkSize: number }
  | { strategy: "page-aligned"; pageSize: number; maxChunkSize: number };

export type CasGcResult = {
  referenced: string[];
  deleted: string[];
  retained: string[];
};

type CasBlockStoreOption =
  | { casDir: string; casStore?: never }
  | { casStore: CasBlockStore; casDir?: never };

export async function snapshotToCas(options: {
  dataDir: string;
  casDir: string;
  chunkSize?: number;
  chunking?: CasChunking;
  snapshotId?: string;
  parentSnapshotIds?: string[];
  filter?: CasSnapshotFilter;
  compress?: boolean;
}): Promise<CasManifest> {
  return snapshotFilesystemToCas({
    fs: new NodeDataDirFilesystem(options.dataDir),
    casStore: new NodeCasBlockStore(options.casDir),
    chunkSize: options.chunkSize,
    chunking: options.chunking,
    snapshotId: options.snapshotId,
    parentSnapshotIds: options.parentSnapshotIds,
    filter: options.filter,
    compress: options.compress,
  });
}

export async function snapshotFilesystemToCas(options: {
  fs: SnapshotDataFilesystem;
  chunkSize?: number;
  chunking?: CasChunking;
  snapshotId?: string;
  parentSnapshotIds?: string[];
  filter?: CasSnapshotFilter;
  compress?: boolean;
} & CasBlockStoreOption): Promise<CasManifest> {
  const casStore = resolveCasBlockStore(options);
  const chunking = options.chunking ?? fixedChunking(options.chunkSize ?? 64 * 1024);
  const chunkSize = manifestChunkSize(chunking);
  const inventory = await inventoryFilesystem({ fs: options.fs });
  const selectedFiles = options.filter
    ? inventory.files.filter((file) => options.filter?.includeFile(file) ?? false)
    : inventory.files;
  const files: CasFileManifest[] = [];

  const zstd = options.compress ? await loadZstd() : undefined;
  const compress = zstd ? zstd.compress.bind(zstd) : undefined;

  for (const file of selectedFiles) {
    const content = await options.fs.readFile(file.path);
    const blocks = await writeBlocks(casStore, content, chunking, compress);
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
    ...(options.compress ? { compression: "zstd" as const } : {}),
    directories: options.filter
      ? selectedDirectories(inventory.directories, selectedFiles, options.filter)
      : inventory.directories,
    files,
  };
}

export async function gcCasBlocks(options: {
  casDir: string;
  retainedManifests: readonly CasManifest[];
  dryRun?: boolean;
}): Promise<CasGcResult> {
  return gcCasBlockStore({
    casStore: new NodeCasBlockStore(options.casDir),
    retainedManifests: options.retainedManifests,
    dryRun: options.dryRun,
  });
}

export async function gcCasBlockStore(options: {
  casStore: CasBlockStore;
  retainedManifests: readonly CasManifest[];
  dryRun?: boolean;
}): Promise<CasGcResult> {
  const referenced = new Set<string>();
  for (const manifest of options.retainedManifests) {
    for (const file of validateCasManifest(manifest).files) {
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

export function validateCasManifest(manifest: unknown, expectedSnapshotId?: string): CasManifest {
  if (!isRecord(manifest)) {
    throw new Error("CAS manifest must be an object");
  }
  if (manifest.version !== 1) {
    throw new Error(`invalid CAS manifest version: ${String(manifest.version)}`);
  }
  assertNonEmptyString(manifest.snapshotId, "CAS manifest snapshotId");
  if (expectedSnapshotId !== undefined && manifest.snapshotId !== expectedSnapshotId) {
    throw new Error(`CAS manifest snapshotId mismatch: ${manifest.snapshotId} !== ${expectedSnapshotId}`);
  }
  assertPositiveInteger(manifest.chunkSize, "CAS manifest chunkSize");

  if (!Array.isArray(manifest.parentSnapshotIds)) {
    throw new Error("CAS manifest parentSnapshotIds must be an array");
  }
  for (const [index, parentSnapshotId] of manifest.parentSnapshotIds.entries()) {
    assertNonEmptyString(parentSnapshotId, `CAS manifest parentSnapshotIds[${index}]`);
  }

  if (!Array.isArray(manifest.directories)) {
    throw new Error("CAS manifest directories must be an array");
  }
  const directoryPaths = new Set<string>();
  for (const [index, directory] of manifest.directories.entries()) {
    assertNonEmptyString(directory, `CAS manifest directories[${index}]`);
    const path = normalizeLogicalPath(directory);
    if (directoryPaths.has(path)) {
      throw new Error(`duplicate CAS manifest directory path: ${path}`);
    }
    directoryPaths.add(path);
  }

  if (!Array.isArray(manifest.files)) {
    throw new Error("CAS manifest files must be an array");
  }
  const filePaths = new Set<string>();
  for (const [index, file] of manifest.files.entries()) {
    validateCasFileManifest(file, index);
    const path = normalizeLogicalPath((file as { path: string }).path);
    if (filePaths.has(path)) {
      throw new Error(`duplicate CAS manifest file path: ${path}`);
    }
    if (directoryPaths.has(path)) {
      throw new Error(`CAS manifest path is both a file and directory: ${path}`);
    }
    filePaths.add(path);
  }

  if (manifest.compression !== undefined && manifest.compression !== "zstd") {
    throw new Error(`invalid CAS manifest compression: ${String(manifest.compression)}`);
  }
  if (manifest.chunking !== undefined) {
    validateCasChunking(manifest.chunking);
  }

  return manifest as CasManifest;
}

export async function restoreFromCas(options: {
  manifest: CasManifest;
  casDir: string;
  targetDir: string;
}): Promise<void> {
  await restoreFilesystemFromCas({
    manifest: options.manifest,
    casStore: new NodeCasBlockStore(options.casDir),
    fs: new NodeDataDirFilesystem(options.targetDir),
  });
}

export async function restoreFilesystemFromCas(options: {
  manifest: CasManifest;
  fs: SnapshotDataFilesystem;
} & CasBlockStoreOption): Promise<void> {
  const casStore = resolveCasBlockStore(options);
  const manifest = validateCasManifest(options.manifest);
  await options.fs.mkdir("");

  const zstd = manifest.compression === "zstd" ? await loadZstd() : undefined;
  const decompress = zstd ? zstd.decompress.bind(zstd) : undefined;

  for (const directory of manifest.directories) {
    await options.fs.mkdir(directory);
  }

  for (const file of manifest.files) {
    const chunks: Uint8Array[] = [];
    for (const block of file.blocks) {
      const stored = await casStore.readBlock(block.hash);
      if (stored.length !== block.size || sha256(stored) !== block.hash) {
        throw new Error(`CAS block integrity check failed: ${block.hash}`);
      }
      chunks.push(decompress ? decompress(stored) : stored);
    }

    const restored = Buffer.concat(chunks);
    if (restored.length !== file.size || sha256(restored) !== file.sha256) {
      throw new Error(`CAS file integrity check failed: ${file.path}`);
    }

    await options.fs.writeFile(file.path, restored);
  }
}

export function diffMissingBlocks(
  manifest: CasManifest,
  availableBlockHashes: ReadonlySet<string>,
): CasBlock[] {
  const validated = validateCasManifest(manifest);
  const missing = new Map<string, CasBlock>();
  for (const file of validated.files) {
    for (const block of file.blocks) {
      if (!availableBlockHashes.has(block.hash)) {
        missing.set(block.hash, block);
      }
    }
  }

  return [...missing.values()].sort((left, right) => left.hash.localeCompare(right.hash));
}

export async function listCasBlockHashes(casDir: string): Promise<Set<string>> {
  return new NodeCasBlockStore(casDir).listBlockHashes();
}

export async function transferMissingBlocks(options: {
  manifest: CasManifest;
  sourceCasDir: string;
  targetCasDir: string;
  availableBlockHashes?: ReadonlySet<string>;
}): Promise<CasBlock[]> {
  return transferMissingBlocksBetweenStores({
    manifest: options.manifest,
    sourceCasStore: new NodeCasBlockStore(options.sourceCasDir),
    targetCasStore: new NodeCasBlockStore(options.targetCasDir),
    availableBlockHashes: options.availableBlockHashes,
  });
}

export async function transferMissingBlocksBetweenStores(options: {
  manifest: CasManifest;
  sourceCasStore: CasBlockStore;
  targetCasStore: CasBlockStore;
  availableBlockHashes?: ReadonlySet<string>;
}): Promise<CasBlock[]> {
  const manifest = validateCasManifest(options.manifest);
  const available = options.availableBlockHashes ?? await options.targetCasStore.listBlockHashes();
  const missing = diffMissingBlocks(manifest, available);

  for (const block of missing) {
    const content = await options.sourceCasStore.readBlock(block.hash);
    if (content.length !== block.size || sha256(content) !== block.hash) {
      throw new Error(`CAS source block integrity check failed: ${block.hash}`);
    }
    await options.targetCasStore.writeBlock(block.hash, content);
  }

  return missing;
}

export function manifestFilesEqualInventory(
  manifest: CasManifest,
  inventory: Pick<DataDirInventory, "directories" | "files"> | readonly FileInventoryEntry[],
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
  casStore: CasBlockStore,
  content: Uint8Array,
  chunking: CasChunking,
  compress?: (data: Uint8Array) => Uint8Array,
): Promise<CasBlock[]> {
  const blocks: CasBlock[] = [];
  for (const [start, end] of chunkRanges(content.length, chunking)) {
    const chunk = content.subarray(start, end);
    const stored = compress ? compress(chunk) : chunk;
    const hash = sha256(stored);
    await casStore.writeBlock(hash, stored);
    blocks.push({ hash, size: stored.length });
  }

  return blocks;
}

function chunkRanges(size: number, chunking: CasChunking): Array<[number, number]> {
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

function fixedChunking(chunkSize: number): CasChunking {
  assertPositiveInteger(chunkSize, "chunkSize");
  return { strategy: "fixed", chunkSize };
}

function manifestChunkSize(chunking: CasChunking): number {
  if (chunking.strategy === "fixed") {
    assertPositiveInteger(chunking.chunkSize, "chunkSize");
    return chunking.chunkSize;
  }
  assertPositiveInteger(chunking.pageSize, "pageSize");
  assertPositiveInteger(chunking.maxChunkSize, "maxChunkSize");
  if (chunking.maxChunkSize < chunking.pageSize) {
    throw new Error(`maxChunkSize must be greater than or equal to pageSize: ${chunking.maxChunkSize} < ${chunking.pageSize}`);
  }
  return Math.max(chunking.pageSize, Math.floor(chunking.maxChunkSize / chunking.pageSize) * chunking.pageSize);
}

function validateCasFileManifest(file: unknown, index: number): void {
  if (!isRecord(file)) {
    throw new Error(`CAS manifest files[${index}] must be an object`);
  }
  assertNonEmptyString(file.path, `CAS manifest files[${index}].path`);
  assertNonNegativeInteger(file.size, `CAS manifest files[${index}].size`);
  assertCasBlockHash(file.sha256, `CAS manifest files[${index}].sha256`);
  if (!Array.isArray(file.blocks)) {
    throw new Error(`CAS manifest files[${index}].blocks must be an array`);
  }
  for (const [blockIndex, block] of file.blocks.entries()) {
    validateCasBlock(block, `CAS manifest files[${index}].blocks[${blockIndex}]`);
  }
  if (file.components !== undefined) {
    if (!Array.isArray(file.components) || file.components.length === 0) {
      throw new Error(`CAS manifest files[${index}].components must be a non-empty array`);
    }
    const seen = new Set<StorageSnapshotComponent>();
    for (const component of file.components) {
      if (component !== "catalog" && component !== "data") {
        throw new Error(`invalid CAS manifest file component: ${String(component)}`);
      }
      if (seen.has(component)) {
        throw new Error(`duplicate CAS manifest file component: ${component}`);
      }
      seen.add(component);
    }
  }
}

function validateCasBlock(block: unknown, name: string): void {
  if (!isRecord(block)) {
    throw new Error(`${name} must be an object`);
  }
  assertCasBlockHash(block.hash, `${name}.hash`);
  assertNonNegativeInteger(block.size, `${name}.size`);
}

function validateCasChunking(chunking: unknown): void {
  if (!isRecord(chunking)) {
    throw new Error("CAS manifest chunking must be an object");
  }
  if (chunking.strategy === "fixed") {
    assertPositiveInteger(chunking.chunkSize, "CAS manifest chunking.chunkSize");
    return;
  }
  if (chunking.strategy === "page-aligned") {
    assertPositiveInteger(chunking.pageSize, "CAS manifest chunking.pageSize");
    assertPositiveInteger(chunking.maxChunkSize, "CAS manifest chunking.maxChunkSize");
    if (chunking.maxChunkSize < chunking.pageSize) {
      throw new Error(
        `CAS manifest chunking maxChunkSize must be greater than or equal to pageSize: ${chunking.maxChunkSize} < ${chunking.pageSize}`,
      );
    }
    return;
  }
  throw new Error(`invalid CAS manifest chunking strategy: ${String(chunking.strategy)}`);
}

function assertCasBlockHash(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || !/^[0-9a-f]{64}$/.test(value)) {
    throw new Error(`${name} must be a 64-character lowercase sha256 hash`);
  }
}

function assertNonEmptyString(value: unknown, name: string): asserts value is string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(`${name} must be a non-empty string`);
  }
}

function assertNonNegativeInteger(value: unknown, name: string): asserts value is number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0) {
    throw new Error(`${name} must be a non-negative integer`);
  }
}

function assertPositiveInteger(value: unknown, name: string): asserts value is number {
  if (typeof value !== "number" || !Number.isInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}

function resolveCasBlockStore(options: CasBlockStoreOption): CasBlockStore {
  if (options.casStore) {
    return options.casStore;
  }
  return new NodeCasBlockStore(options.casDir);
}

function stripBlocks(file: CasFileManifest): FileInventoryEntry {
  return {
    path: file.path,
    size: file.size,
    sha256: file.sha256,
  };
}

function isFileInventoryEntries(
  inventory: Pick<DataDirInventory, "directories" | "files"> | readonly FileInventoryEntry[],
): inventory is readonly FileInventoryEntry[] {
  return Array.isArray(inventory);
}

function selectedDirectories(
  directories: readonly string[],
  files: readonly FileInventoryEntry[],
  filter: CasSnapshotFilter,
): string[] {
  if (filter.includeDirectory) {
    return directories.filter((directory) => filter.includeDirectory?.(directory) ?? false);
  }

  const required = new Set<string>();
  for (const file of files) {
    let path = dirname(file.path);
    while (path !== ".") {
      required.add(path);
      path = dirname(path);
    }
  }

  return directories.filter((directory) => required.has(directory));
}
