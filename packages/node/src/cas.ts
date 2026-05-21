import { createHash, randomUUID } from "node:crypto";
import { copyFile, mkdir, readFile, readdir, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import type { BlobRef, SnapshotFile, SnapshotFilesystemManifest } from "@distrb-pglite/core";
import { inventoryDataDir, type DataDirInventory, type FileInventoryEntry } from "./inventory.js";

export type CasBlock = BlobRef;

// Node data-directory CAS snapshots use the core filesystem manifest shape,
// then add per-file hashes so copied/restored host files can be verified.
export type CasFileManifest = SnapshotFile<CasBlock> & {
  sha256: string;
};

// External data-directory snapshot graph. This is not the in-memory filesystem
// adapter manifest; it tracks snapshot lineage for diff transfer.
export type CasManifest = SnapshotFilesystemManifest<CasFileManifest> & {
  snapshotId: string;
  parentSnapshotIds: string[];
};

export type CasSnapshotFilter = {
  includeFile(file: FileInventoryEntry): boolean;
};

export async function snapshotToCas(options: {
  dataDir: string;
  casDir: string;
  chunkSize?: number;
  snapshotId?: string;
  parentSnapshotIds?: string[];
  filter?: CasSnapshotFilter;
}): Promise<CasManifest> {
  const chunkSize = options.chunkSize ?? 64 * 1024;
  const inventory = await inventoryDataDir(options.dataDir);
  const selectedFiles = options.filter
    ? inventory.files.filter((file) => options.filter?.includeFile(file) ?? false)
    : inventory.files;
  const files: CasFileManifest[] = [];

  await mkdir(options.casDir, { recursive: true });

  for (const file of selectedFiles) {
    const content = await readFile(join(options.dataDir, file.path));
    const blocks = await writeBlocks(options.casDir, content, chunkSize);
    files.push({ ...file, blocks });
  }

  return {
    version: 1,
    snapshotId: options.snapshotId ?? randomUUID(),
    parentSnapshotIds: options.parentSnapshotIds ?? [],
    chunkSize,
    directories: options.filter
      ? selectedDirectories(inventory.directories, selectedFiles)
      : inventory.directories,
    files,
  };
}

export async function restoreFromCas(options: {
  manifest: CasManifest;
  casDir: string;
  targetDir: string;
}): Promise<void> {
  await mkdir(options.targetDir, { recursive: true });

  for (const directory of options.manifest.directories) {
    await mkdir(join(options.targetDir, directory), { recursive: true });
  }

  for (const file of options.manifest.files) {
    const chunks: Uint8Array[] = [];
    for (const block of file.blocks) {
      const content = await readFile(blockPath(options.casDir, block.hash));
      if (content.length !== block.size || sha256(content) !== block.hash) {
        throw new Error(`CAS block integrity check failed: ${block.hash}`);
      }
      chunks.push(content);
    }

    const restored = Buffer.concat(chunks);
    if (restored.length !== file.size || sha256(restored) !== file.sha256) {
      throw new Error(`CAS file integrity check failed: ${file.path}`);
    }

    const target = join(options.targetDir, file.path);
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, restored);
  }
}

export function diffMissingBlocks(
  manifest: CasManifest,
  availableBlockHashes: ReadonlySet<string>,
): CasBlock[] {
  const missing = new Map<string, CasBlock>();
  for (const file of manifest.files) {
    for (const block of file.blocks) {
      if (!availableBlockHashes.has(block.hash)) {
        missing.set(block.hash, block);
      }
    }
  }

  return [...missing.values()].sort((left, right) => left.hash.localeCompare(right.hash));
}

export async function listCasBlockHashes(casDir: string): Promise<Set<string>> {
  const hashes = new Set<string>();
  let entries: string[];
  try {
    entries = await readdir(casDir);
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

export async function transferMissingBlocks(options: {
  manifest: CasManifest;
  sourceCasDir: string;
  targetCasDir: string;
  availableBlockHashes?: ReadonlySet<string>;
}): Promise<CasBlock[]> {
  const available = options.availableBlockHashes ?? await listCasBlockHashes(options.targetCasDir);
  const missing = diffMissingBlocks(options.manifest, available);
  await mkdir(options.targetCasDir, { recursive: true });

  for (const block of missing) {
    const source = blockPath(options.sourceCasDir, block.hash);
    const target = blockPath(options.targetCasDir, block.hash);
    const content = await readFile(source);
    if (content.length !== block.size || sha256(content) !== block.hash) {
      throw new Error(`CAS source block integrity check failed: ${block.hash}`);
    }
    await copyFile(source, target);
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
  casDir: string,
  content: Uint8Array,
  chunkSize: number,
): Promise<CasBlock[]> {
  const blocks: CasBlock[] = [];
  for (let offset = 0; offset < content.length || (content.length === 0 && offset === 0); offset += chunkSize) {
    const chunk = content.subarray(offset, Math.min(offset + chunkSize, content.length));
    const hash = sha256(chunk);
    await writeFile(blockPath(casDir, hash), chunk);
    blocks.push({ hash, size: chunk.length });

    if (content.length === 0) {
      break;
    }
  }

  return blocks;
}

function blockPath(casDir: string, hash: string): string {
  return join(casDir, hash);
}

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
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
): string[] {
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
