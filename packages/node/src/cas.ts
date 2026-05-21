import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join } from "node:path";
import { inventoryDataDir, type FileInventoryEntry } from "./inventory.js";

export type CasBlock = {
  hash: string;
  size: number;
};

export type CasFileManifest = {
  path: string;
  size: number;
  sha256: string;
  blocks: CasBlock[];
};

export type CasManifest = {
  version: 1;
  chunkSize: number;
  files: CasFileManifest[];
};

export async function snapshotToCas(options: {
  dataDir: string;
  casDir: string;
  chunkSize?: number;
}): Promise<CasManifest> {
  const chunkSize = options.chunkSize ?? 64 * 1024;
  const inventory = await inventoryDataDir(options.dataDir);
  const files: CasFileManifest[] = [];

  await mkdir(options.casDir, { recursive: true });

  for (const file of inventory.files) {
    const content = await readFile(join(options.dataDir, file.path));
    const blocks = await writeBlocks(options.casDir, content, chunkSize);
    files.push({ ...file, blocks });
  }

  return {
    version: 1,
    chunkSize,
    files,
  };
}

export async function restoreFromCas(options: {
  manifest: CasManifest;
  casDir: string;
  targetDir: string;
}): Promise<void> {
  await mkdir(options.targetDir, { recursive: true });

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

export function manifestFilesEqualInventory(
  manifest: CasManifest,
  inventory: readonly FileInventoryEntry[],
): boolean {
  return JSON.stringify(manifest.files.map(stripBlocks)) === JSON.stringify(inventory);
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
