import { createHash } from "node:crypto";
import { readdir, readFile, stat } from "node:fs/promises";
import { join, relative, sep } from "node:path";

export type FileInventoryEntry = {
  path: string;
  size: number;
  sha256: string;
};

export type DataDirInventory = {
  root: string;
  files: FileInventoryEntry[];
};

export async function inventoryDataDir(root: string): Promise<DataDirInventory> {
  const files = await walk(root);
  return {
    root,
    files: files.sort((left, right) => left.path.localeCompare(right.path)),
  };
}

async function walk(root: string, dir = root): Promise<FileInventoryEntry[]> {
  const entries = await readdir(dir, { withFileTypes: true });
  const files: FileInventoryEntry[] = [];

  for (const entry of entries) {
    const absolutePath = join(dir, entry.name);
    if (entry.isDirectory()) {
      files.push(...await walk(root, absolutePath));
      continue;
    }

    if (!entry.isFile()) {
      continue;
    }

    const metadata = await stat(absolutePath);
    const content = await readFile(absolutePath);
    files.push({
      path: normalizeRelativePath(relative(root, absolutePath)),
      size: metadata.size,
      sha256: createHash("sha256").update(content).digest("hex"),
    });
  }

  return files;
}

function normalizeRelativePath(path: string): string {
  return sep === "/" ? path : path.split(sep).join("/");
}
