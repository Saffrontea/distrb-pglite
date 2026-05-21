import { createHash } from "node:crypto";
import { cp, readdir, readFile, stat } from "node:fs/promises";
import { join, relative, sep } from "node:path";

export type FileInventoryEntry = {
  path: string;
  size: number;
  sha256: string;
};

export type DataDirInventory = {
  root: string;
  directories: string[];
  files: FileInventoryEntry[];
};

export type DataDirSnapshotCopy = {
  source: DataDirInventory;
  target: DataDirInventory;
};

export async function inventoryDataDir(root: string): Promise<DataDirInventory> {
  const entries = await walk(root);
  return {
    root,
    directories: entries.directories.sort(),
    files: entries.files.sort((left, right) => left.path.localeCompare(right.path)),
  };
}

export async function copyDataDirSnapshot(options: {
  sourceDir: string;
  targetDir: string;
}): Promise<DataDirSnapshotCopy> {
  const source = await inventoryDataDir(options.sourceDir);
  await cp(options.sourceDir, options.targetDir, {
    recursive: true,
    errorOnExist: true,
    force: false,
  });
  const target = await inventoryDataDir(options.targetDir);
  return { source, target };
}

async function walk(
  root: string,
  dir = root,
): Promise<{ directories: string[]; files: FileInventoryEntry[] }> {
  const entries = await readdir(dir, { withFileTypes: true });
  const directories: string[] = [];
  const files: FileInventoryEntry[] = [];

  for (const entry of entries) {
    const absolutePath = join(dir, entry.name);
    if (entry.isDirectory()) {
      directories.push(normalizeRelativePath(relative(root, absolutePath)));
      const child = await walk(root, absolutePath);
      directories.push(...child.directories);
      files.push(...child.files);
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

  return { directories, files };
}

function normalizeRelativePath(path: string): string {
  return sep === "/" ? path : path.split(sep).join("/");
}
