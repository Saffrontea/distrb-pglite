import { createHash } from "node:crypto";
import { cp, mkdir, readdir, readFile, writeFile } from "node:fs/promises";
import { dirname, join, resolve, sep } from "node:path";

export type SnapshotFilesystemEntry = {
  name: string;
  type: "file" | "directory";
};

export type SnapshotDataFilesystem = {
  listDir(path: string): Promise<SnapshotFilesystemEntry[]>;
  readFile(path: string): Promise<Uint8Array>;
  writeFile(path: string, data: Uint8Array): Promise<void>;
  mkdir(path: string): Promise<void>;
};

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

export class NodeDataDirFilesystem implements SnapshotDataFilesystem {
  constructor(readonly root: string) {}

  async listDir(path: string): Promise<SnapshotFilesystemEntry[]> {
    const entries = await readdir(this.absolute(path), { withFileTypes: true });
    return entries
      .filter((entry) => entry.isDirectory() || entry.isFile())
      .map((entry) => ({
        name: entry.name,
        type: entry.isDirectory() ? "directory" as const : "file" as const,
      }))
      .sort((left, right) => left.name.localeCompare(right.name));
  }

  async readFile(path: string): Promise<Uint8Array> {
    return readFile(this.absolute(path));
  }

  async writeFile(path: string, data: Uint8Array): Promise<void> {
    const target = this.absolute(path);
    await mkdir(dirname(target), { recursive: true });
    await writeFile(target, data);
  }

  async mkdir(path: string): Promise<void> {
    await mkdir(this.absolute(path), { recursive: true });
  }

  private absolute(path: string): string {
    const root = resolve(this.root);
    const target = resolve(root, fromLogicalPath(path));
    if (target !== root && !target.startsWith(`${root}${sep}`)) {
      throw new Error(`logical path escapes filesystem root: ${path}`);
    }
    return target;
  }
}

export async function inventoryFilesystem(options: {
  fs: SnapshotDataFilesystem;
  rootLabel?: string;
}): Promise<DataDirInventory> {
  const entries = await walkFilesystem(options.fs);
  return {
    root: options.rootLabel ?? "",
    directories: entries.directories.sort(),
    files: entries.files.sort((left, right) => left.path.localeCompare(right.path)),
  };
}

export async function inventoryDataDir(root: string): Promise<DataDirInventory> {
  return inventoryFilesystem({
    fs: new NodeDataDirFilesystem(root),
    rootLabel: root,
  });
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

export function normalizeLogicalPath(path: string): string {
  return path
    .replaceAll("\\", "/")
    .replace(/^\/+/, "")
    .replace(/\/+/g, "/")
    .replace(/\/$/, "");
}

async function walkFilesystem(
  fs: SnapshotDataFilesystem,
  dir = "",
): Promise<{ directories: string[]; files: FileInventoryEntry[] }> {
  const entries = await fs.listDir(dir);
  const directories: string[] = [];
  const files: FileInventoryEntry[] = [];

  for (const entry of entries) {
    const path = joinLogicalPath(dir, entry.name);
    if (entry.type === "directory") {
      directories.push(path);
      const child = await walkFilesystem(fs, path);
      directories.push(...child.directories);
      files.push(...child.files);
      continue;
    }

    const content = await fs.readFile(path);
    files.push({
      path,
      size: content.length,
      sha256: sha256(content),
    });
  }

  return { directories, files };
}

function joinLogicalPath(dir: string, name: string): string {
  return normalizeLogicalPath(dir ? `${dir}/${name}` : name);
}

function fromLogicalPath(path: string): string {
  const normalized = normalizeLogicalPath(path);
  if (sep === "/") {
    return normalized;
  }
  return normalized.split("/").join(sep);
}

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}
