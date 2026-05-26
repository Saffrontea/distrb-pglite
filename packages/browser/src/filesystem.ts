export type BrowserSnapshotFilesystemEntry = {
  name: string;
  type: "file" | "directory";
};

export type BrowserSnapshotDataFilesystem = {
  listDir(path: string): Promise<BrowserSnapshotFilesystemEntry[]>;
  readFile(path: string): Promise<Uint8Array>;
  writeFile(path: string, data: Uint8Array): Promise<void>;
  writeFileChunks?(path: string, chunks: AsyncIterable<Uint8Array>, expectedSize: number): Promise<void>;
  mkdir(path: string): Promise<void>;
};

export type BrowserFileInventoryEntry = {
  path: string;
  size: number;
  sha256: string;
};

export type BrowserDataDirInventory = {
  root: string;
  directories: string[];
  files: BrowserFileInventoryEntry[];
};

export class OpfsDataDirFilesystem implements BrowserSnapshotDataFilesystem {
  constructor(private readonly root: FileSystemDirectoryHandle) {}

  static async open(rootName = "distrb-pglite"): Promise<OpfsDataDirFilesystem> {
    if (!navigator.storage?.getDirectory) {
      throw new Error("OPFS is not available in this browser context");
    }
    const opfsRoot = await navigator.storage.getDirectory();
    const root = await opfsRoot.getDirectoryHandle(rootName, { create: true });
    return new OpfsDataDirFilesystem(root);
  }

  async listDir(path: string): Promise<BrowserSnapshotFilesystemEntry[]> {
    const dir = await this.resolveDir(path, false);
    const entries: BrowserSnapshotFilesystemEntry[] = [];
    for await (const [name, handle] of dir.entries()) {
      if (handle.kind === "directory" || handle.kind === "file") {
        entries.push({ name, type: handle.kind });
      }
    }
    return entries.sort((left, right) => left.name.localeCompare(right.name));
  }

  async readFile(path: string): Promise<Uint8Array> {
    const fileHandle = await this.resolveFile(path, false);
    return new Uint8Array(await (await fileHandle.getFile()).arrayBuffer());
  }

  async writeFile(path: string, data: Uint8Array): Promise<void> {
    const fileHandle = await this.resolveFile(path, true);
    const writable = await fileHandle.createWritable();
    await writable.write(new Uint8Array(data));
    await writable.close();
  }

  async writeFileChunks(
    path: string,
    chunks: AsyncIterable<Uint8Array>,
    expectedSize: number,
  ): Promise<void> {
    const fileHandle = await this.resolveFile(path, true);
    const writable = await fileHandle.createWritable();
    let written = 0;
    try {
      for await (const chunk of chunks) {
        await writable.write(new Uint8Array(chunk));
        written += chunk.length;
      }
    } finally {
      await writable.close();
    }
    if (written !== expectedSize) {
      throw new Error(`chunked write size mismatch: ${path}`);
    }
  }

  async mkdir(path: string): Promise<void> {
    await this.resolveDir(path, true);
  }

  private async resolveDir(path: string, create: boolean): Promise<FileSystemDirectoryHandle> {
    let dir = this.root;
    for (const part of splitLogicalPath(path)) {
      dir = await dir.getDirectoryHandle(part, { create });
    }
    return dir;
  }

  private async resolveFile(path: string, create: boolean): Promise<FileSystemFileHandle> {
    const parts = splitLogicalPath(path);
    const name = parts.pop();
    if (!name) {
      throw new Error("file path must not be empty");
    }
    const dir = await this.resolveDir(parts.join("/"), create);
    return dir.getFileHandle(name, { create });
  }
}

export async function inventoryBrowserFilesystem(options: {
  fs: BrowserSnapshotDataFilesystem;
  rootLabel?: string;
}): Promise<BrowserDataDirInventory> {
  const entries = await walkFilesystem(options.fs);
  return {
    root: options.rootLabel ?? "",
    directories: entries.directories.sort(),
    files: entries.files.sort((left, right) => left.path.localeCompare(right.path)),
  };
}

export function normalizeBrowserLogicalPath(path: string): string {
  return path
    .replaceAll("\\", "/")
    .replace(/^\/+/, "")
    .replace(/\/+/g, "/")
    .replace(/\/$/, "");
}

async function walkFilesystem(
  fs: BrowserSnapshotDataFilesystem,
  dir = "",
): Promise<{ directories: string[]; files: BrowserFileInventoryEntry[] }> {
  const entries = await fs.listDir(dir);
  const directories: string[] = [];
  const files: BrowserFileInventoryEntry[] = [];

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
      sha256: await sha256(content),
    });
  }

  return { directories, files };
}

function joinLogicalPath(dir: string, name: string): string {
  return normalizeBrowserLogicalPath(dir ? `${dir}/${name}` : name);
}

function splitLogicalPath(path: string): string[] {
  const normalized = normalizeBrowserLogicalPath(path);
  return normalized ? normalized.split("/") : [];
}

async function sha256(content: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new Uint8Array(content));
  return [...new Uint8Array(digest)]
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}
