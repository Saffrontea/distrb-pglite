import {
  CasSnapshotFilesystem,
  type BlobStore,
  type SnapshotFilesystemManifest,
} from "@distrb-pglite/core";
import { BaseFilesystem, type FsStats } from "@electric-sql/pglite/basefs";

type OpenFile = {
  path: string;
  flags: string;
  position: number;
};

type Node = {
  kind: "file" | "dir";
  content: Uint8Array;
  mode: number;
  atime: number;
  mtime: number;
  ctime: number;
};

export class PGliteCasFilesystem extends BaseFilesystem {
  private readonly nodes = new Map<string, Node>();
  private readonly fds = new Map<number, OpenFile>();
  private nextFd = 3;

  constructor(
    private readonly blobs: BlobStore,
    private readonly chunkSize = 64 * 1024,
    options: { debug?: boolean } = {},
  ) {
    super(undefined, options);
    this.nodes.set("/", this.createDirNode());
  }

  async snapshot(): Promise<SnapshotFilesystemManifest> {
    const fs = new CasSnapshotFilesystem(this.blobs, this.chunkSize);
    for (const [path, node] of this.sortedFiles()) {
      await fs.writeFile(path, node.content);
    }
    return fs.snapshot();
  }

  async restore(manifest: SnapshotFilesystemManifest): Promise<void> {
    const fs = new CasSnapshotFilesystem(this.blobs, this.chunkSize);
    await fs.restore(manifest);
    this.nodes.clear();
    this.nodes.set("/", this.createDirNode());
    this.fds.clear();
    this.nextFd = 3;

    for (const file of manifest.files) {
      this.mkdir(dirname(file.path), { recursive: true });
      this.writeFile(file.path, await fs.readFile(file.path));
    }
  }

  chmod(path: string, mode: number): void {
    this.requireNode(path).mode = mode;
  }

  close(fd: number): void {
    if (!this.fds.delete(fd)) {
      throwFsError("EBADF", `bad file descriptor: ${fd}`);
    }
  }

  fstat(fd: number): FsStats {
    const file = this.requireFd(fd);
    return statsFor(this.requireNode(file.path));
  }

  lstat(path: string): FsStats {
    return statsFor(this.requireNode(path));
  }

  mkdir(path: string, options?: { recursive?: boolean; mode?: number }): void {
    const normalized = normalizePath(path);
    if (normalized === "/") {
      return;
    }

    const parent = dirname(normalized);
    if (!this.nodes.has(parent)) {
      if (!options?.recursive) {
        throwFsError("ENOENT", `parent directory does not exist: ${parent}`);
      }
      this.mkdir(parent, options);
    }

    const existing = this.nodes.get(normalized);
    if (existing?.kind === "file") {
      throwFsError("EEXIST", `file exists: ${normalized}`);
    }
    if (!existing) {
      this.nodes.set(normalized, this.createDirNode(options?.mode));
    }
  }

  open(path: string, flags = "r", mode = 0o666): number {
    const normalized = normalizePath(path);
    const existing = this.nodes.get(normalized);

    if (!existing && !canCreate(flags)) {
      throwFsError("ENOENT", `file does not exist: ${normalized}`);
    }
    if (existing?.kind === "dir") {
      throwFsError("EISDIR", `cannot open directory: ${normalized}`);
    }

    if (!existing) {
      this.mkdir(dirname(normalized), { recursive: true });
      this.nodes.set(normalized, this.createFileNode(new Uint8Array(), mode));
    } else if (flags.includes("w")) {
      existing.content = new Uint8Array();
      touch(existing);
    }

    const fd = this.nextFd++;
    this.fds.set(fd, {
      path: normalized,
      flags,
      position: flags.includes("a") ? this.requireNode(normalized).content.length : 0,
    });
    return fd;
  }

  readdir(path: string): string[] {
    const normalized = normalizePath(path);
    const dir = this.requireNode(normalized);
    if (dir.kind !== "dir") {
      throwFsError("ENOTDIR", `not a directory: ${normalized}`);
    }

    const prefix = normalized === "/" ? "/" : `${normalized}/`;
    const names = new Set<string>();
    for (const candidate of this.nodes.keys()) {
      if (candidate === normalized || !candidate.startsWith(prefix)) {
        continue;
      }
      const rest = candidate.slice(prefix.length);
      if (rest && !rest.includes("/")) {
        names.add(rest);
      }
    }

    return [...names].sort();
  }

  read(
    fd: number,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    const open = this.requireFd(fd);
    const node = this.requireFile(open.path);
    const start = position < 0 ? open.position : position;
    const chunk = node.content.subarray(start, start + length);
    buffer.set(chunk, offset);
    open.position = start + chunk.length;
    node.atime = Date.now();
    return chunk.length;
  }

  rename(oldPath: string, newPath: string): void {
    const oldNormalized = normalizePath(oldPath);
    const newNormalized = normalizePath(newPath);
    const node = this.requireNode(oldNormalized);
    this.mkdir(dirname(newNormalized), { recursive: true });
    this.nodes.set(newNormalized, node);
    this.nodes.delete(oldNormalized);
  }

  rmdir(path: string): void {
    const normalized = normalizePath(path);
    if (normalized === "/") {
      throwFsError("EINVAL", "cannot remove root directory");
    }
    this.requireNode(normalized);
    const prefix = `${normalized}/`;
    if ([...this.nodes.keys()].some((candidate) => candidate.startsWith(prefix))) {
      throwFsError("ENOTEMPTY", `directory not empty: ${normalized}`);
    }
    this.nodes.delete(normalized);
  }

  truncate(path: string, len: number): void {
    const node = this.requireFile(path);
    const next = new Uint8Array(len);
    next.set(node.content.subarray(0, Math.min(node.content.length, len)));
    node.content = next;
    touch(node);
  }

  unlink(path: string): void {
    const normalized = normalizePath(path);
    const node = this.requireNode(normalized);
    if (node.kind !== "file") {
      throwFsError("EISDIR", `cannot unlink directory: ${normalized}`);
    }
    this.nodes.delete(normalized);
  }

  utimes(path: string, atime: number, mtime: number): void {
    const node = this.requireNode(path);
    node.atime = atime;
    node.mtime = mtime;
  }

  writeFile(
    path: string,
    data: string | Uint8Array,
    options?: { encoding?: string; mode?: number; flag?: string },
  ): void {
    const normalized = normalizePath(path);
    this.mkdir(dirname(normalized), { recursive: true });
    const content = typeof data === "string" ? new TextEncoder().encode(data) : data;
    this.nodes.set(normalized, this.createFileNode(new Uint8Array(content), options?.mode));
  }

  write(
    fd: number,
    buffer: Uint8Array,
    offset: number,
    length: number,
    position: number,
  ): number {
    const bytes = toUint8Array(buffer);
    const open = this.requireFd(fd);
    const node = this.requireFile(open.path);
    const start = open.flags.includes("a")
      ? node.content.length
      : position < 0
        ? open.position
        : position;
    const end = start + length;
    const next = new Uint8Array(Math.max(node.content.length, end));
    next.set(node.content);
    next.set(bytes.subarray(offset, offset + length), start);
    node.content = next;
    open.position = end;
    touch(node);
    return length;
  }

  private requireFd(fd: number): OpenFile {
    const open = this.fds.get(fd);
    if (!open) {
      throwFsError("EBADF", `bad file descriptor: ${fd}`);
    }
    return open;
  }

  private requireFile(path: string): Node {
    const node = this.requireNode(path);
    if (node.kind !== "file") {
      throwFsError("EISDIR", `not a file: ${path}`);
    }
    return node;
  }

  private requireNode(path: string): Node {
    const normalized = normalizePath(path);
    const node = this.nodes.get(normalized);
    if (!node) {
      throwFsError("ENOENT", `path does not exist: ${normalized}`);
    }
    return node;
  }

  private sortedFiles(): [string, Node][] {
    return [...this.nodes]
      .filter((entry): entry is [string, Node] => entry[1].kind === "file")
      .sort(([left], [right]) => left.localeCompare(right));
  }

  private createDirNode(mode = 0o777): Node {
    return this.createNode("dir", new Uint8Array(), mode);
  }

  private createFileNode(content: Uint8Array, mode = 0o666): Node {
    return this.createNode("file", content, mode);
  }

  private createNode(kind: Node["kind"], content: Uint8Array, mode: number): Node {
    const now = Date.now();
    return { kind, content, mode, atime: now, mtime: now, ctime: now };
  }
}

function canCreate(flags: string): boolean {
  return flags.includes("w") || flags.includes("a") || flags.includes("+");
}

function dirname(path: string): string {
  const normalized = normalizePath(path);
  const index = normalized.lastIndexOf("/");
  if (index <= 0) {
    return "/";
  }
  return normalized.slice(0, index);
}

function normalizePath(path: string): string {
  const normalized = path.replaceAll("\\", "/").replace(/\/+/g, "/");
  return normalized.startsWith("/") ? normalized : `/${normalized}`;
}

function statsFor(node: Node): FsStats {
  return {
    dev: 1,
    ino: 1,
    mode: node.mode,
    nlink: 1,
    uid: 0,
    gid: 0,
    rdev: 0,
    size: node.content.length,
    blksize: 4096,
    blocks: Math.ceil(node.content.length / 4096),
    atime: node.atime,
    mtime: node.mtime,
    ctime: node.ctime,
  };
}

function touch(node: Node): void {
  const now = Date.now();
  node.mtime = now;
  node.ctime = now;
}

const ERRNO_CODES: Record<string, number> = {
  EBADF: 8,
  EBADFD: 127,
  EEXIST: 20,
  EINVAL: 28,
  EISDIR: 31,
  ENODEV: 43,
  ENOENT: 44,
  ENOTDIR: 54,
  ENOTEMPTY: 55,
};

function toUint8Array(buffer: Uint8Array | ArrayBuffer): Uint8Array {
  return buffer instanceof Uint8Array ? buffer : new Uint8Array(buffer);
}

function throwFsError(code: string, message: string): never {
  throw Object.assign(new Error(message), { code: ERRNO_CODES[code] ?? 28 });
}
