import type { BlobRef, BlobStore } from "@distrb-pglite/core";
import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";

export class NodeFileBlobStore implements BlobStore {
  constructor(private readonly root: string) {}

  async put(content: Uint8Array): Promise<BlobRef> {
    const hash = sha256(content);
    await mkdir(this.root, { recursive: true });
    await writeFile(this.path(hash), content);
    return { hash, size: content.length };
  }

  async get(hash: string): Promise<Uint8Array> {
    return new Uint8Array(await readFile(this.path(hash)));
  }

  async has(hash: string): Promise<boolean> {
    try {
      await readFile(this.path(hash));
      return true;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return false;
      }
      throw error;
    }
  }

  private path(hash: string): string {
    return join(this.root, hash);
  }
}

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}
