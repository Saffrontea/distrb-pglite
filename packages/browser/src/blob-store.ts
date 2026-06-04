import type { BlobRef, BlobStore } from "@distrb-pglite/core";

export class BrowserSha256BlobStore implements BlobStore {
  private readonly blobs = new Map<string, Uint8Array>();

  async put(content: Uint8Array): Promise<BlobRef> {
    const hash = await sha256(content);
    this.blobs.set(hash, new Uint8Array(content));
    return { hash, size: content.length };
  }

  async get(hash: string): Promise<Uint8Array> {
    const content = this.blobs.get(hash);
    if (!content) {
      throw new Error(`blob not found: ${hash}`);
    }
    return new Uint8Array(content);
  }

  async has(hash: string): Promise<boolean> {
    return this.blobs.has(hash);
  }
}

export async function sha256(content: Uint8Array): Promise<string> {
  const digest = await crypto.subtle.digest("SHA-256", new Uint8Array(content));
  return [...new Uint8Array(digest)].map((byte) => byte.toString(16).padStart(2, "0")).join("");
}
