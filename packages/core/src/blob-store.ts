export type BlobRef = {
  hash: string;
  size: number;
};

export type BlobStore = {
  put(content: Uint8Array): Promise<BlobRef>;
  get(hash: string): Promise<Uint8Array>;
  has(hash: string): Promise<boolean>;
};

export class MemoryBlobStore implements BlobStore {
  private readonly blobs = new Map<string, Uint8Array>();

  constructor(private readonly hash: (content: Uint8Array) => string) {}

  async put(content: Uint8Array): Promise<BlobRef> {
    const hash = this.hash(content);
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
