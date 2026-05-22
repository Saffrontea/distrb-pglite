import { join } from "node:path";
import { describe, expect, it } from "vitest";
import { NodeFileBlobStore } from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("NodeFileBlobStore", () => {
  it("stores content-addressed blobs on the host filesystem", async () => {
    const root = await makeTempDir("blobs");
    const store = new NodeFileBlobStore(root);
    const content = new TextEncoder().encode("blob content");

    const ref = await store.put(content);

    await expect(store.has(ref.hash)).resolves.toBe(true);
    await expect(store.get(ref.hash)).resolves.toEqual(content);
    await expect(store.has("missing")).resolves.toBe(false);
  });
});
