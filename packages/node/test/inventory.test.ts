import { PGlite } from "@electric-sql/pglite";
import { mkdtemp } from "node:fs/promises";
import { join } from "node:path";
import { tmpdir } from "node:os";
import { describe, expect, it } from "vitest";
import { inventoryDataDir, PGlitePartitionStore } from "../src/index.js";

describe("inventoryDataDir", () => {
  it("lists PGlite data directory files with deterministic hashes", async () => {
    const dataDir = await mkdtemp(join(tmpdir(), "distrb-pglite-inventory-"));
    const db = new PGlite(dataDir);
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "inventory target" },
    });
    await db.close();

    const first = await inventoryDataDir(dataDir);
    const second = await inventoryDataDir(dataDir);

    expect(first.root).toBe(dataDir);
    expect(first.files.length).toBeGreaterThan(0);
    expect(first.files).toEqual(second.files);
    expect(first.files).toEqual(
      [...first.files].sort((left, right) => left.path.localeCompare(right.path)),
    );
    expect(first.files.every((file) => file.size >= 0)).toBe(true);
    expect(first.files.some((file) => file.size > 0)).toBe(true);
    expect(first.files.every((file) => /^[0-9a-f]{64}$/.test(file.sha256))).toBe(true);
  }, 120_000);
});
