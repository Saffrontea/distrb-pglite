import { MemoryBlobStore } from "@distrb-pglite/core";
import { PGlite } from "@electric-sql/pglite";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
  createPartitionFilesystemSnapshot,
  discoverStoragePartitionBinding,
  PGliteCasFilesystem,
  PGlitePartitionStore,
  replayEventsAfterPartitionSnapshot,
  restorePartitionFilesystemSnapshot,
} from "../src/index.js";

describe("PGliteCasFilesystem PGlite integration", () => {
  it("can be provided as a custom PGlite filesystem", async () => {
    const blobs = new MemoryBlobStore((content) =>
      createHash("sha256").update(content).digest("hex"),
    );
    const fs = new PGliteCasFilesystem(blobs, 4096);
    const db = new PGlite({ fs: fs as never });

    await db.query("CREATE TABLE notes (id serial PRIMARY KEY, title text NOT NULL)");
    await db.query("INSERT INTO notes (title) VALUES ($1)", ["custom fs"]);

    const result = await db.query<{ title: string }>("SELECT title FROM notes");
    expect(result.rows).toEqual([{ title: "custom fs" }]);

    await db.close();
    const manifest = await fs.snapshot();
    expect(manifest.files.length).toBeGreaterThan(0);
  }, 120_000);

  it("restores a PGlite database from a CAS filesystem snapshot", async () => {
    const blobs = new MemoryBlobStore((content) =>
      createHash("sha256").update(content).digest("hex"),
    );
    const sourceFs = new PGliteCasFilesystem(blobs, 4096);
    const sourceDb = new PGlite({ fs: sourceFs as never });

    await sourceDb.query("CREATE TABLE notes (id serial PRIMARY KEY, title text NOT NULL)");
    await sourceDb.query("INSERT INTO notes (title) VALUES ($1)", ["restored"]);
    await sourceDb.close();

    const manifest = await sourceFs.snapshot();
    const restoredFs = new PGliteCasFilesystem(blobs, 4096);
    await restoredFs.restore(manifest);
    const restoredDb = new PGlite({ fs: restoredFs as never });

    const result = await restoredDb.query<{ title: string }>(
      "SELECT title FROM notes ORDER BY id",
    );

    expect(result.rows).toEqual([{ title: "restored" }]);
    await restoredDb.close();
  }, 120_000);

  it("runs partition snapshot restore and replay through the custom filesystem", async () => {
    const blobs = new MemoryBlobStore((content) =>
      createHash("sha256").update(content).digest("hex"),
    );
    const sourceFs = new PGliteCasFilesystem(blobs, 4096);
    let sourceDb = new PGlite({ fs: sourceFs as never });
    let sourceStore = new PGlitePartitionStore("node-a", sourceDb);
    await sourceStore.migrate();
    await sourceDb.query("CREATE TABLE notes (id serial PRIMARY KEY, title text NOT NULL)");
    await sourceDb.query("INSERT INTO notes (title) VALUES ($1)", ["snapshotted"]);
    const snapshottedEvent = await sourceStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "snapshotted" },
    });
    const binding = await discoverStoragePartitionBinding({
      db: sourceDb,
      partitionId: "tenant:jp",
      rootRelation: "public.notes",
      snapshotScope: "catalog-and-data",
    });

    const snapshot = await createPartitionFilesystemSnapshot({
      store: sourceStore,
      partitionId: "tenant:jp",
      storageBinding: binding,
      fs: sourceFs,
      beforeStorageSnapshot: async () => {
        await sourceDb.close();
      },
    });

    sourceDb = new PGlite({ fs: sourceFs as never });
    sourceStore = new PGlitePartitionStore("node-a", sourceDb);
    await sourceStore.migrate();
    await sourceDb.query("INSERT INTO notes (title) VALUES ($1)", ["replayed"]);
    const replayedEvent = await sourceStore.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { title: "replayed" },
    });
    await sourceStore.appendLocal({
      partitionId: "tenant:us",
      schemaVersion: 1,
      payload: { title: "ignored" },
    });

    const targetFs = new PGliteCasFilesystem(blobs, 4096);
    await restorePartitionFilesystemSnapshot({ snapshot, fs: targetFs });
    const targetDb = new PGlite({ fs: targetFs as never });
    const targetStore = new PGlitePartitionStore("node-target", targetDb);
    await targetStore.migrate();

    await expect(
      targetStore.listEventsAfterCursors("tenant:jp", new Map()),
    ).resolves.toMatchObject([
      {
        eventId: snapshottedEvent.eventId,
        payload: { title: "snapshotted" },
      },
    ]);

    await expect(replayEventsAfterPartitionSnapshot({
      sourceStore,
      targetStore,
      snapshot,
    })).resolves.toBe(1);
    await expect(
      targetStore.listEventsAfterCursors("tenant:jp", new Map()),
    ).resolves.toMatchObject([
      {
        eventId: snapshottedEvent.eventId,
        payload: { title: "snapshotted" },
      },
      {
        eventId: replayedEvent.eventId,
        payload: { title: "replayed" },
      },
    ]);
    await expect(
      targetStore.listEventsAfterCursors("tenant:us", new Map()),
    ).resolves.toHaveLength(0);

    await targetDb.close();
    await sourceDb.close();
  }, 120_000);
});
