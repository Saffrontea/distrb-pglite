import { MemoryBlobStore } from "@distrb-pglite/core";
import { PGlite } from "@electric-sql/pglite";
import { createHash } from "node:crypto";
import { describe, expect, it } from "vitest";
import { PGliteCasFilesystem } from "../src/index.js";

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
});
