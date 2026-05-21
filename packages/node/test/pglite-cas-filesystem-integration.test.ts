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
});
