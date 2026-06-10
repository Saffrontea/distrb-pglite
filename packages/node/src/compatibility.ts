import { readFileSync } from "node:fs";
import { createRequire } from "node:module";
import { dirname, join } from "node:path";
import { CAS_MANIFEST_FORMAT, type CasCompatibility } from "@distrb-pglite/core";
import { NodeDataDirFilesystem, type SnapshotDataFilesystem } from "./inventory.js";

let cachedPgliteVersion: string | null | undefined;

// The @electric-sql/pglite exports map does not expose package.json, so the
// version is recovered by resolving the entry module and walking up to the
// nearest owning package.json.
export function pgliteRuntimeVersion(): string | undefined {
  if (cachedPgliteVersion !== undefined) {
    return cachedPgliteVersion ?? undefined;
  }

  cachedPgliteVersion = null;
  try {
    const require = createRequire(import.meta.url);
    let dir = dirname(require.resolve("@electric-sql/pglite"));
    while (true) {
      try {
        const pkg = JSON.parse(readFileSync(join(dir, "package.json"), "utf8")) as {
          name?: string;
          version?: string;
        };
        if (pkg.name === "@electric-sql/pglite" && typeof pkg.version === "string") {
          cachedPgliteVersion = pkg.version;
          break;
        }
      } catch {
        // keep walking up
      }
      const parent = dirname(dir);
      if (parent === dir) {
        break;
      }
      dir = parent;
    }
  } catch {
    cachedPgliteVersion = null;
  }

  return cachedPgliteVersion ?? undefined;
}

export async function readSnapshotFilesystemCasCompatibility(
  fs: SnapshotDataFilesystem,
): Promise<CasCompatibility | undefined> {
  let pgVersionFile: Uint8Array;
  try {
    pgVersionFile = await fs.readFile("PG_VERSION");
  } catch {
    return undefined;
  }

  const pgMajorVersion = new TextDecoder().decode(pgVersionFile).trim();
  if (!/^\d+(\.\d+)?$/.test(pgMajorVersion)) {
    return undefined;
  }

  const pgliteVersion = pgliteRuntimeVersion();
  return {
    manifestFormat: CAS_MANIFEST_FORMAT,
    pgMajorVersion,
    ...(pgliteVersion ? { pgliteVersion } : {}),
  };
}

export async function readDataDirCasCompatibility(
  dataDir: string,
): Promise<CasCompatibility | undefined> {
  return readSnapshotFilesystemCasCompatibility(new NodeDataDirFilesystem(dataDir));
}
