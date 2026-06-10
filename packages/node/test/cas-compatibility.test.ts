import { CAS_MANIFEST_FORMAT, type CasCompatibility } from "@distrb-pglite/core";
import { mkdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { describe, expect, it } from "vitest";
import {
  pgliteRuntimeVersion,
  readDataDirCasCompatibility,
  restoreFromCas,
  restorePartitionCasSnapshot,
  restorePartitionDataOnlyCasSnapshot,
  snapshotToCas,
  validateCasManifest,
  type CasManifest,
} from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("CAS manifest compatibility stamps", () => {
  it("detects the runtime PGlite package version", () => {
    expect(pgliteRuntimeVersion()).toMatch(/^\d+\.\d+\.\d+/);
  });

  it("stamps snapshots of data directories that carry PG_VERSION", async () => {
    const root = await makeTempDir("cas-compat-stamp");
    const dataDir = join(root, "data");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "PG_VERSION"), "17\n");
    await writeFile(join(dataDir, "db-file"), "content");

    const manifest = await snapshotToCas({ dataDir, casDir: join(root, "cas") });

    expect(manifest.compatibility).toEqual({
      manifestFormat: CAS_MANIFEST_FORMAT,
      pgMajorVersion: "17",
      pgliteVersion: pgliteRuntimeVersion(),
    });
    expect(validateCasManifest(manifest)).toBe(manifest);
  });

  it("leaves snapshots of non-PGlite directories unstamped", async () => {
    const root = await makeTempDir("cas-compat-unstamped");
    const dataDir = join(root, "data");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "db-file"), "content");

    const manifest = await snapshotToCas({ dataDir, casDir: join(root, "cas") });

    expect(manifest.compatibility).toBeUndefined();
  });

  it("reads local compatibility from an initialized data directory", async () => {
    const root = await makeTempDir("cas-compat-local");
    const dataDir = join(root, "data");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "PG_VERSION"), "17");

    await expect(readDataDirCasCompatibility(dataDir)).resolves.toMatchObject({
      manifestFormat: CAS_MANIFEST_FORMAT,
      pgMajorVersion: "17",
    });
    await expect(readDataDirCasCompatibility(join(root, "missing"))).resolves.toBeUndefined();
  });

  it("rejects restore when expected compatibility does not match", async () => {
    const root = await makeTempDir("cas-compat-restore");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    await mkdir(dataDir, { recursive: true });
    await writeFile(join(dataDir, "PG_VERSION"), "17");
    await writeFile(join(dataDir, "db-file"), "content");
    const manifest = await snapshotToCas({ dataDir, casDir });

    const incompatible: CasCompatibility = {
      manifestFormat: CAS_MANIFEST_FORMAT,
      pgMajorVersion: "16",
    };
    await expect(
      restoreFromCas({
        manifest,
        casDir,
        targetDir: join(root, "target"),
        expectedCompatibility: incompatible,
      }),
    ).rejects.toThrow("compatibility mismatch");
  });

  it("rejects catalog-and-data partition restore of unstamped manifests", async () => {
    const root = await makeTempDir("cas-compat-bootstrap");

    await expect(
      restorePartitionCasSnapshot({
        snapshot: partitionSnapshot(unstampedManifest(), "catalog-and-data"),
        casDir: join(root, "cas"),
        targetDir: join(root, "target"),
      }),
    ).rejects.toThrow("no compatibility stamp");
  });

  it("rejects data-only restore into an uninitialized target directory", async () => {
    const root = await makeTempDir("cas-compat-data-only-uninit");

    await expect(
      restorePartitionDataOnlyCasSnapshot({
        snapshot: partitionSnapshot(stampedManifest("17"), "data-only"),
        casDir: join(root, "cas"),
        targetDir: join(root, "target"),
      }),
    ).rejects.toThrow("not an initialized PGlite data directory");
  });

  it("rejects data-only restore when the target Postgres major version differs", async () => {
    const root = await makeTempDir("cas-compat-data-only-mismatch");
    const targetDir = join(root, "target");
    await mkdir(targetDir, { recursive: true });
    await writeFile(join(targetDir, "PG_VERSION"), "16");

    await expect(
      restorePartitionDataOnlyCasSnapshot({
        snapshot: partitionSnapshot(stampedManifest("17"), "data-only"),
        casDir: join(root, "cas"),
        targetDir,
      }),
    ).rejects.toThrow("compatibility mismatch");
  });

  it("rejects data-only restore of unstamped manifests", async () => {
    const root = await makeTempDir("cas-compat-data-only-unstamped");
    const targetDir = join(root, "target");
    await mkdir(targetDir, { recursive: true });
    await writeFile(join(targetDir, "PG_VERSION"), "17");

    await expect(
      restorePartitionDataOnlyCasSnapshot({
        snapshot: partitionSnapshot(unstampedManifest(), "data-only"),
        casDir: join(root, "cas"),
        targetDir,
      }),
    ).rejects.toThrow("no compatibility stamp");
  });

  it("rejects manifests with malformed compatibility stamps", () => {
    const manifest = {
      ...unstampedManifest(),
      compatibility: { manifestFormat: 99, pgMajorVersion: "17" },
    };
    expect(() => validateCasManifest(manifest)).toThrow("manifestFormat is unsupported");
  });
});

function stampedManifest(pgMajorVersion: string): CasManifest {
  return {
    ...unstampedManifest(),
    compatibility: {
      manifestFormat: CAS_MANIFEST_FORMAT,
      pgMajorVersion,
      pgliteVersion: pgliteRuntimeVersion(),
    },
  };
}

function unstampedManifest(): CasManifest {
  return {
    version: 1,
    snapshotId: "partition-snapshot:tenant:jp:fixed",
    parentSnapshotIds: [],
    chunkSize: 64 * 1024,
    directories: [],
    files: [],
  };
}

function partitionSnapshot(manifest: CasManifest, snapshotScope: "catalog-and-data" | "data-only") {
  return {
    version: 1 as const,
    snapshotId: manifest.snapshotId,
    partitionId: "tenant:jp",
    storageBinding: {
      partitionId: "tenant:jp",
      rootRelationOid: 16_384,
      snapshotScope,
      relations: [
        {
          databaseOid: 5,
          tablespaceOid: 1_668,
          relationOid: 16_384,
          relfilenode: 16_384,
        },
      ],
    },
    highWatermark: [] as [string, string][],
    manifest,
  };
}
