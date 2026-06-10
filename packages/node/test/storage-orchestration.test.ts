import { createHash } from "node:crypto";
import { mkdir, readFile, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { describe, expect, it } from "vitest";
import { CAS_MANIFEST_FORMAT, type CasCompatibility } from "@distrb-pglite/core";
import {
  bindStoragePartition,
  createManagedPartitionCasSnapshot,
  createStorageBindingGroup,
  loadStorageBindingGroup,
  listStorageSnapshotCatalogEntries,
  PGlitePartitionStore,
  pgliteRuntimeVersion,
  pullPartitionCasSnapshot,
  type CasManifest,
} from "../src/index.js";
import { makeTempDir } from "./temp-dir.js";

describe("storage orchestration", () => {
  it("refreshes storage drift and saves a snapshot catalog entry before CAS snapshot", async () => {
    const root = await makeTempDir("managed-snapshot");
    const dataDir = join(root, "data");
    const casDir = join(root, "cas");
    const db = new PGlite(dataDir, { relaxedDurability: false });
    const store = new PGlitePartitionStore("node-a", db);
    await store.migrate();
    await db.exec(`
      CREATE TABLE sensor_readings (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
      CREATE TABLE sensor_readings_tenant_jp (
        tenant_id text NOT NULL,
        sensor_id text NOT NULL,
        reading integer NOT NULL
      );
    `);
    await createStorageBindingGroup({
      db,
      groupId: "sensor_readings",
      rootRelation: "public.sensor_readings",
    });
    await bindStoragePartition({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      relations: ["public.sensor_readings_tenant_jp"],
      snapshotScope: "data-only",
    });
    await db.exec(
      "CREATE INDEX sensor_readings_tenant_jp_sensor_id ON sensor_readings_tenant_jp (sensor_id)",
    );
    await db.query(
      "INSERT INTO sensor_readings_tenant_jp (tenant_id, sensor_id, reading) VALUES ($1, $2, $3)",
      ["tenant:jp", "tokyo-1", 42],
    );
    await store.appendLocal({
      partitionId: "tenant:jp",
      schemaVersion: 1,
      payload: { sensorId: "tokyo-1", reading: 42 },
    });

    const managed = await createManagedPartitionCasSnapshot({
      db,
      store,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
      dataDir,
      casDir,
    });
    const entries = await listStorageSnapshotCatalogEntries<CasManifest>({
      db,
      groupId: "sensor_readings",
      partitionId: "tenant:jp",
    });
    await db.close();

    expect(managed.drift).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          currentRelation: expect.objectContaining({ role: "index" }),
        }),
      ]),
    );
    expect(managed.fence).toEqual([
      expect.objectContaining({ relationName: "sensor_readings_tenant_jp" }),
    ]);
    expect(managed.snapshot.storageBinding).toMatchObject({
      ddlGeneration: 3,
      relations: expect.arrayContaining([expect.objectContaining({ role: "index" })]),
    });
    expect(entries).toHaveLength(1);
    expect(entries[0]).toMatchObject({
      snapshotId: managed.snapshot.snapshotId,
      ddlGeneration: 3,
      manifestKind: "cas",
    });
  }, 120_000);

  it("applies catalog manifests, selects pull snapshots, transfers blocks, and restores CAS data", async () => {
    const root = await makeTempDir("managed-pull");
    const sourceCasDir = join(root, "source-cas");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    await mkdir(sourceCasDir, { recursive: true });
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);
    const catalogContent = Buffer.from("catalog payload");
    const catalogHash = sha256(catalogContent);
    await writeFile(join(sourceCasDir, hash), content);
    await writeFile(join(sourceCasDir, catalogHash), catalogContent);

    const targetDb = new PGlite(join(root, "catalog"));
    const catalogManifest = {
      version: 1 as const,
      groupId: "files",
      rootRelationOid: 1,
      ddlGeneration: 2,
      bindings: [
        {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          boundChildRelationOids: [1],
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child" as const,
              parentRelationOid: 1,
            },
          ],
          snapshotScope: "catalog-and-data" as const,
          snapshotComponents: ["catalog" as const, "data" as const],
          pullRequires: ["catalog" as const, "data" as const],
          ddlGeneration: 2,
        },
      ],
      snapshots: [
        {
          partitionId: "tenant:jp",
          snapshotId: "newer",
          components: ["catalog" as const, "data" as const],
          ddlGeneration: 2,
          manifestKind: "cas" as const,
        },
      ],
    };
    const manifest = manifestForCatalogAndDataPayload(
      "newer",
      content,
      hash,
      catalogContent,
      catalogHash,
    );

    const pulled = await pullPartitionCasSnapshot({
      db: targetDb,
      catalogManifest,
      snapshotEntries: [
        {
          groupId: "files",
          partitionId: "tenant:jp",
          snapshotId: "newer",
          components: ["catalog", "data"],
          highWatermark: [],
          ddlGeneration: 2,
          manifestKind: "cas",
          manifest,
        },
      ],
      groupId: "files",
      partitionId: "tenant:jp",
      requirement: "bootstrap",
      sourceCasDir,
      targetCasDir,
      targetDir,
    });
    const restored = await readFile(join(targetDir, "payload.txt"), "utf8");
    await targetDb.close();

    expect(pulled).toMatchObject({
      transferredBlocks: 2,
      entry: { snapshotId: "newer" },
    });
    expect(restored).toBe("snapshot payload");
  }, 120_000);

  it("does not pull snapshot entries that are not advertised by the applied catalog manifest", async () => {
    const root = await makeTempDir("managed-pull-unadvertised-snapshot");
    const sourceCasDir = join(root, "source-cas");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    await mkdir(sourceCasDir, { recursive: true });
    const advertisedContent = Buffer.from("advertised snapshot");
    const advertisedHash = sha256(advertisedContent);
    const advertisedCatalogContent = Buffer.from("advertised catalog");
    const advertisedCatalogHash = sha256(advertisedCatalogContent);
    const unadvertisedContent = Buffer.from("unadvertised snapshot");
    const unadvertisedHash = sha256(unadvertisedContent);
    const unadvertisedCatalogContent = Buffer.from("unadvertised catalog");
    const unadvertisedCatalogHash = sha256(unadvertisedCatalogContent);
    await writeFile(join(sourceCasDir, advertisedHash), advertisedContent);
    await writeFile(join(sourceCasDir, advertisedCatalogHash), advertisedCatalogContent);
    await writeFile(join(sourceCasDir, unadvertisedHash), unadvertisedContent);
    await writeFile(join(sourceCasDir, unadvertisedCatalogHash), unadvertisedCatalogContent);

    const targetDb = new PGlite(join(root, "catalog"));
    const catalogManifest = {
      version: 1 as const,
      groupId: "files",
      rootRelationOid: 1,
      ddlGeneration: 2,
      bindings: [
        {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          boundChildRelationOids: [1],
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child" as const,
              parentRelationOid: 1,
            },
          ],
          snapshotScope: "catalog-and-data" as const,
          snapshotComponents: ["catalog" as const, "data" as const],
          pullRequires: ["catalog" as const, "data" as const],
          ddlGeneration: 2,
        },
      ],
      snapshots: [
        {
          partitionId: "tenant:jp",
          snapshotId: "advertised",
          components: ["catalog" as const, "data" as const],
          ddlGeneration: 2,
          manifestKind: "cas" as const,
        },
      ],
    };

    const pulled = await pullPartitionCasSnapshot({
      db: targetDb,
      catalogManifest,
      snapshotEntries: [
        {
          groupId: "files",
          partitionId: "tenant:jp",
          snapshotId: "zz-unadvertised",
          components: ["catalog", "data"],
          highWatermark: [],
          ddlGeneration: 2,
          manifestKind: "cas",
          manifest: manifestForCatalogAndDataPayload(
            "zz-unadvertised",
            unadvertisedContent,
            unadvertisedHash,
            unadvertisedCatalogContent,
            unadvertisedCatalogHash,
          ),
        },
        {
          groupId: "files",
          partitionId: "tenant:jp",
          snapshotId: "advertised",
          components: ["catalog", "data"],
          highWatermark: [],
          ddlGeneration: 2,
          manifestKind: "cas",
          manifest: manifestForCatalogAndDataPayload(
            "advertised",
            advertisedContent,
            advertisedHash,
            advertisedCatalogContent,
            advertisedCatalogHash,
          ),
        },
      ],
      groupId: "files",
      partitionId: "tenant:jp",
      requirement: "bootstrap",
      sourceCasDir,
      targetCasDir,
      targetDir,
    });
    const restored = await readFile(join(targetDir, "payload.txt"), "utf8");
    await targetDb.close();

    expect(pulled).toMatchObject({
      transferredBlocks: 2,
      entry: { snapshotId: "advertised" },
    });
    expect(restored).toBe("advertised snapshot");
  }, 120_000);

  it("rejects data-only pulls when the target storage binding is on a different DDL generation", async () => {
    const root = await makeTempDir("managed-pull-ddl-mismatch");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);
    const manifest: CasManifest = {
      version: 1,
      snapshotId: "newer",
      parentSnapshotIds: [],
      chunkSize: 64 * 1024,
      directories: [],
      files: [
        {
          path: "payload.txt",
          size: content.length,
          sha256: hash,
          components: ["data"],
          blocks: [{ hash, size: content.length }],
        },
      ],
    };

    await expect(
      pullPartitionCasSnapshot({
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "newer",
            components: ["data"],
            highWatermark: [],
            ddlGeneration: 3,
            manifestKind: "cas",
            manifest,
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "data-refresh",
        targetCasDir,
        targetDir,
        storageBinding: {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child",
            },
          ],
          snapshotScope: "data-only",
          ddlGeneration: 2,
        },
      }),
    ).rejects.toThrow("storage binding DDL generation mismatch");
  });

  it("rejects pull snapshot entries whose CAS manifest belongs to another snapshot", async () => {
    const root = await makeTempDir("managed-pull-manifest-id-mismatch");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);

    await expect(
      pullPartitionCasSnapshot({
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "advertised",
            components: ["data"],
            highWatermark: [],
            ddlGeneration: 2,
            manifestKind: "cas",
            manifest: manifestForPayload("different", content, hash),
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "data-refresh",
        targetCasDir,
        targetDir,
        storageBinding: {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child",
            },
          ],
          snapshotScope: "data-only",
          ddlGeneration: 2,
        },
      }),
    ).rejects.toThrow("CAS manifest snapshotId mismatch");
  });

  it("does not apply a pulled catalog manifest when snapshot entry validation fails", async () => {
    const root = await makeTempDir("managed-pull-invalid-entry-no-catalog-side-effect");
    const targetDb = new PGlite(join(root, "catalog"));
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);
    const catalogManifest = {
      version: 1 as const,
      groupId: "files",
      rootRelationOid: 1,
      ddlGeneration: 2,
      bindings: [
        {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          boundChildRelationOids: [1],
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child" as const,
              parentRelationOid: 1,
            },
          ],
          snapshotScope: "data-only" as const,
          ddlGeneration: 2,
        },
      ],
      snapshots: [
        {
          partitionId: "tenant:jp",
          snapshotId: "advertised",
          components: ["data" as const],
          ddlGeneration: 2,
          manifestKind: "cas" as const,
        },
      ],
    };

    await expect(
      pullPartitionCasSnapshot({
        db: targetDb,
        catalogManifest,
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "advertised",
            components: ["data"],
            highWatermark: [],
            ddlGeneration: 2,
            manifestKind: "cas",
            manifest: manifestForPayload("different", content, hash),
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "data-refresh",
        targetCasDir,
        targetDir,
      }),
    ).rejects.toThrow("CAS manifest snapshotId mismatch");

    await expect(loadStorageBindingGroup(targetDb, "files")).resolves.toBeUndefined();
    await targetDb.close();
  });

  it("rejects pull snapshot entries whose components are not present in the CAS manifest", async () => {
    const root = await makeTempDir("managed-pull-manifest-component-mismatch");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);

    await expect(
      pullPartitionCasSnapshot({
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "bootstrap",
            components: ["catalog", "data"],
            highWatermark: [],
            ddlGeneration: 2,
            manifestKind: "cas",
            manifest: manifestForPayload("bootstrap", content, hash),
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "bootstrap",
        targetCasDir,
        targetDir,
        storageBinding: {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child",
            },
          ],
          snapshotScope: "catalog-and-data",
          ddlGeneration: 2,
        },
      }),
    ).rejects.toThrow("snapshot entry component is not present in CAS manifest");
  });

  it("rejects pull snapshot entries whose CAS manifest contains components not advertised by the entry", async () => {
    const root = await makeTempDir("managed-pull-manifest-extra-component");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);
    const catalogContent = Buffer.from("catalog payload");
    const catalogHash = sha256(catalogContent);

    await expect(
      pullPartitionCasSnapshot({
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "data-only-advertised",
            components: ["data"],
            highWatermark: [],
            ddlGeneration: 2,
            manifestKind: "cas",
            manifest: manifestForCatalogAndDataPayload(
              "data-only-advertised",
              content,
              hash,
              catalogContent,
              catalogHash,
            ),
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "data-refresh",
        targetCasDir,
        targetDir,
        storageBinding: {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child",
            },
          ],
          snapshotScope: "data-only",
          ddlGeneration: 2,
        },
      }),
    ).rejects.toThrow("CAS manifest component is not advertised by snapshot entry");
  });

  it("rejects pull snapshot entries with invalid high-watermarks before restore", async () => {
    const root = await makeTempDir("managed-pull-invalid-high-watermark");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);

    await expect(
      pullPartitionCasSnapshot({
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "invalid-hwm",
            components: ["data"],
            highWatermark: [["node-a", "not-a-uuid"]],
            ddlGeneration: 2,
            manifestKind: "cas",
            manifest: manifestForPayload("invalid-hwm", content, hash),
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "data-refresh",
        targetCasDir,
        targetDir,
        storageBinding: {
          partitionId: "tenant:jp",
          rootRelationOid: 1,
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child",
            },
          ],
          snapshotScope: "data-only",
          ddlGeneration: 2,
        },
      }),
    ).rejects.toThrow("invalid high-watermark event id");
  });

  it("skips incompatible newer pull snapshots when an older compatible entry exists", async () => {
    const root = await makeTempDir("managed-pull-compatible-fallback");
    const sourceCasDir = join(root, "source-cas");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    await mkdir(sourceCasDir, { recursive: true });
    const compatibleContent = Buffer.from("compatible snapshot");
    const compatibleHash = sha256(compatibleContent);
    const compatibleCatalogContent = Buffer.from("compatible catalog");
    const compatibleCatalogHash = sha256(compatibleCatalogContent);
    await writeFile(join(sourceCasDir, compatibleHash), compatibleContent);
    await writeFile(join(sourceCasDir, compatibleCatalogHash), compatibleCatalogContent);
    const incompatibleContent = Buffer.from("incompatible snapshot");
    const incompatibleHash = sha256(incompatibleContent);

    const pulled = await pullPartitionCasSnapshot({
      snapshotEntries: [
        {
          groupId: "files",
          partitionId: "tenant:jp",
          snapshotId: "incompatible-newer",
          components: ["catalog", "data"],
          highWatermark: [],
          ddlGeneration: 3,
          manifestKind: "cas",
          manifest: manifestForPayload("incompatible-newer", incompatibleContent, incompatibleHash),
        },
        {
          groupId: "files",
          partitionId: "tenant:jp",
          snapshotId: "compatible-older",
          components: ["catalog", "data"],
          highWatermark: [],
          ddlGeneration: 2,
          manifestKind: "cas",
          manifest: manifestForCatalogAndDataPayload(
            "compatible-older",
            compatibleContent,
            compatibleHash,
            compatibleCatalogContent,
            compatibleCatalogHash,
          ),
        },
      ],
      groupId: "files",
      partitionId: "tenant:jp",
      requirement: "bootstrap",
      sourceCasDir,
      targetCasDir,
      targetDir,
      storageBinding: {
        partitionId: "tenant:jp",
        rootRelationOid: 1,
        relations: [
          {
            databaseOid: 1,
            tablespaceOid: 1,
            relationOid: 1,
            relfilenode: 1,
            storagePath: "payload.txt",
            role: "child",
          },
        ],
        snapshotScope: "catalog-and-data",
        ddlGeneration: 2,
      },
    });
    const restored = await readFile(join(targetDir, "payload.txt"), "utf8");

    expect(pulled).toMatchObject({
      transferredBlocks: 2,
      entry: { snapshotId: "compatible-older" },
    });
    expect(restored).toBe("compatible snapshot");
  });

  it("skips version-incompatible pull snapshots before selection (ADR 0007 version fence)", async () => {
    const root = await makeTempDir("managed-pull-version-fence");
    const sourceCasDir = join(root, "source-cas");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    await mkdir(sourceCasDir, { recursive: true });
    const compatibleContent = Buffer.from("compatible snapshot");
    const compatibleHash = sha256(compatibleContent);
    const compatibleCatalogContent = Buffer.from("compatible catalog");
    const compatibleCatalogHash = sha256(compatibleCatalogContent);
    await writeFile(join(sourceCasDir, compatibleHash), compatibleContent);
    await writeFile(join(sourceCasDir, compatibleCatalogHash), compatibleCatalogContent);
    const foreignContent = Buffer.from("foreign-version snapshot");
    const foreignHash = sha256(foreignContent);
    const foreignManifest: CasManifest = {
      ...manifestForCatalogAndDataPayload(
        "foreign-version-newer",
        foreignContent,
        foreignHash,
        compatibleCatalogContent,
        compatibleCatalogHash,
      ),
      compatibility: {
        manifestFormat: CAS_MANIFEST_FORMAT,
        pgMajorVersion: "17",
        pgliteVersion: "0.0.1-foreign",
      },
    };
    const storageBinding = {
      partitionId: "tenant:jp",
      rootRelationOid: 1,
      relations: [
        {
          databaseOid: 1,
          tablespaceOid: 1,
          relationOid: 1,
          relfilenode: 1,
          storagePath: "payload.txt",
          role: "child" as const,
        },
      ],
      snapshotScope: "catalog-and-data" as const,
      ddlGeneration: 2,
    };

    const pulled = await pullPartitionCasSnapshot({
      snapshotEntries: [
        {
          groupId: "files",
          partitionId: "tenant:jp",
          snapshotId: "foreign-version-newer",
          components: ["catalog", "data"],
          highWatermark: [],
          ddlGeneration: 2,
          manifestKind: "cas",
          manifest: foreignManifest,
        },
        {
          groupId: "files",
          partitionId: "tenant:jp",
          snapshotId: "compatible-older",
          components: ["catalog", "data"],
          highWatermark: [],
          ddlGeneration: 2,
          manifestKind: "cas",
          manifest: manifestForCatalogAndDataPayload(
            "compatible-older",
            compatibleContent,
            compatibleHash,
            compatibleCatalogContent,
            compatibleCatalogHash,
          ),
        },
      ],
      groupId: "files",
      partitionId: "tenant:jp",
      requirement: "bootstrap",
      sourceCasDir,
      targetCasDir,
      targetDir,
      storageBinding,
    });

    expect(pulled).toMatchObject({ entry: { snapshotId: "compatible-older" } });
    await expect(readFile(join(targetDir, "payload.txt"), "utf8")).resolves.toBe(
      "compatible snapshot",
    );

    await expect(
      pullPartitionCasSnapshot({
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "foreign-version-newer",
            components: ["catalog", "data"],
            highWatermark: [],
            ddlGeneration: 2,
            manifestKind: "cas",
            manifest: foreignManifest,
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "bootstrap",
        sourceCasDir,
        targetCasDir,
        targetDir: join(root, "target-2"),
        storageBinding,
      }),
    ).rejects.toThrow("PGlite version mismatch");
  });

  it("rejects pulls when an explicit target storage binding belongs to another partition", async () => {
    const root = await makeTempDir("managed-pull-partition-mismatch");
    const targetCasDir = join(root, "target-cas");
    const targetDir = join(root, "target");
    const content = Buffer.from("snapshot payload");
    const hash = sha256(content);

    await expect(
      pullPartitionCasSnapshot({
        snapshotEntries: [
          {
            groupId: "files",
            partitionId: "tenant:jp",
            snapshotId: "newer",
            components: ["catalog", "data"],
            highWatermark: [],
            ddlGeneration: 2,
            manifestKind: "cas",
            manifest: {
              version: 1,
              snapshotId: "newer",
              parentSnapshotIds: [],
              chunkSize: 64 * 1024,
              directories: [],
              files: [
                {
                  path: "payload.txt",
                  size: content.length,
                  sha256: hash,
                  components: ["data"],
                  blocks: [{ hash, size: content.length }],
                },
              ],
            },
          },
        ],
        groupId: "files",
        partitionId: "tenant:jp",
        requirement: "bootstrap",
        targetCasDir,
        targetDir,
        storageBinding: {
          partitionId: "tenant:us",
          rootRelationOid: 1,
          relations: [
            {
              databaseOid: 1,
              tablespaceOid: 1,
              relationOid: 1,
              relfilenode: 1,
              storagePath: "payload.txt",
              role: "child",
            },
          ],
          snapshotScope: "catalog-and-data",
          ddlGeneration: 2,
        },
      }),
    ).rejects.toThrow("storage binding partition mismatch");
  });
});

function sha256(content: Uint8Array): string {
  return createHash("sha256").update(content).digest("hex");
}

function fixtureCompatibility(): CasCompatibility {
  const pgliteVersion = pgliteRuntimeVersion();
  return {
    manifestFormat: CAS_MANIFEST_FORMAT,
    pgMajorVersion: "17",
    ...(pgliteVersion ? { pgliteVersion } : {}),
  };
}

function manifestForPayload(snapshotId: string, content: Uint8Array, hash: string): CasManifest {
  return {
    version: 1,
    snapshotId,
    parentSnapshotIds: [],
    chunkSize: 64 * 1024,
    compatibility: fixtureCompatibility(),
    directories: [],
    files: [
      {
        path: "payload.txt",
        size: content.length,
        sha256: hash,
        components: ["data"],
        blocks: [{ hash, size: content.length }],
      },
    ],
  };
}

function manifestForCatalogAndDataPayload(
  snapshotId: string,
  content: Uint8Array,
  hash: string,
  catalogContent: Uint8Array,
  catalogHash: string,
): CasManifest {
  return {
    version: 1,
    snapshotId,
    parentSnapshotIds: [],
    chunkSize: 64 * 1024,
    compatibility: fixtureCompatibility(),
    directories: [],
    files: [
      {
        path: "catalog.meta",
        size: catalogContent.length,
        sha256: catalogHash,
        components: ["catalog"],
        blocks: [{ hash: catalogHash, size: catalogContent.length }],
      },
      {
        path: "payload.txt",
        size: content.length,
        sha256: hash,
        components: ["data"],
        blocks: [{ hash, size: content.length }],
      },
    ],
  };
}
