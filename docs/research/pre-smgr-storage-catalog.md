# Pre-smgr Storage Catalog Plan

Before moving below the filesystem boundary into smgr, the prototype needs a persistent storage catalog and DDL control plane. The goal is to avoid patching Postgres until the FS/CAS layer has exhausted the invariants it can express.

## 1. Catalog And Data Snapshot Components

Snapshot production and pull requirements must be separate.

- A snapshot producer may publish `catalog`, `data`, or both components.
- A pulling node must declare what it needs: bootstrap usually needs catalog plus data; an already-compatible node may need only data; DDL catch-up may need catalog without data.
- `catalog-and-data` remains the current restorable bootstrap path, but it should become an explicit component set rather than a single opaque mode.

Open point: catalog components cannot safely mean "all catalog files forever". The next prototype step should define a catalog manifest shape that records relation definitions, binding metadata, and DDL generation before selecting physical catalog pages.

## 2. Catalog Persistence

Storage bindings need durable catalog rows, not only CLI JSON or discovered runtime metadata.

Minimum catalog tables:

- binding group: root relation, DDL generation, vacuum/GC policy,
- binding: `PartitionId`, root relation OID, snapshot components, pull requirements,
- relation locator: database OID, tablespace OID, relation OID, relfilenode, storage path, relation role,
- snapshot catalog: snapshot id, component set, high-watermarks, manifest roots, DDL generation.

This can start as PGlite application tables, then move into a Postgres extension catalog once the DDL surface stabilizes.

Prototype status: `distrb_storage_binding_groups`, `distrb_storage_bindings`, and `distrb_storage_binding_relations` now persist binding groups, partition bindings, and physical relation locators in PGlite. Node can reload a binding group after reopening PGlite and use the stored binding to create a partition snapshot without CLI JSON. `distrb_storage_snapshots` now records snapshot ids, component sets, high-watermarks, DDL generations, manifest kind, and manifest JSON for produced partition snapshots.

## 3. Postgres Extension DDL

The extension should own DDL helpers instead of requiring application code to maintain bindings by hand.

Candidate DDL/API:

```sql
SELECT distrb_create_binding_group(
  group_name => 'sensor_readings',
  root_relation => 'public.sensor_readings'
);

SELECT distrb_create_root_table(
  group_name => 'sensor_readings',
  root_relation_name => 'public.sensor_readings',
  table_definition => 'tenant_id text NOT NULL, sensor_id text NOT NULL, reading integer NOT NULL'
);

SELECT distrb_bind_partition(
  group_name => 'sensor_readings',
  partition_id => 'tenant:jp',
  child_relation => 'public.sensor_readings_tenant_jp'
);

SELECT distrb_create_bound_table(
  group_name => 'sensor_readings',
  partition_id => 'tenant:jp',
  child_relation_name => 'public.sensor_readings_tenant_jp',
  table_definition => 'LIKE public.sensor_readings INCLUDING ALL'
);

SELECT distrb_snapshot_policy(
  group_name => 'sensor_readings',
  snapshot_components => ARRAY['catalog', 'data'],
  pull_requires => ARRAY['data']
);
```

The early implementation can simulate this with ordinary SQL tables and SQL functions. Node helpers may call those functions for tests and demos, but the catalog contract must remain SQL-owned so it can move into a Postgres extension without becoming a Node API.

Prototype status: PGlite now installs `distrb_create_binding_group`, `distrb_bind_partition`, `distrb_create_root_table`, and `distrb_create_bound_table` functions. `distrb_create_binding_group` records an existing root relation and initial DDL generation. `distrb_create_root_table` creates the root table and records the group in one SQL call. `distrb_bind_partition` attaches one or more existing child relations to a partition, increments the group DDL generation, records relation roles and parent relation OIDs in normalized relation rows, and persists the updated binding. `distrb_create_bound_table` creates a child table and binds it in one SQL call. Node helpers are wrappers around these SQL functions where possible.

## 4. Child Relation Groups

A single logical partition may map to multiple physical relations:

- child tables,
- indexes,
- TOAST relations,
- future side relations owned by extension features.

`StorageBindingGroup` groups those bindings under one root relation and DDL generation. Each `StorageRelationLocator` can carry a role and parent relation OID so snapshot selection and replay can include all required child files.

Prototype status: child table relations are now discovered from PGlite catalog metadata and stored with `role: "child"` and `parentRelationOid`.

## 5. Vacuum And GC Coordination

Vacuum and CAS GC must be delayed until no active snapshot generation references the old relation files or blocks.

Policy inputs:

- binding group DDL generation,
- snapshot generation retention,
- active pull cursors,
- CAS manifest roots still reachable from partition snapshots,
- relation file lineage after DDL or vacuum rewrites.

The first enforceable rule should be conservative: do not GC blocks referenced by any retained snapshot manifest, and do not advance `gcAfterGeneration` past the minimum active pull generation.

Prototype status: core now exposes pull requirement helpers that resolve `bootstrap`, `catalog-refresh`, and `data-refresh` into explicit snapshot components. Core can select the newest persisted snapshot catalog entry that satisfies a pull requirement. Core also computes a conservative GC plan from binding group DDL generation, retained generations, active pull generations, and retained snapshot manifest generations.

## Before smgr

Do not start smgr patching until these are true:

- snapshot component requirements are explicit in metadata,
- binding groups persist across process restarts,
- Node can derive bindings from catalog rows rather than CLI JSON,
- child table/index/TOAST relation discovery is represented in bindings,
- vacuum/GC retention can be computed from binding groups and snapshot manifests.
