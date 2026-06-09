#!/usr/bin/env node
import { parseArgs } from "node:util";
import { restoreFromCas, snapshotToCas } from "./cas.js";
import { DistributedPGlite } from "./distributed-pglite.js";
import { copyDataDirSnapshot, inventoryDataDir } from "./inventory.js";
import { createPartitionCasSnapshot } from "./partition-snapshot.js";
import { discoverStoragePartitionBinding } from "./storage-binding.js";
import { WebSocketHub } from "./websocket-transport.js";
import { runNodeDemo } from "./demo.js";

const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: {
    offline: { type: "boolean", default: false },
    port: { type: "string", default: "8787" },
    url: { type: "string", default: "ws://localhost:8787" },
    node: { type: "string" },
    peer: { type: "string" },
    dataDir: { type: "string" },
    casDir: { type: "string" },
    targetDir: { type: "string" },
    manifest: { type: "string" },
    partition: { type: "string" },
    rootRelation: { type: "string" },
    relation: { type: "string", multiple: true },
    snapshotScope: { type: "string", default: "catalog-and-data" },
    payload: { type: "string" },
    storageBinding: { type: "string" },
  },
});

const command = positionals[0];

if (command === "hub") {
  const port = Number(values.port);
  new WebSocketHub({ port });
  console.log(`distrb-pglite hub listening on ws://localhost:${port}`);
} else if (command === "node") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition) {
    throw new Error("--node and --partition are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  await openDistributedNode(nodeId, dataDir, values.url, [partition]);

  console.log(`${nodeId} subscribed to ${partition} via ${values.url}`);
} else if (command === "write") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition || !values.payload) {
    throw new Error("--node, --partition, and --payload are required");
  }

  const db = await openDistributedNode(nodeId, values.dataDir, values.url, [], !values.offline);
  const payload = JSON.parse(values.payload);

  if (values.offline) {
    const event = await db.writeLocal({
      partitionId: partition,
      payload,
    });
    await db.close();
    console.log(JSON.stringify(event, null, 2));
  } else {
    const event = await db.write({
      partitionId: partition,
      payload,
    });
    await db.close();
    console.log(JSON.stringify(event, null, 2));
  }
} else if (command === "list") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition) {
    throw new Error("--node and --partition are required");
  }

  const db = await openDistributedNode(nodeId, values.dataDir, values.url, [], false);
  const events = await db.store.listEventsAfterCursors(partition, new Map());
  await db.close();
  console.log(JSON.stringify(events, null, 2));
} else if (command === "catchup") {
  const nodeId = values.node;
  const peerNodeId = values.peer;
  const partition = values.partition;
  if (!nodeId || !peerNodeId || !partition) {
    throw new Error("--node, --peer, and --partition are required");
  }

  const db = await openDistributedNode(nodeId, values.dataDir, values.url, [partition]);
  const applied = await db.catchUpFrom(peerNodeId, partition);
  await db.close();
  console.log(JSON.stringify({ applied }, null, 2));
} else if (command === "inventory") {
  const nodeId = values.node;
  if (!nodeId) {
    throw new Error("--node is required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  console.log(JSON.stringify(await inventoryDataDir(dataDir), null, 2));
} else if (command === "copy-snapshot") {
  const nodeId = values.node;
  if (!nodeId || !values.targetDir) {
    throw new Error("--node and --targetDir are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  console.log(
    JSON.stringify(
      await copyDataDirSnapshot({
        sourceDir: dataDir,
        targetDir: values.targetDir,
      }),
      null,
      2,
    ),
  );
} else if (command === "cas-snapshot") {
  const nodeId = values.node;
  if (!nodeId || !values.casDir) {
    throw new Error("--node and --casDir are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  const manifest = await snapshotToCas({ dataDir, casDir: values.casDir });
  console.log(JSON.stringify(manifest, null, 2));
} else if (command === "partition-snapshot") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition || !values.casDir) {
    throw new Error("--node, --partition, and --casDir are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  const handle = await openDistributedNode(nodeId, dataDir, values.url, [], false);
  const storageBinding = values.storageBinding
    ? JSON.parse(values.storageBinding)
    : await discoverStoragePartitionBinding({
        db: handle.db,
        partitionId: partition,
        rootRelation: requireString(
          values.rootRelation,
          "--rootRelation is required without --storageBinding",
        ),
        relations: relationValues(values.relation),
        snapshotScope: snapshotScopeValue(values.snapshotScope),
      });
  const snapshot = await createPartitionCasSnapshot({
    store: handle.eventStore,
    partitionId: partition,
    storageBinding,
    dataDir,
    casDir: values.casDir,
  });
  await handle.close();
  console.log(JSON.stringify(snapshot, null, 2));
} else if (command === "storage-binding") {
  const nodeId = values.node;
  const partition = values.partition;
  if (!nodeId || !partition || !values.rootRelation) {
    throw new Error("--node, --partition, and --rootRelation are required");
  }

  const dataDir = values.dataDir ?? `./.data/${nodeId}`;
  const handle = await openDistributedNode(nodeId, dataDir, values.url, [], false);
  const binding = await discoverStoragePartitionBinding({
    db: handle.db,
    partitionId: partition,
    rootRelation: values.rootRelation,
    relations: relationValues(values.relation),
    snapshotScope: snapshotScopeValue(values.snapshotScope),
  });
  await handle.close();
  console.log(JSON.stringify(binding, null, 2));
} else if (command === "cas-restore") {
  if (!values.casDir || !values.targetDir || !values.manifest) {
    throw new Error("--casDir, --targetDir, and --manifest are required");
  }

  await restoreFromCas({
    casDir: values.casDir,
    targetDir: values.targetDir,
    manifest: JSON.parse(values.manifest),
  });
} else if (command === "demo") {
  const summary = await runNodeDemo({
    baseDir: values.dataDir,
    port: Number(values.port),
  });
  console.log(JSON.stringify(summary, null, 2));
} else {
  console.log(`Usage:
  distrb-pglite hub --port 8787
  distrb-pglite demo --dataDir ./.data/phase6-demo --port 8787
  distrb-pglite node --node node-a --partition tenant:jp --url ws://localhost:8787
  distrb-pglite write --node node-a --partition tenant:jp --payload '{"title":"hello"}' --url ws://localhost:8787
  distrb-pglite write --node node-a --partition tenant:jp --payload '{"title":"hello"}' --offline
  distrb-pglite list --node node-a --partition tenant:jp
  distrb-pglite catchup --node node-b --peer node-a --partition tenant:jp --url ws://localhost:8787
  distrb-pglite inventory --node node-a
  distrb-pglite copy-snapshot --node node-a --targetDir ./.restore/node-a
  distrb-pglite cas-snapshot --node node-a --casDir ./.cas
  distrb-pglite storage-binding --node node-a --partition tenant:jp --rootRelation public.sensor_readings --snapshotScope data-only
  distrb-pglite partition-snapshot --node node-a --partition tenant:jp --casDir ./.cas --storageBinding '<json>'
  distrb-pglite partition-snapshot --node node-a --partition tenant:jp --casDir ./.cas --rootRelation public.sensor_readings --snapshotScope data-only
  distrb-pglite cas-restore --casDir ./.cas --targetDir ./.restore/node-a --manifest '<json>'
`);
}

async function openDistributedNode(
  nodeId: string,
  dataDir = `./.data/${nodeId}`,
  url: string | boolean | undefined,
  partitions: readonly string[] = [],
  autoStart = true,
): Promise<DistributedPGlite> {
  return DistributedPGlite.open({
    nodeId,
    dataDir,
    transport: { type: "websocket", url: typeof url === "string" ? url : "ws://localhost:8787" },
    autoStart,
    partitions: partitions.map((id) => ({ id })),
  });
}

function relationValues(value: string | string[] | undefined): string[] | undefined {
  if (!value) {
    return undefined;
  }
  return Array.isArray(value) ? value : [value];
}

function snapshotScopeValue(value: string | boolean | undefined): "data-only" | "catalog-and-data" {
  if (value === "data-only" || value === "catalog-and-data") {
    return value;
  }
  throw new Error("--snapshotScope must be data-only or catalog-and-data");
}

function requireString(value: string | boolean | undefined, message: string): string {
  if (typeof value !== "string" || value.length === 0) {
    throw new Error(message);
  }
  return value;
}
