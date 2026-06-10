import {
  InMemoryPartitionStore,
  MemoryTransport,
  SyncNode,
  type NodeId,
} from "@distrb-pglite/core";

export {
  expectStoresConverged,
  FaultInjectionTransport,
  mulberry32,
  partitionEventKey,
} from "./fault-injection.js";
export type { TransportFaultOptions, TransportFaultStats } from "./fault-injection.js";

export type TestNode = {
  node: SyncNode;
  store: InMemoryPartitionStore;
};

export function createMemoryCluster(nodeIds: NodeId[]): {
  transport: MemoryTransport;
  nodes: Map<NodeId, TestNode>;
} {
  const transport = new MemoryTransport();
  const nodes = new Map<NodeId, TestNode>();

  for (const nodeId of nodeIds) {
    const store = new InMemoryPartitionStore(nodeId);
    nodes.set(nodeId, {
      node: new SyncNode(nodeId, store, transport),
      store,
    });
  }

  return { transport, nodes };
}
