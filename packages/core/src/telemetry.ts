import type { EventId, NodeId, PartitionId, SnapshotId } from "./types.js";

// Telemetry stays a plain structured-event sink so the library never depends
// on OpenTelemetry. An OTel (or logging, or metrics) integration is one
// adapter function from SyncTelemetryEvent to spans/counters on the app side.
export type SyncTelemetryEvent =
  | {
      type: "sync-started";
      nodeId: NodeId;
    }
  | {
      type: "sync-stopped";
      nodeId: NodeId;
    }
  | {
      type: "subscription-recovered";
      nodeId: NodeId;
      partitionId: PartitionId;
      peerNodeId: NodeId;
      applied: number;
    }
  | {
      type: "local-write";
      nodeId: NodeId;
      partitionId: PartitionId;
      eventId: EventId;
    }
  | {
      type: "remote-event";
      nodeId: NodeId;
      partitionId: PartitionId;
      sourceNodeId: NodeId;
      eventId: EventId;
      // applied: new event merged; duplicate: idempotent re-delivery;
      // rejected-single-writer: skipped by the ADR 0006 registry.
      outcome: "applied" | "duplicate" | "rejected-single-writer";
      // live: pushed by the transport; replay: pulled during catchup.
      context: "live" | "replay";
    }
  | {
      type: "replay-served";
      nodeId: NodeId;
      partitionId: PartitionId;
      events: number;
    }
  | {
      type: "catchup";
      nodeId: NodeId;
      peerNodeId: NodeId;
      partitionId: PartitionId;
      // catchup starts from the local high-watermark; reconcile from zero.
      mode: "catchup" | "reconcile";
      received: number;
      applied: number;
      durationMs: number;
    }
  | {
      type: "snapshot-created";
      nodeId: NodeId;
      // Browser whole-filesystem snapshots are not partition-scoped.
      partitionId?: PartitionId;
      snapshotId: SnapshotId;
      durationMs: number;
    }
  | {
      type: "snapshot-pulled";
      nodeId: NodeId;
      partitionId?: PartitionId;
      snapshotId: SnapshotId;
      transferredBlocks: number;
      durationMs: number;
    }
  | {
      type: "cas-gc";
      nodeId: NodeId;
      groupId: string;
      deletedBlocks: number;
      retainedBlocks: number;
      dryRun: boolean;
      durationMs: number;
    };

export type SyncTelemetrySink = (event: SyncTelemetryEvent) => void;

// A sink must never be able to break replication: errors are swallowed.
export function emitTelemetry(
  sink: SyncTelemetrySink | undefined,
  event: SyncTelemetryEvent,
): void {
  if (!sink) {
    return;
  }
  try {
    sink(event);
  } catch {
    // Telemetry is observe-only by contract; a throwing sink is a bug in the
    // sink, not a reason to fail the sync operation that emitted the event.
  }
}
