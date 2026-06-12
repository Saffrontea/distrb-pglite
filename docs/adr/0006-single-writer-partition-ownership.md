# ADR 0006: Single-Writer Partition Ownership

## Decision

Each partition has exactly one writer node at a time. Conflict resolution between concurrent writers is out of scope: the system does not implement LWW, CRDTs, or merge procedures. Other nodes replicate a partition as read-only subscribers until ownership is explicitly transferred.

## Consequences

- Eventual consistency means "subscribers converge to the owner's stream", not "concurrent writes merge".
- Idempotent apply keyed by `(source_node_id, event_id)` remains sufficient; no merge logic is layered on top.
- Ownership transfer is an explicit operation and must fence the old writer before the new writer starts emitting.
- Events for one partition from two source nodes without an intervening transfer indicate a protocol violation, not a conflict to resolve.
- Application schemas must choose partition keys that match real write ownership (per tenant, per device, per site).

## Guarantee Level (Amendment)

The library detects violations; it does not prevent them. The stance is
"writing without ownership is allowed by the system, but nothing about the
result is guaranteed":

- Enforcement is opt-in. Without a `SingleWriterRegistry`, nodes accept any
  event stream and never check ownership.
- Ownership is local knowledge. `claimPartition` / `transferPartition` update
  only the local registry; they are not replicated, persisted, or fenced.
  Distributing ownership state (and fencing the old writer) is the
  application's or a coordination layer's responsibility.
- With a registry, violating remote events are rejected (skipped and
  reported via `onSingleWriterViolation` / telemetry), and violating local
  writes throw. Rejected events are not buffered: a node that rejected
  events for the wrong-owner stream does not converge with nodes that
  accepted them.
- A durable, replicated ownership-transfer protocol stays out of scope until
  it is designed end to end.
