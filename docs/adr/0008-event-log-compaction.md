# ADR 0008: Snapshot-Anchored Event Log Compaction

## Status

Accepted (design). Implementation phased; see Rollout.

## Context

The event log is append-only and theoretically unbounded. A system that runs
indefinitely on finite storage needs compaction. Pruning interacts with
ADR 0007: as long as the full log exists, the logical layer alone is the
source of truth. Once events are deleted, something must take over their
role for consumers that still need them.

Consumers of old events are: peers catching up from a cursor, new nodes
bootstrapping via full replay (`reconcileFrom`), and local projections
(already applied, no longer needed). The CAS snapshot machinery already
records per-source high-watermarks at snapshot time, which provides a
natural compaction anchor.

## Decision

1. **Snapshot-anchored pruning.** Events for a partition may be deleted only
   at or below the high-watermark of a retained, bootstrap-capable
   (`catalog-and-data`) snapshot catalog entry. A node that has fallen
   behind the prune boundary recovers by snapshot pull plus replay of
   events after the snapshot high-watermark — never by partial replay.
2. **`pruned_through` is a durable per-partition cursor vector**
   (`source_node_id -> event_id`), not a scalar. Ownership transfers
   (ADR 0006) leave multiple source nodes in one partition's history, so a
   scalar boundary is insufficient. Pruning advances `pruned_through`
   atomically with event deletion.
3. **Replay gaps are a first-class wire response, not an error.** A replay
   request with any cursor below `pruned_through` is answered with a
   `snapshot-required` response variant carrying the `pruned_through`
   vector (and, when available, references to satisfying snapshot catalog
   entries). Serving a silently partial replay is forbidden: it causes
   undetectable divergence. `reconcileFrom` handles the variant by falling
   back to snapshot pull. This introduces the first wire-protocol version
   field; pre-versioned peers are assumed to predate pruning.
4. **Pruning is explicit-API-only in the first phase.** Automatic pruning
   driven by `vacuumPolicy` (symmetric with CAS block GC) is deferred until
   the gap protocol has been exercised. Deleting data automatically starts
   on the conservative side, consistent with the library's
   detection-over-enforcement stance.
5. **Subscriber prune is a separate, weaker class.** A node that serves
   replay to no peer (no inbound `recoverFrom` dependency) may prune
   applied remote events without a snapshot anchor; only its own
   serveability is affected. The strict rules above apply to nodes that
   serve replay.

## Relationship to ADR 0007

ADR 0007 stands unchanged for the unpruned suffix of the log: the logical
layer remains authoritative, and physical snapshots remain optimizations
with validity fences. Below the prune boundary, the retained anchor
snapshot is no longer an optimization — it is the only recovery path and
therefore carries a durability requirement: an anchor snapshot may not be
GC'd while `pruned_through` depends on it. CAS GC retention and event
pruning must therefore be driven by the same retention catalog.

## Consequences

- "Every node must remain reconstructible" (ADR 0007 §4) is preserved, but
  reconstruction below the prune boundary requires the anchor snapshot;
  losing all anchor snapshots after pruning is data loss by design.
- The wire protocol gains a version field and a `snapshot-required`
  response; all transports (memory, WebSocket hub, P2P) carry it
  identically.
- Browser subscriber nodes get a usable storage bound without needing the
  full snapshot stack, via the subscriber-prune class.
- Telemetry gains an `events-pruned` event (partition, pruned counts,
  new `pruned_through`).

## Rollout

1. Core: durable `pruned_through`, store-level prune operation with anchor
   validation, replay-gap rejection in `listEventsAfterCursors` serving.
2. Wire: `snapshot-required` response variant plus protocol version field;
   `reconcileFrom` / catchup fallback to snapshot pull.
3. Facade: explicit prune API wired to the snapshot catalog; auto-prune
   policy alongside auto CAS GC once (1)–(2) are proven.
