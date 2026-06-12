# ADR 0009: Physical Replication Contract (SnapshotSource)

## Status

Accepted (design). Wire-protocol implementation deferred; see Consequences.

## Context

ADR 0002 gave the logical plane one contract: every way of moving events —
in-process, hub WebSocket, P2P — implements `Transport`. The physical plane
never received the equivalent. Snapshot _semantics_ are specified (ADR 0007
validity fences, ADR 0008 anchors), but snapshot _acquisition_ — how a node
discovers which snapshots a peer advertises and obtains their blocks — has
no interface. The ambiguity is visible in the API:

- `pullSnapshot` takes `snapshotEntries` as a parameter: discovery leaked
  to the caller.
- `pullSnapshot` takes `sourceCasDir`: a physical-location assumption
  (shared filesystem) leaked into a facade signature.
- The browser playground solves the same problem differently (an HTTP CAS
  relay namespace). Two ad hoc shapes for one problem is the signature of
  a missing abstraction.
- `SnapshotRequiredError` (ADR 0008) marks the exact point where the
  logical plane must invoke the physical plane, and there is nothing to
  invoke.

## Decision

1. **Core defines `SnapshotSource`, the physical-plane peer contract**,
   with exactly two responsibilities:
   - _Discovery_: list the snapshot catalog entries a peer advertises for
     a partition (high-watermarks, components, compatibility stamps
     included), plus optionally the peer's storage catalog manifest.
   - _Transfer_: ensure the blocks referenced by one entry's manifest are
     present in the local block store, returning how many were fetched.
2. **Validity, selection, and restore stay out of the contract.** ADR 0007
   fencing and pull-requirement selection remain core policy applied to
   whatever a source returns; restore remains the local storage layer
   (`SnapshotDataFilesystem` / `CasBlockStore`). A source implementation
   must never get smarter than "list and fetch": a malicious or buggy
   source can at worst fail verification, not corrupt state.
3. **Content addressing is what makes the contract small.** Blocks are
   hash-verified on receipt, so transfer is idempotent, resumable, and
   requires no trust in the provider. A relay carrying this contract stays
   dumb — transport and authorization only — consistent with the
   ecosystem boundary already documented in ARCH.md.
4. **Facades accept a configured `SnapshotSource` and use it for
   recovery.** `catchUpFrom` that hits `SnapshotRequiredError` falls back
   automatically: discover entries from the peer, pull through the source,
   replay after the snapshot high-watermark, then catch up again. Without
   a configured source the error propagates as before.
5. **The parameter-based forms are a migration path.** `snapshotEntries` /
   `sourceCasDir` on `pullSnapshot` (and `sourceCasStore` on the browser
   restore API) remain as the degenerate "caller is the source" case, but
   new capability lands on the contract, not on new parameters.

## Consequences

- In-process (tests), shared-directory (Node), HTTP relay (browser), and a
  future wire-protocol `block-request`/`block-response` become
  implementations of one interface, exactly as MemoryTransport/WebSocket/
  P2P are for `Transport`.
- ADR 0008's automatic recovery story closes: snapshot-required is no
  longer a dead end that surfaces to the application by default.
- The wire-protocol implementation is deferred work with a known shape; it
  extends the protocol introduced as v2, not the contract.
- The browser facade's whole-filesystem restore API predates partition
  snapshots and adapts to the contract when browser partition snapshots
  land; until then it keeps the parameter form.

## Relationship to prior ADRs

- ADR 0002: this is the same decision applied to the physical plane.
- ADR 0007: fences run on the consumer side of the contract, unchanged.
- ADR 0008: `SnapshotSource` is the interface the snapshot-required
  recovery path calls into.
