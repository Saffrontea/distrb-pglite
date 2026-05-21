# ADR 0005: Escalate From PGlite Black Box To Storage-Layer Work

## Decision

The MVP starts by treating PGlite as a black box, but the full architecture requires storage-layer control. The project will escalate in stages:

1. Observe and snapshot PGlite data-directory files.
2. Add an external CoW/CAS layer around those files.
3. Integrate CoW/CAS through the closest supported PGlite filesystem boundary.
4. Research smgr-level Postgres changes only after the file-level invariants are proven.

## Consequences

- Early phases remain useful without maintaining a PGlite fork.
- The CoW manifest, snapshot, GC, and replay semantics can be tested before touching Postgres internals.
- smgr work is kept as a deliberate research track because it changes the build, recovery, and compatibility surface.
- The final architecture is still allowed to patch PGlite/Postgres if file-level integration is insufficient.
