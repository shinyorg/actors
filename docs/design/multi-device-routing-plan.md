# Plan: multi-device routing ("the mesh")

Status: **parked** · 2026-10-02 · design: [multi-device-routing.md](multi-device-routing.md)

Pick-up order: answer the decisions, build phase 0, then go phase by phase. Each phase ends green (all tests
passing, console sample still NativeAOT with zero warnings) and can ship on its own.

## Decisions to make before starting

| # | Question | Default if undecided | Affects |
|---|---|---|---|
| D1 | Target topology: peer-to-peer phones, a hub with satellites, or both? | Both; build `Local`/`Explicit` first | Phase 5 priority |
| D2 | Trust: one shared mesh key, or per-pair keys from pairing? | Shared key; the sample hands it out via pairing | Phase 1 security |
| D3 | Will a real deployment have shared storage (Postgres/Cosmos)? | No, so phase 5 waits | Phase 5 |
| D4 | Package split: `Shiny.Actors.Mesh` (HttpServer + Discovery), with only the seam in core? | Yes | All |
| D5 | Fold `Shiny.Actors.Discovery` into Mesh, or keep it as a standalone helper? | Fold it in; Mesh uses it | Phase 1 |

## Phase 0: groundwork (core, no behavior change)

- [ ] `ActorAddress(string Contract, string Id, NodeId? Home)` with `GetActorAddress()` on proxies, local and remote.
- [ ] **Router seam.** An `IActorRouter` that `ActorReference.Ask/Tell` consults, defaulting to "always local". The
      typed lambda stays the zero-cost local path.
- [ ] **Stable turn ids.** `CallChain` nodes get a stable turn id (node + GUID), so a chain can be serialized later.
- [ ] **Contract fingerprints.** Generator-emitted hash of method keys, parameter and return types, on
      `ActorContract`.
- [ ] **In-process mesh test harness** (in `Shiny.Actors.Testing`): N `ActorSystem`s, a fake membership source,
      an in-memory transport, partitions you can inject, and mesh-wide `WaitForQuietAsync`.
- **Exit:** all existing tests pass unchanged; the harness can run two systems side by side.

## Phase 1: identity and membership (`Shiny.Actors.Mesh`)

- [ ] **NodeId.** A GUID persisted on first run (via the configured storage), plus an incarnation counter bumped on
      each start.
- [ ] `actors.UseMesh(mesh => ...)` (a `ShinyActorBuilder` extension): `Name`, `Key`, `UseMdns()`, `AddStaticNode(uri)`, `Expose<T>()`, `ExposeStream<T>()`.
- [ ] `IActorMesh`: `Local`, `Nodes`, and a `Changed` event (joined / left / suspected / incompatible).
- [ ] **mDNS source.** TXT records carry mesh hash, node id, wire version, incarnation and contracts hash.
      Static-node source alongside it.
- [ ] **Liveness.** `GET {prefix}/mesh/ping` probes every 10s and after a failed call. States go
      Alive → Suspected → Dead; a higher incarnation replaces a node's old entry.
- [ ] **Request signing.** HMAC-SHA256 over method, path, timestamp, nonce and a body hash, with an HKDF-derived key;
      reject anything older than 60s or replayed. Applies to every mesh route (calls, streams, ping).
- [ ] **Version check.** Wire version or fingerprint mismatch marks the node `Incompatible`.
- **Tests:**
  - Nodes join and leave; a wrong key never appears.
  - Stale incarnations are ignored; suspicion recovers.
  - A replayed or expired signature is rejected; mismatched versions show as `Incompatible`.
  - Real LAN discovery (Category=Network).
- **Exit:** two MAUI simulators see each other as `Alive`; a build with a changed contract shows as `Incompatible`.

## Phase 2: routing

- [ ] A mesh router: `Local`/`Explicit` placement, the remote path through the existing `HttpActorTransport`, and one
      pooled `HttpClient` per node.
- [ ] `Get<T>(id, on: ActorNode)`, plus `Get<T>(ActorAddress)`.
- [ ] `ActorUnavailableException(address, node, MayHaveRun)` and `ActorVersionMismatchException`.
- [ ] **Telemetry.** `mesh.node` and `mesh.route` tags, a live-nodes gauge, membership events.
- [ ] **Sample.** The chat app drops `ChatConnection` and uses `Get<T>(id, on: node)` directly.
- **Tests:**
  - Calls route local vs remote correctly, and results, exceptions, request context and filters cross.
  - A dead node fails fast; a timeout reports `MayHaveRun`; a node that comes back can be called again.
- **Exit:** the chat sample works across two simulators with no hand-written connection switching.

## Phase 3: actor references as values

- [ ] **Generator.** An AOT-safe JSON converter for `IActor`-typed parameters, return values and members, serializing
      as `ActorAddress`.
- [ ] **Deserialization** yields a routed proxy. An unknown node gives a proxy whose calls throw
      `ActorUnavailableException` until the node appears.
- [ ] **New diagnostic.** An `IActor` member crossing the wire on a contract that isn't exposed.
- **Tests:** `Task<IChatRoom> Find(...)` returns a reference homed on a third node, and calling it routes there.
- **Exit:** the sample's directory returns room references, and devices call them directly.

## Phase 4: cross-node call chains and streams

- [ ] **Deadlock detection across devices.** The serialized call chain travels as a header (bounded); the receiving
      node checks it against its own running turns.
- [ ] **Stream homes.** `GetStream<T>(key, on: node)`. Publishing forwards to the home; subscribing uses remote SSE
      with durable resume.
- [ ] **Implicit stream consumers** follow their actor type's placement.
- **Tests:**
  - `A@1 → B@2 → A@1` throws `ActorDeadlockException` on both sides.
  - A remote durable stream resumes across a node restart.
- **Exit:** the sample's rooms stream live to every device in the mesh.

## Phase 5: `Hashed` and `Any` placement (gated by D3)

- [ ] `[Placement(PlacementStrategy.Hashed | Any)]`, read by the generator.
- [ ] **Rendezvous hashing** over `Alive` nodes that host the type.
- [ ] **Startup check.** Refuse `Hashed` when the state provider is local-only; providers declare `IsShared`.
- [ ] **`Any`.** Stateless workers spread to the least-loaded node, using load reported in pings.
- **Tests:**
  - Ownership is the same from every node, and moves minimally when nodes join or leave.
  - During churn, ETags stop conflicting writes and the loser reloads.
- **Exit:** a hub-and-satellite demo using DocumentDb on Postgres.

## Later / not planned

- **`[OneWay]` outbox:** persisted, retried with backoff, idempotency keys.
- **State replication**, or failover of `Local`-homed actors. Out of scope by design: local-first state has one home.
- **Routing beyond the LAN** (relay or tunnel nodes); static nodes cover the simple case.

## Known risks

- **iOS background.** A suspended node stops answering, so the mesh must show it as `Suspected` quickly, not hang
  callers. Probe intervals versus battery need tuning on a real device.
- **mDNS on locked-down networks** (guest Wi-Fi, client isolation). Static nodes are the fallback; the sample needs
  a manual address entry.
- **Clock skew** breaks the 60s signature window. Use a skew-tolerant window, or nonce-only replay protection with a
  server-issued challenge.
- **The DevFlow port clash** on emulators with several debug apps (seen during sample testing). That's test tooling,
  not product, but worth a note in the sample README.
