# Design: multi-device routing ("the mesh")

Status: proposal · 2026-10-02

## Problem

Today one process owns its actors. To call another device's actors you pick the device and point a
`RemoteActorSystem` at it, so the caller decides *where* every call goes. Orleans hides that behind location
transparency: you call `Get<T>(id)`, and a cluster with membership, a directory and routing finds the actor.

We want the useful part of that for apps running on several devices on one network: a POS terminal and its tablets,
a home hub and the family's phones, a classroom. We don't want the ceremony or the failure modes of a server cluster.

## Constraints that shape everything

1. **Nodes are phones.** They sleep, get suspended, change networks and vanish without a goodbye. Membership is a
   best-effort view, never a guarantee.
2. **State is local.** Each device keeps actor state in its own SQLite or IndexedDB. Without shared storage, an
   actor's state lives on exactly one device, and moving the actor means losing its state.
3. **No consensus.** Raft or Paxos on phones that disappear at will is the wrong trade. Any design that needs
   agreement among the nodes is out.
4. **Same rules as the rest of the library.** AOT-safe, no reflection, low ceremony, and a single-device app pays
   nothing for this.

Constraint 2 decides the central question. **A stateful actor has a home node, and it lives there.** Routing is
then about getting calls to that home, not about deciding where actors live.

## Goals / non-goals

**Goals.**
- **Location-transparent references.** A reference carries its home, so it works wherever it's passed.
- **Automatic routing.** Calls go to the home node for you.
- **Membership from discovery.** Which nodes are in the mesh comes from mDNS (or static addresses), with health
  checks on top.
- **Clear failure.** When a home node is unreachable, the call fails fast with a typed error.
- **Spreading stateless work.** Stateless and shared-storage actors can be spread across nodes.
- **Trust.** Only nodes that share the mesh's secret can join, and every request is authenticated.
- **Version safety.** Incompatible app versions refuse to talk, with a clear error.

**Non-goals (v1).**
- State replication, or automatic failover of an actor whose home is gone.
- Consensus or a global directory.
- Routing across routers or the internet. A static node address is the escape hatch.
- Exactly-once remote delivery.

## Concepts

| Concept | Meaning |
|---|---|
| **Node** | A process running an `ActorSystem` in the mesh. Its identity is a `NodeId`: a GUID generated once and persisted, never the device name, which can change and collide. |
| **Mesh** | The nodes sharing a mesh name and key. Different apps, or two families on one Wi-Fi, never mix. |
| **Membership** | Each node's current view of the live nodes: id, endpoints, versions, which actor contracts it hosts. Eventually consistent. |
| **Home** | The node an actor lives on. It's part of the actor's address. |
| **Address** | `(contract, id, home)`, the serializable identity of an actor. |
| **Placement** | Per actor type, how its home is chosen. See below. |

## Placement

Each actor type declares how its home is chosen. The default is chosen so existing code keeps its meaning.

| Strategy | Home is | For | State |
|---|---|---|---|
| `Local` *(default)* | The node that calls `Get<T>(id)`, unless told otherwise | Everything today: room, cart, wallet | Local storage, works as now |
| `Explicit` | The node named in `Get<T>(id, on: node)` or carried in a received address | Another device's room, a hub's catalog | Lives on that node |
| `Hashed` | Rendezvous hash of `(type, id)` over live nodes hosting the type | Actors whose state is in **shared** storage (Postgres or Cosmos via DocumentDb), or computed caches | Must be shared storage. A startup check refuses `Hashed` on a local-storage provider. |
| `Any` | The local node, or the least-loaded node hosting the type | `[StatelessWorker]` | None |

`Local` + `Explicit` cover the local-first phone case and need no coordination at all. `Hashed` is what makes
`Get<T>(id)` resolve to the same node from every device with no directory. It's only correct when state doesn't live
on the node, so it's opt-in and checked.

Rendezvous (highest-random-weight) hashing beats a hash ring here. A node joining or leaving moves only the keys
it wins or loses, it needs no virtual nodes, and it's a few lines of code. With shared storage plus the existing
ETag checks, a brief disagreement during churn is safe: two nodes may briefly activate the same id, but their writes
conflict instead of overwriting, which is exactly what ETags are for.

```csharp
[Placement(PlacementStrategy.Hashed)]     // state in Postgres - any node can host it
public class CatalogActor : Actor<Catalog>, ICatalog { ... }
```

## API sketch

```csharp
builder.Services.AddShinyActors(actors => actors.UseMesh(mesh =>
{
    mesh.Name = "shiny-chat";                   // only nodes with this name join
    mesh.Key = secrets.MeshKey;                 // shared secret: authenticates every request
    mesh.UseMdns();                             // membership source (or AddStaticNode(uri), or both)
    mesh.Expose<IChatRoom>().Expose<IRoomDirectory>().ExposeStream<ChatMessage>();
}));
```

```csharp
public interface IActorMesh
{
    ActorNode Local { get; }
    IReadOnlyList<ActorNode> Nodes { get; }     // live, as this node sees it
    event Action<MeshChange>? Changed;          // joined / left / suspected / version-mismatch
}

var hub = mesh.Nodes.First(n => n.Name == "Front Counter");
var room = actors.Get<IChatRoom>("general", on: hub);   // Explicit: lives on the hub
await room.Post("tablet-3", "order up");                // routed there - same call as a local one

ActorAddress where = room.GetActorAddress();            // ("IChatRoom", "general", hubNodeId)
```

`Get<T>(id)` without `on:` keeps today's meaning: `Local` placement, or `Hashed`/`Any` when the type says so. A
single-device app never sees any of this.

### References cross the wire

An actor reference is a valid parameter or return type. It serializes as its address, and it becomes a routed proxy
on the other side:

```csharp
public interface IRoomDirectory : IActor
{
    Task<IChatRoom> Find(string name);    // the caller gets a proxy homed wherever the room lives
}
```

This is what makes the routing transparent in practice. A device asks the hub's directory for a room and gets back
a reference it calls directly, with no idea where it lives. The generator emits the JSON converter for `IActor`-typed
members and parameters (AOT-safe). Deserializing an address for an unknown node yields a proxy whose calls fail with
`ActorUnavailableException` until that node shows up.

## How a call routes

```
proxy.Post(...)
  -> ActorReference: method descriptor + args + typed local call   (already true today)
  -> router: home = placement(address)
       home == local  -> the existing mailbox path, unchanged and unserialized
       home == remote -> transport.InvokeAsync(home endpoint, contract, id, method, args)
                         (the existing HttpActorTransport, one pooled HttpClient per node)
```

The generated local proxy already passes the method descriptor, the boxed arguments and the typed lambda into
`ActorReference.Ask`. The router decides between the lambda (local) and the wire (remote), so **the generator doesn't
need to change for routing itself**, only for references-as-values. What already crosses the wire carries on:
request context, trace parents, filters, and durable-stream resume.

## Membership

**Sources** (pluggable `IMeshMembershipSource`):
- **mDNS** (`Shiny.Net.Discovery`). Each node publishes `_shinyactors._tcp` with TXT records `mesh=<hash of mesh name>`,
  `node=<NodeId>`, `wire=<version>`, `inc=<incarnation>`, and `c=<hash of hosted contracts>`. The mesh name is hashed
  so it isn't broadcast in the clear.
- **Static.** `AddStaticNode(uri)` for networks where multicast is blocked, or a hub at a known address.

**Liveness.** mDNS alone is too slow and too optimistic, since a suspended iPhone keeps its last announcement for
minutes. So:
- **Probing.** Each node probes members with a cheap authenticated `GET {prefix}/mesh/ping` every 10s, plus
  immediately after a failed call.
- **States.** `Alive` → `Suspected` after one failure (calls are still attempted, with a short timeout) → `Dead`
  after N failures or an mDNS goodbye. `Dead` members are dropped from `Hashed` placement.
- **Incarnation.** A counter that increases each time a node starts or rejoins. A node that comes back with a higher
  incarnation replaces its old entry, so stale information can't resurrect a node that has since restarted.

**Mismatch.** A node whose `wire` version differs, or whose fingerprint for a contract differs, is listed as
`Incompatible`. Calls to it fail with `ActorVersionMismatchException` instead of a confusing 400. A contract
fingerprint is a hash of its method keys, parameter types and return types, which the generator already has.

## Security

- **Mesh key.** Every mesh request is signed: an HMAC-SHA256 over method, path, timestamp, nonce and a body hash,
  keyed with a key derived from the mesh key (HKDF). Signatures older than 60s, or replayed nonces, are rejected.
  The secret is never sent over the network.
- **Distributing the key** is the app's job, since it's an account or pairing decision. The sample pairs once with a
  short code, then stores the derived mesh key in secure storage.
- **Transport.** Plain HTTP on the LAN by default. Bodies are authenticated but readable on the network.
  `mesh.UseTls(cert)` uses Shiny.Net.HttpServer's self-signed certificates plus pinning (pin exchanged during
  pairing) for confidentiality.
- **Exposure.** Nothing is reachable unless exposed, exactly as `MapActors` works today. The mesh is just another
  caller.

## Failure semantics

| Situation | Behavior |
|---|---|
| Home node unreachable or `Dead` | `ActorUnavailableException(address, node)`, fast. No silent retries: the call may or may not have run. |
| Timeout mid-call | `ActorUnavailableException` with `MayHaveRun = true`, so the caller decides whether to retry. |
| `[OneWay]` to an unreachable node | Fails at send. An optional outbox (persisted, retried with backoff) is a later phase. |
| Incompatible node | `ActorVersionMismatchException` naming the contract and both fingerprints. |
| `Hashed` owner changes during churn | The next call goes to the new owner. ETags stop split-brain writes, and the loser deactivates and reloads. |
| A home node leaves for good | Its actors are unavailable until it returns. Documented: local-first state has one home. |

**Deadlock detection across nodes.** Today `CallChain` is a set of in-memory nodes. It gets a stable turn id
(node + GUID), and the chain travels as a header (bounded length). A node receiving a call checks the incoming
chain against its own running turns, so `A@phone → B@hub → A@phone` throws `ActorDeadlockException` instead of
hanging both devices.

## Streams across nodes

A stream key has a home just like an actor: the node calling `GetStream<T>(key)`, or `on: node`.
- **Publishing** to a remote home forwards to that node's hub (the existing `POST /streams`).
- **Subscribing** to a remote home uses the existing SSE path. Durable streams resume via `Last-Event-ID`, which
  already survives a node vanishing and returning.
- **Implicit consumers** follow their actor type's placement.

## Reminders, state, telemetry

- **Reminders** stay with their actor's home: the local reminder store, the local background job, the local
  notifications. Nothing new.
- **State** is unchanged for `Local`/`Explicit`. `Hashed` requires a shared provider, checked at startup.
- **Telemetry** spans already cross HTTP. New spans and tags: `mesh.node`, `mesh.route=local|remote`, membership
  changes as events, and a gauge of live nodes.

## What gets built (phases)

1. **Identity and membership.** `NodeId` and incarnation, `IActorMesh`, the mDNS and static sources, probing, mesh
   signing, contract fingerprints. *Exit: two devices see each other as `Alive`, a third with the wrong key never
   appears, and an incompatible build shows as `Incompatible`.*
2. **Routing.** A router behind `ActorReference`, `Get<T>(id, on:)`, `ActorAddress`, `ActorUnavailableException`, and
   pooled transports per node. *Exit: the chat sample drops `ChatConnection`; tablets call the hub's rooms through
   plain `Get<T>(id, on: hub)`.*
3. **References as values.** Generator-emitted converters for `IActor` members and parameters. *Exit:
   `Task<IChatRoom> Find(...)` works across devices.*
4. **Cross-node chains and streams.** Serialized call chains (deadlock detection across devices) and stream homes.
5. **`Hashed` and `Any` placement.** Rendezvous hashing, a startup check for shared storage, and spreading
   stateless workers.
6. *(later)* A `[OneWay]` outbox, mesh-wide quiet detection in `Shiny.Actors.Testing`, and an in-process
   multi-node test harness (several `ActorSystem`s with loopback transports), which phases 1 to 5 need anyway.

Testing approach: an in-process mesh harness (N systems, a fake membership source, loopback HTTP or in-memory
transport, injectable partitions) covers the logic deterministically. A real-LAN test (Category=Network) and the
MAUI sample on two simulators cover the rest.

## Decisions to make

1. **Target topology.** Is it peer-to-peer phones (the chat sample), or a hub with satellites (a POS terminal and
   tablets)? Both work with this design, but hub-and-spoke is where `Hashed` placement and shared storage earn
   their keep. Peer-to-peer is all `Local`/`Explicit`.
2. **Trust model.** A shared mesh key (simple, app-managed) versus per-pair keys from pairing (stronger, more UX).
   This proposal is shared key now, with pairing as how the sample hands it out.
3. **Shared storage.** Does any real deployment have it? If not, `Hashed` placement (phase 5) can wait.
4. **Package.** A new `Shiny.Actors.Mesh` (depends on HttpServer + Discovery), so the core stays free of HTTP. The
   core gains only the router seam and `ActorAddress`.
