# Shiny.Actors

Orleans-style virtual actors without the ceremony. Like `Shiny.Net.HttpServer`, it runs anywhere .NET runs,
including .NET MAUI, and is AOT, trim and single-file clean.

- **Only `Microsoft.Extensions.*` abstractions** as dependencies.
- **Nothing discovered by reflection.** A source generator (shipped inside the package) writes the proxies and
  registrations; state goes through `JsonTypeInfo` from your own `JsonSerializerContext`.
- **One process owns its actors.** There is no cluster, silo or placement, but other processes and devices can
  call them over HTTP (see Remoting).

## The whole setup

```csharp
builder.Services.AddShinyActors();   // every actor in the app is already known
```

Everything else hangs off the same builder: storage, streams, filters, and every add-on package.

```csharp
builder.Services.AddShinyActors(actors => actors
    .UseDocumentDb(new SqliteDatabaseProvider($"Data Source={path}"))   // Shiny.Actors.DocumentDb
    .AddDurableStream<ChatMessage>()
    .UseAutoSave()
    .UseBackgroundReminders()                                           // Shiny.Actors.Jobs
    .UseReminderNotifications()                                         // Shiny.Actors.Notifications
    .UseDiscovery()                                                     // Shiny.Actors.Discovery
    .ServeOverHttp(expose => expose.Expose<IChatRoom>())                // Shiny.Actors.HttpServer
    .Configure(o => o.IdleTimeout = TimeSpan.FromMinutes(2)));
```

- **It accumulates.** Calling `AddShinyActors` again (from a library, say) adds to the same configuration rather than
  replacing it.
- **Container-built pieces.** `AddCallFilter<T>()`, `UseStateProvider<T>()` and `AddReminderObserver<T>()` are
  created by the container, so they can take dependencies.
- **Late configuration.** `Configure((options, services) => ...)` covers anything that only exists once the
  container is built.

```csharp
public interface ICounter : IActor
{
    ValueTask<int> Increment(int by = 1);
    [OneWay] Task Reset();      // queued; the caller doesn't wait
}

public class CounterState { public int Count { get; set; } }

public class CounterActor(ILogger<CounterActor> logger) : Actor<CounterState>, ICounter
{
    public async ValueTask<int> Increment(int by)
    {
        State.Count += by;      // one call at a time - no locks
        await WriteStateAsync();
        return State.Count;
    }

    public Task Reset() => ClearStateAsync().AsTask();
}

[JsonSerializable(typeof(CounterState))]          // the generator finds this for the actor's state
partial class AppJson : JsonSerializerContext;
```

```csharp
var count = await actors.Get<ICounter>("bob").Increment();
```

No container? `await using var actors = new ActorSystem(new ActorSystemOptions { ... });`. `ActorSystemOptions` is
the same raw settings object the builder fills in.

## What you get

| | |
|---|---|
| **Virtual actors** | `Get<T>(id)` always works. Activation happens on the first call; deactivation after `IdleTimeout` (5 min). |
| **Turn-based** | One call at a time per actor, and calls to different actors run in parallel. Not reentrant. |
| **Deadlock detection** | A call that would wait on itself (`A -> B -> A`) throws `ActorDeadlockException` with the path instead of hanging. |
| **State** | `Actor<TState>` plus any number of named `IActorState<T>`s. ETag-checked writes, optional auto-save. See below. |
| **State providers** | In-memory, files, or Shiny.DocumentDb (SQLite, PostgreSQL, SQL Server, Cosmos DB, IndexedDB, and more). |
| **Lifecycle** | `OnActivateAsync`, `OnDeactivateAsync(reason)`, `DeactivateOnIdle()`, constructor DI from a scope per activation. |
| **Timers** | `RegisterTimer(...)`: ticks run as turns, never pile up, and stop at deactivation. |
| **Reminders** | `RegisterReminderAsync(name, due, period)`: persistent, survive restarts, and activate the actor. See below. |
| **Concurrency control** | `[Reentrant]`, `[AlwaysInterleave]`, `[ReadOnly]` and `[StatelessWorker]`. Code between awaits never overlaps. |
| **Event sourcing** | `JournaledActor<TState, TEvent>`: state rebuilt from an event log, with snapshots and full history. |
| **Migrations** | `[StateVersion(n)]` + `AddStateMigration<T>(...)` upgrade stored state, and upcast stored events, as they're read. |
| **Streams** | `GetStream<T>(key)`: typed pub/sub. Make one durable and subscribers can replay what they missed. |
| **Call filters** | `IActorCallFilter` runs around every call (local or remote) for logging, validation, auth and more. |
| **Request context** | `ActorRequestContext` values flow with a call, onward to other actors, and across remote calls. |
| **Telemetry** | OpenTelemetry-ready traces and metrics from `ActivitySource`/`Meter` named `Shiny.Actors`. |
| **Testing** | `Shiny.Actors.Testing`: fake time, in-memory storage, recorded calls, and state you can seed and inspect. |
| **Cancellation** | A `CancellationToken` parameter flows to the actor, and a caller that cancels stops waiting even while queued. |
| **Back pressure** | `MailboxCapacity` bounds each mailbox; callers wait for room. |

## State

```csharp
[AutoSave]   // optional: write whatever changed after every call
public class WalletActor(
    [ActorState("balance")] IActorState<Balance> balance,     // named states, stored separately
    [ActorState("history", Provider = "archive")] IActorState<History> history
) : Actor<Profile>, IWallet                                   // ...alongside the actor's own State
{
    public async Task Deposit(decimal amount)
    {
        balance.State.Amount += amount;
        await balance.WriteStateAsync();   // or let [AutoSave] do it
    }
}
```

- **Loading.** Every state is read before `OnActivateAsync`. `CreateState<T>("name")` in a constructor works like
  the attribute.
- **ETags.** Every write and clear is conditional on the version that was read. If another writer got there
  first (a second process, or another device on a shared database), the write throws
  `ActorStateConflictException` instead of overwriting. The actor then deactivates, so its next call reloads the
  real state.
- **`[AutoSave]`** (`AfterEachCall`) writes changed states after each call, before the caller gets its result, so
  a failed write fails the call.
  - A call that throws is not saved, and unchanged state is never rewritten.
  - `[AutoSave(AutoSaveMode.OnDeactivate)]` writes on deactivation instead.
  - `actors.UseAutoSave()` sets the mode for every actor without the attribute.

| Provider | Set with | ETag | Notes |
|---|---|---|---|
| In-memory (default) | — | version counter | Lost when the process ends |
| Files | `actors.UseFileStorage(dir)` | content hash | `{dir}/{actor}/{hash(id)}[.{state}].json`. Atomic replace, and a lock file makes it safe across processes. Reminders go in `reminders.json`. |
| Shiny.DocumentDb | `actors.UseDocumentDb(new SqliteDatabaseProvider("Data Source=actors.db"))` | document version | `Shiny.Actors.DocumentDb`. Any DocumentDb backend, and state, reminders and event logs together. For a store you already have: `UseDocumentDb(store)` + `MapActorDocuments()` on its options. If the store only exists in the container (Blazor's IndexedDB): `UseDocumentDb()` with no arguments. |

Several providers can be mixed: `actors.AddStateProvider("archive", ...)` or `actors.AddDocumentDbStateProvider("archive", store)`.
Pick one per actor with `[StateProvider("name")]`, or per state with `[ActorState(..., Provider = "name")]`.
Writing your own means implementing `IActorStateProvider`'s three methods, and the conformance tests in
`StateProviderTests` show exactly what it must do.

> **Shiny.DocumentDb.Sqlite 14.0.0** pulls in `SQLitePCLRaw` 2.1.11, which has a high-severity advisory
> (GHSA-2m69-gcr7-jv3q). Pin 2.1.12 or later, per platform: `SQLitePCLRaw.lib.e_sqlite3.android` on Android,
> `.ios` on iOS, and the plain `SQLitePCLRaw.lib.e_sqlite3` elsewhere. Don't add the plain one to an Android target:
> its Linux `libe_sqlite3.so` shadows Android's and the app crashes at startup (`libc.so.6 not found`). See
> `samples/Sample.Maui/Sample.Maui.csproj`.

## Streams

```csharp
var stream = actors.GetStream<OrderPlaced>("store-1");
await stream.PublishAsync(new OrderPlaced("coffee", 2));

// anywhere - a MAUI view model, a background service
await foreach (var order in stream.ReadAllAsync(ct)) { ... }

// inside an actor: each event is a turn of that actor, and the subscription ends with the activation
Actors.GetStream<OrderPlaced>("store-1").Subscribe((order, ct) => { ... });

// implicit: events on key "store-1" go to the actor whose id is "store-1", activating it
public class StoreSales : Actor<Sales>, IStoreSales, IActorStreamConsumer<OrderPlaced>
{
    public ValueTask OnNextAsync(OrderPlaced order, CancellationToken ct) { ... }
}
```

By default delivery is in order per publisher and at most once, and nothing is stored: a subscriber only sees events
published after it subscribed.

**Durable streams** keep the last N events of every stream of a type in the event store, numbered:

```csharp
actors.AddDurableStream<ChatMessage>(retain: 500);

await foreach (var e in stream.ReadFromAsync(afterSequence: lastSeen, ct))   // replay, then live - no gap, no duplicates
    lastSeen = e.Sequence;
```

Read remotely, `ReadAllAsync` reconnects on its own. On a durable stream it resumes from the last sequence it saw
(SSE `Last-Event-ID`), so a phone that drops off Wi-Fi misses nothing.

## Reminders

```csharp
public class Billing : Actor<BillingState>, IBilling, IRemindable
{
    public Task Start() => RegisterReminderAsync("invoice", TimeSpan.FromHours(1), TimeSpan.FromDays(1)).AsTask();

    public ValueTask ReceiveReminderAsync(ReminderTick tick, CancellationToken ct) { ... }
}
```

- Reminders persist in the `IActorReminderStore`. `UseFileStorage(dir)` keeps them in `dir/reminders.json`.
- While the app runs, the system fires them on time.
- Missed ticks (the app wasn't running) coalesce into one, and `tick.DueAt` tells you how late it is.
- Delivery is at least once: a reminder only moves on after its actor handled it, and a failure retries within a minute.
- A `null` period fires once.

**Background (MAUI):** the OS suspends or kills apps, so add `Shiny.Actors.Jobs`:

```csharp
actors.UseBackgroundReminders();   // a Shiny.Jobs IJob: BGTaskScheduler / WorkManager
```

The job fires everything due, then deactivates the actors it woke so their state is written before the app is
suspended again. The OS picks the timing (typically every 15+ minutes), so treat background reminders as "soon".

**On time, even when the app is dead:** add `Shiny.Actors.Notifications` and give the reminder a notification:

```csharp
actors.UseReminderNotifications();   // registers Shiny.Notifications too, on iOS/Android/Mac/Windows

await RegisterReminderAsync("standup", TimeSpan.FromHours(1),
    notification: new ReminderNotification("Standup", "Starts in 5 minutes") { Channel = "alerts" });
```

Each such reminder becomes an OS-scheduled local notification. It's moved when the reminder advances, cancelled when
it's removed, and re-synced at startup. The OS shows it on time whether or not the app is running, and tapping it opens
the app, where the overdue reminder fires. `IActorReminderObserver` is the hook, if you want to mirror reminders anywhere else.

## Concurrency control

Every turn runs on the actor's own exclusive scheduler, so the actor's code between awaits never overlaps. What you
choose is when a call may *start*:

| | |
|---|---|
| default | One call at a time. The next waits for the previous to finish. |
| `[Reentrant]` (class) | Any call may start while another waits at an `await`. The actor can call itself. |
| `[AlwaysInterleave]` (method) | This method starts even while another call is waiting, e.g. status, cancel or health checks. |
| `[ReadOnly]` (method) | Read-only calls run together, never beside a call that changes state. |
| `[StatelessWorker(MaxLocalWorkers = 4)]` (class) | Several activations per id, and a call goes to an idle one. No state or reminders (SACT011). |

A call that would wait on itself, directly or through other actors, throws `ActorDeadlockException` naming the path.
Interleaving calls are exempt, because they can't deadlock. While an actor is shutting down, a call from one of its own
running calls is still taken, and any other call is handed to the next activation, in order.
Don't use `ConfigureAwait(false)` inside an interleaving actor: that leaves its scheduler.

## Event sourcing

```csharp
[AutoSave]   // confirms raised events after each call
public class AccountActor : JournaledActor<AccountState, AccountEvent>, IAccount
{
    protected override int SnapshotEvery => 100;

    protected override void Apply(AccountState state, AccountEvent e) => state.Balance += e switch
    {
        Deposited d => d.Amount,
        Withdrawn w => -w.Amount,
        _ => 0
    };

    public Task Deposit(decimal amount) { RaiseEvent(new Deposited(amount)); return Task.CompletedTask; }
}

[JsonPolymorphic, JsonDerivedType(typeof(Deposited), "deposited"), JsonDerivedType(typeof(Withdrawn), "withdrawn")]
public abstract record AccountEvent;
```

- **Rebuilt, not stored.** The state is rebuilt from the log on activation, starting from the latest snapshot if
  there is one.
- **Full history.** `ReadEventsAsync()` returns the whole history, for audit, undo or projections.
- **Conditional appends.** An append is conditional on the version this activation last saw, like an ETag.
- **Storage.** Logs live in the `IActorEventStore`: in-memory, files (atomic batch files), or DocumentDb (one
  document per event).

## State migrations

```csharp
[StateVersion(2)]
public class Profile { public string First { get; set; } = ""; public string Last { get; set; } = ""; }

actors.AddStateMigration<Profile>(1, json =>       // v1 had a single "Name"
{
    var parts = json["Name"]!.GetValue<string>().Split(' ', 2);
    json["First"] = parts[0];
    json["Last"] = parts.Length > 1 ? parts[1] : "";
    json.Remove("Name");
});
```

- **Upgraded on read.** Stored JSON is upgraded one step at a time as it's read, and the next write stamps the
  current version (`"$v"`).
- **Events too.** The same works for event types: history is never rewritten, events are upcast on replay.
- **Clear failures.** A missing step, or data written by a newer app, fails activation with a clear message instead
  of a mangled object.

## Call filters and request context

```csharp
actors.AddCallFilter<AuditFilter>();   // created by the container - or AddCallFilter(instance)

public sealed class AuditFilter : IActorCallFilter
{
    public async ValueTask InvokeAsync(ActorCallContext ctx, ActorCallDelegate next)
    {
        if (ActorRequestContext.Get("user") is null) throw new UnauthorizedAccessException();
        await next(ctx);   // ctx.Method, ctx.Arguments, ctx.Result (settable), ctx.ActorId
    }
}

ActorRequestContext.Set("user", "allan");   // flows into the call, onward, and across remote calls
```

Filters run inside the actor's turn, for asks and one-way calls, local or remote. An actor that implements
`IActorCallFilter` filters its own calls, innermost.

## Telemetry

```csharp
services.AddOpenTelemetry()
    .WithTracing(t => t.AddSource(ActorTelemetry.Name))
    .WithMetrics(m => m.AddMeter(ActorTelemetry.Name));
```

**Traces.** There's one span per call, named `{interface}/{method}` and parented to the caller's span. That holds
across actors and across HTTP (client span, then server span, then the actor's).

**Metrics.** None of them carry actor ids, because ids are unbounded:

- calls, by outcome
- call duration
- active, activated and deactivated actors
- queued calls
- state writes, write duration and conflicts
- reminders fired
- stream events

## Testing

```csharp
await using var host = new ActorTestHost(services => services.AddSingleton<IClock, FakeClock>());

await host.SetStateAsync<CartActor, CartState>(new CartState { Items = ["coffee"] }, id: "allan");
await host.Get<ICart>("allan").Checkout();
await host.AdvanceAsync(TimeSpan.FromHours(1));      // timers, reminders, idle deactivation - no sleeps

Assert.Single(host.Calls, c => c.Method == "Checkout" && !c.Failed);
Assert.Equal([new Deposited(10m)], await host.GetEventsAsync<AccountActor, AccountEvent>("a"));
using var orders = host.Record<OrderPlaced>("store-1");   // .Items, await .WaitForAsync(n)
```

Fake time is shared through DI as `TimeProvider`. `WaitForQuietAsync()` waits until nothing is queued, running, or
mid-reminder anywhere in the system.

## Remoting

Other processes and devices call your actors through the same `IActorSystem`. The generator already emitted a
serializable contract and a remote proxy for every actor interface.

**Server** (`Shiny.Actors.HttpServer`, so it runs on a phone too):

```csharp
builder.Services.AddShinyActors(actors => actors.ServeOverHttp(
    expose => expose
        .Expose<ICounter>()            // nothing is reachable unless exposed
        .ExposeStream<OrderPlaced>(),  // publish + server-sent events
    http =>
    {
        http.AddAuthentication().AddApiKey(o => o.AddKey(key, "client"));
        http.AddAuthorization(_ => { });
        http.Configure((HttpServer s) => { s.UseAuthentication(); s.UseAuthorization(); });
    },
    authorizationPolicies: []          // every actor route requires authorization
));
```

Already building the server yourself? `server.MapActors(expose => ...).RequireAuthorization()` does the mapping alone.

**Client** (in the core package, plain `HttpClient`):

```csharp
IActorSystem actors = new RemoteActorSystem(new HttpClient { BaseAddress = new("http://192.168.1.20:8080/actors/") });
await actors.Get<ICounter>("bob").Increment();
await foreach (var order in actors.GetStream<OrderPlaced>("store-1").ReadAllAsync(ct)) { ... }
```

| | |
|---|---|
| Wire | `POST {prefix}/call/{actor}/{id}/{method}`, with a JSON array of arguments in and a JSON result out. Streams: `POST`/`GET` (SSE) `{prefix}/streams/{type}/{key}`. |
| JSON | Every `JsonSerializerContext` in a project with actors is registered automatically, and primitives are built in. A type crossing the wire without metadata fails at `MapActors`, not on the first call. |
| Errors | `RemoteActorException.StatusCode`: 400 bad arguments, 401/403 auth, 404 not exposed, 409 deadlock, 500 the actor threw. Exception details stay hidden unless `IncludeExceptionDetails`. |
| Ids | Any string. Ids that aren't plain are sent as `~base64url`, because `%2F` and `..` don't survive every proxy. |
| Remote streams | `ReadAllAsync` and `Subscribe` reconnect with backoff (1s, up to 30s). Durable streams resume from the last sequence seen; others miss what was published meanwhile. |
| Tracing & context | `traceparent` and `x-actor-context` headers carry the caller's trace and `ActorRequestContext`. |

`IActorTransport` is the seam for a transport other than HTTP. `ActorDispatcher` does the server side and works with bytes only.

**Finding devices** (`Shiny.Actors.Discovery`, mDNS/Bonjour, no addresses to type):

```csharp
await using var published = await mdns.PublishActorsAsync(DeviceInfo.Current.Name, port);   // server side

await foreach (var peer in mdns.BrowseActorPeersAsync(ct))                                   // client side
    if (peer.IsAvailable) actors = peer.Connect(http => http.DefaultRequestHeaders.Add("X-Api-Key", code));
```

The server must listen on the LAN (`Address = IPAddress.Any`; Shiny.Net.HttpServer binds loopback by default).
Anyone on the network can find it, so put authentication in front of `MapActors`.
On Apple platforms, add `_shinyactors._tcp` to `NSBonjourServices`, plus an `NSLocalNetworkUsageDescription`.

## MAUI

```csharp
builder.Services.AddShinyActors(actors => actors.UseDocumentDb(
    new SqliteDatabaseProvider($"Data Source={Path.Combine(FileSystem.AppDataDirectory, "actors.db")}")
));
```

The OS can kill a backgrounded app without warning. Either use `[AutoSave]` (or `actors.UseAutoSave()` for every actor),
so nothing is ever only in memory, or flush when the app sleeps:

```csharp
protected override async void OnSleep() => await actorSystem.DeactivateAllAsync();
```

## Build-time diagnostics

| Code | |
|---|---|
| SACT001 | Interface member can't be proxied (property, generic method, ref/out/in) |
| SACT002 | Method isn't `Task`/`Task<T>`/`ValueTask`/`ValueTask<T>` |
| SACT003 | `[OneWay]` method returns a value |
| SACT004 | Class implements an actor interface but doesn't derive from `Actor` |
| SACT005 | Actor can't be constructed (generic, private, ambiguous constructors) |
| SACT006 | *Warning*: two classes implement one interface. Pick one with `actors.AddActor<IFoo, Foo>()` |
| SACT007 | *Warning*: a state or event type (`Actor<T>`, `JournaledActor<,>`, `[ActorState]`) isn't on any `JsonSerializerContext` |
| SACT008 | Actor interface is generic or not reachable |
| SACT009 | Two actors share an `[ActorName]` |
| SACT010 | An `[ActorState]` parameter isn't an `IActorState<T>` |
| SACT011 | A `[StatelessWorker]` has persistent state (`Actor<T>`, `JournaledActor<,>` or `[ActorState]`) |

The generated code for each actor interface consists of a local proxy, a remote proxy and a wire contract; for each
class, a registration. All of it is wired up by one module initializer.

## Layout

```
src/Shiny.Actors                    runtime + remote client (net10.0, IsAotCompatible)
src/Shiny.Actors.SourceGenerators   proxies, contracts, registration (packed into analyzers/)
src/Shiny.Actors.HttpServer         MapActors() on Shiny.Net.HttpServer
src/Shiny.Actors.Jobs               ActorReminderJob on Shiny.Jobs
src/Shiny.Actors.DocumentDb         state, reminders + event logs on Shiny.DocumentDb (SQLite, Postgres, IndexedDB, ...)
src/Shiny.Actors.Notifications      reminders -> OS-scheduled local notifications (Shiny.Notifications)
src/Shiny.Actors.Discovery          find actor servers on the LAN (mDNS, Shiny.Net.Discovery)
src/Shiny.Actors.Testing            ActorTestHost: fake time, in-memory storage, recorders
tests/Shiny.Actors.Tests            xunit v3 (the mDNS test is Category=Network)
samples/Sample.Console              NativeAOT: SQLite state, named state, auto-save, HTTP, streams, reminders
samples/Sample.Blazor               WebAssembly: event-sourced to-do lists in IndexedDB, streams, reminders
samples/Sample.Maui                 "Shiny Chat" for iOS/Android/Mac: rooms as actors, LAN sharing with pairing + mDNS,
                                    durable streams, reminder notifications, background job
```

## Samples

- **`Sample.Maui`** is a chat app where every room is an actor:
  - SQLite storage, `[AutoSave]`, and a named "members" state.
  - "Remind me" sets a reminder with an OS notification.
  - "Nearby" shares this device's rooms on the LAN behind a pairing code, and finds and joins other devices' rooms
    over mDNS. Same UI, through `RemoteActorSystem`.

  It runs on iOS, Android and Mac Catalyst. Debug builds include the `maui devflow` agent for UI automation.
- **`Sample.Blazor`** runs entirely in the browser. To-do lists are event-sourced actors whose events live in
  IndexedDB, a stream drives the UI, and a reminder fires while the tab is open.
- **`Sample.Console`** publishes as NativeAOT with zero trim warnings: an HTTP actor server and client, SQLite,
  durable streams and reminders in one binary.
```
