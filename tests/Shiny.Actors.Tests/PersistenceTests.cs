using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json.Nodes;
using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors.DocumentDb;
using Shiny.Actors.Remoting;
using Shiny.Net.HttpServer;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class EventStoreTests : IDisposable
{
    static readonly ActorStateKey Key = new("account", "a/b c", "journal");
    static readonly DateTimeOffset Now = new(2026, 10, 2, 9, 0, 0, TimeSpan.Zero);
    readonly Stores stores = new();
    public void Dispose() => this.stores.Dispose();

    static ReadOnlyMemory<byte> Json(string json) => Encoding.UTF8.GetBytes(json);

    static async Task<List<(long Version, string Json)>> All(IActorEventStore store, long after = 0)
    {
        var list = new List<(long, string)>();
        await foreach (var e in store.ReadAsync(Key, after, Ct))
            list.Add((e.Version, Encoding.UTF8.GetString(e.Json.Span)));
        return list;
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Appends_Number_Events_In_Order(string kind)
    {
        var store = this.stores.CreateEvents(kind);
        Assert.Equal(0, await store.GetVersionAsync(Key, Ct));

        Assert.Equal(2, await store.AppendAsync(Key, 0, [Json("{\"n\":1}"), Json("{\"n\":2}")], Now, Ct));
        Assert.Equal(3, await store.AppendAsync(Key, 2, [Json("{\"n\":3}")], Now, Ct));

        var all = await All(store);
        Assert.Equal([1L, 2, 3], all.Select(x => x.Version));
        Assert.Equal("{\"n\":3}", all[2].Json.Replace(" ", ""));
        Assert.Equal([3L], (await All(store, after: 2)).Select(x => x.Version));
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task A_Stale_Append_Is_A_Conflict_And_Writes_Nothing(string kind)
    {
        var store = this.stores.CreateEvents(kind);
        await store.AppendAsync(Key, 0, [Json("1"), Json("2")], Now, Ct);

        await Assert.ThrowsAsync<ActorStateConflictException>(() => store.AppendAsync(Key, 1, [Json("99")], Now, Ct).AsTask());
        await Assert.ThrowsAsync<ActorStateConflictException>(() => store.AppendAsync(Key, 0, [Json("99")], Now, Ct).AsTask());

        Assert.Equal(2, (await All(store)).Count);
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Trimming_Never_Lets_A_Version_Be_Reused(string kind)
    {
        var store = this.stores.CreateEvents(kind);
        await store.AppendAsync(Key, 0, [Json("1")], Now, Ct);
        await store.AppendAsync(Key, 1, [Json("2")], Now, Ct);
        await store.AppendAsync(Key, 2, [Json("3")], Now, Ct);

        await store.TrimAsync(Key, 2, Ct);
        Assert.Equal([3L], (await All(store)).Select(x => x.Version));

        await store.TrimAsync(Key, 3, Ct);
        Assert.Empty(await All(store));
        Assert.Equal(3, await store.GetVersionAsync(Key, Ct));
        Assert.Equal(4, await store.AppendAsync(Key, 3, [Json("4")], Now, Ct));
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Concurrent_Appenders_Never_Share_A_Version(string kind)
    {
        var location = this.stores.NewLocation();
        var a = this.stores.CreateEvents(kind, location);
        var b = kind == "memory" ? a : this.stores.CreateEvents(kind, location);

        await Task.WhenAll(Enumerable.Range(0, 20).Select(i => Task.Run(async () =>
        {
            var store = i % 2 == 0 ? a : b;
            while (true)
            {
                var version = await store.GetVersionAsync(Key, Ct);
                try
                {
                    await store.AppendAsync(Key, version, [Json(i.ToString())], Now, Ct);
                    return;
                }
                catch (ActorStateConflictException) { }
            }
        }, Ct)));

        var all = await All(a);
        Assert.Equal(Enumerable.Range(1, 20).Select(x => (long)x), all.Select(x => x.Version));
        Assert.Equal(20, all.Select(x => x.Json).Distinct().Count());
    }
}


public class JournaledActorTests : IDisposable
{
    readonly Stores stores = new();
    public void Dispose() => this.stores.Dispose();


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task State_Is_Rebuilt_From_Events_After_A_Restart(string kind)
    {
        var location = this.stores.NewLocation();
        var events = this.stores.CreateEvents(kind, location);
        var (state, _) = this.stores.Create(kind, location);

        var (first, _, _) = Create(o => { o.EventStore = events; o.StateProvider = state; });
        await first.Get<IAccount>("acc").Deposit(100m);
        await first.Get<IAccount>("acc").Withdraw(30m);
        await first.Get<IAccount>("acc").Deposit(5m);
        await Assert.ThrowsAsync<InvalidOperationException>(() => first.Get<IAccount>("acc").Withdraw(1000m));
        await first.DisposeAsync();

        var (second, _, _) = Create(o => { o.EventStore = events; o.StateProvider = state; });
        Assert.Equal(75m, await second.Get<IAccount>("acc").Balance());
        Assert.Equal(3, await second.Get<IAccount>("acc").CurrentVersion());
        Assert.Equal(
            ["1:Deposited { Amount = 100 }", "2:Withdrawn { Amount = 30 }", "3:Deposited { Amount = 5 }"],
            await second.Get<IAccount>("acc").History()
        );
    }


    [Fact]
    public async Task Snapshots_Shorten_Replay()
    {
        var events = new InMemoryActorEventStore();
        var state = new InMemoryActorStateProvider();
        var (actors, _, _) = Create(o => { o.EventStore = events; o.StateProvider = state; });

        for (var i = 0; i < 4; i++)
            await actors.Get<IAccount>("snap").Deposit(10m);

        var snapshot = await state.ReadAsync(new ActorStateKey("Shiny.Actors.Tests.AccountActor", "snap", "journal-snapshot"), TestJson.Default.JsonObject, Ct);
        Assert.Equal(3, snapshot.State!["version"]!.GetValue<long>());

        // trim what the snapshot covers - activation must still get the full balance
        await events.TrimAsync(new ActorStateKey("Shiny.Actors.Tests.AccountActor", "snap", "journal"), 3, Ct);
        await actors.DeactivateAllAsync(Ct);
        Assert.Equal(40m, await actors.Get<IAccount>("snap").Balance());
        Assert.Equal(4, await actors.Get<IAccount>("snap").CurrentVersion());
    }


    [Fact]
    public async Task Two_Systems_Cannot_Both_Append_The_Same_Version()
    {
        var events = new InMemoryActorEventStore();
        var (a, _, _) = Create(o => o.EventStore = events);
        var (b, _, _) = Create(o => o.EventStore = events);

        await b.Get<IAccount>("shared").Balance();   // B replays an empty log
        await a.Get<IAccount>("shared").Deposit(50m); // A appends event 1

        await Assert.ThrowsAsync<ActorStateConflictException>(() => b.Get<IAccount>("shared").Deposit(1m));
        await Eventually(() => Task.FromResult(b.ActivationCount), c => c == 0, "the conflicted actor deactivates");
        Assert.Equal(51m, await b.Get<IAccount>("shared").Deposit(1m)); // replayed A's event, then appended its own
    }
}


public class MigrationTests
{
    static readonly ActorStateKey PersonKey = new("Shiny.Actors.Tests.PersonActor", "p");


    static Action<ActorSystemOptions> Configure(IActorStateProvider? state = null, IActorEventStore? events = null, bool withMigrations = true) => o =>
    {
        if (state is not null) o.StateProvider = state;
        if (events is not null) o.EventStore = events;
        if (!withMigrations) return;

        o.AddStateMigration<Person>(1, json =>
        {
            var parts = json["Name"]!.GetValue<string>().Split(' ', 2);
            json["First"] = parts[0];
            json["Last"] = parts.Length > 1 ? parts[1] : "";
            json.Remove("Name");
        });
        o.AddStateMigration<TagAdded>(1, json =>
        {
            json["Tag"] = json["Name"]!.GetValue<string>().ToUpperInvariant();
            json.Remove("Name");
        });
    };


    [Fact]
    public async Task Old_State_Is_Migrated_As_It_Is_Read_And_Stamped_When_Written()
    {
        var provider = new InMemoryActorStateProvider();
        await provider.WriteAsync(PersonKey, new JsonObject { ["Name"] = "Allan Ritchie" }, TestJson.Default.JsonObject, null, Ct); // v1 shape, no stamp

        var (actors, _, _) = Create(Configure(provider));
        Assert.Equal("Allan Ritchie", await actors.Get<IPerson>("p").FullName());

        await actors.DisposeAsync(); // nothing changed, so nothing was rewritten
        var raw = await provider.ReadAsync(PersonKey, TestJson.Default.JsonObject, Ct);
        Assert.Equal("Allan Ritchie", raw.State!["Name"]!.GetValue<string>());
    }


    [Fact]
    public async Task A_Missing_Migration_Step_Fails_Activation_Clearly()
    {
        var provider = new InMemoryActorStateProvider();
        await provider.WriteAsync(PersonKey, new JsonObject { ["Name"] = "Allan" }, TestJson.Default.JsonObject, null, Ct);

        var (actors, _, _) = Create(Configure(provider, withMigrations: false));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => actors.Get<IPerson>("p").FullName());
        Assert.Contains("no migration from version 1 to 2", ex.InnerException!.Message);
    }


    [Fact]
    public async Task State_From_A_Newer_App_Is_Refused()
    {
        var provider = new InMemoryActorStateProvider();
        await provider.WriteAsync(PersonKey, new JsonObject { ["$v"] = 3, ["First"] = "A" }, TestJson.Default.JsonObject, null, Ct);

        var (actors, _, _) = Create(Configure(provider));
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => actors.Get<IPerson>("p").FullName());
        Assert.Contains("newer version of the app", ex.InnerException!.Message);
    }


    [Fact]
    public async Task Old_Events_Are_Upcast_On_Replay_And_New_Ones_Stamped()
    {
        var events = new InMemoryActorEventStore();
        var key = new ActorStateKey("Shiny.Actors.Tests.TagLogActor", "t", "journal");
        await events.AppendAsync(key, 0, [Encoding.UTF8.GetBytes("{\"Name\":\"old\"}")], DateTimeOffset.UtcNow, Ct); // a v1 event

        var (actors, _, _) = Create(Configure(events: events));
        await actors.Get<ITagLog>("t").Add("new");
        Assert.Equal(["OLD", "new"], await actors.Get<ITagLog>("t").Tags());

        var stored = new List<string>();
        await foreach (var e in events.ReadAsync(key, 0, Ct))
            stored.Add(Encoding.UTF8.GetString(e.Json.Span));
        Assert.Equal("{\"Name\":\"old\"}", stored[0]);   // history is never rewritten
        Assert.Contains("\"$v\":2", stored[1]);
    }
}


public class DurableStreamTests
{
    [Fact]
    public async Task A_Subscriber_Replays_What_It_Missed_Then_Goes_Live()
    {
        var (actors, _, _) = Create(o => o.AddDurableStream<Note>());
        var stream = actors.GetStream<Note>("feed");
        for (var i = 1; i <= 5; i++)
            await stream.PublishAsync(new Note($"n{i}"), Ct);

        var seen = new List<ActorStreamEvent<Note>>();
        await using var reader = stream.ReadFromAsync(afterSequence: 2, Ct).GetAsyncEnumerator(Ct);
        while (seen.Count < 3 && await reader.MoveNextAsync())
            seen.Add(reader.Current);

        Assert.Equal([3L, 4, 5], seen.Select(x => x.Sequence));
        Assert.Equal(["n3", "n4", "n5"], seen.Select(x => x.Item.Text));

        var next = reader.MoveNextAsync();
        await stream.PublishAsync(new Note("n6"), Ct);
        Assert.True(await next);
        Assert.Equal((6L, "n6"), (reader.Current.Sequence, reader.Current.Item.Text));
    }


    [Fact]
    public async Task Only_Durable_Streams_Replay()
    {
        var (actors, _, _) = Create();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
        {
            await foreach (var _ in actors.GetStream<Note>("x").ReadFromAsync(0, Ct)) { }
        });
    }


    [Fact]
    public async Task Durable_Streams_Keep_Only_What_They_Retain()
    {
        var events = new InMemoryActorEventStore();
        var (actors, _, _) = Create(o =>
        {
            o.EventStore = events;
            o.AddDurableStream<Note>(retain: 10);
        });
        for (var i = 0; i < 50; i++)
            await actors.GetStream<Note>("busy").PublishAsync(new Note(i.ToString()), Ct);

        var kept = new List<long>();
        await foreach (var e in events.ReadAsync(new ActorStateKey("$stream:Shiny.Actors.Tests.Note", "busy", "events"), 0, Ct))
            kept.Add(e.Version);
        Assert.InRange(kept.Count, 10, 11);
        Assert.Equal(50, kept[^1]);
    }


    [Fact]
    public async Task A_Remote_Reader_Resumes_After_The_Server_Restarts()
    {
        var port = FreePort();
        var builder = HttpServer.CreateBuilder();
        builder.Configure((HttpServerOptions o) => o.Port = port);
        builder.Services.AddSingleton(new Probe()).AddShinyActors(actors => actors.AddDurableStream<Note>());
        var server = builder.Build();
        await using var _ = server;
        server.MapActors(o => o.ExposeStream<Note>());
        await server.StartAsync(Ct);

        var local = server.Services!.GetRequiredService<ActorSystem>();
        var remote = new RemoteActorSystem(new HttpClient { BaseAddress = new Uri($"http://127.0.0.1:{port}/actors/") });

        await local.GetStream<Note>("live").PublishAsync(new Note("before"), Ct);
        var received = new List<string>();
        await using var reader = remote.GetStream<Note>("live").ReadFromAsync(0, Ct).GetAsyncEnumerator(Ct);
        Assert.True(await reader.MoveNextAsync());
        received.Add(reader.Current.Item.Text);

        // the server goes away; events keep happening; it comes back
        await server.StopAsync(Ct);
        await local.GetStream<Note>("live").PublishAsync(new Note("while-down-1"), Ct);
        await local.GetStream<Note>("live").PublishAsync(new Note("while-down-2"), Ct);
        await server.StartAsync(Ct);

        // the reader reconnects with Last-Event-ID and gets what it missed, in order, without asking
        while (received.Count < 3)
        {
            Assert.True(await reader.MoveNextAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(15), Ct));
            received.Add(reader.Current.Item.Text);
        }
        Assert.Equal(["before", "while-down-1", "while-down-2"], received);
    }


    [Fact]
    public async Task Remote_Replay_Of_A_Non_Durable_Stream_Is_Refused()
    {
        var builder = HttpServer.CreateBuilder();
        builder.Configure((HttpServerOptions o) => o.Port = 0);
        builder.Services.AddSingleton(new Probe()).AddShinyActors();
        var server = builder.Build();
        await using var _ = server;
        server.MapActors(o => o.ExposeStream<Note>());
        await server.StartAsync(Ct);
        var remote = new RemoteActorSystem(new HttpClient { BaseAddress = new Uri(server.ListenUrl!.TrimEnd('/') + "/actors/") });

        var ex = await Assert.ThrowsAsync<RemoteActorException>(async () =>
        {
            await foreach (var __ in remote.GetStream<Note>("x").ReadFromAsync(0, Ct)) { }
        });
        Assert.Equal(409, ex.StatusCode);
    }


    static int FreePort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return ((IPEndPoint)listener.LocalEndpoint).Port;
    }
}
