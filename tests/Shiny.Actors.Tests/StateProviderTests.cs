using System.Text.Json;
using Shiny.Actors.DocumentDb;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


/// <summary>Every provider must behave the same - these run against each one.</summary>
public sealed class Stores : IDisposable
{
    readonly List<string> cleanup = [];

    public static TheoryData<string> Kinds => ["memory", "file", "documentdb"];


    public IActorEventStore CreateEvents(string kind, string? shareWith = null)
    {
        var path = shareWith ?? this.NewLocation();
        return kind switch
        {
            "memory" => new InMemoryActorEventStore(),
            "file" => new FileActorEventStore(Path.Combine(path, "events")),
            _ => new DocumentDbActorEventStore(NewDocumentStore(path))
        };
    }


    public (IActorStateProvider State, IActorReminderStore Reminders) Create(string kind, string? shareWith = null)
    {
        var path = shareWith ?? Path.Combine(Path.GetTempPath(), "shiny-actors-" + Guid.NewGuid().ToString("N"));
        this.cleanup.Add(path);

        switch (kind)
        {
            case "memory":
                return (new InMemoryActorStateProvider(), new InMemoryActorReminderStore());

            case "file":
                return (new FileActorStateProvider(path), new FileActorReminderStore(Path.Combine(path, "reminders.json")));

            default:
                var store = NewDocumentStore(path);
                return (new DocumentDbActorStateProvider(store), new DocumentDbActorReminderStore(store));
        }
    }


    public static IDocumentStore NewDocumentStore(string path)
    {
        Directory.CreateDirectory(path);
        return new DocumentStore(new DocumentStoreOptions
        {
            DatabaseProvider = new SqliteDatabaseProvider($"Data Source={Path.Combine(path, "actors.db")}"),
            JsonSerializerOptions = new JsonSerializerOptions { TypeInfoResolver = ActorDocumentsJsonContext.Default },
            UseReflectionFallback = false
        }.MapActorDocuments());
    }


    /// <summary>A second handle on the same storage - what another process would see.</summary>
    public string NewLocation()
    {
        var path = Path.Combine(Path.GetTempPath(), "shiny-actors-" + Guid.NewGuid().ToString("N"));
        this.cleanup.Add(path);
        return path;
    }


    public void Dispose()
    {
        Microsoft.Data.Sqlite.SqliteConnection.ClearAllPools();
        foreach (var path in this.cleanup.Distinct())
            try { if (Directory.Exists(path)) Directory.Delete(path, true); } catch { /* best effort */ }
    }
}


public class StateProviderTests : IDisposable
{
    static readonly ActorStateKey Key = new("counter", "a/b c");
    readonly Stores stores = new();
    static System.Text.Json.Serialization.Metadata.JsonTypeInfo<CounterState> Info => TestJson.Default.CounterState;

    public void Dispose() => this.stores.Dispose();


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Nothing_Stored_Reads_As_Null(string kind)
    {
        var (provider, _) = this.stores.Create(kind);
        var stored = await provider.ReadAsync(Key, Info, Ct);
        Assert.Null(stored.State);
        Assert.Null(stored.ETag);
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Writes_Round_Trip_And_Change_The_ETag(string kind)
    {
        var (provider, _) = this.stores.Create(kind);

        var first = await provider.WriteAsync(Key, new CounterState { Count = 1 }, Info, null, Ct);
        var read = await provider.ReadAsync(Key, Info, Ct);
        Assert.Equal(1, read.State!.Count);
        Assert.Equal(first, read.ETag);

        var second = await provider.WriteAsync(Key, new CounterState { Count = 2 }, Info, first, Ct);
        Assert.NotEqual(first, second);
        Assert.Equal(2, (await provider.ReadAsync(Key, Info, Ct)).State!.Count);
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Stale_Writes_Are_Conflicts(string kind)
    {
        var (provider, _) = this.stores.Create(kind);
        var first = await provider.WriteAsync(Key, new CounterState { Count = 1 }, Info, null, Ct);
        await provider.WriteAsync(Key, new CounterState { Count = 2 }, Info, first, Ct);

        // a writer that thinks nothing is stored, and one holding the old version
        var ex = await Assert.ThrowsAsync<ActorStateConflictException>(() => provider.WriteAsync(Key, new CounterState(), Info, null, Ct).AsTask());
        Assert.Equal(Key, ex.Key);
        await Assert.ThrowsAsync<ActorStateConflictException>(() => provider.WriteAsync(Key, new CounterState(), Info, first, Ct).AsTask());

        Assert.Equal(2, (await provider.ReadAsync(Key, Info, Ct)).State!.Count); // neither got through
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Clears_Are_Version_Checked_Too(string kind)
    {
        var (provider, _) = this.stores.Create(kind);
        var first = await provider.WriteAsync(Key, new CounterState { Count = 1 }, Info, null, Ct);
        var second = await provider.WriteAsync(Key, new CounterState { Count = 2 }, Info, first, Ct);

        await Assert.ThrowsAsync<ActorStateConflictException>(() => provider.ClearAsync(Key, first, Ct).AsTask());

        var cleared = await provider.ClearAsync(Key, second, Ct);
        var read = await provider.ReadAsync(Key, Info, Ct);
        Assert.Null(read.State);
        Assert.Equal(cleared, read.ETag); // null, or a tombstone's version

        // and writing again starts from whatever the clear left
        await provider.WriteAsync(Key, new CounterState { Count = 3 }, Info, cleared, Ct);
        Assert.Equal(3, (await provider.ReadAsync(Key, Info, Ct)).State!.Count);
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Named_States_Are_Stored_Separately(string kind)
    {
        var (provider, _) = this.stores.Create(kind);
        var named = Key with { StateName = "other" };

        await provider.WriteAsync(Key, new CounterState { Count = 1 }, Info, null, Ct);
        await provider.WriteAsync(named, new CounterState { Count = 2 }, Info, null, Ct);

        Assert.Equal(1, (await provider.ReadAsync(Key, Info, Ct)).State!.Count);
        Assert.Equal(2, (await provider.ReadAsync(named, Info, Ct)).State!.Count);
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Reminder_Stores_Save_Replace_And_Remove(string kind)
    {
        var (_, store) = this.stores.Create(kind);
        var due = new DateTimeOffset(2026, 10, 1, 9, 0, 0, TimeSpan.Zero);
        var reminder = new ActorReminder("alarm", "a/b", "tick", due, TimeSpan.FromMinutes(5));

        await store.SaveAsync(reminder, Ct);
        await store.SaveAsync(reminder with { Name = "other" }, Ct);
        Assert.Equal(2, (await store.GetAllAsync(Ct)).Count);

        await store.SaveAsync(reminder with { DueAt = due.AddHours(1), Period = null }, Ct); // replace, period removed
        var replaced = (await store.GetAllAsync(Ct)).Single(x => x.Name == "tick");
        Assert.Equal(due.AddHours(1), replaced.DueAt);
        Assert.Null(replaced.Period);

        await store.RemoveAsync(reminder, Ct);
        Assert.Equal(["other"], (await store.GetAllAsync(Ct)).Select(x => x.Name));
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Concurrent_Writers_Never_Lose_An_Update(string kind)
    {
        // two handles on one store - two processes - hammering one key with read-modify-write
        var location = this.stores.NewLocation();
        var (a, _) = this.stores.Create(kind, location);
        var b = kind == "memory" ? a : this.stores.Create(kind, location).State;
        var conflicts = 0;
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var writers = Enumerable.Range(0, 40).Select(i => Task.Run(async () =>
        {
            var provider = i % 2 == 0 ? a : b;
            await start.Task; // everyone at once
            while (true)
            {
                var read = await provider.ReadAsync(Key, Info, Ct);
                await Task.Yield(); // let other writers in between the read and the write - a fast provider otherwise may never race
                try
                {
                    await provider.WriteAsync(Key, new CounterState { Count = (read.State?.Count ?? 0) + 1 }, Info, read.ETag, Ct);
                    return;
                }
                catch (ActorStateConflictException)
                {
                    Interlocked.Increment(ref conflicts);
                }
            }
        }, Ct)).ToList();
        start.SetResult();
        await Task.WhenAll(writers);

        Assert.Equal(40, (await a.ReadAsync(Key, Info, Ct)).State!.Count);
        Assert.True(conflicts > 0, "the test should actually have raced");
    }
}
