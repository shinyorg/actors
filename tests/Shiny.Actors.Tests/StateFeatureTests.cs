using System.Text.Json.Serialization.Metadata;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class StateFeatureTests : IDisposable
{
    readonly Stores stores = new();
    public void Dispose() => this.stores.Dispose();


    [Fact]
    public async Task Named_States_Load_Save_And_Persist_Independently()
    {
        var provider = new InMemoryActorStateProvider();
        var (first, _, _) = Create(o => o.StateProvider = provider);

        await first.Get<IWallet>("w").Deposit(10m, "USD");
        await first.Get<IWallet>("w").Deposit(5m, "EUR");
        await first.DisposeAsync();

        var (second, _, _) = Create(o => o.StateProvider = provider);
        var (amount, history, currency) = await second.Get<IWallet>("w").Read();
        Assert.Equal(15m, amount);
        Assert.Equal(["+10", "+5"], history);
        Assert.Equal("EUR", currency);

        var balance = await provider.ReadAsync(new ActorStateKey("Shiny.Actors.Tests.WalletActor", "w", "balance"), TestJson.Default.Balance, Ct);
        Assert.Equal(15m, balance.State!.Amount);
    }


    [Fact]
    public async Task Named_States_Are_Separate_Files()
    {
        var dir = this.stores.NewLocation();
        var (actors, _, _) = Create(o => o.UseFileStateProvider(dir));
        await actors.Get<IWallet>("w").Deposit(1m, "CAD");

        var files = Directory.GetFiles(Path.Combine(dir, "Shiny.Actors.Tests.WalletActor")).Select(Path.GetFileName).Order().ToArray();
        Assert.Equal(3, files.Length);
        Assert.All(files, f => Assert.Matches(@"^[0-9a-f]{64}\.(balance|history|settings)\.json$", f));
    }


    [Fact]
    public async Task AutoSave_Writes_After_Each_Call_Only_When_Changed()
    {
        var counting = new CountingProvider(new InMemoryActorStateProvider());
        var (actors, _, _) = Create(o => o.StateProvider = counting);
        var ledger = actors.Get<ILedger>("l");

        await ledger.Add(5);
        await ledger.Add(3);
        Assert.Equal(2, counting.Writes);

        await ledger.Total();
        await ledger.Total();
        Assert.Equal(2, counting.Writes); // reads change nothing, so nothing is written

        var stored = await counting.ReadAsync(new ActorStateKey("Shiny.Actors.Tests.LedgerActor", "l"), TestJson.Default.LedgerState, Ct);
        Assert.Equal(8, stored.State!.Total);
    }


    [Fact]
    public async Task AutoSave_Does_Not_Save_A_Call_That_Threw()
    {
        var counting = new CountingProvider(new InMemoryActorStateProvider());
        var (actors, _, _) = Create(o => o.StateProvider = counting);

        await actors.Get<ILedger>("l").Add(2);
        await Assert.ThrowsAsync<InvalidOperationException>(() => actors.Get<ILedger>("l").Boom());

        Assert.Equal(1, counting.Writes);
    }


    [Fact]
    public async Task A_Failed_AutoSave_Fails_The_Call()
    {
        var failing = new CountingProvider(new InMemoryActorStateProvider()) { FailWrites = true };
        var (actors, _, _) = Create(o => o.StateProvider = failing);

        var ex = await Assert.ThrowsAsync<IOException>(() => actors.Get<ILedger>("l").Add(1));
        Assert.Equal("disk full", ex.Message);
    }


    [Fact]
    public async Task AutoSave_OnDeactivate_Waits_For_Deactivation()
    {
        var counting = new CountingProvider(new InMemoryActorStateProvider());
        var (actors, _, _) = Create(o => o.StateProvider = counting);

        await actors.Get<ILazyLedger>("l").Add(4);
        await actors.Get<ILazyLedger>("l").Add(4);
        Assert.Equal(0, counting.Writes);

        await actors.DeactivateAllAsync(Ct);
        Assert.Equal(1, counting.Writes);
        var stored = await counting.ReadAsync(new ActorStateKey("Shiny.Actors.Tests.LazyLedgerActor", "l"), TestJson.Default.LedgerState, Ct);
        Assert.Equal(8, stored.State!.Total);
    }


    [Fact]
    public async Task DefaultAutoSave_Applies_To_Actors_Without_The_Attribute()
    {
        var counting = new CountingProvider(new InMemoryActorStateProvider());
        var (actors, _, _) = Create(o =>
        {
            o.StateProvider = counting;
            o.DefaultAutoSave = AutoSaveMode.AfterEachCall;
        });

        await actors.Get<ICounter>("c").Bump(); // CounterActor never writes on its own during calls
        await actors.Get<ICounter>("c").GetCount();
        Assert.Equal(1, counting.Writes);
    }


    [Theory]
    [MemberData(nameof(Stores.Kinds), MemberType = typeof(Stores))]
    public async Task Two_Systems_On_One_Store_Never_Overwrite_Each_Other(string kind)
    {
        // two processes (or two app instances) sharing storage - the second handle sees the same data
        var location = this.stores.NewLocation();
        var (providerA, _) = this.stores.Create(kind, location);
        var providerB = kind == "memory" ? providerA : this.stores.Create(kind, location).State;

        var (a, _, _) = Create(o => o.StateProvider = providerA);
        var (b, _, _) = Create(o => o.StateProvider = providerB);

        Assert.Equal(0, await b.Get<ILedger>("shared").Total()); // B loads "nothing stored"
        Assert.Equal(5, await a.Get<ILedger>("shared").Add(5));  // A writes first

        // B's write is conditional on what it read, so it is refused rather than wiping out A's 5
        await Assert.ThrowsAsync<ActorStateConflictException>(() => b.Get<ILedger>("shared").Add(1));

        // and B deactivated itself: the next call reloads A's write and builds on it
        await Eventually(() => Task.FromResult(b.ActivationCount), c => c == 0, "the conflicted actor deactivates");
        Assert.Equal(6, await b.Get<ILedger>("shared").Add(1));
        Assert.Equal(6, (await providerA.ReadAsync(new ActorStateKey("Shiny.Actors.Tests.LedgerActor", "shared"), TestJson.Default.LedgerState, Ct)).State!.Total);
    }


    [Fact]
    public async Task UseDocumentDb_Keeps_State_And_Reminders_In_Sqlite()
    {
        var dir = this.stores.NewLocation();
        Directory.CreateDirectory(dir);
        var db = $"Data Source={Path.Combine(dir, "app.db")}";

        var (first, _, _) = Create(o => o.UseDocumentDb(new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider(db)));
        await first.Get<ILedger>("db").Add(7);
        await first.Get<IAlarm>("db").Set("later", TimeSpan.FromHours(1), null);
        await first.DisposeAsync();

        var (second, _, _) = Create(o => o.UseDocumentDb(new Shiny.DocumentDb.Sqlite.SqliteDatabaseProvider(db)));
        Assert.Equal(7, await second.Get<ILedger>("db").Total());
        Assert.Equal(["later"], await second.Get<IAlarm>("db").Reminders());
    }


    sealed class CountingProvider(IActorStateProvider inner) : IActorStateProvider
    {
        int writes;
        public int Writes => Volatile.Read(ref this.writes);
        public bool FailWrites { get; init; }

        public ValueTask<StoredState<TState>> ReadAsync<TState>(ActorStateKey key, JsonTypeInfo<TState> typeInfo, CancellationToken cancellationToken) where TState : class
            => inner.ReadAsync(key, typeInfo, cancellationToken);

        public ValueTask<string> WriteAsync<TState>(ActorStateKey key, TState state, JsonTypeInfo<TState> typeInfo, string? etag, CancellationToken cancellationToken) where TState : class
        {
            if (this.FailWrites)
                throw new IOException("disk full");

            Interlocked.Increment(ref this.writes);
            return inner.WriteAsync(key, state, typeInfo, etag, cancellationToken);
        }

        public ValueTask<string?> ClearAsync(ActorStateKey key, string? etag, CancellationToken cancellationToken)
            => inner.ClearAsync(key, etag, cancellationToken);
    }
}
