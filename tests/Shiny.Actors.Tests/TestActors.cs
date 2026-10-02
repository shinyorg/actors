using System.Collections.Concurrent;
using System.Text.Json.Serialization;

namespace Shiny.Actors.Tests;


/// <summary>One per test - records what actors did, so tests running in parallel never share it.</summary>
public sealed class Probe
{
    int concurrent;
    public ConcurrentQueue<string> Events { get; } = new();
    public int MaxConcurrent;
    public int ActivationFailuresLeft;

    public void Log(string message) => this.Events.Enqueue(message);


    public IDisposable Enter()
    {
        var now = Interlocked.Increment(ref this.concurrent);
        int max;
        while (now > (max = Volatile.Read(ref this.MaxConcurrent)) && Interlocked.CompareExchange(ref this.MaxConcurrent, now, max) != max) { }
        return new Exit(this);
    }

    sealed class Exit(Probe probe) : IDisposable
    {
        public void Dispose() => Interlocked.Decrement(ref probe.concurrent);
    }
}


public class CounterState
{
    public int Count { get; set; }
}


public record Note(string Text);


public record Profile(string Name, int Age, string[] Tags);


public class AlarmState
{
    public List<string> Rings { get; set; } = [];
}


public class Balance { public decimal Amount { get; set; } }
public class History { public List<string> Entries { get; set; } = []; }
public class Settings { public string Currency { get; set; } = "CAD"; }
public class LedgerState { public int Total { get; set; } }


[JsonPolymorphic]
[JsonDerivedType(typeof(Deposited), "deposited")]
[JsonDerivedType(typeof(Withdrawn), "withdrawn")]
public abstract record AccountEvent;
public record Deposited(decimal Amount) : AccountEvent;
public record Withdrawn(decimal Amount) : AccountEvent;
public class AccountState { public decimal Balance { get; set; } public int Transactions { get; set; } }

[StateVersion(2)]
public class Person { public string First { get; set; } = ""; public string Last { get; set; } = ""; }

[StateVersion(2)]
public record TagAdded(string Tag);
public class TagState { public List<string> Tags { get; set; } = []; }


[JsonSerializable(typeof(CounterState))]
[JsonSerializable(typeof(AlarmState))]
[JsonSerializable(typeof(AccountState))]
[JsonSerializable(typeof(AccountEvent))]
[JsonSerializable(typeof(Person))]
[JsonSerializable(typeof(TagAdded))]
[JsonSerializable(typeof(TagState))]
[JsonSerializable(typeof(System.Text.Json.Nodes.JsonObject))]
[JsonSerializable(typeof(Balance))]
[JsonSerializable(typeof(History))]
[JsonSerializable(typeof(Settings))]
[JsonSerializable(typeof(LedgerState))]
[JsonSerializable(typeof(Profile))]
[JsonSerializable(typeof(Note))]
public partial class TestJson : JsonSerializerContext;


public interface ICounter : IActor
{
    ValueTask<int> Increment(int by = 1);
    Task<int> GetCount();
    [OneWay] ValueTask Bump();
    ValueTask<int> Slow(int milliseconds, CancellationToken cancellationToken);
    Task Fail(string message);
    ValueTask DeactivateSoon();
}


public interface IResettable : IActor
{
    Task Reset();
}


[ActorName("counter")]
public class CounterActor(Probe probe) : Actor<CounterState>, ICounter, IResettable
{
    protected override ValueTask OnActivateAsync(CancellationToken cancellationToken)
    {
        probe.Log($"activate:{this.Id}:{this.State.Count}");
        return default;
    }


    protected override async ValueTask OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken)
    {
        probe.Log($"deactivate:{this.Id}:{reason}");
        await this.WriteStateAsync(cancellationToken);
    }


    public async ValueTask<int> Increment(int by)
    {
        using var _ = probe.Enter();
        await Task.Yield(); // give another call the chance to interleave, if the runtime would let it
        this.State.Count += by;
        return this.State.Count;
    }


    public Task<int> GetCount() => Task.FromResult(this.State.Count);


    public ValueTask Bump()
    {
        this.State.Count++;
        return default;
    }


    public async ValueTask<int> Slow(int milliseconds, CancellationToken cancellationToken)
    {
        await Task.Delay(milliseconds, cancellationToken);
        return ++this.State.Count;
    }


    public Task Fail(string message) => throw new InvalidOperationException(message);


    public ValueTask DeactivateSoon()
    {
        this.DeactivateOnIdle();
        return default;
    }


    public Task Reset()
    {
        this.State.Count = 0;
        return Task.CompletedTask;
    }
}


public interface IFlaky : IActor
{
    Task<string> Hello();
}


public class FlakyActor(Probe probe) : Actor, IFlaky
{
    protected override ValueTask OnActivateAsync(CancellationToken cancellationToken)
    {
        if (Interlocked.Decrement(ref probe.ActivationFailuresLeft) >= 0)
            throw new IOException("disk on fire");
        return default;
    }

    public Task<string> Hello() => Task.FromResult("hi " + this.Id);
}


public interface IPingPong : IActor
{
    Task<string> Ping();
    Task<string> CallSelf();
    Task<string> CallOther(string otherId);
    Task<string> CallBack(string originId);
}


public class PingPongActor : Actor, IPingPong
{
    public Task<string> Ping() => Task.FromResult("pong " + this.Id);
    public Task<string> CallSelf() => this.Actors.Get<IPingPong>(this.Id).Ping();
    public Task<string> CallOther(string otherId) => this.Actors.Get<IPingPong>(otherId).CallBack(this.Id);
    public Task<string> CallBack(string originId) => this.Actors.Get<IPingPong>(originId).Ping();}


public interface ITicker : IActor
{
    Task<int> Ticks();
}


public class TickerActor : Actor, ITicker
{
    int ticks;

    protected override ValueTask OnActivateAsync(CancellationToken cancellationToken)
    {
        this.RegisterTimer(_ => { this.ticks++; return default; }, TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1));
        return default;
    }

    public Task<int> Ticks() => Task.FromResult(this.ticks);
}


/// <summary>Implicit stream subscription: every Note on stream key == Id arrives here.</summary>
public interface IInbox : IActor
{
    Task<string[]> Received();
}


public class InboxActor(Probe probe) : Actor, IInbox, IActorStreamConsumer<Note>
{
    readonly List<string> received = [];

    public ValueTask OnNextAsync(Note item, CancellationToken cancellationToken)
    {
        using var _ = probe.Enter();
        this.received.Add(item.Text);
        return default;
    }

    public Task<string[]> Received() => Task.FromResult(this.received.ToArray());
}


/// <summary>Explicit subscription made from inside the actor.</summary>
public interface IListener : IActor
{
    Task Listen(string streamKey);
    Task<string[]> Heard();
}


public class ListenerActor : Actor, IListener
{
    readonly List<string> heard = [];

    public Task Listen(string streamKey)
    {
        this.Actors.GetStream<Note>(streamKey).Subscribe((note, _) =>
        {
            this.heard.Add(note.Text);
            return default;
        });
        return Task.CompletedTask;
    }

    public Task<string[]> Heard() => Task.FromResult(this.heard.ToArray());
}


[Reentrant]
public class GateActor(Probe probe) : Actor, IGate
{
    readonly TaskCompletionSource<string> gate = new(TaskCreationOptions.RunContinuationsAsynchronously);

    public async Task<string> WaitForOpen() => await this.gate.Task;

    public Task Open(string value)
    {
        this.gate.TrySetResult(value);
        return Task.CompletedTask;
    }

    public Task<string> CallSelf() => this.Actors.Get<IGate>(this.Id).Ping();

    public Task<string> Ping() => Task.FromResult("pong");

    public async Task<string> WorkThenCallSelf()
    {
        await Task.Delay(100);
        return await this.Actors.Get<IGate>(this.Id).Ping(); // while the activation is shutting down
    }

    public async Task Work()
    {
        using (probe.Enter())
            Thread.Sleep(5);

        await Task.Delay(50);

        using (probe.Enter())
            Thread.Sleep(5);
    }
}


public interface IGate : IActor
{
    Task<string> WorkThenCallSelf();
    Task<string> WaitForOpen();
    Task Open(string value);
    Task<string> CallSelf();
    Task<string> Ping();
    Task Work();
}


public interface IAlarm : IActor
{
    Task SetWithAlert(string name, TimeSpan dueTime, TimeSpan? period, string title);
    Task Set(string name, TimeSpan dueTime, TimeSpan? period);
    Task<bool> Cancel(string name);
    Task<string[]> Reminders();
    Task<string[]> Rings();
}


public class AlarmActor(Probe probe) : Actor<AlarmState>, IAlarm, IRemindable
{
    public Task Set(string name, TimeSpan dueTime, TimeSpan? period) => this.RegisterReminderAsync(name, dueTime, period).AsTask();

    public Task SetWithAlert(string name, TimeSpan dueTime, TimeSpan? period, string title)
        => this.RegisterReminderAsync(name, dueTime, period, new ReminderNotification(title, $"{name} is due")).AsTask();

    public async Task<bool> Cancel(string name) => await this.UnregisterReminderAsync(name);

    public async Task<string[]> Reminders() => [.. (await this.GetRemindersAsync()).Select(x => x.Name)];

    public Task<string[]> Rings() => Task.FromResult(this.State.Rings.ToArray());

    public async ValueTask ReceiveReminderAsync(ReminderTick tick, CancellationToken cancellationToken)
    {
        probe.Log($"ring:{this.Id}:{tick.Name}");
        this.State.Rings.Add(tick.Name);
        await this.WriteStateAsync(cancellationToken);
    }
}


/// <summary>Remoting: complex types, nullable results and overloads.</summary>
public interface IProfile : IActor
{
    Task Save(Profile profile);
    Task<Profile?> Find();
    Task<string> Greet(string name);
    Task<string> Greet(string name, int times);
}


public class ProfileActor : Actor, IProfile
{
    Profile? profile;

    public Task Save(Profile value)
    {
        this.profile = value;
        return Task.CompletedTask;
    }

    public Task<Profile?> Find() => Task.FromResult(this.profile);
    public Task<string> Greet(string name) => Task.FromResult($"hi {name}");
    public Task<string> Greet(string name, int times) => Task.FromResult(string.Join(" ", Enumerable.Repeat($"hi {name}", times)));
}


/// <summary>A contract whose parameter has no JSON metadata - MapActors must refuse it at startup.</summary>
public interface IUnserializable : IActor
{
    Task Take(Probe probe);
}


/// <summary>Several named states: two from [ActorState] constructor parameters, one from CreateState.</summary>
public interface IWallet : IActor
{
    Task Deposit(decimal amount, string currency);
    Task<(decimal Amount, string[] History, string Currency)> Read();
}


public class WalletActor : Actor, IWallet
{
    readonly IActorState<Balance> balance;
    readonly IActorState<History> history;
    readonly IActorState<Settings> settings;

    public WalletActor([ActorState("balance")] IActorState<Balance> balance, [ActorState("history")] IActorState<History> history)
    {
        this.balance = balance;
        this.history = history;
        this.settings = this.CreateState<Settings>("settings");
    }


    public async Task Deposit(decimal amount, string currency)
    {
        this.balance.State.Amount += amount;
        this.history.State.Entries.Add($"+{amount}");
        this.settings.State.Currency = currency;

        await this.balance.WriteStateAsync();
        await this.history.WriteStateAsync();
        await this.settings.WriteStateAsync();
    }


    public Task<(decimal Amount, string[] History, string Currency)> Read()
        => Task.FromResult((this.balance.State.Amount, this.history.State.Entries.ToArray(), this.settings.State.Currency));
}


/// <summary>Never calls WriteStateAsync - [AutoSave] does it after every call.</summary>
public interface ILedger : IActor
{
    Task<int> Add(int amount);
    Task<int> Total();
    Task Boom();
}


[AutoSave]
public class LedgerActor : Actor<LedgerState>, ILedger
{
    public Task<int> Add(int amount) => Task.FromResult(this.State.Total += amount);
    public Task<int> Total() => Task.FromResult(this.State.Total);

    public Task Boom()
    {
        this.State.Total = -1;
        throw new InvalidOperationException("boom");
    }
}


public interface ILazyLedger : IActor
{
    Task<int> Add(int amount);
}


[AutoSave(AutoSaveMode.OnDeactivate)]
public class LazyLedgerActor : Actor<LedgerState>, ILazyLedger
{
    public Task<int> Add(int amount) => Task.FromResult(this.State.Total += amount);
}


/// <summary>A long exclusive job, with status and cancel calls that must not queue behind it.</summary>
public interface IWorkflow : IActor
{
    Task<string> Run(int milliseconds);
    [AlwaysInterleave] Task<string> Status();
    [AlwaysInterleave] Task Cancel();
    Task<string> RunAndCheckOwnStatus();
}


public class WorkflowActor : Actor, IWorkflow
{
    CancellationTokenSource? running;
    string status = "idle";

    public async Task<string> Run(int milliseconds)
    {
        this.running = new CancellationTokenSource();
        this.status = "running";
        try
        {
            await Task.Delay(milliseconds, this.running.Token);
            return this.status = "done";
        }
        catch (OperationCanceledException)
        {
            return this.status = "cancelled";
        }
    }

    public Task<string> Status() => Task.FromResult(this.status);

    public Task Cancel()
    {
        this.running?.Cancel();
        return Task.CompletedTask;
    }

    // calling an [AlwaysInterleave] method of yourself can't deadlock
    public async Task<string> RunAndCheckOwnStatus()
    {
        this.status = "checking";
        return await this.Actors.Get<IWorkflow>(this.Id).Status();
    }
}


public interface ILibrary : IActor
{
    [ReadOnly] Task<int> Read(int milliseconds);
    Task Write(int milliseconds);
}


public class LibraryActor(Probe probe) : Actor, ILibrary
{
    int readers;
    bool writing;

    public async Task<int> Read(int milliseconds)
    {
        if (this.writing)
            probe.Log("read during write!");

        var now = ++this.readers;
        lock (probe)
            probe.MaxConcurrent = Math.Max(probe.MaxConcurrent, now);

        await Task.Delay(milliseconds);
        this.readers--;
        return now;
    }

    public async Task Write(int milliseconds)
    {
        if (this.readers > 0)
            probe.Log("write during read!");

        this.writing = true;
        await Task.Delay(milliseconds);
        this.writing = false;
        probe.Log("wrote");
    }
}


public interface IResizer : IActor
{
    Task<int> Resize(int milliseconds);
}


[StatelessWorker(MaxLocalWorkers = 4)]
public class ResizerActor(Probe probe) : Actor, IResizer
{
    public async Task<int> Resize(int milliseconds)
    {
        using var _ = probe.Enter();
        await Task.Delay(milliseconds);
        return this.GetHashCode();
    }
}


/// <summary>Request context flows in, onward, and never back out.</summary>
public interface IContextual : IActor
{
    Task<string?> Read(string key);
    Task<string?> ReadVia(string otherId, string key);
    Task<string?> SetAndReturn(string key, string value);
}


public class ContextualActor : Actor, IContextual
{
    public Task<string?> Read(string key) => Task.FromResult(ActorRequestContext.Get(key));

    public Task<string?> ReadVia(string otherId, string key) => this.Actors.Get<IContextual>(otherId).Read(key);

    public Task<string?> SetAndReturn(string key, string value)
    {
        ActorRequestContext.Set(key, value);
        return Task.FromResult(ActorRequestContext.Get(key));
    }
}


/// <summary>An actor that filters its own calls.</summary>
public interface IGuarded : IActor
{
    Task<int> Square(int value);
}


public class GuardedActor : Actor, IGuarded, IActorCallFilter
{
    public Task<int> Square(int value) => Task.FromResult(value * value);

    public async ValueTask InvokeAsync(ActorCallContext context, ActorCallDelegate next)
    {
        if (context.Arguments[0] is int n && n < 0)
            throw new ArgumentOutOfRangeException("value", "no negatives");

        await next(context);
    }
}


/// <summary>Only the metrics test uses it, so its measurements can't mix with other tests'.</summary>
public interface IMetered : IActor
{
    Task Tick();
}


public class MeteredActor : Actor, IMetered
{
    public Task Tick() => Task.CompletedTask;
}


public interface IAccount : IActor
{
    Task<decimal> Deposit(decimal amount);
    Task<decimal> Withdraw(decimal amount);
    Task<decimal> Balance();
    Task<string[]> History();
    Task<long> CurrentVersion();
}


/// <summary>Event-sourced: the balance is never stored, only the deposits and withdrawals.</summary>
[AutoSave]
public class AccountActor : JournaledActor<AccountState, AccountEvent>, IAccount
{
    protected override int SnapshotEvery => 3;

    protected override void Apply(AccountState state, AccountEvent @event)
    {
        state.Transactions++;
        state.Balance += @event switch
        {
            Deposited d => d.Amount,
            Withdrawn w => -w.Amount,
            _ => 0
        };
    }

    public Task<decimal> Deposit(decimal amount)
    {
        this.RaiseEvent(new Deposited(amount));
        return Task.FromResult(this.State.Balance);
    }

    public Task<decimal> Withdraw(decimal amount)
    {
        if (amount > this.State.Balance)
            throw new InvalidOperationException("insufficient funds");

        this.RaiseEvent(new Withdrawn(amount));
        return Task.FromResult(this.State.Balance);
    }

    public Task<decimal> Balance() => Task.FromResult(this.State.Balance);

    public async Task<string[]> History()
    {
        var entries = new List<string>();
        await foreach (var e in this.ReadEventsAsync())
            entries.Add($"{e.Version}:{e.Event}");
        return [.. entries];
    }

    public Task<long> CurrentVersion() => Task.FromResult(this.Version);
}


public interface IPerson : IActor
{
    Task<string> FullName();
}


public class PersonActor : Actor<Person>, IPerson
{
    public Task<string> FullName() => Task.FromResult($"{this.State.First} {this.State.Last}");
}


public interface ITagLog : IActor
{
    Task<string[]> Tags();
    Task Add(string tag);
}


[AutoSave]
public class TagLogActor : JournaledActor<TagState, TagAdded>, ITagLog
{
    protected override void Apply(TagState state, TagAdded @event) => state.Tags.Add(@event.Tag);
    public Task<string[]> Tags() => Task.FromResult(this.State.Tags.ToArray());

    public Task Add(string tag)
    {
        this.RaiseEvent(new TagAdded(tag));
        return Task.CompletedTask;
    }
}
