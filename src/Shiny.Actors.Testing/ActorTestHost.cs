using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Shiny.Actors.Remoting;

namespace Shiny.Actors.Testing;


/// <summary>
/// An actor system for tests: fake time you advance by hand, in-memory state/reminders/events you can seed and
/// inspect, and a record of every call. Framework-agnostic - use it from xUnit, NUnit, MSTest.
/// </summary>
/// <example>
/// <code>
/// await using var host = new ActorTestHost(services => services.AddSingleton&lt;IClock, FakeClock&gt;());
/// await host.Get&lt;ICounter&gt;().Increment(5);
/// await host.AdvanceAsync(TimeSpan.FromMinutes(10));     // timers, reminders and idle deactivation run
/// Assert.Equal(5, (await host.GetStateAsync&lt;CounterActor, CounterState&gt;("test"))!.Count);
/// </code>
/// </example>
public sealed class ActorTestHost : IAsyncDisposable
{
    /// <summary>The id <see cref="Get{TActor}"/> uses when you don't pass one.</summary>
    public const string DefaultId = "test";

    readonly ServiceProvider services;
    readonly ConcurrentQueue<RecordedCall> calls = new();


    /// <param name="services">Register what your actors' constructors need.</param>
    /// <param name="actors">Further actor setup, as in <c>AddShinyActors</c> - applied after the test defaults, so it can override them.</param>
    /// <param name="start">Where the fake clock starts. Default: 2026-01-01 00:00 UTC.</param>
    public ActorTestHost(Action<IServiceCollection>? services = null, Action<ShinyActorBuilder>? actors = null, DateTimeOffset? start = null)
    {
        this.Time = new FakeTimeProvider(start ?? new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

        var collection = new ServiceCollection();
        collection.AddSingleton<TimeProvider>(this.Time); // anything else that tells time sees the fake clock too
        services?.Invoke(collection);
        collection.AddShinyActors(builder =>
        {
            builder
                .Configure(o => o.TimeProvider = this.Time)
                .UseStateProvider(this.StateStore)
                .UseReminderStore(this.ReminderStore)
                .UseEventStore(this.EventStore)
                .AddCallFilter(new Recorder(this.calls));

            actors?.Invoke(builder);
        });

        this.services = collection.BuildServiceProvider();
        this.System = this.services.GetRequiredService<ActorSystem>();
    }


    public FakeTimeProvider Time { get; }
    public ActorSystem System { get; }
    public IServiceProvider Services => this.services;

    public InMemoryActorStateProvider StateStore { get; } = new();
    public InMemoryActorReminderStore ReminderStore { get; } = new();
    public InMemoryActorEventStore EventStore { get; } = new();

    /// <summary>Every actor method call so far, in the order they finished.</summary>
    public IReadOnlyList<RecordedCall> Calls => [.. this.calls];


    public TActor Get<TActor>(string id = DefaultId) where TActor : IActor => this.System.Get<TActor>(id);

    public IActorStream<T> GetStream<T>(string key) => this.System.GetStream<T>(key);


    /// <summary>Collects everything published to a stream from now on.</summary>
    public StreamRecorder<T> Record<T>(string key) => new(this.System.GetStream<T>(key));


    /// <summary>Moves the clock forward, then waits for whatever that set off - timers, reminders, idle deactivation.</summary>
    /// <param name="step">
    /// The clock moves in steps of this size (default one second), settling after each, so every due timer fires at its
    /// own moment - a 5-minute reminder fires twice in 10 minutes rather than once at the end.
    /// </param>
    public async Task AdvanceAsync(TimeSpan by, TimeSpan? step = null, TimeSpan? timeout = null)
    {
        var size = step ?? TimeSpan.FromSeconds(1);
        var remaining = by;
        while (remaining > TimeSpan.Zero)
        {
            var next = remaining < size ? remaining : size;
            this.Time.Advance(next);
            remaining -= next;
            await this.WaitForQuietAsync(timeout).ConfigureAwait(false);
        }
    }


    /// <summary>Waits until nothing is queued or running, anywhere in the system.</summary>
    public async Task WaitForQuietAsync(TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (true)
        {
            // quiet, and still quiet after letting anything just scheduled run - then nothing is left
            if (this.System.IsQuiet)
            {
                await Task.Yield();
                if (this.System.IsQuiet)
                    return;
            }

            if (DateTime.UtcNow > deadline)
                throw new TimeoutException("The actor system did not go quiet - something is still running or keeps queuing work.");

            await Task.Delay(2).ConfigureAwait(false);
        }
    }


    /// <summary>Fires reminders that are due now and waits for them.</summary>
    public Task<int> FireDueRemindersAsync() => this.System.RunDueRemindersAsync();

    public Task DeactivateAllAsync() => this.System.DeactivateAllAsync();


    /// <summary>What <typeparamref name="TActor"/> has stored for <paramref name="id"/> - null when nothing is.</summary>
    public async Task<TState?> GetStateAsync<TActor, TState>(string id = DefaultId, string stateName = ActorStateKey.DefaultStateName)
        where TActor : Actor
        where TState : class
        => (await this.StateStore.ReadAsync(Key<TActor>(id, stateName), TypeInfo<TState>(), CancellationToken.None).ConfigureAwait(false)).State;


    /// <summary>Seeds state before the actor activates - set up a scenario without driving calls to reach it.</summary>
    public async Task SetStateAsync<TActor, TState>(TState state, string id = DefaultId, string stateName = ActorStateKey.DefaultStateName)
        where TActor : Actor
        where TState : class
    {
        var key = Key<TActor>(id, stateName);
        var current = await this.StateStore.ReadAsync(key, TypeInfo<TState>(), CancellationToken.None).ConfigureAwait(false);
        await this.StateStore.WriteAsync(key, state, TypeInfo<TState>(), current.ETag, CancellationToken.None).ConfigureAwait(false);
    }


    /// <summary>The confirmed events of an event-sourced actor.</summary>
    public async Task<IReadOnlyList<TEvent>> GetEventsAsync<TActor, TEvent>(string id = DefaultId)
        where TActor : Actor
        where TEvent : class
    {
        var events = new List<TEvent>();
        var info = TypeInfo<TEvent>();
        await foreach (var e in this.EventStore.ReadAsync(Key<TActor>(id, "journal"), 0, CancellationToken.None).ConfigureAwait(false))
            events.Add(JsonSerializer.Deserialize(e.Json.Span, info)!);
        return events;
    }


    /// <summary>Reminders <typeparamref name="TActor"/> has registered for <paramref name="id"/>.</summary>
    public async Task<IReadOnlyList<ActorReminder>> GetRemindersAsync<TActor>(string id = DefaultId) where TActor : Actor
    {
        var name = NameOf<TActor>();
        return [.. (await this.ReminderStore.GetAllAsync(CancellationToken.None).ConfigureAwait(false)).Where(x => x.ActorName == name && x.ActorId == id)];
    }


    public async ValueTask DisposeAsync()
    {
        await this.System.DisposeAsync().ConfigureAwait(false);
        await this.services.DisposeAsync().ConfigureAwait(false);
    }


    static ActorStateKey Key<TActor>(string id, string stateName) => new(NameOf<TActor>(), id, stateName);


    static string NameOf<TActor>()
    {
        ActorRegistry.EnsureInitialized(typeof(TActor).Assembly);
        return ActorRegistry.FindByImplementation(typeof(TActor))?.Name ?? typeof(TActor).FullName!;
    }


    static JsonTypeInfo<T> TypeInfo<T>()
        => ActorJson.FindContextTypeInfo(typeof(T)) as JsonTypeInfo<T>
           ?? throw new InvalidOperationException($"No JSON metadata for '{typeof(T).Name}' - declare it on a JsonSerializerContext in the actor's project.");


    sealed class Recorder(ConcurrentQueue<RecordedCall> calls) : IActorCallFilter
    {
        public async ValueTask InvokeAsync(ActorCallContext context, ActorCallDelegate next)
        {
            try
            {
                await next(context).ConfigureAwait(false);
                calls.Enqueue(new RecordedCall(context.ActorName, context.ActorId, context.Method.Name, [.. context.Arguments], context.Result, null));
            }
            catch (Exception ex)
            {
                calls.Enqueue(new RecordedCall(context.ActorName, context.ActorId, context.Method.Name, [.. context.Arguments], null, ex));
                throw;
            }
        }
    }
}


public sealed record RecordedCall(string ActorName, string ActorId, string Method, IReadOnlyList<object?> Arguments, object? Result, Exception? Exception)
{
    public bool Failed => this.Exception is not null;
}


/// <summary>Everything published to one stream since it was created.</summary>
public sealed class StreamRecorder<T> : IDisposable
{
    readonly List<T> items = [];
    readonly IDisposable subscription;
    TaskCompletionSource signal = new(TaskCreationOptions.RunContinuationsAsynchronously);


    internal StreamRecorder(IActorStream<T> stream)
        => this.subscription = stream.Subscribe((item, _) =>
        {
            lock (this.items)
            {
                this.items.Add(item);
                this.signal.TrySetResult();
                this.signal = new(TaskCreationOptions.RunContinuationsAsynchronously);
            }
            return default;
        });


    public IReadOnlyList<T> Items
    {
        get
        {
            lock (this.items)
                return [.. this.items];
        }
    }


    /// <summary>Waits until at least <paramref name="count"/> events have arrived.</summary>
    public async Task<IReadOnlyList<T>> WaitForAsync(int count, TimeSpan? timeout = null)
    {
        var deadline = DateTime.UtcNow + (timeout ?? TimeSpan.FromSeconds(10));
        while (true)
        {
            Task next;
            lock (this.items)
            {
                if (this.items.Count >= count)
                    return [.. this.items];
                next = this.signal.Task;
            }

            var left = deadline - DateTime.UtcNow;
            if (left <= TimeSpan.Zero || await Task.WhenAny(next, Task.Delay(left)).ConfigureAwait(false) != next)
                throw new TimeoutException($"Expected {count} event(s) on the stream, got {this.Items.Count}.");
        }
    }


    public void Dispose() => this.subscription.Dispose();
}
