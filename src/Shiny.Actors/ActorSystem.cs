using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Shiny.Actors.Internals;
using Shiny.Actors.Streams;

namespace Shiny.Actors;


/// <summary>
/// Hosts actors in this process. Actors are virtual: they exist as soon as you ask for one, are
/// activated on the first call, and are deactivated when idle.
/// </summary>
/// <example>
/// <code>
/// // no container
/// await using var actors = new ActorSystem();
/// await actors.Get&lt;ICounter&gt;("bob").Increment(1);
///
/// // with one (MAUI, generic host)
/// builder.Services.AddActors(o => o.UseFileStateProvider(Path.Combine(FileSystem.AppDataDirectory, "actors")));
/// </code>
/// </example>
public sealed class ActorSystem : IActorSystem, IAsyncDisposable
{
    readonly ConcurrentDictionary<ActorKey, ActorActivation> activations = new();
    readonly ConcurrentDictionary<(Type, string), WorkerPool> workers = new();
    readonly ConcurrentDictionary<Type, ActorRegistration> resolved = new();
    readonly ConcurrentDictionary<Type, ActorRegistration[]> implicitConsumers = new();
    readonly Dictionary<Type, ActorRegistration> explicitActors = [];
    readonly CancellationTokenSource shutdown = new();
    readonly IActorStateProvider defaultStateProvider;
    readonly ILoggerFactory loggerFactory;
    readonly ActorStreamHub streams;
    readonly ActorReminderService reminders;
    readonly ITimer? idleTimer;
    int implicitVersion = -1;
    int disposed;


    public ActorSystem(ActorSystemOptions? options = null, IServiceProvider? services = null)
    {
        this.Options = options ?? new();
        this.Services = services ?? EmptyServiceProvider.Instance;
        this.TimeProvider = this.Options.TimeProvider ?? this.Services.GetService<TimeProvider>() ?? TimeProvider.System;
        this.loggerFactory = this.Options.LoggerFactory ?? this.Services.GetService<ILoggerFactory>() ?? NullLoggerFactory.Instance;
        this.defaultStateProvider = this.Options.StateProvider ?? this.Services.GetService<IActorStateProvider>() ?? new InMemoryActorStateProvider();
        this.EventStore = this.Options.EventStore ?? this.Services.GetService<IActorEventStore>() ?? new InMemoryActorEventStore();
        this.Schemas = new StateSchemas(this.Options.Migrations);
        this.streams = new ActorStreamHub(this, this.loggerFactory.CreateLogger("Shiny.Actors.Streams"));
        this.CallFilters = [
            .. this.Options.CallFilters,
            .. (this.Services.GetService(typeof(IEnumerable<IActorCallFilter>)) as IEnumerable<IActorCallFilter> ?? [])
        ];
        this.reminders = new ActorReminderService(
            this,
            this.Options.ReminderStore ?? this.Services.GetService<IActorReminderStore>() ?? new InMemoryActorReminderStore(),
            [
                .. this.Options.ReminderObservers,
                .. (this.Services.GetService(typeof(IEnumerable<IActorReminderObserver>)) as IEnumerable<IActorReminderObserver> ?? [])
            ],
            this.loggerFactory.CreateLogger("Shiny.Actors.Reminders")
        );

        foreach (var factory in this.Options.ExplicitActors)
        {
            var registration = factory();
            foreach (var contract in registration.Interfaces)
                this.explicitActors[contract] = registration;
        }

        var idle = this.Options.IdleTimeout;
        if (idle != Timeout.InfiniteTimeSpan)
        {
            if (idle <= TimeSpan.Zero)
                throw new ArgumentOutOfRangeException(nameof(options), "IdleTimeout must be positive, or Timeout.InfiniteTimeSpan.");

            var interval = TimeSpan.FromTicks(Math.Min(Math.Max(idle.Ticks / 2, 1), TimeSpan.FromMinutes(1).Ticks));
            this.idleTimer = this.TimeProvider.CreateTimer(static s => ((ActorSystem)s!).SweepIdle(), this, interval, interval);
        }
        this.reminders.Start();
    }


    public ActorSystemOptions Options { get; }
    public TimeProvider TimeProvider { get; }
    internal IServiceProvider Services { get; }
    internal CancellationToken ShutdownToken => this.shutdown.Token;
    internal ActorReminderService Reminders => this.reminders;
    internal IActorCallFilter[] CallFilters { get; }
    internal IActorEventStore EventStore { get; }
    internal StateSchemas Schemas { get; }

    /// <summary>How many actors are active right now.</summary>
    public int ActivationCount => this.activations.Count;


    public TActor Get<TActor>(string id) where TActor : IActor
    {
        ArgumentNullException.ThrowIfNull(id);
        this.ThrowIfDisposed();

        var contract = typeof(TActor);
        if (!contract.IsInterface)
            throw new ArgumentException($"'{contract.FullName}' is a class. Get an actor by its IActor interface.", nameof(TActor));

        var registration = this.GetRegistration(contract);
        var proxy = ActorRegistry.FindProxy(contract);
        if (proxy is null)
        {
            ActorRegistry.EnsureInitialized(contract.Assembly);
            proxy = ActorRegistry.FindProxy(contract) ?? throw new InvalidOperationException(
                $"No proxy was generated for '{contract.FullName}'. Actor interfaces must be public or internal, non-generic, " +
                "and declared in a project that references Shiny.Actors (which runs its source generator)."
            );
        }
        return (TActor)proxy(new ActorReference(this, registration, id));
    }


    public IActorStream<T> GetStream<T>(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new ActorStream<T>(this.streams, key);
    }


    internal bool IsDurableStream(Type eventType) => this.streams.IsDurable(eventType);


    /// <summary>Live events with their sequence - what a remote subscriber needs to resume later.</summary>
    internal async IAsyncEnumerable<ActorStreamEvent<T>> ReadLiveAsync<T>(string key, [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var channel = System.Threading.Channels.Channel.CreateUnbounded<ActorStreamEvent<T>>(new() { SingleReader = true });
        using var _ = this.streams.Add(key, new ChannelSubscriber<T>(channel.Writer));
        await foreach (var e in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            yield return e;
    }


    /// <summary>Deactivates one actor now, if it is active, after the calls already queued for it.</summary>
    public Task DeactivateAsync<TActor>(string id) where TActor : IActor
        => this.DeactivateAsync(this.GetRegistration(typeof(TActor)), id);


    internal async Task DeactivateAsync(ActorRegistration registration, string id)
    {
        var matching = this.activations.Values.Where(a => a.Registration.ImplementationType == registration.ImplementationType && a.Id == id);
        await Task.WhenAll(matching.Select(a => a.StopAsync(DeactivationReason.Requested))).ConfigureAwait(false);
    }


    /// <summary>
    /// Fires every reminder that is due now and waits for the actors to handle them. The system already does this on
    /// a timer while it runs; call it from a background job (see Shiny.Actors.Jobs) when the OS wakes the app.
    /// </summary>
    /// <param name="deactivateAfter">Deactivate actors this run activated once they are done, so nothing is left half-written if the OS suspends the app.</param>
    /// <returns>How many reminders were delivered.</returns>
    public Task<int> RunDueRemindersAsync(bool deactivateAfter = false, CancellationToken cancellationToken = default)
        => this.reminders.RunDueAsync(deactivateAfter, cancellationToken);


    /// <summary>
    /// Deactivates every actor, running each one's <c>OnDeactivateAsync</c>. Call it when a mobile app moves to
    /// the background - the OS may end the process without warning after that. Actors reactivate on the next call.
    /// </summary>
    public async Task DeactivateAllAsync(CancellationToken cancellationToken = default)
    {
        while (!this.activations.IsEmpty)
        {
            var stopping = this.activations.Values.Select(x => x.StopAsync(DeactivationReason.Shutdown));
            await Task.WhenAll(stopping).WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }


    public async ValueTask DisposeAsync()
    {
        if (Volatile.Read(ref this.disposed) != 0)
            return;

        this.idleTimer?.Dispose();
        await this.reminders.DisposeAsync().ConfigureAwait(false);
        await this.DeactivateAllAsync().ConfigureAwait(false);

        if (Interlocked.Exchange(ref this.disposed, 1) == 0)
        {
            this.shutdown.Cancel();
            this.shutdown.Dispose();
        }
    }


    internal async ValueTask PostAsync(ActorRegistration registration, string id, MailboxItem item, CancellationToken cancellationToken)
    {
        var key = new ActorKey(registration, id);
        while (true)
        {
            this.ThrowIfDisposed();
            var activation = registration.IsStatelessWorker
                ? this.PickWorker(registration, id)
                : this.activations.GetOrAdd(
                    key,
                    static (k, s) => new ActorActivation(s.System, s.Registration, k.Id),
                    (System: this, Registration: registration)
                );

            if (activation.WouldDeadlock(item))
                throw new ActorDeadlockException(
                    $"Calling '{activation}' would deadlock: {item.Chain!.Describe(activation)}. " +
                    "Actors are not reentrant - break the cycle, make one of the calls [OneWay], or mark the method [AlwaysInterleave]."
                );

            bool posted;
            try
            {
                posted = await activation.TryPostAsync(item, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                if (registration.IsStatelessWorker)
                    activation.Unreserve(); // now counted by its mailbox instead
            }
            if (posted)
                return;

            // it is deactivating - wait until it has finished (and flushed its state), then go again
            await activation.Completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
    }


    internal ActorRegistration GetRegistration(Type contract)
        => this.resolved.GetOrAdd(contract, static (c, self) => self.Resolve(c), this);


    internal ActorRegistration? FindRegistrationByName(string name)
        => this.explicitActors.Values.FirstOrDefault(x => x.Name == name)
           ?? ActorRegistry.All.FirstOrDefault(x => x.Name == name);


    /// <summary>Nothing queued, nothing running, no reminder run in flight - for tests and orderly shutdown.</summary>
    internal bool IsQuiet => !this.reminders.IsBusy && this.activations.Values.All(a => a.IsSettled);


    internal bool IsActive(ActorRegistration registration, string id)
        => this.activations.ContainsKey(new ActorKey(registration, id));


    internal void Remove(ActorActivation activation)
    {
        this.activations.TryRemove(new KeyValuePair<ActorKey, ActorActivation>(activation.Key, activation));
        if (activation.Registration.IsStatelessWorker && this.workers.TryGetValue((activation.Registration.ImplementationType, activation.Id), out var pool))
            pool.Remove(activation);
    }


    // an idle worker if there is one, a new one while under the limit, otherwise the least busy
    ActorActivation PickWorker(ActorRegistration registration, string id)
    {
        var pool = this.workers.GetOrAdd((registration.ImplementationType, id), static _ => new WorkerPool());
        lock (pool)
        {
            ActorActivation? best = null;
            var bestLoad = int.MaxValue;
            foreach (var worker in pool.Members)
            {
                var load = worker.Load;
                if (load < bestLoad)
                    (best, bestLoad) = (worker, load);
            }

            if (bestLoad > 0 && pool.Members.Count < registration.MaxWorkers)
            {
                var index = 1;
                while (pool.Members.Any(x => x.Worker == index))
                    index++;

                best = new ActorActivation(this, registration, id, index);
                this.activations[best.Key] = best;
                pool.Members.Add(best);
            }

            best!.Reserve();
            return best;
        }
    }


    sealed class WorkerPool
    {
        public List<ActorActivation> Members { get; } = [];

        public void Remove(ActorActivation activation)
        {
            lock (this)
                this.Members.Remove(activation);
        }
    }


    internal ILogger CreateLogger(ActorRegistration registration)
        => this.loggerFactory.CreateLogger("Shiny.Actors." + registration.Name);


    internal IActorStateProvider GetStateProvider(string? name)
    {
        if (name is null)
            return this.defaultStateProvider;

        if (this.Options.NamedStateProviders.TryGetValue(name, out var provider))
            return provider;

        if (this.Services is IKeyedServiceProvider keyed && keyed.GetKeyedService(typeof(IActorStateProvider), name) is IActorStateProvider fromServices)
            return fromServices;

        throw new InvalidOperationException(
            $"No state provider named '{name}'. Add it with options.AddStateProvider(\"{name}\", ...) or register a keyed IActorStateProvider."
        );
    }


    internal ActorRegistration[] GetImplicitConsumers(Type eventType)
    {
        var version = ActorRegistry.Version;
        if (Interlocked.Exchange(ref this.implicitVersion, version) != version)
            this.implicitConsumers.Clear();

        return this.implicitConsumers.GetOrAdd(
            eventType,
            static t => [.. ActorRegistry.All.Where(r => r.ImplicitStreams.Contains(t))]
        );
    }


    ActorRegistration Resolve(Type contract)
    {
        if (this.explicitActors.TryGetValue(contract, out var chosen))
            return chosen;

        var registration = ActorRegistry.FindByInterface(contract, out var conflicting);
        if (registration is null)
        {
            ActorRegistry.EnsureInitialized(contract.Assembly);
            registration = ActorRegistry.FindByInterface(contract, out conflicting);
        }

        if (conflicting is not null)
            throw new InvalidOperationException(
                $"More than one class implements '{contract.FullName}' ({string.Join(", ", conflicting.Select(x => x.FullName))}). " +
                $"Choose one with options.AddActor<{contract.Name}, TImplementation>()."
            );

        return registration ?? throw new InvalidOperationException(
            $"No actor implements '{contract.FullName}'. Implement it on a class deriving from Actor in a project that references Shiny.Actors. " +
            "If that class is in a library nothing else in the app uses, call options.AddAssembly(typeof(TheActor).Assembly)."
        );
    }


    void SweepIdle()
    {
        foreach (var activation in this.activations.Values)
            if (activation.IsIdle)
                activation.QueueIdleCheck();
    }


    void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref this.disposed) != 0, this);


    sealed class EmptyServiceProvider : IServiceProvider
    {
        public static readonly EmptyServiceProvider Instance = new();
        public object? GetService(Type serviceType) => null;
    }
}
