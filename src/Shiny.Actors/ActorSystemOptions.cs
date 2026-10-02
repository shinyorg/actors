using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Shiny.Actors;


public sealed class ActorSystemOptions
{
    /// <summary>
    /// An actor that receives no calls for this long is deactivated. <see cref="Timeout.InfiniteTimeSpan"/> keeps
    /// actors until shutdown. Default: 5 minutes.
    /// </summary>
    public TimeSpan IdleTimeout { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Auto-save for actors without an <see cref="AutoSaveAttribute"/>. Default: <see cref="AutoSaveMode.None"/>.</summary>
    public AutoSaveMode DefaultAutoSave { get; set; } = AutoSaveMode.None;

    /// <summary>How long <see cref="Actor.OnDeactivateAsync"/> gets before its token is cancelled. Default: 30 seconds.</summary>
    public TimeSpan DeactivationTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// 0 (default) is unbounded. A bounded mailbox makes callers wait for room - back pressure instead of memory growth.
    /// </summary>
    public int MailboxCapacity { get; set; }

    /// <summary>Default state provider. Falls back to a registered <see cref="IActorStateProvider"/>, then in-memory.</summary>
    public IActorStateProvider? StateProvider { get; set; }

    /// <summary>
    /// Where reminders persist. Falls back to a registered <see cref="IActorReminderStore"/>, then in-memory
    /// (reminders then fire only while the process lives). <see cref="UseFileStateProvider"/> sets a file store.
    /// </summary>
    public IActorReminderStore? ReminderStore { get; set; }

    /// <summary>
    /// Where event-sourced actors and durable streams keep their logs. Falls back to a registered
    /// <see cref="IActorEventStore"/>, then in-memory. <see cref="UseFileStateProvider"/> sets a file store.
    /// </summary>
    public IActorEventStore? EventStore { get; set; }

    public TimeProvider? TimeProvider { get; set; }

    public ILoggerFactory? LoggerFactory { get; set; }


    internal Dictionary<string, IActorStateProvider> NamedStateProviders { get; } = new(StringComparer.Ordinal);
    internal List<IActorCallFilter> CallFilters { get; } = [];
    internal Dictionary<Type, int> DurableStreams { get; } = [];
    internal List<IActorReminderObserver> ReminderObservers { get; } = [];


    /// <summary>Mirrors reminders somewhere else (an OS scheduler). <see cref="IActorReminderObserver"/> services are added too.</summary>
    public ActorSystemOptions AddReminderObserver(IActorReminderObserver observer)
    {
        this.ReminderObservers.Add(observer);
        return this;
    }


    /// <summary>
    /// Keeps the most recent <paramref name="retain"/> events of every <typeparamref name="T"/> stream in the event store,
    /// numbered - so a subscriber can replay what it missed with <c>ReadFromAsync</c>, and a remote reader resumes after
    /// a reconnect. <typeparamref name="T"/> needs JSON metadata.
    /// </summary>
    public ActorSystemOptions AddDurableStream<T>(int retain = 1000)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(retain);
        this.DurableStreams[typeof(T)] = retain;
        return this;
    }
    internal Dictionary<Type, Dictionary<int, Action<System.Text.Json.Nodes.JsonObject>>> Migrations { get; } = [];


    /// <summary>
    /// Upgrades stored <typeparamref name="T"/> JSON from <paramref name="fromVersion"/> to the next version, as it is
    /// read - for state types and event types alike. Pair it with <see cref="StateVersionAttribute"/> on the type.
    /// </summary>
    /// <example><code>
    /// // v1 had "Name"; v2 splits it
    /// o.AddStateMigration&lt;Profile&gt;(1, json =&gt;
    /// {
    ///     var parts = json["Name"]!.GetValue&lt;string&gt;().Split(' ', 2);
    ///     json["First"] = parts[0];
    ///     json["Last"] = parts.Length &gt; 1 ? parts[1] : "";
    ///     json.Remove("Name");
    /// });
    /// </code></example>
    public ActorSystemOptions AddStateMigration<T>(int fromVersion, Action<System.Text.Json.Nodes.JsonObject> migrate)
    {
        if (!this.Migrations.TryGetValue(typeof(T), out var steps))
            this.Migrations[typeof(T)] = steps = [];

        steps[fromVersion] = migrate;
        return this;
    }


    /// <summary>Runs around every actor method call, outermost first. <see cref="IActorCallFilter"/> services are added after these.</summary>
    public ActorSystemOptions AddCallFilter(IActorCallFilter filter)
    {
        this.CallFilters.Add(filter);
        return this;
    }
    internal List<Func<ActorRegistration>> ExplicitActors { get; } = [];


    /// <summary>
    /// Persists state as JSON files under <paramref name="rootDirectory"/>, reminders in
    /// <c>{rootDirectory}/reminders.json</c> and event logs under <c>{rootDirectory}/events</c> - each unless already set.
    /// </summary>
    public ActorSystemOptions UseFileStateProvider(string rootDirectory)
    {
        this.StateProvider = new FileActorStateProvider(rootDirectory);
        this.ReminderStore ??= new FileActorReminderStore(Path.Combine(rootDirectory, "reminders.json"));
        this.EventStore ??= new FileActorEventStore(Path.Combine(rootDirectory, "events"));
        return this;
    }


    /// <summary>A provider for actors marked <c>[StateProvider("name")]</c>. Keyed DI services work too.</summary>
    public ActorSystemOptions AddStateProvider(string name, IActorStateProvider provider)
    {
        this.NamedStateProviders[name] = provider;
        return this;
    }


    /// <summary>
    /// Chooses <typeparamref name="TImpl"/> for <typeparamref name="TActor"/> - only needed when more than one
    /// class implements the same contract. The generated registration (factory, state metadata) is reused.
    /// </summary>
    public ActorSystemOptions AddActor<TActor, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TImpl>()
        where TActor : IActor
        where TImpl : Actor, TActor
    {
        this.ExplicitActors.Add(() =>
        {
            ActorRegistry.EnsureInitialized(typeof(TImpl).Assembly);
            var generated = ActorRegistry.FindByImplementation(typeof(TImpl));
            return generated is null
                ? new(typeof(TImpl), typeof(TImpl).FullName!, sp => ActivatorUtilities.CreateInstance<TImpl>(sp), [typeof(TActor)])
                : new(generated.ImplementationType, generated.Name, generated.Factory, [typeof(TActor)], generated.StateTypeInfo, generated.StateProviderName, generated.ImplicitStreams, generated.IsReentrant, generated.AutoSave, generated.MaxWorkers);
        });
        return this;
    }


    /// <summary>Registers an actor the generator cannot see, with a factory of your own.</summary>
    public ActorSystemOptions AddActor<TActor, TImpl>(
        Func<IServiceProvider, TImpl> factory,
        string? name = null,
        JsonTypeInfo? stateTypeInfo = null,
        string? stateProviderName = null
    )
        where TActor : IActor
        where TImpl : Actor, TActor
    {
        this.ExplicitActors.Add(() => new(typeof(TImpl), name ?? typeof(TImpl).FullName!, factory, [typeof(TActor)], stateTypeInfo, stateProviderName));
        return this;
    }


    /// <summary>
    /// Loads the actors generated into <paramref name="assembly"/>. Only needed when implementations live in a
    /// library nothing else in the app touches; actors in the app's own assembly are always found.
    /// </summary>
    public ActorSystemOptions AddAssembly(Assembly assembly)
    {
        ActorRegistry.EnsureInitialized(assembly);
        return this;
    }
}
