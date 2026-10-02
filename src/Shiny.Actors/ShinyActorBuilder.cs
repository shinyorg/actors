using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Shiny.Actors;


/// <summary>
/// The one place Shiny.Actors is set up. Everything hangs off it - storage, streams, filters, and every add-on package
/// (DocumentDb, background reminders, notifications, discovery, HTTP) through extension methods on this builder.
/// </summary>
/// <example>
/// <code>
/// builder.Services.AddShinyActors(actors => actors
///     .UseDocumentDb(new SqliteDatabaseProvider($"Data Source={path}"))
///     .AddDurableStream&lt;ChatMessage&gt;()
///     .UseBackgroundReminders()
///     .UseReminderNotifications()
///     .Configure(o => o.IdleTimeout = TimeSpan.FromMinutes(2)));
/// </code>
/// </example>
public sealed class ShinyActorBuilder
{
    readonly ShinyActorConfiguration configuration;

    internal ShinyActorBuilder(IServiceCollection services, ShinyActorConfiguration configuration)
    {
        this.Services = services;
        this.configuration = configuration;
    }


    /// <summary>The container - for add-on packages registering what they need.</summary>
    public IServiceCollection Services { get; }


    /// <summary>Plain settings: idle timeout, mailbox capacity, default auto-save, time provider...</summary>
    public ShinyActorBuilder Configure(Action<ActorSystemOptions> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        this.configuration.Add((options, _) => configure(options));
        return this;
    }


    /// <summary>
    /// Settings that need something from the container, applied when the actor system is created - how add-ons hand
    /// over services that only exist once the container is built.
    /// </summary>
    public ShinyActorBuilder Configure(Action<ActorSystemOptions, IServiceProvider> configure)
    {
        ArgumentNullException.ThrowIfNull(configure);
        this.configuration.Add(configure);
        return this;
    }


    #region Storage

    /// <summary>State as JSON files, reminders in <c>reminders.json</c> and event logs under <c>events/</c>, all in <paramref name="rootDirectory"/>.</summary>
    public ShinyActorBuilder UseFileStorage(string rootDirectory)
        => this.Configure(o => o.UseFileStateProvider(rootDirectory));

    public ShinyActorBuilder UseStateProvider(IActorStateProvider provider)
        => this.Configure(o => o.StateProvider = provider);

    /// <summary>The default state provider, created by the container (so it can take dependencies).</summary>
    public ShinyActorBuilder UseStateProvider<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TProvider>()
        where TProvider : class, IActorStateProvider
    {
        this.Services.Replace(ServiceDescriptor.Singleton<IActorStateProvider, TProvider>());
        return this;
    }

    /// <summary>A provider for actors marked <c>[StateProvider("name")]</c> or states with <c>[ActorState(..., Provider = "name")]</c>.</summary>
    public ShinyActorBuilder AddStateProvider(string name, IActorStateProvider provider)
        => this.Configure(o => o.AddStateProvider(name, provider));

    public ShinyActorBuilder UseReminderStore(IActorReminderStore store)
        => this.Configure(o => o.ReminderStore = store);

    public ShinyActorBuilder UseEventStore(IActorEventStore store)
        => this.Configure(o => o.EventStore = store);

    #endregion


    #region Actors, streams, state shape

    /// <summary>Chooses <typeparamref name="TImpl"/> for <typeparamref name="TActor"/> - only needed when two classes implement it.</summary>
    public ShinyActorBuilder AddActor<TActor, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TImpl>()
        where TActor : IActor
        where TImpl : Actor, TActor
        => this.Configure(o => o.AddActor<TActor, TImpl>());

    /// <summary>Loads the actors generated into a library nothing else in the app touches.</summary>
    public ShinyActorBuilder AddAssembly(Assembly assembly)
        => this.Configure(o => o.AddAssembly(assembly));

    /// <summary>Keeps the last <paramref name="retain"/> events of every <typeparamref name="T"/> stream so subscribers can replay them.</summary>
    public ShinyActorBuilder AddDurableStream<T>(int retain = 1000)
        => this.Configure(o => o.AddDurableStream<T>(retain));

    /// <summary>Upgrades stored <typeparamref name="T"/> JSON from <paramref name="fromVersion"/> to the next version as it is read.</summary>
    public ShinyActorBuilder AddStateMigration<T>(int fromVersion, Action<JsonObject> migrate)
        => this.Configure(o => o.AddStateMigration<T>(fromVersion, migrate));

    /// <summary>Auto-save for every actor without an <see cref="AutoSaveAttribute"/>.</summary>
    public ShinyActorBuilder UseAutoSave(AutoSaveMode mode = AutoSaveMode.AfterEachCall)
        => this.Configure(o => o.DefaultAutoSave = mode);

    #endregion


    #region Pipeline

    public ShinyActorBuilder AddCallFilter(IActorCallFilter filter)
        => this.Configure(o => o.AddCallFilter(filter));

    /// <summary>A call filter created by the container, so it can take dependencies. Filters run in the order added.</summary>
    public ShinyActorBuilder AddCallFilter<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TFilter>()
        where TFilter : class, IActorCallFilter
    {
        this.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IActorCallFilter, TFilter>());
        return this;
    }

    public ShinyActorBuilder AddReminderObserver(IActorReminderObserver observer)
        => this.Configure(o => o.AddReminderObserver(observer));

    public ShinyActorBuilder AddReminderObserver<[DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TObserver>()
        where TObserver : class, IActorReminderObserver
    {
        this.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IActorReminderObserver, TObserver>());
        return this;
    }

    #endregion
}


/// <summary>Everything the builder collected - one per container, however many times <c>AddShinyActors</c> is called.</summary>
sealed class ShinyActorConfiguration
{
    readonly List<Action<ActorSystemOptions, IServiceProvider>> steps = [];

    public void Add(Action<ActorSystemOptions, IServiceProvider> step) => this.steps.Add(step);


    public ActorSystemOptions Build(IServiceProvider services)
    {
        var options = new ActorSystemOptions();
        foreach (var step in this.steps)
            step(options, services);
        return options;
    }
}
