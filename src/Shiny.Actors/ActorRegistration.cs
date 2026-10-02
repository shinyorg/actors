using System.Text.Json.Serialization.Metadata;

namespace Shiny.Actors;


/// <summary>
/// How to create one actor implementation and which contracts it answers for.
/// The source generator builds these; you only build one by hand for an actor it cannot see.
/// </summary>
public sealed class ActorRegistration
{
    public ActorRegistration(
        Type implementationType,
        string name,
        Func<IServiceProvider, Actor> factory,
        IEnumerable<Type> interfaces,
        JsonTypeInfo? stateTypeInfo = null,
        string? stateProviderName = null,
        IEnumerable<Type>? implicitStreams = null,
        bool isReentrant = false,
        AutoSaveMode? autoSave = null,
        int maxWorkers = 0
    )
    {
        ArgumentNullException.ThrowIfNull(implementationType);
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        ArgumentNullException.ThrowIfNull(factory);

        this.ImplementationType = implementationType;
        this.Name = name;
        this.Factory = factory;
        this.Interfaces = [.. interfaces];
        this.StateTypeInfo = stateTypeInfo;
        this.StateProviderName = stateProviderName;
        this.ImplicitStreams = [.. implicitStreams ?? []];
        this.IsReentrant = isReentrant;
        this.AutoSave = autoSave;
        this.MaxWorkers = maxWorkers < 0 ? Environment.ProcessorCount : maxWorkers;
    }


    public Type ImplementationType { get; }

    /// <summary>Stable name - part of every state key.</summary>
    public string Name { get; }

    public Func<IServiceProvider, Actor> Factory { get; }

    /// <summary>The <see cref="IActor"/> contracts this implementation answers for.</summary>
    public IReadOnlyList<Type> Interfaces { get; }

    /// <summary>Metadata for the <c>TState</c> of an <see cref="Actor{TState}"/>.</summary>
    public JsonTypeInfo? StateTypeInfo { get; }

    /// <summary>A named <see cref="IActorStateProvider"/>, or null for the default.</summary>
    public string? StateProviderName { get; }

    /// <summary>Event types from <see cref="IActorStreamConsumer{T}"/> - delivered to the actor whose id is the stream key.</summary>
    public IReadOnlyList<Type> ImplicitStreams { get; }

    /// <summary>Calls interleave at await points - see <see cref="ReentrantAttribute"/>.</summary>
    public bool IsReentrant { get; }

    /// <summary>From <see cref="AutoSaveAttribute"/>; null uses <see cref="ActorSystemOptions.DefaultAutoSave"/>.</summary>
    public AutoSaveMode? AutoSave { get; }

    /// <summary>From <see cref="StatelessWorkerAttribute"/>: how many activations may share an id. 0 = an ordinary actor.</summary>
    public int MaxWorkers { get; }

    public bool IsStatelessWorker => this.MaxWorkers > 0;

    public override string ToString() => this.Name;
}
