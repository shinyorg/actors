using Microsoft.Extensions.Logging;
using Shiny.Actors.Internals;

namespace Shiny.Actors;


/// <summary>
/// Base class for an actor implementation. One instance exists per id while it is active, and
/// it only ever runs one call at a time, so fields need no locking.
/// </summary>
public abstract class Actor
{
    ActorActivation? activation;


    protected Actor()
    {
        // the system constructs actors inside an activation, so Id and CreateState work from the constructor
        this.activation = ActorActivation.Constructing;
    }

    internal ActorActivation Activation => this.activation ?? throw new InvalidOperationException(
        "This actor has not been activated by an actor system. Actors are created for you - get one with IActorSystem.Get<T>(id)."
    );

    internal void Attach(ActorActivation value) => this.activation = value;


    /// <summary>The id this instance was activated for.</summary>
    public string Id => this.Activation.Id;

    /// <summary>The system this actor lives in - use it to call other actors and get streams.</summary>
    protected IActorSystem Actors => this.Activation.System;

    protected ILogger Logger => this.Activation.Logger;

    protected TimeProvider TimeProvider => this.Activation.System.TimeProvider;


    /// <summary>Runs before the first call is delivered, after state has been read.</summary>
    protected internal virtual ValueTask OnActivateAsync(CancellationToken cancellationToken) => default;

    /// <summary>Runs after the last call. Write anything that must survive here.</summary>
    protected internal virtual ValueTask OnDeactivateAsync(DeactivationReason reason, CancellationToken cancellationToken) => default;


    /// <summary>
    /// A named, persistent state, stored separately from any other. Call it from the constructor (or use an
    /// <see cref="ActorStateAttribute"/> constructor parameter); it is read before <see cref="OnActivateAsync"/>.
    /// </summary>
    /// <param name="provider">A named <see cref="IActorStateProvider"/>; the actor's (or the default) provider otherwise.</param>
    /// <param name="typeInfo">Only needed when no JsonSerializerContext in the app declares <typeparamref name="TState"/>.</param>
    protected IActorState<TState> CreateState<TState>(string name, string? provider = null, System.Text.Json.Serialization.Metadata.JsonTypeInfo<TState>? typeInfo = null)
        where TState : class, new()
        => ActorState.Create<TState>(name, typeInfo, provider);


    /// <summary>Deactivates this actor once the current call finishes.</summary>
    protected void DeactivateOnIdle() => this.Activation.RequestDeactivation();


    /// <summary>
    /// Runs <paramref name="callback"/> as a turn of this actor - never concurrently with a call.
    /// A tick that arrives while the previous one is still queued or running is skipped.
    /// Timers do not keep an actor active and are disposed when it deactivates.
    /// Must be called from inside the actor (e.g. <see cref="OnActivateAsync"/>).
    /// </summary>
    protected IDisposable RegisterTimer(Func<CancellationToken, ValueTask> callback, TimeSpan dueTime, TimeSpan period)
        => this.Activation.RegisterTimer(callback, dueTime, period);


    /// <summary>
    /// A persistent timer: it survives deactivation and restarts, and activates this actor when due. The actor
    /// class must implement <see cref="IRemindable"/>. Registering an existing name replaces it.
    /// </summary>
    /// <param name="period">Null fires once, then the reminder is removed.</param>
    /// <remarks>
    /// While the app runs, reminders fire on time. When it is not running, they fire the next time it starts or a
    /// background job runs them (Shiny.Actors.Jobs) - on a phone, that can be 15 minutes or more late.
    /// </remarks>
    /// <param name="notification">Shown by the OS when due - see <see cref="ReminderNotification"/>.</param>
    protected ValueTask RegisterReminderAsync(string name, TimeSpan dueTime, TimeSpan? period = null, ReminderNotification? notification = null, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (this.Activation.Registration.IsStatelessWorker)
            throw new InvalidOperationException($"'{this.GetType().Name}' is a [StatelessWorker] - several may run per id, so it cannot own reminders.");
        if (this is not IRemindable)
            throw new InvalidOperationException($"'{this.GetType().Name}' must implement IRemindable to receive reminders.");
        if (dueTime < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(dueTime), "dueTime cannot be negative.");
        if (period is { } p && p <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(period), "period must be positive, or null for a one-shot reminder.");

        var activation = this.Activation;
        var reminder = new ActorReminder(activation.Registration.Name, this.Id, name, activation.System.TimeProvider.GetUtcNow() + dueTime, period, notification);
        return activation.System.Reminders.RegisterAsync(reminder, cancellationToken);
    }


    /// <summary>Returns false when no reminder had that name.</summary>
    protected ValueTask<bool> UnregisterReminderAsync(string name, CancellationToken cancellationToken = default)
        => this.Activation.System.Reminders.UnregisterAsync(this.Activation.Registration.Name, this.Id, name, cancellationToken);


    protected ValueTask<IReadOnlyList<ActorReminder>> GetRemindersAsync(CancellationToken cancellationToken = default)
        => this.Activation.System.Reminders.GetAsync(this.Activation.Registration.Name, this.Id, cancellationToken);
}


/// <summary>
/// An actor with one persistent state - <see cref="State"/>. It is read before <see cref="Actor.OnActivateAsync"/> and
/// written when you call <see cref="WriteStateAsync"/> (or automatically, with <see cref="AutoSaveAttribute"/>).
/// Need more than one? Add named states with <see cref="ActorStateAttribute"/> or <see cref="Actor.CreateState{TState}"/>.
/// </summary>
/// <remarks>
/// The state type must be declared on a <c>JsonSerializerContext</c> in the same project
/// (<c>[JsonSerializable(typeof(TState))]</c>) - the generator finds it. Override
/// <see cref="StateTypeInfo"/> when the metadata lives elsewhere.
/// </remarks>
public abstract class Actor<TState> : Actor where TState : class, new()
{
    readonly ActorState<TState>? state;


    protected Actor()
    {
        if (ActorActivation.Constructing is { } activation)
            this.state = activation.AddState(new ActorState<TState>(activation, ActorStateKey.DefaultStateName, () => this.StateTypeInfo, null));
    }


    IActorState<TState> Persistent => this.state ?? throw new InvalidOperationException(
        "This actor was not created by an actor system, so it has no state. Get it with IActorSystem.Get<T>(id)."
    );


    public TState State
    {
        get => this.Persistent.State;
        protected set => this.Persistent.State = value;
    }

    /// <summary>True when stored state was found, or has been written by this activation.</summary>
    protected bool StateExists => this.Persistent.RecordExists;

    /// <summary>The version of what was last read or written.</summary>
    protected string? StateETag => this.Persistent.ETag;

    /// <summary>Supplies the JSON metadata for <typeparamref name="TState"/> by hand.</summary>
    protected virtual System.Text.Json.Serialization.Metadata.JsonTypeInfo<TState>? StateTypeInfo => null;


    /// <summary>Re-reads state from the provider, replacing <see cref="State"/>.</summary>
    protected ValueTask ReadStateAsync(CancellationToken cancellationToken = default) => this.Persistent.ReadStateAsync(cancellationToken);

    /// <exception cref="ActorStateConflictException">Another writer changed it since it was read; the actor deactivates and reloads on its next call.</exception>
    protected ValueTask WriteStateAsync(CancellationToken cancellationToken = default) => this.Persistent.WriteStateAsync(cancellationToken);

    /// <summary>Removes stored state and resets <see cref="State"/> to a new instance.</summary>
    protected ValueTask ClearStateAsync(CancellationToken cancellationToken = default) => this.Persistent.ClearStateAsync(cancellationToken);
}
