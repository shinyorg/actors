namespace Shiny.Actors;


/// <summary>
/// Marks an interface as an actor contract. Every method must return <see cref="Task"/>,
/// <see cref="Task{TResult}"/>, <see cref="ValueTask"/> or <see cref="ValueTask{TResult}"/>.
/// The source generator writes the proxy - there is nothing to register by hand.
/// </summary>
public interface IActor;


/// <summary>
/// The call is queued and the caller does not wait for the actor to run it.
/// The method must return <see cref="Task"/> or <see cref="ValueTask"/>.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class OneWayAttribute : Attribute;


/// <summary>
/// Pins the name an actor is known by. On a class, the name is part of every state and reminder key; on an
/// interface, it is the name remote callers use. Pin it before shipping if the type may be renamed or moved.
/// Defaults to the full type name.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Interface, Inherited = false)]
public sealed class ActorNameAttribute(string name) : Attribute
{
    public string Name => name;
}


/// <summary>
/// Stores this actor's state through a named <see cref="IActorStateProvider"/> instead of the default one.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class StateProviderAttribute(string name) : Attribute
{
    public string Name => name;
}


/// <summary>
/// Lets calls interleave at <c>await</c> points instead of queueing behind each other. Code between awaits
/// still never runs in parallel, so fields stay safe - but state can change across an await. A reentrant
/// actor can call itself, and cycles through it are not treated as deadlocks.
/// </summary>
/// <remarks><c>ConfigureAwait(false)</c> inside a reentrant actor leaves its scheduler - don't use it there.</remarks>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class ReentrantAttribute : Attribute;


/// <summary>
/// This method may run while another call to the actor is waiting at an <c>await</c> - and others may run while it
/// waits. Code between awaits still never overlaps. For calls that must not queue behind slow work: status
/// queries, cancellation, health checks.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class AlwaysInterleaveAttribute : Attribute;


/// <summary>
/// The method does not change the actor's state, so several of them may run together - but never alongside a
/// call that is not read-only. The actor can't check this; marking a method that does change state is a bug.
/// </summary>
[AttributeUsage(AttributeTargets.Method)]
public sealed class ReadOnlyAttribute : Attribute;


/// <summary>
/// The actor holds no state of its own, so the system may run several activations of the same id in parallel -
/// calls go to an idle one, or a new one up to <see cref="MaxLocalWorkers"/>. For CPU-bound or I/O fan-out work
/// (resizing images, calling an API). Such an actor cannot have persistent state or reminders.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class StatelessWorkerAttribute : Attribute
{
    /// <summary>How many may run at once per id. 0 (default) = the processor count.</summary>
    public int MaxLocalWorkers { get; set; }
}


/// <summary>When an actor's states are written without calling <c>WriteStateAsync</c>.</summary>
public enum AutoSaveMode
{
    /// <summary>Only when the actor calls <c>WriteStateAsync</c>.</summary>
    None,

    /// <summary>States that changed are written when the actor deactivates (idle, requested, or shutdown).</summary>
    OnDeactivate,

    /// <summary>
    /// States that changed are written after every call, before the caller gets its result - a failed write fails the
    /// call. A call that throws is not saved. The safest choice on a phone, where the OS can kill the app at any moment.
    /// </summary>
    AfterEachCall
}


/// <summary>Saves this actor's states automatically - see <see cref="AutoSaveMode"/>. Changes are detected, so an unchanged state is not rewritten.</summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false)]
public sealed class AutoSaveAttribute(AutoSaveMode mode = AutoSaveMode.AfterEachCall) : Attribute
{
    public AutoSaveMode Mode => mode;
}


/// <summary>
/// Marks an <see cref="IActorState{TState}"/> constructor parameter as a named, persistent state of this actor.
/// Each is read before <c>OnActivateAsync</c> and stored separately.
/// </summary>
[AttributeUsage(AttributeTargets.Parameter)]
public sealed class ActorStateAttribute(string name) : Attribute
{
    public string Name => name;

    /// <summary>A named <see cref="IActorStateProvider"/> for this state; the actor's (or the default) provider otherwise.</summary>
    public string? Provider { get; set; }
}


public enum DeactivationReason
{
    /// <summary>No calls arrived within <see cref="ActorSystemOptions.IdleTimeout"/>.</summary>
    Idle,
    /// <summary>The actor called <c>DeactivateOnIdle()</c>, or someone called <see cref="ActorSystem.DeactivateAsync{TActor}"/>.</summary>
    Requested,
    /// <summary>The system is being disposed, or <see cref="ActorSystem.DeactivateAllAsync"/> was called.</summary>
    Shutdown
}
