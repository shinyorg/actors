using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.Actors.Internals;
using Shiny.Actors.Remoting;

namespace Shiny.Actors;


/// <summary>
/// An event-sourced actor: its state is never stored directly - it is rebuilt from the events that produced it.
/// <see cref="RaiseEvent"/> applies an event at once; <see cref="ConfirmEventsAsync"/> appends the raised events to the
/// actor's log (or let <see cref="AutoSaveAttribute"/> confirm them after each call).
/// </summary>
/// <remarks>
/// <para>The full history stays readable through <see cref="ReadEventsAsync"/> - audit trails, undo, projections.</para>
/// <para>
/// Both <typeparamref name="TState"/> and <typeparamref name="TEvent"/> need JSON metadata on a JsonSerializerContext in
/// the project. For several kinds of event, make <typeparamref name="TEvent"/> a base type with
/// <c>[JsonPolymorphic]</c>/<c>[JsonDerivedType]</c> - that is source-generated, so it stays AOT-safe.
/// </para>
/// <para>Appends are conditional on the version this activation last saw, exactly like ETag-checked state.</para>
/// </remarks>
public abstract class JournaledActor<TState, TEvent> : Actor
    where TState : class, new()
    where TEvent : class
{
    readonly Journal<TState, TEvent>? journal;


    protected JournaledActor()
    {
        if (ActorActivation.Constructing is { } activation)
            this.journal = activation.AddState(new Journal<TState, TEvent>(activation, this));
    }


    Journal<TState, TEvent> Log => this.journal ?? throw new InvalidOperationException(
        "This actor was not created by an actor system, so it has no journal. Get it with IActorSystem.Get<T>(id)."
    );


    /// <summary>The current state: every confirmed event, plus any raised but not yet confirmed.</summary>
    protected TState State => this.Log.State;

    /// <summary>The version of the last confirmed event; 0 before the first.</summary>
    protected long Version => this.Log.Version;

    /// <summary>Raised but not yet appended to the log.</summary>
    protected IReadOnlyList<TEvent> UnconfirmedEvents => this.Log.Pending;


    /// <summary>Write a snapshot every this many events so activation doesn't replay the whole log. 0 = never.</summary>
    protected virtual int SnapshotEvery => 0;

    protected virtual JsonTypeInfo<TState>? StateTypeInfo => null;

    protected virtual JsonTypeInfo<TEvent>? EventTypeInfo => null;


    /// <summary>Change the state to reflect one event. Must be deterministic - it runs again on every replay.</summary>
    protected abstract void Apply(TState state, TEvent @event);


    protected void RaiseEvent(TEvent @event) => this.Log.Raise(@event);

    protected void RaiseEvents(IEnumerable<TEvent> events)
    {
        foreach (var e in events)
            this.Log.Raise(e);
    }


    /// <exception cref="ActorStateConflictException">Another writer appended since this activation read the log; it deactivates and replays on its next call.</exception>
    protected ValueTask ConfirmEventsAsync(CancellationToken cancellationToken = default) => this.Log.ConfirmAsync(cancellationToken);


    /// <summary>The confirmed history after <paramref name="afterVersion"/>.</summary>
    protected IAsyncEnumerable<JournalEntry<TEvent>> ReadEventsAsync(long afterVersion = 0, CancellationToken cancellationToken = default)
        => this.Log.ReadAsync(afterVersion, cancellationToken);


    internal void ApplyEvent(TState state, TEvent @event) => this.Apply(state, @event);
    internal int SnapshotInterval => this.SnapshotEvery;
    internal JsonTypeInfo<TState>? ExplicitStateTypeInfo => this.StateTypeInfo;
    internal JsonTypeInfo<TEvent>? ExplicitEventTypeInfo => this.EventTypeInfo;
}


public readonly record struct JournalEntry<TEvent>(long Version, DateTimeOffset Timestamp, TEvent Event);


sealed class Journal<TState, TEvent> : IActorStateBinding
    where TState : class, new()
    where TEvent : class
{
    const string SnapshotState = "journal-snapshot";

    readonly ActorActivation activation;
    readonly JournaledActor<TState, TEvent> owner;
    readonly SemaphoreSlim gate = new(1, 1);
    readonly List<TEvent> pending = [];
    JsonTypeInfo<TState>? stateInfo;
    JsonTypeInfo<TEvent>? eventInfo;
    string? snapshotETag;
    long snapshotVersion;


    public Journal(ActorActivation activation, JournaledActor<TState, TEvent> owner)
    {
        this.activation = activation;
        this.owner = owner;
    }


    public string Name => "journal";
    public TState State { get; private set; } = new();
    public long Version { get; private set; }
    public IReadOnlyList<TEvent> Pending => this.pending;

    ActorStateKey Key => new(this.activation.Registration.Name, this.activation.Id, this.Name);
    IActorEventStore Store => this.activation.System.EventStore;

    JsonTypeInfo<TState> StateInfo => this.stateInfo ??= this.owner.ExplicitStateTypeInfo
        ?? ActorJson.FindContextTypeInfo(typeof(TState)) as JsonTypeInfo<TState>
        ?? throw Missing(typeof(TState));

    JsonTypeInfo<TEvent> EventInfo => this.eventInfo ??= this.owner.ExplicitEventTypeInfo
        ?? ActorJson.FindContextTypeInfo(typeof(TEvent)) as JsonTypeInfo<TEvent>
        ?? throw Missing(typeof(TEvent));


    public void Raise(TEvent @event)
    {
        this.owner.ApplyEvent(this.State, @event);
        this.pending.Add(@event);
    }


    public async ValueTask ReadStateAsync(CancellationToken cancellationToken)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.pending.Clear();
            this.State = new();
            this.Version = 0;

            // a snapshot, if there is one, saves replaying everything before it
            var provider = this.activation.System.GetStateProvider(this.activation.Registration.StateProviderName);
            var snapshot = await provider.ReadAsync(this.Key with { StateName = SnapshotState }, RemotingJsonContext.Default.JsonObject, cancellationToken).ConfigureAwait(false);
            this.snapshotETag = snapshot.ETag;
            if (snapshot.State is { } stored && stored["version"]?.GetValue<long>() is { } version && stored["state"] is { } state)
            {
                this.State = state.Deserialize(this.StateInfo) ?? new();
                this.Version = this.snapshotVersion = version;
            }

            await foreach (var e in this.Store.ReadAsync(this.Key, this.Version, cancellationToken).ConfigureAwait(false))
            {
                var @event = this.ReadEvent(e.Json);
                this.owner.ApplyEvent(this.State, @event);
                this.Version = e.Version;
            }
        }
        finally
        {
            this.gate.Release();
        }
    }


    public ValueTask SaveIfChangedAsync(CancellationToken cancellationToken) => this.ConfirmAsync(cancellationToken);


    public async ValueTask ConfirmAsync(CancellationToken cancellationToken)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (this.pending.Count == 0)
                return;

            var json = this.pending.Select(this.WriteEvent).ToList();
            var started = Stopwatch.GetTimestamp();
            try
            {
                this.Version = await this.Store.AppendAsync(this.Key, this.Version, json, this.activation.System.TimeProvider.GetUtcNow(), cancellationToken).ConfigureAwait(false);
            }
            catch (ActorStateConflictException)
            {
                ActorTelemetry.StateConflicts.Add(1, new KeyValuePair<string, object?>("actor", this.activation.Registration.Name));
                this.activation.RequestDeactivation(discardState: true); // our state includes events that never made it
                throw;
            }
            ActorTelemetry.StateWrites.Add(json.Count, new KeyValuePair<string, object?>("actor", this.activation.Registration.Name));
            ActorTelemetry.StateWriteDuration.Record(Stopwatch.GetElapsedTime(started).TotalMilliseconds, new KeyValuePair<string, object?>("actor", this.activation.Registration.Name));
            this.pending.Clear();

            var every = this.owner.SnapshotInterval;
            if (every > 0 && this.Version - this.snapshotVersion >= every)
                await this.WriteSnapshotAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.gate.Release();
        }
    }


    async ValueTask WriteSnapshotAsync(CancellationToken cancellationToken)
    {
        var provider = this.activation.System.GetStateProvider(this.activation.Registration.StateProviderName);
        var snapshot = new JsonObject
        {
            ["version"] = this.Version,
            ["state"] = JsonSerializer.SerializeToNode(this.State, this.StateInfo)
        };
        try
        {
            this.snapshotETag = await provider.WriteAsync(this.Key with { StateName = SnapshotState }, snapshot, RemotingJsonContext.Default.JsonObject, this.snapshotETag, cancellationToken).ConfigureAwait(false);
            this.snapshotVersion = this.Version;
        }
        catch (ActorStateConflictException)
        {
            // a snapshot is only a shortcut - the log is the truth; the next one will catch up
            this.activation.Logger.LogSnapshotSkipped(this.activation);
        }
    }


    public async IAsyncEnumerable<JournalEntry<TEvent>> ReadAsync(long afterVersion, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        await foreach (var e in this.Store.ReadAsync(this.Key, afterVersion, cancellationToken).ConfigureAwait(false))
            yield return new JournalEntry<TEvent>(e.Version, e.Timestamp, this.ReadEvent(e.Json));
    }


    // events are history and never rewritten - a versioned event type is upcast as it is read
    StateSchema EventSchema => this.activation.System.Schemas.For(typeof(TEvent));

    TEvent ReadEvent(ReadOnlyMemory<byte> json)
    {
        if (!this.EventSchema.IsVersioned)
            return JsonSerializer.Deserialize(json.Span, this.EventInfo)!;

        var node = JsonNode.Parse(json.Span)!.AsObject();
        return this.EventSchema.Upgrade(node).Deserialize(this.EventInfo)!;
    }

    ReadOnlyMemory<byte> WriteEvent(TEvent @event)
    {
        if (!this.EventSchema.IsVersioned)
            return JsonSerializer.SerializeToUtf8Bytes(@event, this.EventInfo);

        var node = (JsonObject)JsonSerializer.SerializeToNode(@event, this.EventInfo)!;
        return JsonSerializer.SerializeToUtf8Bytes(this.EventSchema.Stamp(node), RemotingJsonContext.Default.JsonObject);
    }


    InvalidOperationException Missing(Type type) => new(
        $"No JSON metadata for '{type.FullName}' in event-sourced actor '{this.activation.Registration.Name}'. " +
        $"Add [JsonSerializable(typeof({type.Name}))] to a JsonSerializerContext in the actor's project."
    );
}


static class JournalLogging
{
    public static void LogSnapshotSkipped(this Microsoft.Extensions.Logging.ILogger logger, ActorActivation activation)
        => Microsoft.Extensions.Logging.LoggerExtensions.LogDebug(logger, "Actor {Actor} skipped a journal snapshot - another writer got there first", activation);
}
