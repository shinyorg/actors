using System.ComponentModel;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.Json.Serialization.Metadata;
using Shiny.Actors.Internals;
using Shiny.Actors.Remoting;

namespace Shiny.Actors;


/// <summary>
/// One named, persistent piece of an actor's state - the counterpart of Orleans' <c>IPersistentState&lt;T&gt;</c>.
/// Read before <c>OnActivateAsync</c>; written when you call <see cref="WriteStateAsync"/> (or by <see cref="AutoSaveAttribute"/>).
/// </summary>
public interface IActorState<TState> where TState : class, new()
{
    string Name { get; }

    TState State { get; set; }

    /// <summary>True when stored state was found, or has been written by this activation.</summary>
    bool RecordExists { get; }

    /// <summary>The version of what was last read or written - every write is conditional on it.</summary>
    string? ETag { get; }

    /// <summary>Re-reads from the provider, replacing <see cref="State"/>.</summary>
    ValueTask ReadStateAsync(CancellationToken cancellationToken = default);

    /// <exception cref="ActorStateConflictException">Someone else wrote it since it was read. The actor deactivates so the next call reloads.</exception>
    ValueTask WriteStateAsync(CancellationToken cancellationToken = default);

    /// <summary>Removes the stored state and resets <see cref="State"/> to a new instance.</summary>
    ValueTask ClearStateAsync(CancellationToken cancellationToken = default);
}


/// <summary>Creates states for generated actor factories. Use <c>CreateState</c> or <c>[ActorState]</c> instead.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class ActorState
{
    /// <summary>Must run while the actor is being constructed - from its factory or constructor.</summary>
    public static IActorState<TState> Create<TState>(string name, JsonTypeInfo? typeInfo = null, string? provider = null) where TState : class, new()
    {
        var activation = ActorActivation.Constructing ?? throw new InvalidOperationException(
            "Actor states can only be created while the actor is constructed - use an [ActorState] constructor parameter or call CreateState in the constructor."
        );
        if (name == ActorStateKey.DefaultStateName)
            throw new ArgumentException($"'{ActorStateKey.DefaultStateName}' is reserved for Actor<TState>'s own state.", nameof(name));

        return activation.AddState(new ActorState<TState>(activation, name, () => typeInfo as JsonTypeInfo<TState>, provider));
    }
}


/// <summary>What the activation needs from any state, whatever its type.</summary>
interface IActorStateBinding
{
    string Name { get; }
    ValueTask ReadStateAsync(CancellationToken cancellationToken);

    /// <summary>Writes only if the state differs from what was last read or written.</summary>
    ValueTask SaveIfChangedAsync(CancellationToken cancellationToken);
}


sealed class ActorState<TState> : IActorState<TState>, IActorStateBinding where TState : class, new()
{
    readonly ActorActivation activation;
    readonly Func<JsonTypeInfo<TState>?> explicitTypeInfo;
    readonly string? providerName;
    readonly SemaphoreSlim gate = new(1, 1); // a reentrant actor can interleave two writes
    JsonTypeInfo<TState>? typeInfo;
    IActorStateProvider? provider;
    byte[]? persisted;


    public ActorState(ActorActivation activation, string name, Func<JsonTypeInfo<TState>?> typeInfo, string? providerName)
    {
        this.activation = activation;
        this.Name = name;
        this.explicitTypeInfo = typeInfo;
        this.providerName = providerName;
    }


    public string Name { get; }
    public TState State { get; set; } = new();
    public bool RecordExists { get; private set; }
    public string? ETag { get; private set; }

    ActorStateKey Key => new(this.activation.Registration.Name, this.activation.Id, this.Name);

    StateSchema Schema => this.activation.System.Schemas.For(typeof(TState));

    IActorStateProvider Provider => this.provider ??= this.activation.System.GetStateProvider(this.providerName ?? this.activation.Registration.StateProviderName);

    JsonTypeInfo<TState> TypeInfo => this.typeInfo ??=
        this.explicitTypeInfo()
        ?? (this.Name == ActorStateKey.DefaultStateName ? this.activation.Registration.StateTypeInfo as JsonTypeInfo<TState> : null)
        ?? ActorJson.FindContextTypeInfo(typeof(TState)) as JsonTypeInfo<TState>
        ?? throw new InvalidOperationException(
            $"No JSON metadata for state '{this.Name}' ({typeof(TState).FullName}) of actor '{this.activation.Registration.Name}'. " +
            $"Add [JsonSerializable(typeof({typeof(TState).Name}))] to a JsonSerializerContext in the actor's project."
        );


    public async ValueTask ReadStateAsync(CancellationToken cancellationToken = default)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            TState? state;
            if (this.Schema.IsVersioned)
            {
                // read raw so older shapes can be migrated before they meet the current type
                var raw = await this.Provider.ReadAsync(this.Key, RemotingJsonContext.Default.JsonObject, cancellationToken).ConfigureAwait(false);
                this.ETag = raw.ETag;
                state = raw.State is null ? null : this.Schema.Upgrade(raw.State).Deserialize(this.TypeInfo);
            }
            else
            {
                var stored = await this.Provider.ReadAsync(this.Key, this.TypeInfo, cancellationToken).ConfigureAwait(false);
                this.ETag = stored.ETag;
                state = stored.State;
            }

            this.State = state ?? new();
            this.RecordExists = state is not null;
            this.persisted = state is null ? null : JsonSerializer.SerializeToUtf8Bytes(this.State, this.TypeInfo);
        }
        finally
        {
            this.gate.Release();
        }
    }


    public async ValueTask WriteStateAsync(CancellationToken cancellationToken = default)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await this.WriteCoreAsync(JsonSerializer.SerializeToUtf8Bytes(this.State, this.TypeInfo), cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.gate.Release();
        }
    }


    public async ValueTask SaveIfChangedAsync(CancellationToken cancellationToken)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var current = JsonSerializer.SerializeToUtf8Bytes(this.State, this.TypeInfo);
            if (this.persisted is null ? !this.IsNew(current) : !current.AsSpan().SequenceEqual(this.persisted))
                await this.WriteCoreAsync(current, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            this.gate.Release();
        }
    }


    public async ValueTask ClearStateAsync(CancellationToken cancellationToken = default)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            this.ETag = await this.Guard(() => this.Provider.ClearAsync(this.Key, this.ETag, cancellationToken)).ConfigureAwait(false);
            this.State = new();
            this.RecordExists = false;
            this.persisted = null;
        }
        finally
        {
            this.gate.Release();
        }
    }


    async ValueTask WriteCoreAsync(byte[] json, CancellationToken cancellationToken)
    {
        var started = System.Diagnostics.Stopwatch.GetTimestamp();
        this.ETag = await this.Guard(() => this.Schema.IsVersioned
            ? this.Provider.WriteAsync(this.Key, this.Schema.Stamp((JsonObject)JsonSerializer.SerializeToNode(this.State, this.TypeInfo)!), RemotingJsonContext.Default.JsonObject, this.ETag, cancellationToken)
            : this.Provider.WriteAsync(this.Key, this.State, this.TypeInfo, this.ETag, cancellationToken)
        ).ConfigureAwait(false);

        var tag = new KeyValuePair<string, object?>("actor", this.activation.Registration.Name);
        ActorTelemetry.StateWrites.Add(1, tag);
        ActorTelemetry.StateWriteDuration.Record(System.Diagnostics.Stopwatch.GetElapsedTime(started).TotalMilliseconds, tag);
        this.RecordExists = true;
        this.persisted = json;
    }


    // stale state must not stay in memory: deactivate, so the next call reads what is really stored.
    // The operation is invoked in here so a provider that throws synchronously is caught too.
    async ValueTask<T> Guard<T>(Func<ValueTask<T>> operation)
    {
        try
        {
            return await operation().ConfigureAwait(false);
        }
        catch (ActorStateConflictException)
        {
            ActorTelemetry.StateConflicts.Add(1, new KeyValuePair<string, object?>("actor", this.activation.Registration.Name));
            this.activation.RequestDeactivation(discardState: true);
            throw;
        }
    }


    // a never-stored state that still equals new TState() is not worth a write
    bool IsNew(byte[] current) => current.AsSpan().SequenceEqual(JsonSerializer.SerializeToUtf8Bytes(new TState(), this.TypeInfo));
}
