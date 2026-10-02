using System.Text.Json.Serialization.Metadata;

namespace Shiny.Actors;


/// <summary>
/// Where actor state lives. Every write and clear is conditional on an ETag, so two processes sharing a store
/// cannot silently overwrite each other. Serialization metadata is always handed in - never fall back to
/// reflection-based JSON.
/// </summary>
/// <remarks>
/// ETag rules: <c>null</c> means "nothing is stored". A write or clear whose ETag does not match what is stored
/// throws <see cref="ActorStateConflictException"/>.
/// </remarks>
public interface IActorStateProvider
{
    /// <summary><c>State</c> is null when nothing is stored (or it was cleared); the ETag may still be set for a cleared record.</summary>
    ValueTask<StoredState<TState>> ReadAsync<TState>(ActorStateKey key, JsonTypeInfo<TState> typeInfo, CancellationToken cancellationToken) where TState : class;

    /// <returns>The new ETag.</returns>
    ValueTask<string> WriteAsync<TState>(ActorStateKey key, TState state, JsonTypeInfo<TState> typeInfo, string? etag, CancellationToken cancellationToken) where TState : class;

    /// <returns>The new ETag - null when nothing remains, or a tombstone's version for stores that keep one.</returns>
    ValueTask<string?> ClearAsync(ActorStateKey key, string? etag, CancellationToken cancellationToken);
}


public readonly record struct StoredState<TState>(TState? State, string? ETag) where TState : class;


/// <summary>
/// The stored state changed since this actor read it - another process (or a second system on the same store) wrote it.
/// The actor is deactivated, so the next call reloads the current state.
/// </summary>
public sealed class ActorStateConflictException(ActorStateKey key, string? expectedETag, string? actualETag)
    : Exception($"State '{key}' was changed by another writer (expected ETag '{expectedETag ?? "<none>"}', found '{actualETag ?? "<none>"}'). The actor will reload it on its next call.")
{
    public ActorStateKey Key => key;
    public string? ExpectedETag => expectedETag;
    public string? ActualETag => actualETag;
}
