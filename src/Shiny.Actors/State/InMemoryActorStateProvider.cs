using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.Actors;


/// <summary>
/// The default provider. State survives deactivation but not the process. Values are stored serialized, so an
/// actor never shares an instance with what was stored. ETags are a per-key version counter.
/// </summary>
public sealed class InMemoryActorStateProvider : IActorStateProvider
{
    readonly Dictionary<ActorStateKey, (byte[] Json, long Version)> store = [];


    public ValueTask<StoredState<TState>> ReadAsync<TState>(ActorStateKey key, JsonTypeInfo<TState> typeInfo, CancellationToken cancellationToken) where TState : class
    {
        lock (this.store)
        {
            return new(this.store.TryGetValue(key, out var entry)
                ? new StoredState<TState>(JsonSerializer.Deserialize(entry.Json, typeInfo), ETag(entry.Version))
                : default);
        }
    }


    public ValueTask<string> WriteAsync<TState>(ActorStateKey key, TState state, JsonTypeInfo<TState> typeInfo, string? etag, CancellationToken cancellationToken) where TState : class
    {
        var json = JsonSerializer.SerializeToUtf8Bytes(state, typeInfo);
        lock (this.store)
        {
            var version = this.Check(key, etag) + 1;
            this.store[key] = (json, version);
            return new(ETag(version));
        }
    }


    public ValueTask<string?> ClearAsync(ActorStateKey key, string? etag, CancellationToken cancellationToken)
    {
        lock (this.store)
        {
            this.Check(key, etag);
            this.store.Remove(key);
            return new((string?)null);
        }
    }


    long Check(ActorStateKey key, string? etag)
    {
        var current = this.store.TryGetValue(key, out var entry) ? ETag(entry.Version) : null;
        if (current != etag)
            throw new ActorStateConflictException(key, etag, current);

        return current is null ? 0 : entry.Version;
    }


    static string ETag(long version) => version.ToString(CultureInfo.InvariantCulture);
}
