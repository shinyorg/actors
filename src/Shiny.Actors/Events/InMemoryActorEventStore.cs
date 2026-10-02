using System.Runtime.CompilerServices;

namespace Shiny.Actors;


/// <summary>The default event store: logs live as long as the process.</summary>
public sealed class InMemoryActorEventStore : IActorEventStore
{
    readonly Dictionary<ActorStateKey, Log> logs = [];


    public async IAsyncEnumerable<StoredEvent> ReadAsync(ActorStateKey key, long afterVersion, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        StoredEvent[] snapshot;
        lock (this.logs)
            snapshot = this.logs.TryGetValue(key, out var log) ? [.. log.Events.Where(e => e.Version > afterVersion)] : [];

        foreach (var e in snapshot)
        {
            cancellationToken.ThrowIfCancellationRequested();
            yield return e;
        }
        await Task.CompletedTask.ConfigureAwait(false);
    }


    public ValueTask<long> AppendAsync(ActorStateKey key, long expectedVersion, IReadOnlyList<ReadOnlyMemory<byte>> events, DateTimeOffset timestamp, CancellationToken cancellationToken)
    {
        lock (this.logs)
        {
            if (!this.logs.TryGetValue(key, out var log))
                this.logs[key] = log = new Log();

            if (log.Version != expectedVersion)
                throw new ActorStateConflictException(key, expectedVersion.ToString(), log.Version.ToString());

            foreach (var e in events)
                log.Events.Add(new StoredEvent(++log.Version, timestamp, e.ToArray()));

            return new(log.Version);
        }
    }


    public ValueTask<long> GetVersionAsync(ActorStateKey key, CancellationToken cancellationToken)
    {
        lock (this.logs)
            return new(this.logs.TryGetValue(key, out var log) ? log.Version : 0);
    }


    public ValueTask TrimAsync(ActorStateKey key, long throughVersion, CancellationToken cancellationToken)
    {
        lock (this.logs)
            if (this.logs.TryGetValue(key, out var log))
                log.Events.RemoveAll(e => e.Version <= throughVersion);

        return default;
    }


    sealed class Log
    {
        public long Version;
        public List<StoredEvent> Events { get; } = [];
    }
}
