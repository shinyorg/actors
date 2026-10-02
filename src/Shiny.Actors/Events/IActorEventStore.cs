namespace Shiny.Actors;


/// <summary>
/// Append-only event logs - one per key - for event-sourced actors (<see cref="JournaledActor{TState, TEvent}"/>)
/// and durable streams. Events are opaque JSON here; typing happens above.
/// </summary>
/// <remarks>
/// Versions start at 1 and have no gaps. An append is conditional: it succeeds only if the log's current version is
/// <c>expectedVersion</c>, otherwise it throws <see cref="ActorStateConflictException"/> - so two writers can never both
/// append "event 5". A batch is all-or-nothing.
/// </remarks>
public interface IActorEventStore
{
    /// <summary>Events with a version greater than <paramref name="afterVersion"/>, oldest first.</summary>
    IAsyncEnumerable<StoredEvent> ReadAsync(ActorStateKey key, long afterVersion, CancellationToken cancellationToken);

    /// <returns>The log's new version - the version of the last event appended.</returns>
    ValueTask<long> AppendAsync(ActorStateKey key, long expectedVersion, IReadOnlyList<ReadOnlyMemory<byte>> events, DateTimeOffset timestamp, CancellationToken cancellationToken);

    /// <summary>The version of the newest event; 0 for an empty log.</summary>
    ValueTask<long> GetVersionAsync(ActorStateKey key, CancellationToken cancellationToken);

    /// <summary>Drops events up to and including <paramref name="throughVersion"/>. Versions are never reused.</summary>
    ValueTask TrimAsync(ActorStateKey key, long throughVersion, CancellationToken cancellationToken);
}


public readonly record struct StoredEvent(long Version, DateTimeOffset Timestamp, ReadOnlyMemory<byte> Json);
