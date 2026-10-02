using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb;

namespace Shiny.Actors.DocumentDb;


/// <summary>
/// Event logs as one document per event. An append checks the log's version, then inserts the batch; a writer that
/// raced past the check collides on the event id and gets the conflict instead.
/// </summary>
public sealed class DocumentDbActorEventStore(IDocumentStore store) : IActorEventStore
{
    static JsonTypeInfo<ActorEventDocument> Info => ActorDocumentsJsonContext.Default.ActorEventDocument;
    static JsonTypeInfo<ActorEventStreamDocument> StreamInfo => ActorDocumentsJsonContext.Default.ActorEventStreamDocument;


    public async IAsyncEnumerable<StoredEvent> ReadAsync(ActorStateKey key, long afterVersion, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var stream = Stream(key);
        var events = store.Query(Info)
            .Where(d => d.Stream == stream && d.Version > afterVersion)
            .OrderBy(d => d.Version)
            .ToAsyncEnumerable(cancellationToken);

        await foreach (var e in events.ConfigureAwait(false))
            yield return new StoredEvent(e.Version, e.Timestamp, JsonSerializer.SerializeToUtf8Bytes(e.Data, ActorDocumentsJsonContext.Default.JsonElement));
    }


    public async ValueTask<long> AppendAsync(ActorStateKey key, long expectedVersion, IReadOnlyList<ReadOnlyMemory<byte>> events, DateTimeOffset timestamp, CancellationToken cancellationToken)
    {
        var stream = Stream(key);
        var current = await this.GetVersionAsync(key, cancellationToken).ConfigureAwait(false);
        if (current != expectedVersion)
            throw Conflict(key, expectedVersion, current);

        if (events.Count == 0)
            return current;

        var documents = events.Select((json, i) =>
        {
            var version = expectedVersion + i + 1;
            using var parsed = JsonDocument.Parse(json);
            return new ActorEventDocument
            {
                Id = EventId(stream, version),
                Stream = stream,
                Version = version,
                Timestamp = timestamp,
                Data = parsed.RootElement.Clone()
            };
        }).ToList();

        try
        {
            await store.BatchInsert(documents, Info, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            var now = await this.GetVersionAsync(key, cancellationToken).ConfigureAwait(false);
            if (now == expectedVersion)
                throw; // not a version collision - a real failure

            throw Conflict(key, expectedVersion, now);
        }

        var last = expectedVersion + events.Count;
        await store.ReplaceAsync(new ActorEventStreamDocument { Id = stream, Version = last }, stream, StreamInfo, cancellationToken).ConfigureAwait(false);
        return last;
    }


    public async ValueTask<long> GetVersionAsync(ActorStateKey key, CancellationToken cancellationToken)
    {
        var stream = Stream(key);
        var newest = await store.Query(Info)
            .Where(d => d.Stream == stream)
            .OrderByDescending(d => d.Version)
            .Paginate(0, 1)
            .FirstOrDefault(cancellationToken)
            .ConfigureAwait(false);

        // the marker covers a log trimmed to nothing; the newest event covers a marker that lags a crash
        var marker = await store.Get(stream, StreamInfo, cancellationToken).ConfigureAwait(false);
        return Math.Max(newest?.Version ?? 0, marker?.Version ?? 0);
    }


    public async ValueTask TrimAsync(ActorStateKey key, long throughVersion, CancellationToken cancellationToken)
    {
        var stream = Stream(key);
        var version = await this.GetVersionAsync(key, cancellationToken).ConfigureAwait(false);
        await store.ReplaceAsync(new ActorEventStreamDocument { Id = stream, Version = version }, stream, StreamInfo, cancellationToken).ConfigureAwait(false);
        await store.Query(Info).Where(d => d.Stream == stream && d.Version <= throughVersion).ExecuteDelete(cancellationToken).ConfigureAwait(false);
    }


    static string Stream(ActorStateKey key) => $"{key.ActorName}/{key.StateName}/{key.ActorId}";

    static string EventId(string stream, long version) => $"{stream}#{version.ToString("D12", CultureInfo.InvariantCulture)}";

    static ActorStateConflictException Conflict(ActorStateKey key, long expected, long actual)
        => new(key, expected.ToString(CultureInfo.InvariantCulture), actual.ToString(CultureInfo.InvariantCulture));
}
