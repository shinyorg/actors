using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Shiny.DocumentDb;

namespace Shiny.Actors.DocumentDb;


/// <summary>
/// Actor state as documents in any Shiny.DocumentDb store. The document version is the ETag: an insert expects
/// nothing to exist, and every update and clear is conditional on the version that was read.
/// </summary>
public sealed class DocumentDbActorStateProvider(IDocumentStore store) : IActorStateProvider
{
    static JsonTypeInfo<ActorStateDocument> Info => ActorDocumentsJsonContext.Default.ActorStateDocument;


    public async ValueTask<StoredState<TState>> ReadAsync<TState>(ActorStateKey key, JsonTypeInfo<TState> typeInfo, CancellationToken cancellationToken) where TState : class
    {
        var document = await store.Get(Id(key), Info, cancellationToken).ConfigureAwait(false);
        if (document is null)
            return default;

        var state = document.Data is { } data ? data.Deserialize(typeInfo) : null;
        return new(state, ETag(document.Version));
    }


    public async ValueTask<string> WriteAsync<TState>(ActorStateKey key, TState state, JsonTypeInfo<TState> typeInfo, string? etag, CancellationToken cancellationToken) where TState : class
    {
        var document = Document(key, JsonSerializer.SerializeToElement(state, typeInfo));
        await this.SaveAsync(key, document, etag, cancellationToken).ConfigureAwait(false);
        return ETag(document.Version);
    }


    public async ValueTask<string?> ClearAsync(ActorStateKey key, string? etag, CancellationToken cancellationToken)
    {
        if (etag is null)
        {
            // "nothing is stored" - true unless someone else wrote meanwhile
            var current = await store.Get(Id(key), Info, cancellationToken).ConfigureAwait(false);
            return current is null ? null : throw new ActorStateConflictException(key, null, ETag(current.Version));
        }

        var tombstone = Document(key, null);
        await this.SaveAsync(key, tombstone, etag, cancellationToken).ConfigureAwait(false);
        return ETag(tombstone.Version);
    }


    async ValueTask SaveAsync(ActorStateKey key, ActorStateDocument document, string? etag, CancellationToken cancellationToken)
    {
        if (etag is null)
        {
            try
            {
                await store.Insert(document, Info, cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (InvalidOperationException)
            {
                // a duplicate insert - report it as the conflict it is; anything else is a real failure
                var current = await store.Get(document.Id, Info, cancellationToken).ConfigureAwait(false);
                if (current is null)
                    throw;

                throw new ActorStateConflictException(key, null, ETag(current.Version));
            }
        }

        if (!int.TryParse(etag, NumberStyles.None, CultureInfo.InvariantCulture, out var version))
            throw new ActorStateConflictException(key, etag, null);

        document.Version = version;
        try
        {
            await store.Update(document, Info, cancellationToken).ConfigureAwait(false);
        }
        catch (ConcurrencyException ex)
        {
            throw new ActorStateConflictException(key, etag, ex.ActualVersion is { } actual ? ETag(actual) : null);
        }
    }


    static ActorStateDocument Document(ActorStateKey key, JsonElement? data) => new()
    {
        Id = Id(key),
        ActorName = key.ActorName,
        ActorId = key.ActorId,
        StateName = key.StateName,
        Data = data
    };


    internal static string Id(ActorStateKey key) => $"{key.ActorName}/{key.StateName}/{key.ActorId}";

    static string ETag(int version) => version.ToString(CultureInfo.InvariantCulture);
}


/// <summary>Reminders as documents - one per reminder, keyed by actor name, actor id and reminder name.</summary>
public sealed class DocumentDbActorReminderStore(IDocumentStore store) : IActorReminderStore
{
    static JsonTypeInfo<ActorReminderDocument> Info => ActorDocumentsJsonContext.Default.ActorReminderDocument;


    public async ValueTask<IReadOnlyList<ActorReminder>> GetAllAsync(CancellationToken cancellationToken)
    {
        var documents = await store.Query(Info).ToList(cancellationToken).ConfigureAwait(false);
        return [.. documents.Select(d => new ActorReminder(
            d.ActorName,
            d.ActorId,
            d.Name,
            d.DueAt,
            d.Period,
            d.NotificationTitle is null || d.NotificationMessage is null
                ? null
                : new ReminderNotification(d.NotificationTitle, d.NotificationMessage) { Channel = d.NotificationChannel }
        ))];
    }


    public async ValueTask SaveAsync(ActorReminder reminder, CancellationToken cancellationToken)
    {
        var document = new ActorReminderDocument
        {
            Id = Id(reminder),
            ActorName = reminder.ActorName,
            ActorId = reminder.ActorId,
            Name = reminder.Name,
            DueAt = reminder.DueAt,
            Period = reminder.Period,
            NotificationTitle = reminder.Notification?.Title,
            NotificationMessage = reminder.Notification?.Message,
            NotificationChannel = reminder.Notification?.Channel
        };
        // a whole-document replace, so a period changed to null does not survive as the old value (a merge upsert keeps
        // members the patch leaves null, and IndexedDB has no replacing upsert)
        await store.ReplaceAsync(document, document.Id, Info, cancellationToken).ConfigureAwait(false);
    }


    public async ValueTask RemoveAsync(ActorReminder reminder, CancellationToken cancellationToken)
        => await store.Remove<ActorReminderDocument>(Id(reminder), cancellationToken).ConfigureAwait(false);


    static string Id(ActorReminder reminder) => $"{reminder.ActorName}/{reminder.ActorId}/{reminder.Name}";
}


static class DocumentStoreReplaceExtensions
{
    /// <summary>
    /// Insert, or replace the whole document - on every provider. A losing race on the insert falls back to the update.
    /// </summary>
    public static async Task ReplaceAsync<T>(this IDocumentStore store, T document, string id, JsonTypeInfo<T> info, CancellationToken cancellationToken) where T : class
    {
        if (await store.Get(id, info, cancellationToken).ConfigureAwait(false) is not null)
        {
            await store.Update(document, info, cancellationToken).ConfigureAwait(false);
            return;
        }

        try
        {
            await store.Insert(document, info, cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidOperationException)
        {
            // someone inserted it first - replace theirs; anything else is a real failure
            if (await store.Get(id, info, cancellationToken).ConfigureAwait(false) is null)
                throw;

            await store.Update(document, info, cancellationToken).ConfigureAwait(false);
        }
    }
}
