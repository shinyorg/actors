using System.Text.Json;
using System.Text.Json.Serialization;
using Shiny.DocumentDb;

namespace Shiny.Actors.DocumentDb;


/// <summary>
/// One actor state. <see cref="Data"/> holds the state's own JSON; null marks a cleared state, kept as a tombstone so
/// a clear is version-checked like a write.
/// </summary>
public sealed class ActorStateDocument
{
    public string Id { get; set; } = "";
    public string ActorName { get; set; } = "";
    public string ActorId { get; set; } = "";
    public string StateName { get; set; } = "";

    /// <summary>Maintained by DocumentDb - every update is conditional on it. This is the state's ETag.</summary>
    public int Version { get; set; }

    public JsonElement? Data { get; set; }
}


public sealed class ActorReminderDocument
{
    public string Id { get; set; } = "";
    public string ActorName { get; set; } = "";
    public string ActorId { get; set; } = "";
    public string Name { get; set; } = "";
    public DateTimeOffset DueAt { get; set; }
    public TimeSpan? Period { get; set; }
    public string? NotificationTitle { get; set; }
    public string? NotificationMessage { get; set; }
    public string? NotificationChannel { get; set; }
}


/// <summary>One event in a log. Its id ends in the zero-padded version, so a second writer of the same version collides.</summary>
public sealed class ActorEventDocument
{
    public string Id { get; set; } = "";
    public string Stream { get; set; } = "";
    public long Version { get; set; }
    public DateTimeOffset Timestamp { get; set; }
    public JsonElement Data { get; set; }
}


/// <summary>Remembers a log's version, so trimming every event never lets a version be reused.</summary>
public sealed class ActorEventStreamDocument
{
    public string Id { get; set; } = "";
    public long Version { get; set; }
}


[JsonSerializable(typeof(ActorStateDocument))]
[JsonSerializable(typeof(ActorEventDocument))]
[JsonSerializable(typeof(ActorEventStreamDocument))]
[JsonSerializable(typeof(JsonElement))]
[JsonSerializable(typeof(ActorReminderDocument))]
public partial class ActorDocumentsJsonContext : JsonSerializerContext;


public static class ActorDocumentStoreOptionsExtensions
{
    /// <summary>
    /// Maps the actor documents' ids and the state version. Required on a store you build yourself and pass to
    /// <c>UseDocumentDb(IDocumentStore)</c> - without the version mapping, concurrent writers are not detected.
    /// </summary>
    public static TOptions MapActorDocuments<TOptions>(this TOptions options) where TOptions : IDocumentStoreOptions
    {
        options.Mappings.MapActorDocuments();
        return options;
    }


    /// <summary>The same mappings for any store's registry - IndexedDB's options, for one.</summary>
    public static DocumentMappingRegistry MapActorDocuments(this DocumentMappingRegistry mappings)
    {
        mappings.MapIdProperty<ActorStateDocument>(nameof(ActorStateDocument.Id));
        mappings.MapIdProperty<ActorReminderDocument>(nameof(ActorReminderDocument.Id));
        mappings.MapIdProperty<ActorEventDocument>(nameof(ActorEventDocument.Id));
        mappings.MapIdProperty<ActorEventStreamDocument>(nameof(ActorEventStreamDocument.Id));
        mappings.MapVersionProperty<ActorStateDocument>(nameof(ActorStateDocument.Version), d => d.Version, (d, v) => d.Version = v);
        return mappings;
    }
}
