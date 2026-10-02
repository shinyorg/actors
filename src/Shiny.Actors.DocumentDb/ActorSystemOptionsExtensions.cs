using System.Text.Json;
using Shiny.Actors.DocumentDb;
using Shiny.DocumentDb;

namespace Shiny.Actors;


public static class DocumentDbActorSystemOptionsExtensions
{
    /// <summary>
    /// Keeps actor state, reminders and event logs in a document store built for them on <paramref name="databaseProvider"/>.
    /// </summary>
    /// <example>
    /// <code>
    /// // a phone: one SQLite file
    /// o.UseDocumentDb(new SqliteDatabaseProvider($"Data Source={Path.Combine(FileSystem.AppDataDirectory, "actors.db")}"));
    /// </code>
    /// </example>
    public static ActorSystemOptions UseDocumentDb(this ActorSystemOptions options, IDatabaseProvider databaseProvider, Action<DocumentStoreOptions>? configure = null)
    {
        var storeOptions = new DocumentStoreOptions
        {
            DatabaseProvider = databaseProvider,
            JsonSerializerOptions = new JsonSerializerOptions { TypeInfoResolver = ActorDocumentsJsonContext.Default },
            UseReflectionFallback = false
        }.MapActorDocuments();

        configure?.Invoke(storeOptions);
        return options.UseDocumentDb(new DocumentStore(storeOptions));
    }


    /// <summary>
    /// Keeps actor state and reminders in an existing store - call <c>MapActorDocuments()</c>
    /// on its options when you build it.
    /// </summary>
    public static ActorSystemOptions UseDocumentDb(this ActorSystemOptions options, IDocumentStore store)
    {
        options.StateProvider = new DocumentDbActorStateProvider(store);
        options.ReminderStore = new DocumentDbActorReminderStore(store);
        options.EventStore = new DocumentDbActorEventStore(store);
        return options;
    }


    /// <summary>A named state provider on an existing store, for actors marked <c>[StateProvider("name")]</c>.</summary>
    public static ActorSystemOptions AddDocumentDbStateProvider(this ActorSystemOptions options, string name, IDocumentStore store)
        => options.AddStateProvider(name, new DocumentDbActorStateProvider(store));
}
