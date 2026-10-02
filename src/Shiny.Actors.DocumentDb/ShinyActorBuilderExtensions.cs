using Shiny.Actors;
using Shiny.DocumentDb;

namespace Microsoft.Extensions.DependencyInjection;


public static class DocumentDbShinyActorBuilderExtensions
{
    /// <summary>
    /// Actor state, reminders and event logs in a document store built for them on <paramref name="databaseProvider"/>.
    /// </summary>
    /// <example><code>
    /// actors.UseDocumentDb(new SqliteDatabaseProvider($"Data Source={Path.Combine(FileSystem.AppDataDirectory, "actors.db")}"));
    /// </code></example>
    public static ShinyActorBuilder UseDocumentDb(this ShinyActorBuilder builder, IDatabaseProvider databaseProvider, Action<DocumentStoreOptions>? configure = null)
        => builder.Configure(o => o.UseDocumentDb(databaseProvider, configure));


    /// <summary>
    /// Actor storage in an existing store - call <c>MapActorDocuments()</c> on its options when you build it.
    /// </summary>
    public static ShinyActorBuilder UseDocumentDb(this ShinyActorBuilder builder, IDocumentStore store)
        => builder.Configure(o => o.UseDocumentDb(store));


    /// <summary>
    /// Actor storage in the <see cref="IDocumentStore"/> registered in the container - for stores that only exist there
    /// (Blazor's IndexedDB store needs the JS runtime). Map its documents with <c>MapActorDocuments()</c>.
    /// </summary>
    public static ShinyActorBuilder UseDocumentDb(this ShinyActorBuilder builder)
        => builder.Configure((o, services) => o.UseDocumentDb(services.GetRequiredService<IDocumentStore>()));


    /// <summary>A named state provider on a store, for actors marked <c>[StateProvider("name")]</c>.</summary>
    public static ShinyActorBuilder AddDocumentDbStateProvider(this ShinyActorBuilder builder, string name, IDocumentStore store)
        => builder.Configure(o => o.AddDocumentDbStateProvider(name, store));
}
