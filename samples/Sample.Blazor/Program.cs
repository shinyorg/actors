using System.Text.Json;
using Microsoft.AspNetCore.Components.Web;
using Microsoft.AspNetCore.Components.WebAssembly.Hosting;
using Sample.Blazor;
using Shiny.Actors.DocumentDb;
using Shiny.DocumentDb;
using Shiny.DocumentDb.IndexedDb;

var builder = WebAssemblyHostBuilder.CreateDefault(args);
builder.RootComponents.Add<App>("#app");
builder.RootComponents.Add<HeadOutlet>("head::after");

// actor state, reminders and event logs live in the browser's IndexedDB
builder.Services.AddSingleton<IDocumentStore>(sp =>
{
    var options = new IndexedDbDocumentStoreOptions
    {
        DatabaseName = "shiny-actors-sample",
        JsonSerializerOptions = new JsonSerializerOptions { TypeInfoResolver = ActorDocumentsJsonContext.Default },
        UseReflectionFallback = false
    };
    return new IndexedDbDocumentStore(options.MapActorDocuments(), sp);
});
builder.Services.AddShinyActors(actors => actors
    .UseDocumentDb()                                            // the IndexedDB store registered above
    .Configure(o => o.IdleTimeout = TimeSpan.FromMinutes(2)));

await builder.Build().RunAsync();
