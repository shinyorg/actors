using System.Collections.Concurrent;
using System.Text.Json;
using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors.DocumentDb;
using Shiny.Actors.Remoting;
using Shiny.DocumentDb;
using Shiny.DocumentDb.Sqlite;
using Shiny.Net.HttpServer;
using Shiny.Net.HttpServer.Security;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class BuilderTests : IDisposable
{
    readonly Stores stores = new();
    public void Dispose() => this.stores.Dispose();


    [Fact]
    public async Task Repeated_Calls_Add_To_One_Configuration()
    {
        var services = new ServiceCollection().AddSingleton(new Probe());
        services.AddShinyActors(actors => actors.AddDurableStream<Note>(retain: 7));                 // a library
        services.AddShinyActors(actors => actors.Configure(o => o.IdleTimeout = TimeSpan.FromMinutes(9))); // the app

        var system = services.BuildServiceProvider().GetRequiredService<ActorSystem>();
        Assert.Equal(TimeSpan.FromMinutes(9), system.Options.IdleTimeout);

        // the library's call took effect too: the stream is durable, so a published event can be replayed
        await system.GetStream<Note>("lib").PublishAsync(new Note("kept"), Ct);
        await using var replay = system.GetStream<Note>("lib").ReadFromAsync(0, Ct).GetAsyncEnumerator(Ct);
        Assert.True(await replay.MoveNextAsync());
        Assert.Equal("kept", replay.Current.Item.Text);
        Assert.Single(services, d => d.ServiceType == typeof(ActorSystem));
    }


    [Fact]
    public async Task Container_Built_Filters_And_Providers_Get_Their_Dependencies()
    {
        var provider = new ServiceCollection()
            .AddSingleton(new Probe())
            .AddSingleton<CallLog>()
            .AddShinyActors(actors => actors
                .AddCallFilter<LoggingFilter>()
                .UseStateProvider<CountingStateProvider>())
            .BuildServiceProvider();

        var system = provider.GetRequiredService<ActorSystem>();
        await system.Get<ILedger>("di").Add(3); // [AutoSave] - goes through the container-built provider

        Assert.Equal(["Add"], provider.GetRequiredService<CallLog>().Calls);
        Assert.Equal(1, provider.GetRequiredService<CallLog>().Writes);
    }


    [Fact]
    public async Task UseDocumentDb_Takes_The_Store_From_The_Container()
    {
        var dir = this.stores.NewLocation();
        var provider = new ServiceCollection()
            .AddSingleton(new Probe())
            .AddSingleton(Stores.NewDocumentStore(dir))
            .AddShinyActors(actors => actors.UseDocumentDb())
            .BuildServiceProvider();

        var system = provider.GetRequiredService<ActorSystem>();
        await system.Get<ILedger>("db").Add(5);

        var stored = await new DocumentDbActorStateProvider(provider.GetRequiredService<IDocumentStore>())
            .ReadAsync(new ActorStateKey("Shiny.Actors.Tests.LedgerActor", "db"), TestJson.Default.LedgerState, Ct);
        Assert.Equal(5, stored.State!.Total);
    }


    [Fact]
    public async Task ServeOverHttp_Stands_Up_An_Authorized_Actor_Server()
    {
        var services = new ServiceCollection()
            .AddLogging()
            .AddSingleton(new Probe())
            .AddShinyActors(actors => actors.ServeOverHttp(
                expose => expose.Expose<ICounter>(),
                http =>
                {
                    http.Configure((HttpServerOptions o) => o.Port = 0);
                    http.AddAuthentication().AddApiKey(o =>
                    {
                        o.HeaderName = "X-Api-Key";
                        o.AddKey("secret", "test");
                    });
                    http.AddAuthorization(_ => { });
                    http.Configure((HttpServer s) =>
                    {
                        s.UseAuthentication();
                        s.UseAuthorization();
                    });
                },
                autoStart: false,
                authorizationPolicies: []
            ));

        await using var provider = services.BuildServiceProvider();
        var server = provider.GetRequiredService<HttpServer>();
        await server.StartAsync(Ct);
        var root = new Uri(server.ListenUrl!.TrimEnd('/') + "/actors/");

        var anonymous = new RemoteActorSystem(new HttpClient { BaseAddress = root });
        Assert.Equal(401, (await Assert.ThrowsAsync<RemoteActorException>(() => anonymous.Get<ICounter>("h").GetCount())).StatusCode);

        var keyed = new RemoteActorSystem(new HttpClient { BaseAddress = root, DefaultRequestHeaders = { { "X-Api-Key", "secret" } } });
        Assert.Equal(2, await keyed.Get<ICounter>("h").Increment(2));
        await server.StopAsync(Ct);
    }


    public sealed class CallLog
    {
        public ConcurrentQueue<string> Calls { get; } = new();
        public int Writes;
    }

    public sealed class LoggingFilter(CallLog log) : IActorCallFilter
    {
        public async ValueTask InvokeAsync(ActorCallContext context, ActorCallDelegate next)
        {
            log.Calls.Enqueue(context.Method.Name);
            await next(context);
        }
    }

    public sealed class CountingStateProvider(CallLog log) : IActorStateProvider
    {
        readonly InMemoryActorStateProvider inner = new();

        public ValueTask<StoredState<TState>> ReadAsync<TState>(ActorStateKey key, System.Text.Json.Serialization.Metadata.JsonTypeInfo<TState> typeInfo, CancellationToken cancellationToken) where TState : class
            => this.inner.ReadAsync(key, typeInfo, cancellationToken);

        public ValueTask<string> WriteAsync<TState>(ActorStateKey key, TState state, System.Text.Json.Serialization.Metadata.JsonTypeInfo<TState> typeInfo, string? etag, CancellationToken cancellationToken) where TState : class
        {
            Interlocked.Increment(ref log.Writes);
            return this.inner.WriteAsync(key, state, typeInfo, etag, cancellationToken);
        }

        public ValueTask<string?> ClearAsync(ActorStateKey key, string? etag, CancellationToken cancellationToken)
            => this.inner.ClearAsync(key, etag, cancellationToken);
    }
}
