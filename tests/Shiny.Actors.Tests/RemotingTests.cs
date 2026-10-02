using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors.Remoting;
using Shiny.Net.HttpServer;
using Shiny.Net.HttpServer.Security;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class RemotingTests
{
    static async Task<(ActorSystem Local, RemoteActorSystem Remote, HttpServer Server)> StartAsync(Action<ActorEndpointOptions> expose)
    {
        var builder = HttpServer.CreateBuilder();
        builder.Configure((HttpServerOptions o) => o.Port = 0);
        builder.Services.AddSingleton(new Probe()).AddShinyActors();

        var server = builder.Build();
        server.MapActors(expose);
        await server.StartAsync(Ct);

        var http = new HttpClient { BaseAddress = new Uri(server.ListenUrl!.TrimEnd('/') + "/actors/") };
        return (server.Services!.GetRequiredService<ActorSystem>(), new RemoteActorSystem(http), server);
    }


    [Fact]
    public async Task Calls_Reach_The_Server_Side_Actor()
    {
        var (local, remote, server) = await StartAsync(o => o.Expose<ICounter>());
        await using var _ = server;

        Assert.Equal(5, await remote.Get<ICounter>("c").Increment(5));
        Assert.Equal(6, await remote.Get<ICounter>("c").Increment());
        Assert.Equal(6, await local.Get<ICounter>("c").GetCount());
    }


    [Fact]
    public async Task Complex_Types_Nulls_And_Overloads_Round_Trip()
    {
        var (_, remote, server) = await StartAsync(o => o.Expose<IProfile>());
        await using var _ = server;
        var profile = remote.Get<IProfile>("p");

        Assert.Null(await profile.Find());
        await profile.Save(new Profile("Allan", 40, ["dev", "shiny"]));

        var found = await profile.Find();
        Assert.Equal("Allan", found!.Name);
        Assert.Equal(["dev", "shiny"], found.Tags);

        Assert.Equal("hi bob", await profile.Greet("bob"));
        Assert.Equal("hi bob hi bob", await profile.Greet("bob", 2));
    }


    [Fact]
    public async Task OneWay_Calls_Return_Before_The_Actor_Runs()
    {
        var (local, remote, server) = await StartAsync(o => o.Expose<ICounter>());
        await using var _ = server;

        for (var i = 0; i < 10; i++)
            await remote.Get<ICounter>("bumps").Bump();

        Assert.Equal(10, await local.Get<ICounter>("bumps").GetCount());
    }


    [Fact]
    public async Task Ids_Can_Hold_Any_Character()
    {
        var (local, remote, server) = await StartAsync(o => o.Expose<ICounter>());
        await using var _ = server;
        const string id = "a/b c?d#e%f";

        await remote.Get<ICounter>(id).Increment(3);
        Assert.Equal(3, await local.Get<ICounter>(id).GetCount());
    }


    [Fact]
    public async Task Failures_Hide_Details_Unless_Asked()
    {
        var (_, remote, server) = await StartAsync(o => o.Expose<ICounter>());
        await using var _ = server;

        var ex = await Assert.ThrowsAsync<RemoteActorException>(() => remote.Get<ICounter>("f").Fail("secret"));
        Assert.Equal(500, ex.StatusCode);
        Assert.Equal("ActorFailed", ex.Error);
        Assert.DoesNotContain("secret", ex.Message);

        var (_, detailed, server2) = await StartAsync(o =>
        {
            o.Expose<ICounter>();
            o.IncludeExceptionDetails = true;
        });
        await using var __ = server2;

        ex = await Assert.ThrowsAsync<RemoteActorException>(() => detailed.Get<ICounter>("f").Fail("secret"));
        Assert.Equal("System.InvalidOperationException", ex.Error);
        Assert.Equal("secret", ex.Message);
    }


    [Fact]
    public async Task Only_Exposed_Actors_Are_Reachable()
    {
        var (_, remote, server) = await StartAsync(o => o.Expose<ICounter>());
        await using var _ = server;

        var ex = await Assert.ThrowsAsync<RemoteActorException>(() => remote.Get<IFlaky>("x").Hello());
        Assert.Equal(404, ex.StatusCode);
        Assert.Equal("ActorNotFound", ex.Error);
    }


    [Fact]
    public async Task A_Deadlock_On_The_Server_Is_A_409()
    {
        var (_, remote, server) = await StartAsync(o => o.Expose<IPingPong>());
        await using var _ = server;

        var ex = await Assert.ThrowsAsync<RemoteActorException>(() => remote.Get<IPingPong>("me").CallSelf());
        Assert.Equal(409, ex.StatusCode);
    }


    [Fact]
    public async Task Cancelling_Stops_Waiting()
    {
        var (_, remote, server) = await StartAsync(o => o.Expose<ICounter>());
        await using var _ = server;
        using var cts = new CancellationTokenSource(100);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => remote.Get<ICounter>("slow").Slow(5000, cts.Token).AsTask());
    }


    [Fact]
    public void Types_Without_Json_Metadata_Fail_At_Startup()
    {
        var builder = HttpServer.CreateBuilder();
        builder.Services.AddShinyActors();
        var server = builder.Build();

        var ex = Assert.Throws<InvalidOperationException>(() => server.MapActors(o => o.Expose<IUnserializable>()));
        Assert.Contains(typeof(Probe).FullName!, ex.Message);
    }


    [Fact]
    public async Task Remote_Publishes_Reach_Local_Subscribers_And_Consumers()
    {
        var (local, remote, server) = await StartAsync(o => o.ExposeStream<Note>());
        await using var _ = server;

        await using var reader = local.GetStream<Note>("bob").ReadAllAsync(Ct).GetAsyncEnumerator(Ct);
        var next = reader.MoveNextAsync();

        await remote.GetStream<Note>("bob").PublishAsync(new Note("from afar"), Ct);

        Assert.True(await next);
        Assert.Equal("from afar", reader.Current.Text);
        Assert.Equal(["from afar"], await Eventually(() => local.Get<IInbox>("bob").Received(), r => r.Length == 1, "the implicit consumer gets it"));
    }


    [Fact]
    public async Task Local_Publishes_Reach_Remote_Subscribers()
    {
        var (local, remote, server) = await StartAsync(o => o.ExposeStream<Note>());
        await using var _ = server;

        var heard = new List<string>();
        using var sub = remote.GetStream<Note>("news").Subscribe((n, _) =>
        {
            lock (heard)
                heard.Add(n.Text);
            return default;
        });

        // the subscription is established asynchronously - publish until it lands
        await Eventually(async () =>
        {
            await local.GetStream<Note>("news").PublishAsync(new Note("ping"), Ct);
            lock (heard)
                return heard.Count;
        }, c => c > 0, "the remote subscriber hears the stream");
    }


    [Fact]
    public async Task Unexposed_Streams_Are_404()
    {
        var (_, remote, server) = await StartAsync(o => o.Expose<ICounter>());
        await using var _ = server;

        var ex = await Assert.ThrowsAsync<RemoteActorException>(() => remote.GetStream<Note>("x").PublishAsync(new Note("no"), Ct).AsTask());
        Assert.Equal(404, ex.StatusCode);
    }


    [Fact]
    public async Task RequireAuthorization_Protects_Calls_And_Streams()
    {
        var builder = HttpServer.CreateBuilder();
        builder.Configure((HttpServerOptions o) => o.Port = 0);
        builder.Services.AddSingleton(new Probe()).AddShinyActors();
        builder.AddAuthentication().AddApiKey(o =>
        {
            o.HeaderName = "X-Api-Key";
            o.AddKey("let-me-in", "phone");
        });
        builder.AddAuthorization(_ => { });

        var server = builder.Build();
        await using var _ = server;
        server.UseAuthentication();
        server.UseAuthorization();
        server.MapActors(o => o.Expose<ICounter>().ExposeStream<Note>()).RequireAuthorization();
        await server.StartAsync(Ct);

        var root = new Uri(server.ListenUrl!.TrimEnd('/') + "/actors/");
        var anonymous = new RemoteActorSystem(new HttpClient { BaseAddress = root });
        var keyed = new RemoteActorSystem(new HttpClient { BaseAddress = root, DefaultRequestHeaders = { { "X-Api-Key", "let-me-in" } } });

        var call = await Assert.ThrowsAsync<RemoteActorException>(() => anonymous.Get<ICounter>("x").GetCount());
        Assert.Equal(401, call.StatusCode);

        var publish = await Assert.ThrowsAsync<RemoteActorException>(() => anonymous.GetStream<Note>("x").PublishAsync(new Note("no"), Ct).AsTask());
        Assert.Equal(401, publish.StatusCode);

        Assert.Equal(1, await keyed.Get<ICounter>("x").Increment());
        await keyed.GetStream<Note>("x").PublishAsync(new Note("yes"), Ct);
    }
}
