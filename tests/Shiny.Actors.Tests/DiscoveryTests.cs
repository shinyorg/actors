using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors.Discovery;
using Shiny.Net.Discovery;
using Shiny.Net.HttpServer;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class DiscoveryTests
{
    // real mDNS over a real LAN interface - filter out with --filter "Category!=Network" where there isn't one
    [Fact]
    [Trait("Category", "Network")]
    public async Task A_Published_Server_Is_Found_And_Its_Actors_Called()
    {
        var builder = HttpServer.CreateBuilder();
        builder.Configure((HttpServerOptions o) =>
        {
            o.Address = System.Net.IPAddress.Any; // advertised on the LAN, so it must listen on the LAN - the default is loopback only
            o.Port = 0;
        });
        builder.Services.AddLogging().AddSingleton(new Probe()).AddShinyActors(actors => actors.UseDiscovery());
        var server = builder.Build();
        await using var _ = server;
        server.MapActors(o => o.Expose<ICounter>());
        await server.StartAsync(Ct);

        var mdns = server.Services!.GetRequiredService<IMdnsManager>();
        var port = new Uri(server.ListenUrl!).Port;
        var name = "test-" + Guid.NewGuid().ToString("N")[..8];
        await using var publication = await mdns.PublishActorsAsync(name, port, cancellationToken: Ct);

        var peers = await mdns.FindActorPeersAsync(TimeSpan.FromSeconds(5), Ct);
        var peer = Assert.Single(peers, p => p.Name == publication.InstanceName);
        Assert.True(peer.IsAvailable);
        Assert.EndsWith("/actors/", peer.BaseAddress!.ToString());

        var remote = peer.Connect();
        Assert.Equal(3, await remote.Get<ICounter>("found").Increment(3));
    }
}
