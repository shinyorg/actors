using System.Net;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using Shiny.Actors.Remoting;
using Shiny.Net.Discovery;

namespace Shiny.Actors.Discovery;


/// <summary>
/// Advertise and find actor servers on the local network over mDNS (Bonjour/Zeroconf), service type
/// <see cref="ServiceType"/>.
/// </summary>
/// <remarks>
/// Apple platforms: add <c>_shinyactors._tcp</c> to <c>NSBonjourServices</c> and set <c>NSLocalNetworkUsageDescription</c>
/// in Info.plist, or browsing silently finds nothing. No multicast entitlement is needed - mDNS goes through Bonjour.
/// </remarks>
public static class ActorDiscovery
{
    /// <summary>The DNS-SD service type actor servers advertise.</summary>
    public const string ServiceType = "_shinyactors._tcp";

    const string PathKey = "path";
    const string VersionKey = "v";
    const string WireVersion = "1";


    /// <summary>
    /// Advertises an actor server listening on <paramref name="port"/>. Dispose the result to stop. The OS may rename
    /// the instance to avoid a clash - read <see cref="IMdnsPublication.InstanceName"/> back.
    /// </summary>
    /// <remarks>
    /// Peers connect to this device's LAN address, so the server must listen on it: Shiny.Net.HttpServer binds loopback by
    /// default - set <c>Address = IPAddress.Any</c>. And put authentication in front of <c>MapActors</c>: anyone on the
    /// network can now find it.
    /// </remarks>
    /// <param name="instanceName">What other devices see - a device or user name.</param>
    /// <param name="prefix">The prefix <c>MapActors</c> was given.</param>
    public static Task<IMdnsPublication> PublishActorsAsync(this IMdnsManager mdns, string instanceName, int port, string prefix = "/actors", CancellationToken cancellationToken = default)
        => mdns.Publish(
            new MdnsServiceRegistration(instanceName, ServiceType, port)
            {
                TxtRecords = new Dictionary<string, string>
                {
                    [PathKey] = "/" + prefix.Trim('/'),
                    [VersionKey] = WireVersion
                }
            },
            cancellationToken
        );


    /// <summary>
    /// Actor servers appearing (<see cref="ActorPeer.IsAvailable"/>) and disappearing on the network. Never completes on its
    /// own - cancel it. A peer that re-announces is reported again; key on <see cref="ActorPeer.Id"/>.
    /// </summary>
    public static async IAsyncEnumerable<ActorPeer> BrowseActorPeersAsync(this IMdnsManager mdns, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var result in mdns.Browse(new MdnsBrowseConfig(ServiceType), cancellationToken).ConfigureAwait(false))
        {
            if (result.Status == MdnsBrowseStatus.Lost)
                yield return new ActorPeer(result.Service.FullName, result.Service.InstanceName, null);
            else if (ToPeer(result.Service) is { } peer)
                yield return peer;
        }
    }


    /// <summary>The actor servers that answer within <paramref name="scanTime"/> (default 5 seconds).</summary>
    public static async Task<IReadOnlyList<ActorPeer>> FindActorPeersAsync(this IMdnsManager mdns, TimeSpan? scanTime = null, CancellationToken cancellationToken = default)
    {
        var services = await mdns.BrowseOnce(ServiceType, scanTime ?? TimeSpan.FromSeconds(5), cancellationToken).ConfigureAwait(false);
        return [.. services.Select(ToPeer).OfType<ActorPeer>()];
    }


    static ActorPeer? ToPeer(MdnsService service)
    {
        if (!service.IsResolved)
            return null;

        // prefer IPv4: link-local IPv6 needs a scope id that a URL can't carry reliably
        var endpoint = service.GetEndPoint(AddressFamily.InterNetwork) ?? service.GetEndPoint();
        if (endpoint is null)
            return null;

        var host = endpoint.AddressFamily == AddressFamily.InterNetworkV6 ? $"[{endpoint.Address}]" : endpoint.Address.ToString();
        var path = service.GetTxt(PathKey) ?? "/actors";
        return new ActorPeer(service.FullName, service.InstanceName, new Uri($"http://{host}:{endpoint.Port}{path.TrimEnd('/')}/"));
    }
}


/// <summary>An actor server on the network.</summary>
/// <param name="Id">Stable for the peer - the mDNS full name.</param>
/// <param name="Name">What it advertised itself as.</param>
/// <param name="BaseAddress">Where its actors are; null when the peer has gone.</param>
public sealed record ActorPeer(string Id, string Name, Uri? BaseAddress)
{
    public bool IsAvailable => this.BaseAddress is not null;


    /// <summary>The peer's actors, through the same <see cref="IActorSystem"/> as local ones.</summary>
    /// <param name="configure">Add authentication headers and the like.</param>
    public RemoteActorSystem Connect(Action<HttpClient>? configure = null)
    {
        var http = new HttpClient { BaseAddress = this.BaseAddress ?? throw new InvalidOperationException($"Peer '{this.Name}' is no longer available.") };
        configure?.Invoke(http);
        return new RemoteActorSystem(http);
    }
}
