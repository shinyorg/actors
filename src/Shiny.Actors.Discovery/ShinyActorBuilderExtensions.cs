using Shiny;
using Shiny.Actors;
using Shiny.Net.Discovery;

namespace Microsoft.Extensions.DependencyInjection;


public static class DiscoveryShinyActorBuilderExtensions
{
    /// <summary>
    /// mDNS for finding actor servers on the network (and advertising this one) - inject <see cref="IMdnsManager"/> and use
    /// <c>PublishActorsAsync</c> / <c>BrowseActorPeersAsync</c>. Apple platforms need <c>_shinyactors._tcp</c> in
    /// <c>NSBonjourServices</c>.
    /// </summary>
    public static ShinyActorBuilder UseDiscovery(this ShinyActorBuilder builder)
    {
        if (!builder.Services.Any(x => x.ServiceType == typeof(IMdnsManager)))
            builder.Services.AddMdns();

        return builder;
    }
}
