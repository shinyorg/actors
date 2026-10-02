using Shiny.Actors;
using Shiny.Net.HttpServer;

namespace Microsoft.Extensions.DependencyInjection;


public static class HttpServerShinyActorBuilderExtensions
{
    /// <summary>
    /// Serves actors to other processes and devices over Shiny.Net.HttpServer - the server registration and
    /// <c>MapActors</c> in one call.
    /// </summary>
    /// <param name="expose">What is reachable - nothing is unless exposed.</param>
    /// <param name="configureServer">Address, port, authentication, middleware. Runs before the actor routes are mapped,
    /// so middleware registered here sits in front of them.</param>
    /// <param name="autoStart">False to start it yourself (a "share" toggle in a mobile app).</param>
    /// <param name="authorizationPolicies">Require authorization on every actor route: an empty array for the default
    /// policy, names for specific ones. Null leaves the routes open - only sensible on a trusted network.</param>
    /// <example><code>
    /// actors.ServeOverHttp(
    ///     expose => expose.Expose&lt;IChatRoom&gt;().ExposeStream&lt;ChatMessage&gt;(),
    ///     http =>
    ///     {
    ///         http.Configure((HttpServerOptions o) => o.Address = IPAddress.Any);
    ///         http.AddAuthentication().AddApiKey(o => o.AddKey(code, "peer"));
    ///         http.AddAuthorization(_ => { });
    ///         http.Configure((HttpServer s) => { s.UseAuthentication(); s.UseAuthorization(); });
    ///     },
    ///     authorizationPolicies: []);
    /// </code></example>
    public static ShinyActorBuilder ServeOverHttp(
        this ShinyActorBuilder builder,
        Action<ActorEndpointOptions> expose,
        Action<ShinyHttpServerBuilder>? configureServer = null,
        bool autoStart = true,
        string[]? authorizationPolicies = null
    )
    {
        ArgumentNullException.ThrowIfNull(expose);
        builder.Services.AddShinyHttpServer(http =>
        {
            configureServer?.Invoke(http);
            http.Configure((HttpServer server) =>
            {
                var endpoints = server.MapActors(expose);
                if (authorizationPolicies is not null)
                    endpoints.RequireAuthorization(authorizationPolicies);
            });
        }, autoStart);
        return builder;
    }
}
