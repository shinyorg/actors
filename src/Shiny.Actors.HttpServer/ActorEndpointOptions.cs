using Shiny.Actors;
using Shiny.Actors.Remoting;

namespace Shiny.Net.HttpServer;


/// <summary>
/// What <c>MapActors</c> serves. Nothing is exposed until you say so: an actor reachable over the network is an
/// API, and should be chosen like one.
/// </summary>
public sealed class ActorEndpointOptions
{
    internal Dictionary<string, ActorContract> Contracts { get; } = new(StringComparer.Ordinal);
    internal Dictionary<string, ActorStreamEndpoint> Streams { get; } = new(StringComparer.Ordinal);


    /// <summary>Routes live under this prefix. Default: <c>/actors</c>.</summary>
    public string Prefix { get; set; } = "/actors";

    /// <summary>Send the exception type and message to remote callers when an actor throws. Off by default; development only.</summary>
    public bool IncludeExceptionDetails { get; set; }


    public ActorEndpointOptions Expose<TActor>() where TActor : IActor
    {
        ActorRegistry.EnsureInitialized(typeof(TActor).Assembly);
        var contract = ActorRegistry.FindContract(typeof(TActor)) ?? throw new InvalidOperationException(
            $"No contract was generated for '{typeof(TActor).FullName}'. Is it an actor interface in a project that references Shiny.Actors?"
        );
        this.Contracts[contract.Name] = contract;
        return this;
    }


    /// <summary>Every actor interface the app knows - convenient on a trusted network, risky anywhere else.</summary>
    public ActorEndpointOptions ExposeAll()
    {
        foreach (var contract in ActorRegistry.Contracts)
            this.Contracts[contract.Name] = contract;
        return this;
    }


    /// <summary>Lets remote callers publish to and subscribe to streams of <typeparamref name="T"/> (any key).</summary>
    public ActorEndpointOptions ExposeStream<T>()
    {
        var endpoint = ActorStreamEndpoint.Create<T>();
        this.Streams[endpoint.Name] = endpoint;
        return this;
    }
}


/// <summary>The routes <c>MapActors</c> added - security applies to all of them at once.</summary>
public sealed class ActorEndpoints(IReadOnlyList<RouteEndpointBuilder> routes)
{
    public IReadOnlyList<RouteEndpointBuilder> Routes => routes;

    public ActorEndpoints RequireAuthorization(params string[] policies) => this.Each(x => x.RequireAuthorization(policies));
    public ActorEndpoints AllowAnonymous() => this.Each(x => x.AllowAnonymous());
    public ActorEndpoints RequireCors(string policyName) => this.Each(x => x.RequireCors(policyName));
    public ActorEndpoints RequireRateLimiting(string policyName) => this.Each(x => x.RequireRateLimiting(policyName));
    public ActorEndpoints RequireIpFilter(string policyName) => this.Each(x => x.RequireIpFilter(policyName));

    ActorEndpoints Each(Action<RouteEndpointBuilder> apply)
    {
        foreach (var route in routes)
            apply(route);
        return this;
    }
}
