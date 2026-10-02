using System.Diagnostics;
using System.Text;
using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors;
using Shiny.Actors.Remoting;
using Shiny.Net.HttpServer.Sse;

namespace Shiny.Net.HttpServer;


public static class ActorEndpointExtensions
{
    /// <summary>
    /// Serves actors to <c>RemoteActorSystem</c> callers:
    /// <list type="bullet">
    /// <item><c>POST {prefix}/call/{actor}/{id}/{method}</c> - body is a JSON array of arguments</item>
    /// <item><c>POST {prefix}/streams/{type}/{key}</c> - publish one JSON event</item>
    /// <item><c>GET {prefix}/streams/{type}/{key}</c> - server-sent events</item>
    /// </list>
    /// The <see cref="ActorSystem"/> comes from the server's container (<c>services.AddActors()</c>).
    /// </summary>
    /// <exception cref="InvalidOperationException">Nothing was exposed, or an exposed type has no JSON metadata - found now rather than on the first call.</exception>
    public static ActorEndpoints MapActors(this HttpServer server, Action<ActorEndpointOptions> configure)
        => server.MapActors(null, configure);


    /// <summary>For a server built without a container - hand it the <see cref="ActorSystem"/> directly.</summary>
    public static ActorEndpoints MapActors(this HttpServer server, ActorSystem? actors, Action<ActorEndpointOptions> configure)
    {
        var options = new ActorEndpointOptions();
        configure(options);

        if (options.Contracts.Count == 0 && options.Streams.Count == 0)
            throw new InvalidOperationException("MapActors exposes nothing - call Expose<TActor>(), ExposeStream<T>() or ExposeAll().");

        var missing = options.Contracts.Values.SelectMany(x => x.GetTypesMissingJsonMetadata())
            .Concat(options.Streams.Values.Select(x => x.EventType).Where(t => !ActorJson.CanSerialize(t)))
            .Distinct()
            .Select(t => t.FullName)
            .ToList();

        if (missing.Count > 0)
            throw new InvalidOperationException(
                $"These types cross the wire but have no JSON metadata: {string.Join(", ", missing)}. " +
                "Add [JsonSerializable(typeof(...))] for each to a JsonSerializerContext in the project that declares them."
            );

        var prefix = "/" + options.Prefix.Trim('/');
        ActorDispatcher? dispatcher = null;
        ActorDispatcher Dispatcher() => dispatcher ??= new ActorDispatcher(
            actors
                ?? server.Services?.GetService<ActorSystem>()
                ?? throw new InvalidOperationException("No ActorSystem - call services.AddActors() on the server's container, or pass one to MapActors."),
            options.IncludeExceptionDetails
        );

        var routes = new List<RouteEndpointBuilder>
        {
            Route(server, "POST", $"{prefix}/call/{{actor}}/{{id}}/{{method}}", async ctx =>
            {
                if (!TryGetSegments(ctx, out var segments, "actor", "id", "method"))
                    return;

                if (!options.Contracts.TryGetValue(segments[0], out var contract))
                {
                    await WriteAsync(ctx, ActorInvocationResult.Failure(404, "ActorNotFound", $"No actor '{segments[0]}' is exposed here."));
                    return;
                }

                var body = await ReadBodyAsync(ctx);
                var requestContext = ActorWire.DecodeRequestContext(Header(ctx, ActorWire.RequestContextHeader));

                // continue the caller's trace: a server span parented to its traceparent
                ActivityContext.TryParse(Header(ctx, "traceparent"), Header(ctx, "tracestate"), out var parent);
                using var activity = ActorTelemetry.ActivitySource.StartActivity($"{contract.Name}/{segments[2]}", ActivityKind.Server, parent);

                var result = await Dispatcher().InvokeAsync(contract, segments[1], segments[2], body, ctx.RequestAborted, requestContext);
                if (result.StatusCode >= 500)
                    activity?.SetStatus(ActivityStatusCode.Error);

                await WriteAsync(ctx, result);
            })
        };

        if (options.Streams.Count > 0)
        {
            routes.Add(Route(server, "POST", $"{prefix}/streams/{{stream}}/{{key}}", async ctx =>
            {
                if (await FindStreamAsync(ctx, options) is not var (stream, key))
                    return;

                try
                {
                    await stream.PublishAsync(Dispatcher().System, key, await ReadBodyAsync(ctx), ctx.RequestAborted);
                    ctx.Response.StatusCode = 202;
                }
                catch (RemoteActorException ex)
                {
                    await WriteAsync(ctx, ActorInvocationResult.Failure(ex.StatusCode, ex.Error, ex.Message));
                }
            }));

            routes.Add(Route(server, "GET", $"{prefix}/streams/{{stream}}/{{key}}", async ctx =>
            {
                if (await FindStreamAsync(ctx, options) is not var (stream, key))
                    return;

                var system = Dispatcher().System;
                if (ctx.Request.Query.TryGetValue("durable", out _) && !stream.IsDurable(system))
                {
                    await WriteAsync(ctx, ActorInvocationResult.Failure(409, "StreamNotDurable", $"Streams of '{stream.Name}' are not durable here - there is nothing to replay."));
                    return;
                }

                // Last-Event-ID: a reconnecting client resumes after the last sequence it saw
                long? after = long.TryParse(Header(ctx, "Last-Event-ID"), System.Globalization.NumberStyles.None, System.Globalization.CultureInfo.InvariantCulture, out var id) ? id : null;
                await ctx.SendEventsAsync(async events =>
                {
                    try
                    {
                        await foreach (var e in stream.SubscribeAsync(system, key, after, events.Aborted))
                        {
                            var message = new ServerSentEvent
                            {
                                Data = Encoding.UTF8.GetString(e.Json),
                                Id = e.Sequence > 0 ? e.Sequence.ToString(System.Globalization.CultureInfo.InvariantCulture) : null
                            };
                            await events.SendAsync(message, events.Aborted);
                        }
                    }
                    catch (OperationCanceledException) when (events.Aborted.IsCancellationRequested) { }
                });
            }));
        }

        return new ActorEndpoints(routes);
    }


    static string? Header(HttpContext ctx, string name)
        => ctx.Request.Headers.TryGetValue(name, out var values) ? values.ToString() : null;


    static RouteEndpointBuilder Route(HttpServer server, string method, string pattern, RequestDelegate handler)
        => new RouteEndpointBuilder(server.MapRoute(method, pattern, handler)).ExcludeFromDescription();


    static async ValueTask<(ActorStreamEndpoint Stream, string Key)?> FindStreamAsync(HttpContext ctx, ActorEndpointOptions options)
    {
        if (!TryGetSegments(ctx, out var segments, "stream", "key"))
            return null;

        if (options.Streams.TryGetValue(segments[0], out var stream))
            return (stream, segments[1]);

        await WriteAsync(ctx, ActorInvocationResult.Failure(404, "StreamNotFound", $"No stream of '{segments[0]}' is exposed here."));
        return null;
    }


    static bool TryGetSegments(HttpContext ctx, out string[] values, params string[] names)
    {
        values = new string[names.Length];
        try
        {
            for (var i = 0; i < names.Length; i++)
                values[i] = ActorWire.DecodeSegment(ctx.Request.RouteValues[names[i]]!);
            return true;
        }
        catch (RemoteActorException ex)
        {
            // a malformed segment never reaches an actor - answer 400 with no body
            ctx.Response.StatusCode = ex.StatusCode;
            return false;
        }
    }


    static async ValueTask<ReadOnlyMemory<byte>> ReadBodyAsync(HttpContext ctx)
    {
        if (!ctx.Request.HasBody)
            return ReadOnlyMemory<byte>.Empty;

        using var buffer = new MemoryStream();
        await ctx.Request.Body.CopyToAsync(buffer, ctx.RequestAborted);
        return buffer.ToArray();
    }


    static async ValueTask WriteAsync(HttpContext ctx, ActorInvocationResult result)
    {
        ctx.Response.StatusCode = result.StatusCode;
        if (result.Body is { } body)
            await ctx.Response.WriteBytesAsync(body, "application/json", ctx.RequestAborted);
    }
}
