using System.Runtime.CompilerServices;
using System.Text.Json;
using Shiny.Actors.Internals;

namespace Shiny.Actors.Remoting;


/// <summary>The outcome of a remote call, ready for any transport to send: a status code and an optional JSON body.</summary>
public readonly record struct ActorInvocationResult(int StatusCode, byte[]? Body)
{
    public static ActorInvocationResult Failure(int statusCode, string error, string message)
        => new(statusCode, JsonSerializer.SerializeToUtf8Bytes(new RemoteError(error, message), RemotingJsonContext.Default.RemoteError));
}


/// <summary>
/// Runs remote calls against a local <see cref="ActorSystem"/>. Transport-agnostic: Shiny.Actors.HttpServer
/// puts HTTP in front of it, and anything else that moves bytes can too.
/// </summary>
public sealed class ActorDispatcher(ActorSystem system, bool includeExceptionDetails = false)
{
    public ActorSystem System => system;


    /// <param name="requestContext">The caller's <see cref="ActorRequestContext"/>, carried across the wire.</param>
    public async ValueTask<ActorInvocationResult> InvokeAsync(
        ActorContract contract,
        string actorId,
        string methodKey,
        ReadOnlyMemory<byte> arguments,
        CancellationToken cancellationToken,
        IReadOnlyDictionary<string, string>? requestContext = null
    )
    {
        if (!contract.Methods.TryGetValue(methodKey, out var method))
            return ActorInvocationResult.Failure(404, "MethodNotFound", $"'{contract.Name}' has no method '{methodKey}'.");

        try
        {
            var args = ActorWire.ReadArguments(method, arguments);
            var registration = system.GetRegistration(contract.InterfaceType);
            ActorRequestContext.Restore(requestContext); // captured by the item below

            if (method.OneWay)
            {
                var tell = new TellItem<object>(async (actor, ct) => await method.InvokeAsync(actor, args, ct).ConfigureAwait(false)) { Method = method, Arguments = args };
                await system.PostAsync(registration, actorId, tell, cancellationToken).ConfigureAwait(false);
                return new(202, null);
            }

            var ask = new AskItem<object, object?>((actor, ct) => method.InvokeAsync(actor, args, ct), cancellationToken, null) { Method = method, Arguments = args };
            await system.PostAsync(registration, actorId, ask, cancellationToken).ConfigureAwait(false);
            var result = await ask.Task.ConfigureAwait(false);

            return method.ReturnType is null
                ? new(204, null)
                : new(200, JsonSerializer.SerializeToUtf8Bytes(result, ActorJson.GetTypeInfo(method.ReturnType)));
        }
        catch (RemoteActorException ex)
        {
            return ActorInvocationResult.Failure(ex.StatusCode, ex.Error, ex.Message);
        }
        catch (ActorDeadlockException ex)
        {
            return ActorInvocationResult.Failure(409, "Deadlock", ex.Message);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            return ActorInvocationResult.Failure(499, "Cancelled", "The caller went away.");
        }
        catch (Exception ex)
        {
            return includeExceptionDetails
                ? ActorInvocationResult.Failure(500, ex.GetType().FullName ?? ex.GetType().Name, ex.Message)
                : ActorInvocationResult.Failure(500, "ActorFailed", "The actor failed to handle the call.");
        }
    }
}


/// <summary>A stream event type exposed to remote callers - typed closures, so no reflection at call time.</summary>
public sealed class ActorStreamEndpoint
{
    readonly Func<ActorSystem, string, ReadOnlyMemory<byte>, CancellationToken, ValueTask> publish;
    readonly Func<ActorSystem, string, long?, CancellationToken, IAsyncEnumerable<RemoteStreamEvent>> subscribe;

    ActorStreamEndpoint(Type eventType, Func<ActorSystem, string, ReadOnlyMemory<byte>, CancellationToken, ValueTask> publish, Func<ActorSystem, string, long?, CancellationToken, IAsyncEnumerable<RemoteStreamEvent>> subscribe)
    {
        this.EventType = eventType;
        this.Name = ActorWire.StreamName(eventType);
        this.publish = publish;
        this.subscribe = subscribe;
    }


    public Type EventType { get; }
    public string Name { get; }


    public static ActorStreamEndpoint Create<T>() => new(
        typeof(T),
        (system, key, json, ct) =>
        {
            T item;
            try
            {
                item = JsonSerializer.Deserialize(json.Span, ActorJson.GetTypeInfo<T>())!;
            }
            catch (JsonException ex)
            {
                throw new RemoteActorException(400, "BadEvent", $"Bad '{typeof(T).Name}' event: {ex.Message}");
            }
            return system.GetStream<T>(key).PublishAsync(item, ct);
        },
        (system, key, after, ct) => Serialize(
            after is { } from && system.IsDurableStream(typeof(T))
                ? system.GetStream<T>(key).ReadFromAsync(from, ct)   // a reconnect: replay what was missed
                : system.ReadLiveAsync<T>(key, ct),
            ct
        )
    );


    public ValueTask PublishAsync(ActorSystem system, string key, ReadOnlyMemory<byte> json, CancellationToken cancellationToken)
        => this.publish(system, key, json, cancellationToken);

    /// <param name="afterSequence">The client's Last-Event-ID - a durable stream replays everything after it.</param>
    public IAsyncEnumerable<RemoteStreamEvent> SubscribeAsync(ActorSystem system, string key, long? afterSequence, CancellationToken cancellationToken)
        => this.subscribe(system, key, afterSequence, cancellationToken);

    public bool IsDurable(ActorSystem system) => system.IsDurableStream(this.EventType);


    static async IAsyncEnumerable<RemoteStreamEvent> Serialize<T>(IAsyncEnumerable<ActorStreamEvent<T>> items, [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var typeInfo = ActorJson.GetTypeInfo<T>();
        await foreach (var e in items.WithCancellation(cancellationToken).ConfigureAwait(false))
            yield return new RemoteStreamEvent(e.Sequence, JsonSerializer.SerializeToUtf8Bytes(e.Item, typeInfo));
    }
}
