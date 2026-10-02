namespace Shiny.Actors.Remoting;


/// <summary>Moves remote calls and stream events. <see cref="HttpActorTransport"/> is the one in the box.</summary>
public interface IActorTransport
{
    /// <summary>Returns the JSON result, or null for a method without one. Throws <see cref="RemoteActorException"/> on failure.</summary>
    /// <param name="requestContext">The caller's <see cref="ActorRequestContext"/> - send it along; empty when there is none.</param>
    ValueTask<byte[]?> InvokeAsync(ActorContract contract, string actorId, ActorMethod method, byte[] arguments, IReadOnlyDictionary<string, string> requestContext, CancellationToken cancellationToken);

    ValueTask PublishAsync(string streamName, string key, byte[] item, CancellationToken cancellationToken);

    /// <summary>
    /// Events as JSON with their sequence (0 when the stream is not durable). With <paramref name="afterSequence"/>, a durable
    /// stream replays everything after it first; <paramref name="requireDurable"/> makes a non-durable stream an error
    /// rather than live-only.
    /// </summary>
    IAsyncEnumerable<RemoteStreamEvent> SubscribeAsync(string streamName, string key, long? afterSequence, bool requireDurable, CancellationToken cancellationToken);
}


public readonly record struct RemoteStreamEvent(long Sequence, byte[] Json);
