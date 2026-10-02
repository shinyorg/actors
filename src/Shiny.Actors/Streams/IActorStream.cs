namespace Shiny.Actors;


/// <summary>
/// A typed, in-memory event stream identified by its event type and a key.
/// Delivery is in publish order per publisher and at most once - nothing is stored.
/// </summary>
public interface IActorStream<T>
{
    string Key { get; }

    /// <summary>
    /// Hands the event to every subscriber and returns once each has it queued; it does not wait
    /// for handlers to run. Implicit consumers (<see cref="IActorStreamConsumer{T}"/>) are activated as needed.
    /// </summary>
    ValueTask PublishAsync(T item, CancellationToken cancellationToken = default);

    /// <summary>
    /// Calls <paramref name="handler"/> for each event, one at a time. Called from inside an actor, each
    /// event is a turn of that actor - never concurrent with its other calls - and the subscription
    /// ends when the actor deactivates. Dispose to unsubscribe.
    /// </summary>
    IDisposable Subscribe(Func<T, CancellationToken, ValueTask> handler);

    /// <summary>
    /// Events published from now on, until <paramref name="cancellationToken"/> fires. Read remotely, this reconnects
    /// on its own - and, for a durable stream, resumes without missing anything.
    /// </summary>
    IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default);

    /// <summary>
    /// For a durable stream (<c>options.AddDurableStream&lt;T&gt;()</c>): every retained event after
    /// <paramref name="afterSequence"/>, then live events - no gap, no duplicates. Remember the last
    /// <see cref="ActorStreamEvent{T}.Sequence"/> you handled and pass it back to pick up where you left off.
    /// </summary>
    /// <exception cref="InvalidOperationException">The stream is not durable.</exception>
    IAsyncEnumerable<ActorStreamEvent<T>> ReadFromAsync(long afterSequence, CancellationToken cancellationToken = default);
}


/// <param name="Sequence">Position in a durable stream (1, 2, 3...); 0 for a stream that is not durable.</param>
public readonly record struct ActorStreamEvent<T>(long Sequence, T Item);


/// <summary>
/// An implicit subscription: every event published to <c>GetStream&lt;T&gt;(key)</c> is delivered to
/// the actor whose id is <c>key</c>, activating it if needed. Implement it on an actor class - the
/// generator registers it.
/// </summary>
public interface IActorStreamConsumer<T>
{
    ValueTask OnNextAsync(T item, CancellationToken cancellationToken);
}
