using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;
using Shiny.Actors.Internals;
using Shiny.Actors.Remoting;

namespace Shiny.Actors.Streams;


sealed class ActorStream<T>(ActorStreamHub hub, string key) : IActorStream<T>
{
    public string Key => key;


    public ValueTask PublishAsync(T item, CancellationToken cancellationToken = default)
        => hub.PublishAsync(key, item, cancellationToken);


    public IDisposable Subscribe(Func<T, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return CallChain.Current?.Activation is { } activation
            ? hub.Add(key, new ActorSubscriber<T>(activation, handler))
            : hub.Add(key, new LoopSubscriber<T>(handler, hub.Logger));
    }


    public async IAsyncEnumerable<T> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var channel = Channel.CreateUnbounded<ActorStreamEvent<T>>(new UnboundedChannelOptions { SingleReader = true });
        using var _ = hub.Add(key, new ChannelSubscriber<T>(channel.Writer));

        await foreach (var e in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
            yield return e.Item;
    }


    public IAsyncEnumerable<ActorStreamEvent<T>> ReadFromAsync(long afterSequence, CancellationToken cancellationToken = default)
        => hub.ReadFromAsync<T>(key, afterSequence, cancellationToken);
}


sealed class ActorStreamHub(ActorSystem system, ILogger logger)
{
    readonly ConcurrentDictionary<(Type, string), object> topics = new();
    readonly ConcurrentDictionary<(Type, string), DurableLog> logs = new();

    public ILogger Logger => logger;


    public bool IsDurable(Type type) => system.Options.DurableStreams.ContainsKey(type);


    public async ValueTask PublishAsync<T>(string key, T item, CancellationToken cancellationToken)
    {
        ActorTelemetry.StreamEvents.Add(1);

        if (system.Options.DurableStreams.TryGetValue(typeof(T), out var retain))
        {
            // durable: the log assigns the sequence, and delivery happens in sequence order
            var log = this.logs.GetOrAdd((typeof(T), key), static _ => new DurableLog());
            await log.Gate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var sequence = await this.AppendAsync(log, key, item, retain, cancellationToken).ConfigureAwait(false);
                await this.DeliverAsync(key, new ActorStreamEvent<T>(sequence, item), cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                log.Gate.Release();
            }
        }
        else
        {
            await this.DeliverAsync(key, new ActorStreamEvent<T>(0, item), cancellationToken).ConfigureAwait(false);
        }
    }


    async ValueTask<long> AppendAsync<T>(DurableLog log, string key, T item, int retain, CancellationToken cancellationToken)
    {
        var streamKey = LogKey<T>(key);
        var json = JsonSerializer.SerializeToUtf8Bytes(item, ActorJson.GetTypeInfo<T>());
        var store = system.EventStore;

        while (true)
        {
            log.Version ??= await store.GetVersionAsync(streamKey, cancellationToken).ConfigureAwait(false);
            try
            {
                log.Version = await store.AppendAsync(streamKey, log.Version.Value, [json], system.TimeProvider.GetUtcNow(), cancellationToken).ConfigureAwait(false);
                break;
            }
            catch (ActorStateConflictException)
            {
                log.Version = null; // another process shares the store - re-read and go again
            }
        }

        // trim in steps rather than on every publish
        var version = log.Version.Value;
        if (version > retain && version % Math.Max(1, retain / 10) == 0)
            await store.TrimAsync(streamKey, version - retain, cancellationToken).ConfigureAwait(false);

        return version;
    }


    async ValueTask DeliverAsync<T>(string key, ActorStreamEvent<T> e, CancellationToken cancellationToken)
    {
        if (this.topics.TryGetValue((typeof(T), key), out var found))
            foreach (var subscriber in ((Topic<T>)found).Snapshot)
                await subscriber.DeliverAsync(e, cancellationToken).ConfigureAwait(false);

        foreach (var registration in system.GetImplicitConsumers(typeof(T)))
            await system.PostAsync(
                registration,
                key,
                new TellItem<IActorStreamConsumer<T>>((consumer, ct) => consumer.OnNextAsync(e.Item, ct)),
                cancellationToken
            ).ConfigureAwait(false);
    }


    public async IAsyncEnumerable<ActorStreamEvent<T>> ReadFromAsync<T>(string key, long afterSequence, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        if (!this.IsDurable(typeof(T)))
            throw new InvalidOperationException($"Streams of '{typeof(T).Name}' are not durable, so there is nothing to replay. Call options.AddDurableStream<{typeof(T).Name}>().");

        // subscribe before reading the log, so nothing published in between is lost; sequences drop the overlap
        var channel = Channel.CreateUnbounded<ActorStreamEvent<T>>(new UnboundedChannelOptions { SingleReader = true });
        using var _ = this.Add(key, new ChannelSubscriber<T>(channel.Writer));

        var last = afterSequence;
        var typeInfo = ActorJson.GetTypeInfo<T>();
        await foreach (var stored in system.EventStore.ReadAsync(LogKey<T>(key), afterSequence, cancellationToken).ConfigureAwait(false))
        {
            last = stored.Version;
            yield return new ActorStreamEvent<T>(stored.Version, JsonSerializer.Deserialize(stored.Json.Span, typeInfo)!);
        }

        await foreach (var live in channel.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            if (live.Sequence <= last)
                continue;

            last = live.Sequence;
            yield return live;
        }
    }


    public IDisposable Add<T>(string key, Subscriber<T> subscriber)
    {
        var topicKey = (typeof(T), key);
        while (true)
        {
            var topic = (Topic<T>)this.topics.GetOrAdd(topicKey, static _ => new Topic<T>());
            if (topic.TryAdd(subscriber))
            {
                subscriber.OnRemoved = () =>
                {
                    if (topic.Remove(subscriber))
                        this.topics.TryRemove(new KeyValuePair<(Type, string), object>(topicKey, topic));
                };
                return subscriber;
            }
            // lost a race with the last unsubscribe closing this topic - go round for a new one
        }
    }


    static ActorStateKey LogKey<T>(string key) => new("$stream:" + ActorWire.StreamName(typeof(T)), key, "events");


    sealed class DurableLog
    {
        public SemaphoreSlim Gate { get; } = new(1, 1);
        public long? Version { get; set; }
    }
}


sealed class Topic<T>
{
    Subscriber<T>[] subscribers = [];
    bool closed;

    public Subscriber<T>[] Snapshot => Volatile.Read(ref this.subscribers);


    public bool TryAdd(Subscriber<T> subscriber)
    {
        lock (this)
        {
            if (this.closed)
                return false;

            this.subscribers = [.. this.subscribers, subscriber];
            return true;
        }
    }


    /// <summary>Returns true when that was the last subscriber and the topic is now closed.</summary>
    public bool Remove(Subscriber<T> subscriber)
    {
        lock (this)
        {
            this.subscribers = [.. this.subscribers.Where(x => !ReferenceEquals(x, subscriber))];
            this.closed = this.subscribers.Length == 0;
            return this.closed;
        }
    }
}


abstract class Subscriber<T> : IDisposable
{
    int disposed;

    public Action? OnRemoved { get; set; }

    public abstract ValueTask DeliverAsync(ActorStreamEvent<T> e, CancellationToken cancellationToken);


    public void Dispose()
    {
        if (Interlocked.Exchange(ref this.disposed, 1) == 0)
        {
            this.OnRemoved?.Invoke();
            this.OnDisposed();
        }
    }

    protected virtual void OnDisposed() { }
}


/// <summary>Each event is a turn of the subscribing actor.</summary>
sealed class ActorSubscriber<T> : Subscriber<T>
{
    readonly ActorActivation activation;
    readonly Func<T, CancellationToken, ValueTask> handler;

    public ActorSubscriber(ActorActivation activation, Func<T, CancellationToken, ValueTask> handler)
    {
        this.activation = activation;
        this.handler = handler;
        activation.TrackSubscription(this);
    }


    public override async ValueTask DeliverAsync(ActorStreamEvent<T> e, CancellationToken cancellationToken)
        => await this.activation.TryPostAsync(new DelegateItem(ct => this.handler(e.Item, ct)), cancellationToken).ConfigureAwait(false);

    protected override void OnDisposed() => this.activation.UntrackSubscription(this);
}


/// <summary>A subscriber outside any actor: its own queue and loop, so a slow handler never stalls the publisher.</summary>
sealed class LoopSubscriber<T> : Subscriber<T>
{
    readonly Channel<T> channel = Channel.CreateUnbounded<T>(new UnboundedChannelOptions { SingleReader = true });
    readonly CancellationTokenSource cts = new();


    public LoopSubscriber(Func<T, CancellationToken, ValueTask> handler, ILogger logger)
    {
        using (ExecutionContext.SuppressFlow())
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in this.channel.Reader.ReadAllAsync(this.cts.Token).ConfigureAwait(false))
                    {
                        try
                        {
                            await handler(item, this.cts.Token).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (ex is not OperationCanceledException || !this.cts.IsCancellationRequested)
                        {
                            logger.LogError(ex, "Stream subscriber for {EventType} failed", typeof(T).Name);
                        }
                    }
                }
                catch (OperationCanceledException) { }
            });
        }
    }


    public override ValueTask DeliverAsync(ActorStreamEvent<T> e, CancellationToken cancellationToken)
    {
        this.channel.Writer.TryWrite(e.Item);
        return default;
    }


    protected override void OnDisposed()
    {
        this.channel.Writer.TryComplete();
        this.cts.Cancel();
    }
}


sealed class ChannelSubscriber<T>(ChannelWriter<ActorStreamEvent<T>> writer) : Subscriber<T>
{
    public override ValueTask DeliverAsync(ActorStreamEvent<T> e, CancellationToken cancellationToken)
    {
        writer.TryWrite(e);
        return default;
    }

    protected override void OnDisposed() => writer.TryComplete();
}
