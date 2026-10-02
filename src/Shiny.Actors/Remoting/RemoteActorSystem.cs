using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace Shiny.Actors.Remoting;


/// <summary>
/// Actors in another process, through the same <see cref="IActorSystem"/> - code written against
/// <c>Get&lt;ICounter&gt;(id)</c> works unchanged against either.
/// </summary>
/// <example>
/// <code>
/// var actors = new RemoteActorSystem(new HttpClient { BaseAddress = new("http://192.168.1.20:8080/actors/") });
/// await actors.Get&lt;ICounter&gt;("bob").Increment();
/// </code>
/// </example>
public sealed class RemoteActorSystem : IActorSystem
{
    readonly IActorTransport transport;
    readonly ILogger logger;


    public RemoteActorSystem(HttpClient httpClient, ILoggerFactory? loggerFactory = null)
        : this(new HttpActorTransport(httpClient), loggerFactory) { }


    public RemoteActorSystem(IActorTransport transport, ILoggerFactory? loggerFactory = null)
    {
        this.transport = transport;
        this.logger = (loggerFactory ?? NullLoggerFactory.Instance).CreateLogger("Shiny.Actors.Remote");
    }


    public TActor Get<TActor>(string id) where TActor : IActor
    {
        ArgumentNullException.ThrowIfNull(id);
        var contract = typeof(TActor);
        var description = ActorRegistry.FindContract(contract);
        var proxy = ActorRegistry.FindRemoteProxy(contract);

        if (description is null || proxy is null)
        {
            ActorRegistry.EnsureInitialized(contract.Assembly);
            description = ActorRegistry.FindContract(contract);
            proxy = ActorRegistry.FindRemoteProxy(contract);
        }
        if (description is null || proxy is null)
            throw new InvalidOperationException(
                $"No remote proxy was generated for '{contract.FullName}'. Actor interfaces must be public or internal, non-generic, " +
                "and declared in a project that references Shiny.Actors."
            );

        return (TActor)proxy(new RemoteActorReference(this.transport, description, id));
    }


    public IActorStream<T> GetStream<T>(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        return new RemoteActorStream<T>(this.transport, key, this.logger);
    }
}


/// <summary>The address behind a generated remote proxy. Generated code calls this.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class RemoteActorReference
{
    readonly IActorTransport transport;

    internal RemoteActorReference(IActorTransport transport, ActorContract contract, string id)
    {
        this.transport = transport;
        this.Contract = contract;
        this.Id = id;
    }


    public string Id { get; }
    public ActorContract Contract { get; }


    public async Task<TResult> Invoke<TResult>(ActorMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        var json = await this.SendAsync(method, arguments, cancellationToken).ConfigureAwait(false);
        return json is null ? default! : JsonSerializer.Deserialize(json, ActorJson.GetTypeInfo<TResult>())!;
    }


    public async Task Invoke(ActorMethod method, object?[] arguments, CancellationToken cancellationToken)
        => await this.SendAsync(method, arguments, cancellationToken).ConfigureAwait(false);


    async ValueTask<byte[]?> SendAsync(ActorMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        using var activity = ActorTelemetry.StartCall(this.Contract.Name, method.Name, this.Contract.Name, this.Id, ActivityKind.Client, Activity.Current?.Context ?? default);
        try
        {
            var body = ActorWire.WriteArguments(method, arguments);
            return await this.transport.InvokeAsync(this.Contract, this.Id, method, body, ActorRequestContext.Current, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            activity.Failed(ex);
            throw;
        }
    }

    public override string ToString() => $"{this.Contract.Name}/{this.Id} (remote)";
}


/// <summary>Implemented by every generated remote proxy.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IRemoteActorProxy
{
    RemoteActorReference Reference { get; }
}


sealed class RemoteActorStream<T>(IActorTransport transport, string key, ILogger logger) : IActorStream<T>
{
    static readonly string Name = ActorWire.StreamName(typeof(T));
    static readonly TimeSpan MaxBackoff = TimeSpan.FromSeconds(30);

    public string Key => key;


    public ValueTask PublishAsync(T item, CancellationToken cancellationToken = default)
        => transport.PublishAsync(Name, key, JsonSerializer.SerializeToUtf8Bytes(item, ActorJson.GetTypeInfo<T>()), cancellationToken);


    /// <summary>Live events; reconnects with backoff, and a durable stream resumes from the last sequence it saw.</summary>
    public async IAsyncEnumerable<T> ReadAllAsync([EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        await foreach (var e in this.ReadAsync(null, false, cancellationToken).ConfigureAwait(false))
            yield return e.Item;
    }


    public IAsyncEnumerable<ActorStreamEvent<T>> ReadFromAsync(long afterSequence, CancellationToken cancellationToken = default)
        => this.ReadAsync(afterSequence, true, cancellationToken);


    async IAsyncEnumerable<ActorStreamEvent<T>> ReadAsync(long? afterSequence, bool requireDurable, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        var typeInfo = ActorJson.GetTypeInfo<T>();
        var resumeFrom = afterSequence;
        var delay = TimeSpan.FromSeconds(1);

        while (true)
        {
            var events = transport.SubscribeAsync(Name, key, resumeFrom, requireDurable, cancellationToken).GetAsyncEnumerator(cancellationToken);
            try
            {
                while (true)
                {
                    ActorStreamEvent<T> next;
                    try
                    {
                        if (!await events.MoveNextAsync().ConfigureAwait(false))
                            break; // the server ended the stream - reconnect

                        var e = events.Current;
                        if (e.Sequence > 0)
                            resumeFrom = e.Sequence;
                        next = new ActorStreamEvent<T>(e.Sequence, JsonSerializer.Deserialize(e.Json, typeInfo)!);
                        delay = TimeSpan.FromSeconds(1);
                    }
                    catch (Exception ex) when (IsTransient(ex) && !cancellationToken.IsCancellationRequested)
                    {
                        logger.LogWarning(ex, "Remote stream {Stream}/{Key} dropped - reconnecting in {Delay}", Name, key, delay);
                        break;
                    }
                    yield return next;
                }
            }
            finally
            {
                await events.DisposeAsync().ConfigureAwait(false);
            }

            await Task.Delay(delay, cancellationToken).ConfigureAwait(false);
            delay = TimeSpan.FromTicks(Math.Min(delay.Ticks * 2, MaxBackoff.Ticks));
        }
    }


    // the network or the server hiccupped - worth retrying. A 4xx means the request itself is wrong.
    static bool IsTransient(Exception ex) => ex switch
    {
        RemoteActorException remote => remote.StatusCode >= 500,
        HttpRequestException or IOException => true,
        OperationCanceledException => false,
        _ => false
    };


    public IDisposable Subscribe(Func<T, CancellationToken, ValueTask> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var cts = new CancellationTokenSource();

        using (ExecutionContext.SuppressFlow())
        {
            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in this.ReadAllAsync(cts.Token).ConfigureAwait(false))
                    {
                        try
                        {
                            await handler(item, cts.Token).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (!cts.IsCancellationRequested)
                        {
                            logger.LogError(ex, "Remote stream subscriber for {Stream}/{Key} failed", Name, key);
                        }
                    }
                }
                catch (OperationCanceledException) when (cts.IsCancellationRequested) { }
                catch (Exception ex)
                {
                    logger.LogError(ex, "Remote stream subscription {Stream}/{Key} ended", Name, key);
                }
            });
        }
        return new Unsubscriber(cts);
    }


    sealed class Unsubscriber(CancellationTokenSource cts) : IDisposable
    {
        public void Dispose()
        {
            if (!cts.IsCancellationRequested)
                cts.Cancel();
        }
    }
}
