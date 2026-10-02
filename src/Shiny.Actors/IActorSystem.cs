using System.Globalization;

namespace Shiny.Actors;


public interface IActorSystem
{
    /// <summary>
    /// Returns a reference to the actor with this id. Nothing is activated until the first call.
    /// References are cheap - get one whenever you need it rather than caching it.
    /// </summary>
    TActor Get<TActor>(string id) where TActor : IActor;

    /// <summary>
    /// Returns the typed event stream for this key. Streams are in-memory and need no setup.
    /// </summary>
    IActorStream<T> GetStream<T>(string key);
}


public static class ActorSystemExtensions
{
    public static TActor Get<TActor>(this IActorSystem system, Guid id) where TActor : IActor
        => system.Get<TActor>(id.ToString("N"));

    public static TActor Get<TActor>(this IActorSystem system, long id) where TActor : IActor
        => system.Get<TActor>(id.ToString(CultureInfo.InvariantCulture));


    /// <summary>
    /// The id of an actor, from either side of the proxy.
    /// </summary>
    public static string GetActorId(this IActor actor) => actor switch
    {
        IActorProxy proxy => proxy.Reference.Id,
        Remoting.IRemoteActorProxy remote => remote.Reference.Id,
        Actor instance => instance.Id,
        _ => throw new ArgumentException("Not an actor reference or an actor instance.", nameof(actor))
    };
}
