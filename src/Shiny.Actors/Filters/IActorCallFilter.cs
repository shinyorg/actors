using Shiny.Actors.Remoting;

namespace Shiny.Actors;


/// <summary>
/// Runs around every call to an actor method - local or remote, ask or one-way - inside the actor's turn.
/// Log, validate, authorize, measure, translate exceptions, or short-circuit by setting
/// <see cref="ActorCallContext.Result"/> without calling <c>next</c>.
/// </summary>
/// <remarks>
/// Register with <c>options.AddCallFilter(...)</c> or as an <see cref="IActorCallFilter"/> service. An actor class
/// that implements this interface filters its own calls, innermost.
/// </remarks>
public interface IActorCallFilter
{
    ValueTask InvokeAsync(ActorCallContext context, ActorCallDelegate next);
}


public delegate ValueTask ActorCallDelegate(ActorCallContext context);


public sealed class ActorCallContext
{
    internal ActorCallContext(Actor actor, ActorMethod method, object?[] arguments, CancellationToken cancellationToken)
    {
        this.Actor = actor;
        this.Method = method;
        this.Arguments = arguments;
        this.CancellationToken = cancellationToken;
    }


    /// <summary>The actor instance handling the call.</summary>
    public Actor Actor { get; }

    public string ActorId => this.Actor.Id;

    /// <summary>The registered actor name.</summary>
    public string ActorName => this.Actor.Activation.Registration.Name;

    public ActorMethod Method { get; }

    /// <summary>The call's arguments in parameter order (without its CancellationToken). Replace an element to change what the actor receives.</summary>
    public object?[] Arguments { get; }

    /// <summary>The value going back to the caller - boxed. Set it to change or supply the result.</summary>
    public object? Result { get; set; }

    public CancellationToken CancellationToken { get; }
}
