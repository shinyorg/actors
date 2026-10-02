using System.ComponentModel;
using Shiny.Actors.Internals;
using Shiny.Actors.Remoting;

namespace Shiny.Actors;


/// <summary>
/// The address behind a generated proxy. Generated code calls this; application code
/// uses the actor interface instead.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public sealed class ActorReference
{
    readonly ActorSystem system;

    internal ActorReference(ActorSystem system, ActorRegistration registration, string id)
    {
        this.system = system;
        this.Registration = registration;
        this.Id = id;
    }


    public string Id { get; }

    public ActorRegistration Registration { get; }


    public async Task<TResult> Ask<TActor, TResult>(ActorMethod method, object?[] arguments, Func<TActor, CancellationToken, ValueTask<TResult>> call, CancellationToken cancellationToken)
        where TActor : class
    {
        var item = new AskItem<TActor, TResult>(call, cancellationToken, CallChain.Current) { Method = method, Arguments = arguments };
        await this.system.PostAsync(this.Registration, this.Id, item, cancellationToken).ConfigureAwait(false);
        return await item.Task.ConfigureAwait(false);
    }


    public async Task Ask<TActor>(ActorMethod method, object?[] arguments, Func<TActor, CancellationToken, ValueTask> call, CancellationToken cancellationToken)
        where TActor : class
    {
        var item = new AskItem<TActor>(call, cancellationToken, CallChain.Current) { Method = method, Arguments = arguments };
        await this.system.PostAsync(this.Registration, this.Id, item, cancellationToken).ConfigureAwait(false);
        await item.Task.ConfigureAwait(false);
    }


    /// <summary>Queues the call and returns once it is in the mailbox.</summary>
    public ValueTask Tell<TActor>(ActorMethod method, object?[] arguments, Func<TActor, CancellationToken, ValueTask> call)
        where TActor : class
        => this.system.PostAsync(this.Registration, this.Id, new TellItem<TActor>(call) { Method = method, Arguments = arguments }, CancellationToken.None);

    public override string ToString() => $"{this.Registration.Name}/{this.Id}";
}


/// <summary>Implemented by every generated proxy.</summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public interface IActorProxy
{
    ActorReference Reference { get; }
}
