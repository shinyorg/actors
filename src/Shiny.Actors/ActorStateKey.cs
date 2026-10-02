namespace Shiny.Actors;


/// <summary>Identifies one piece of actor state: the registered actor name, the actor id, and the state's name.</summary>
public readonly record struct ActorStateKey(string ActorName, string ActorId, string StateName = ActorStateKey.DefaultStateName)
{
    /// <summary>The name of an <see cref="Actor{TState}"/>'s own state. Reserved - a named state cannot use it.</summary>
    public const string DefaultStateName = "state";

    public bool IsDefault => this.StateName == DefaultStateName;

    public override string ToString() => this.IsDefault ? $"{this.ActorName}/{this.ActorId}" : $"{this.ActorName}/{this.ActorId}#{this.StateName}";
}
