namespace Shiny.Actors;


/// <summary>
/// Thrown instead of hanging when a call would wait on an actor that is itself waiting
/// (directly or through other actors) on the caller. Actors are not reentrant.
/// Break the cycle, or make one of the calls <see cref="OneWayAttribute"/>.
/// </summary>
public sealed class ActorDeadlockException(string message) : InvalidOperationException(message);
