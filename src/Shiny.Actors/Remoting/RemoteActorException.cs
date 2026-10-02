namespace Shiny.Actors.Remoting;


/// <summary>A remote call failed on the other side - or never reached an actor.</summary>
/// <param name="statusCode">The HTTP-style status: 400 bad arguments, 401/403 auth, 404 unknown actor or method, 409 deadlock, 500 the actor threw.</param>
/// <param name="error">A short code - for 500, the exception type when the server shares exception details.</param>
public sealed class RemoteActorException(int statusCode, string error, string message) : Exception(message)
{
    public int StatusCode => statusCode;
    public string Error => error;
}
