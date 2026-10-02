using Sample.Maui.Actors;
using Shiny.Actors;
using Shiny.Actors.Discovery;
using Shiny.Actors.Remoting;

namespace Sample.Maui.Services;


/// <summary>
/// Which actors the app is talking to: this device's own, or another device's over the network. Pages only ever see
/// <see cref="IActorSystem"/>, so the same code works either way.
/// </summary>
public sealed class ChatConnection(ActorSystem local)
{
    public IActorSystem Actors { get; private set; } = local;
    public string Target { get; private set; } = "this device";
    public bool IsRemote => this.Actors is RemoteActorSystem;

    public event Action? Changed;


    /// <summary>Connects to a peer, proving the pairing code with one real call before switching over.</summary>
    public async Task ConnectAsync(ActorPeer peer, string pairingCode, CancellationToken cancellationToken = default)
    {
        var remote = peer.Connect(http =>
        {
            http.Timeout = TimeSpan.FromSeconds(15);
            http.DefaultRequestHeaders.Add(ChatSharing.ApiKeyHeader, pairingCode.Trim());
        });

        try
        {
            await remote.Get<IRoomDirectory>(RoomDirectoryActor.Id_).Rooms().WaitAsync(cancellationToken);
        }
        catch (RemoteActorException ex) when (ex.StatusCode == 401)
        {
            throw new InvalidOperationException("That pairing code isn't right.");
        }

        this.Actors = remote;
        this.Target = peer.Name;
        this.Changed?.Invoke();
    }


    public void UseThisDevice()
    {
        this.Actors = local;
        this.Target = "this device";
        this.Changed?.Invoke();
    }
}
