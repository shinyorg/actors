using System.Net;
using System.Net.NetworkInformation;
using System.Net.Sockets;
using Microsoft.Extensions.Logging;
using Shiny.Actors.Discovery;
using Shiny.Net.Discovery;
using Shiny.Net.HttpServer;

namespace Sample.Maui.Services;


/// <summary>
/// Serves this device's rooms to others on the network - behind a pairing code - and advertises them over mDNS so
/// nearby devices find them without typing an address.
/// </summary>
public sealed class ChatSharing(HttpServer server, IMdnsManager mdns, ILogger<ChatSharing> logger)
{
    public const string ApiKeyHeader = "X-Api-Key";

    IMdnsPublication? publication;


    /// <summary>A code for this install - other devices must enter it to connect.</summary>
    public static string PairingCode { get; } = Preferences.Default.Get("pairing-code", "") is { Length: 4 } stored
        ? stored
        : Remember(Random.Shared.Next(1000, 10000).ToString());

    public bool IsSharing => this.publication is not null;
    public string? AdvertisedAs => this.publication?.InstanceName;
    public string? Address { get; private set; }

    public event Action? Changed;


    public async Task StartAsync()
    {
        if (this.IsSharing)
            return;

        await server.StartAsync();
        var port = new Uri(server.ListenUrl!).Port;
        this.Address = $"{LocalAddress()}:{port}";

        try
        {
            this.publication = await mdns.PublishActorsAsync(DeviceInfo.Current.Name, port);
            logger.LogInformation("Sharing rooms as {Name} on {Address}", this.publication.InstanceName, this.Address);
        }
        catch
        {
            await server.StopAsync();
            this.Address = null;
            throw;
        }
        this.Changed?.Invoke();
    }


    public async Task StopAsync()
    {
        if (this.publication is { } published)
        {
            this.publication = null;
            await published.DisposeAsync();
        }
        await server.StopAsync();
        this.Address = null;
        this.Changed?.Invoke();
    }


    static string Remember(string code)
    {
        Preferences.Default.Set("pairing-code", code);
        return code;
    }


    static string LocalAddress()
        => NetworkInterface.GetAllNetworkInterfaces()
            .Where(n => n.OperationalStatus == OperationalStatus.Up && n.NetworkInterfaceType != NetworkInterfaceType.Loopback)
            .SelectMany(n => n.GetIPProperties().UnicastAddresses)
            .Select(a => a.Address)
            .FirstOrDefault(a => a.AddressFamily == AddressFamily.InterNetwork && !IPAddress.IsLoopback(a))
            ?.ToString() ?? "this device";
}
