using System.Collections.ObjectModel;
using Sample.Maui.Services;
using Shiny.Actors.Discovery;
using Shiny.Net.Discovery;

namespace Sample.Maui.Pages;


public partial class NearbyPage : ContentPage
{
	readonly ChatSharing sharing;
	readonly ChatConnection connection;
	readonly IMdnsManager mdns;
	readonly ObservableCollection<ActorPeer> peers = [];
	CancellationTokenSource? browsing;
	bool updatingSwitch;


	public NearbyPage(ChatSharing sharing, ChatConnection connection, IMdnsManager mdns)
	{
		InitializeComponent();
		this.sharing = sharing;
		this.connection = connection;
		this.mdns = mdns;
		this.Peers.ItemsSource = this.peers;
	}


	protected override void OnAppearing()
	{
		base.OnAppearing();
		this.Render();
		this.Browse();
	}


	protected override void OnDisappearing()
	{
		base.OnDisappearing();
		this.browsing?.Cancel();
	}


	// devices come and go - the list follows mDNS announcements as they happen
	void Browse()
	{
		this.browsing?.Cancel();
		this.browsing = new CancellationTokenSource();
		var token = this.browsing.Token;
		this.peers.Clear();

		_ = Task.Run(async () =>
		{
			try
			{
				await foreach (var peer in this.mdns.BrowseActorPeersAsync(token))
				{
					MainThread.BeginInvokeOnMainThread(() =>
					{
						var existing = this.peers.FirstOrDefault(p => p.Id == peer.Id);
						if (existing is not null)
							this.peers.Remove(existing);

						// list everyone but ourselves
						if (peer.IsAvailable && peer.Name != this.sharing.AdvertisedAs)
							this.peers.Add(peer);
					});
				}
			}
			catch (OperationCanceledException) { }
			catch (Exception ex)
			{
				MainThread.BeginInvokeOnMainThread(() => this.ConnectionLabel.Text = $"Can't look for devices: {ex.Message}");
			}
		});
	}


	void Render()
	{
		this.updatingSwitch = true;
		this.ShareSwitch.IsToggled = this.sharing.IsSharing;
		this.updatingSwitch = false;

		this.ShareStatus.Text = this.sharing.IsSharing
			? $"Sharing as \"{this.sharing.AdvertisedAs}\" at {this.sharing.Address}\nPairing code: {ChatSharing.PairingCode}"
			: $"Not sharing. Pairing code when you do: {ChatSharing.PairingCode}";

		this.ConnectionLabel.Text = this.connection.IsRemote
			? $"Connected to {this.connection.Target}"
			: "Using this device's rooms. Tap a device to join its rooms.";
		this.UseThisDevice.IsVisible = this.connection.IsRemote;
	}


	async void OnShareToggled(object? sender, ToggledEventArgs e)
	{
		if (this.updatingSwitch)
			return;

		try
		{
			if (e.Value)
				await this.sharing.StartAsync();
			else
				await this.sharing.StopAsync();
		}
		catch (Exception ex)
		{
			await this.DisplayAlertAsync("Couldn't change sharing", ex.Message, "OK");
		}
		this.Render();
	}


	async void OnPeerSelected(object? sender, SelectionChangedEventArgs e)
	{
		if (e.CurrentSelection.FirstOrDefault() is not ActorPeer peer)
			return;

		this.Peers.SelectedItem = null;
		var code = await this.DisplayPromptAsync($"Join {peer.Name}", "Enter the pairing code shown on that device.", "Join", "Cancel", maxLength: 4, keyboard: Keyboard.Numeric);
		if (string.IsNullOrWhiteSpace(code))
			return;

		try
		{
			await this.connection.ConnectAsync(peer, code);
			this.Render();
			await Shell.Current.GoToAsync("//rooms");
		}
		catch (Exception ex)
		{
			await this.DisplayAlertAsync("Couldn't connect", ex.Message, "OK");
		}
	}


	void OnUseThisDevice(object? sender, EventArgs e)
	{
		this.connection.UseThisDevice();
		this.Render();
	}
}
