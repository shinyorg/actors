using Sample.Maui.Actors;
using Sample.Maui.Services;
using Shiny;
using Shiny.Notifications;

namespace Sample.Maui.Pages;


public partial class RoomsPage : ContentPage
{
	readonly ChatConnection connection;
	readonly INotificationManager notifications;
	CancellationTokenSource? watching;
	static bool askedForNotifications;


	public RoomsPage(ChatConnection connection, INotificationManager notifications)
	{
		InitializeComponent();
		this.connection = connection;
		this.notifications = notifications;
	}


	protected override async void OnAppearing()
	{
		base.OnAppearing();
		this.connection.Changed += this.OnConnectionChanged;
		await this.RefreshAsync();
		this.Watch();

		if (!askedForNotifications)
		{
			askedForNotifications = true;
			await this.notifications.RequestAccess(AccessRequestFlags.TimeSensitivity);
		}
	}


	protected override void OnDisappearing()
	{
		base.OnDisappearing();
		this.connection.Changed -= this.OnConnectionChanged;
		this.watching?.Cancel();
	}


	void OnConnectionChanged() => MainThread.BeginInvokeOnMainThread(async () =>
	{
		await this.RefreshAsync();
		this.Watch();
	});


	// the directory publishes a change whenever a room is created or gets a message - locally or from another device
	void Watch()
	{
		this.watching?.Cancel();
		this.watching = new CancellationTokenSource();
		var token = this.watching.Token;
		var changes = this.connection.Actors.GetStream<DirectoryChanged>(RoomDirectoryActor.Id_);

		_ = Task.Run(async () =>
		{
			try
			{
				await foreach (var _ in changes.ReadAllAsync(token))
					await MainThread.InvokeOnMainThreadAsync(this.RefreshAsync);
			}
			catch (OperationCanceledException) { }
		});
	}


	async Task RefreshAsync()
	{
		this.TargetLabel.Text = this.connection.IsRemote ? $"Rooms on {this.connection.Target}" : "Rooms on this device";
		try
		{
			this.RoomList.ItemsSource = await this.connection.Actors.Get<IRoomDirectory>(RoomDirectoryActor.Id_).Rooms();
		}
		catch (Exception ex)
		{
			await this.DisplayAlertAsync("Couldn't load rooms", ex.Message, "OK");
		}
	}


	async void OnCreate(object? sender, EventArgs e)
	{
		var name = this.NewRoom.Text?.Trim();
		if (string.IsNullOrEmpty(name))
			return;

		try
		{
			var id = await this.connection.Actors.Get<IRoomDirectory>(RoomDirectoryActor.Id_).Create(name);
			this.NewRoom.Text = "";
			await Shell.Current.GoToAsync($"room?id={Uri.EscapeDataString(id)}");
		}
		catch (Exception ex)
		{
			await this.DisplayAlertAsync("Couldn't create the room", ex.Message, "OK");
		}
	}


	async void OnSelected(object? sender, SelectionChangedEventArgs e)
	{
		if (e.CurrentSelection.FirstOrDefault() is not RoomSummary room)
			return;

		this.RoomList.SelectedItem = null;
		await Shell.Current.GoToAsync($"room?id={Uri.EscapeDataString(room.Id)}");
	}
}
