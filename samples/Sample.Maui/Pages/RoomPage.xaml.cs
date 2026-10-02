using System.Collections.ObjectModel;
using Sample.Maui.Actors;
using Sample.Maui.Services;

namespace Sample.Maui.Pages;


[QueryProperty(nameof(RoomId), "id")]
public partial class RoomPage : ContentPage
{
	readonly ChatConnection connection;
	readonly ObservableCollection<ChatMessage> messages = [];
	CancellationTokenSource? watching;


	public RoomPage(ChatConnection connection)
	{
		InitializeComponent();
		this.connection = connection;
		this.Messages.ItemsSource = this.messages;
	}


	public string RoomId { get; set; } = "general";

	IChatRoom Room => this.connection.Actors.Get<IChatRoom>(this.RoomId);
	static string Me => DeviceInfo.Current.Name;


	protected override async void OnAppearing()
	{
		base.OnAppearing();
		this.Title = $"#{this.RoomId}";

		// listen first, then load history - a message arriving in between shows up either way
		this.Watch();
		try
		{
			await this.Room.Join(Me);
			foreach (var message in await this.Room.Recent(50))
				this.Add(message);

			this.MembersLabel.Text = $"Members: {string.Join(", ", await this.Room.Members())}";
		}
		catch (Exception ex)
		{
			await this.DisplayAlertAsync("Couldn't open the room", ex.Message, "OK");
		}
	}


	protected override void OnDisappearing()
	{
		base.OnDisappearing();
		this.watching?.Cancel();
	}


	void Watch()
	{
		this.watching?.Cancel();
		this.watching = new CancellationTokenSource();
		var token = this.watching.Token;
		var stream = this.connection.Actors.GetStream<ChatMessage>(this.RoomId);

		_ = Task.Run(async () =>
		{
			try
			{
				// remote, this reconnects on its own - and the stream is durable, so nothing is missed meanwhile
				await foreach (var message in stream.ReadAllAsync(token))
					MainThread.BeginInvokeOnMainThread(() => this.Add(message));
			}
			catch (OperationCanceledException) { }
		});
	}


	void Add(ChatMessage message)
	{
		if (this.messages.Contains(message))
			return; // seen in both the history and the live stream
		this.messages.Add(message);
	}


	async void OnSend(object? sender, EventArgs e)
	{
		var text = this.MessageEntry.Text?.Trim();
		if (string.IsNullOrEmpty(text))
			return;

		try
		{
			this.MessageEntry.Text = "";
			await this.Room.Post(Me, text);
		}
		catch (Exception ex)
		{
			this.MessageEntry.Text = text;
			await this.DisplayAlertAsync("Not sent", ex.Message, "OK");
		}
	}


	async void OnRemind(object? sender, EventArgs e)
	{
		try
		{
			await this.Room.RemindIn(Me, 1);
			await this.DisplayAlertAsync("Reminder set", "You'll get a notification in a minute - even if you close the app.", "OK");
		}
		catch (Exception ex)
		{
			await this.DisplayAlertAsync("Couldn't set a reminder", ex.Message, "OK");
		}
	}
}
