namespace Sample.Maui;

public partial class AppShell : Shell
{
	public AppShell()
	{
		InitializeComponent();
		Routing.RegisterRoute("room", typeof(Pages.RoomPage));
	}
}
