using System.Net;
using Microsoft.Extensions.Logging;
using Sample.Maui.Actors;
using Sample.Maui.Pages;
using Sample.Maui.Services;
using Shiny;
using Shiny.Actors;
using Shiny.DocumentDb.Sqlite;
using Shiny.Net.HttpServer;
using Shiny.Net.HttpServer.Security;
#if DEBUG
using Microsoft.Maui.DevFlow.Agent;
#endif

namespace Sample.Maui;

public static class MauiProgram
{
	public static MauiApp CreateMauiApp()
	{
		var builder = MauiApp.CreateBuilder();
		builder
			.UseMauiApp<App>()
			.UseShiny()
			.ConfigureFonts(fonts =>
			{
				fonts.AddFont("OpenSans-Regular.ttf", "OpenSansRegular");
				fonts.AddFont("OpenSans-Semibold.ttf", "OpenSansSemibold");
			});

		var database = Path.Combine(FileSystem.AppDataDirectory, "chat.db");
		builder.Services.AddShinyActors(actors => actors
			// state, reminders and event logs in one SQLite file
			.UseDocumentDb(new SqliteDatabaseProvider($"Data Source={database}"))
			.AddDurableStream<ChatMessage>(retain: 500)

			// reminders: fired in the background by the OS, and shown as notifications on time
			.UseBackgroundReminders()
			.UseReminderNotifications()

			// sharing rooms with nearby devices: found over mDNS, served over HTTP behind a pairing code
			.UseDiscovery()
			.ServeOverHttp(
				expose => expose
					.Expose<IChatRoom>()
					.Expose<IRoomDirectory>()
					.ExposeStream<ChatMessage>()
					.ExposeStream<DirectoryChanged>(),
				http =>
				{
					http.Configure((HttpServerOptions o) =>
					{
						o.Address = IPAddress.Any; // other devices connect to our LAN address - the default is loopback only
						o.Port = 0;
					});
					http.AddAuthentication().AddApiKey(o =>
					{
						o.HeaderName = ChatSharing.ApiKeyHeader;
						o.AddKey(ChatSharing.PairingCode, "peer");
					});
					http.AddAuthorization(_ => { });
					http.Configure((HttpServer server) =>
					{
						server.UseAuthentication();
						server.UseAuthorization();
					});
				},
				autoStart: false,          // the "share" switch starts it
				authorizationPolicies: []  // every actor route needs the pairing code
			));

		builder.Services.AddSingleton<ChatConnection>();
		builder.Services.AddSingleton<ChatSharing>();
		builder.Services.AddTransient<RoomsPage>();
		builder.Services.AddTransient<RoomPage>();
		builder.Services.AddTransient<NearbyPage>();

#if DEBUG
		builder.Logging.AddDebug();
		builder.AddMauiDevFlowAgent();
#endif

		return builder.Build();
	}
}
