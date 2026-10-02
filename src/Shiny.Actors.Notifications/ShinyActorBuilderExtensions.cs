using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny;
using Shiny.Actors;
using Shiny.Actors.Notifications;
using Shiny.Notifications;

namespace Microsoft.Extensions.DependencyInjection;


public static class NotificationsShinyActorBuilderExtensions
{
    /// <summary>
    /// Reminders registered with a <see cref="ReminderNotification"/> become OS-scheduled local notifications - on time,
    /// app running or not. Request notification access from the app.
    /// </summary>
    /// <remarks>
    /// On iOS, Android, Mac and Windows this registers Shiny.Notifications too, unless the app already has (register it
    /// yourself first for a delegate or iOS configuration). On plain .NET, register an <see cref="INotificationManager"/>
    /// yourself - Shiny.Notifications.Linux, for one.
    /// </remarks>
    public static ShinyActorBuilder UseReminderNotifications(this ShinyActorBuilder builder)
    {
#if ANDROID || IOS || MACCATALYST || WINDOWS
        if (!builder.Services.Any(x => x.ServiceType == typeof(INotificationManager)))
            builder.Services.AddNotifications();
#endif

        builder.Services.TryAddEnumerable(ServiceDescriptor.Singleton<IActorReminderObserver, ReminderNotificationObserver>());
        return builder;
    }
}
