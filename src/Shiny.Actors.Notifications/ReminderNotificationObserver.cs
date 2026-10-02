using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Logging;
using Shiny.Notifications;

namespace Shiny.Actors.Notifications;


/// <summary>
/// Keeps one OS-scheduled local notification per reminder that carries a <see cref="ReminderNotification"/>: scheduled
/// for its due time, moved when the reminder advances, cancelled when it goes away. The OS shows it on time whether or
/// not the app is running; tapping it opens the app, where the (now overdue) reminder fires.
/// </summary>
/// <remarks>
/// <para>Only the next occurrence of a repeating reminder is scheduled; the following one is scheduled when the app
/// next runs the reminder (foreground, or a Shiny.Actors.Jobs background run).</para>
/// <para>The notification's payload carries <see cref="PayloadActor"/>, <see cref="PayloadActorId"/> and
/// <see cref="PayloadReminder"/>, so an <c>INotificationDelegate</c> can route the tap.</para>
/// </remarks>
public sealed class ReminderNotificationObserver(INotificationManager notifications, TimeProvider? timeProvider = null, ILogger<ReminderNotificationObserver>? logger = null) : IActorReminderObserver
{
    public const string PayloadActor = "shiny.actors.actor";
    public const string PayloadActorId = "shiny.actors.id";
    public const string PayloadReminder = "shiny.actors.reminder";

    readonly TimeProvider time = timeProvider ?? TimeProvider.System;


    public async ValueTask OnScheduledAsync(ActorReminder reminder, CancellationToken cancellationToken)
    {
        var id = NotificationId(reminder);

        // replace, never duplicate: whatever was scheduled for this reminder goes first
        await notifications.Cancel(id).ConfigureAwait(false);

        // past due means it's firing in-process right now (or at startup) - a notification for it would be stale
        if (reminder.Notification is not { } content || reminder.DueAt <= this.time.GetUtcNow())
            return;

        await notifications.Send(new Shiny.Notifications.Notification
        {
            Id = id,
            Title = content.Title,
            Message = content.Message,
            Channel = content.Channel,
            ScheduleDate = reminder.DueAt,
            Payload = new Dictionary<string, string>
            {
                [PayloadActor] = reminder.ActorName,
                [PayloadActorId] = reminder.ActorId,
                [PayloadReminder] = reminder.Name
            }
        }).ConfigureAwait(false);

        logger?.LogDebug("Scheduled notification {Id} for reminder {Reminder} at {DueAt}", id, reminder.Name, reminder.DueAt);
    }


    public async ValueTask OnRemovedAsync(ActorReminder reminder, CancellationToken cancellationToken)
        => await notifications.Cancel(NotificationId(reminder)).ConfigureAwait(false);


    /// <summary>A stable id per (actor, id, reminder) - the same reminder always maps to the same notification.</summary>
    public static int NotificationId(ActorReminder reminder)
    {
        var hash = SHA256.HashData(Encoding.UTF8.GetBytes($"{reminder.ActorName}\n{reminder.ActorId}\n{reminder.Name}"));
        return (BitConverter.ToInt32(hash, 0) & 0x7FFFFFFF) | 1; // positive, never 0 (0 means "assign one")
    }
}
