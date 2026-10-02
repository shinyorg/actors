namespace Shiny.Actors;


/// <summary>
/// Implement on an actor class to receive reminders it registered with <c>RegisterReminderAsync</c>.
/// A reminder activates the actor if needed - it does not need to be running.
/// </summary>
public interface IRemindable
{
    ValueTask ReceiveReminderAsync(ReminderTick tick, CancellationToken cancellationToken);
}


/// <summary>One firing of a reminder.</summary>
/// <param name="Name">The name it was registered with.</param>
/// <param name="DueAt">When this tick was due. Earlier than <paramref name="FiredAt"/> when the app was not running - missed ticks are coalesced into one.</param>
/// <param name="FiredAt">When it was delivered.</param>
/// <param name="Period">Null for a one-shot reminder, which is removed once delivered.</param>
public readonly record struct ReminderTick(string Name, DateTimeOffset DueAt, DateTimeOffset FiredAt, TimeSpan? Period);


/// <summary>A persisted reminder. <see cref="ActorName"/> is the registered actor name, as for state.</summary>
/// <param name="Notification">Shown by the OS when the reminder is due, if an observer such as Shiny.Actors.Notifications schedules it.</param>
public sealed record ActorReminder(string ActorName, string ActorId, string Name, DateTimeOffset DueAt, TimeSpan? Period, ReminderNotification? Notification = null)
{
    internal (string, string, string) Key => (this.ActorName, this.ActorId, this.Name);
}


/// <summary>
/// What the user sees when a reminder comes due. With Shiny.Actors.Notifications it becomes an OS-scheduled local
/// notification, so it appears on time even when the app is not running - tapping it opens the app, which fires the
/// reminder. Without an observer it is stored and ignored.
/// </summary>
public sealed record ReminderNotification(string Title, string Message)
{
    /// <summary>A notification channel (Android) the app has created; the default channel otherwise.</summary>
    public string? Channel { get; init; }
}


/// <summary>
/// Told about every reminder that is scheduled (registered, advanced to its next due time, or loaded at startup) and
/// removed - to mirror reminders into an OS scheduler. Calls must be idempotent; failures are logged, never thrown.
/// </summary>
public interface IActorReminderObserver
{
    ValueTask OnScheduledAsync(ActorReminder reminder, CancellationToken cancellationToken);
    ValueTask OnRemovedAsync(ActorReminder reminder, CancellationToken cancellationToken);
}
