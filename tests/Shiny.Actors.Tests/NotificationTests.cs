using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors.Notifications;
using Shiny.Actors.Testing;
using Shiny.Notifications;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class NotificationTests
{
    static (ActorTestHost Host, FakeNotifications Notifications) NewHost()
    {
        var notifications = new FakeNotifications();
        var host = new ActorTestHost(
            services => services.AddSingleton(new Probe()).AddSingleton<INotificationManager>(notifications),
            actors => actors.UseReminderNotifications()
        );
        return (host, notifications);
    }


    [Fact]
    public async Task A_Reminder_With_A_Notification_Is_Scheduled_With_The_OS()
    {
        var (host, notifications) = NewHost();
        await using var _ = host;

        await host.Get<IAlarm>().SetWithAlert("standup", TimeSpan.FromHours(1), null, "Standup");

        var scheduled = Assert.Single(notifications.Pending);
        Assert.Equal("Standup", scheduled.Title);
        Assert.Equal("standup is due", scheduled.Message);
        Assert.Equal(host.Time.GetUtcNow().AddHours(1), scheduled.ScheduleDate);
        Assert.Equal("standup", scheduled.Payload[ReminderNotificationObserver.PayloadReminder]);
        Assert.Equal(ActorTestHost.DefaultId, scheduled.Payload[ReminderNotificationObserver.PayloadActorId]);
    }


    [Fact]
    public async Task Reminders_Without_A_Notification_Schedule_Nothing()
    {
        var (host, notifications) = NewHost();
        await using var _ = host;

        await host.Get<IAlarm>().Set("quiet", TimeSpan.FromHours(1), null);
        Assert.Empty(notifications.Pending);
    }


    [Fact]
    public async Task A_Repeating_Reminder_Moves_Its_Notification_To_The_Next_Occurrence()
    {
        var (host, notifications) = NewHost();
        await using var _ = host;
        var start = host.Time.GetUtcNow();

        await host.Get<IAlarm>().SetWithAlert("water", TimeSpan.FromMinutes(30), TimeSpan.FromMinutes(30), "Drink water");
        await host.AdvanceAsync(TimeSpan.FromMinutes(30), step: TimeSpan.FromMinutes(1));

        var next = Assert.Single(notifications.Pending); // replaced, never duplicated
        Assert.Equal(start.AddMinutes(60), next.ScheduleDate);
        Assert.Equal(["water"], await host.Get<IAlarm>().Rings());
    }


    [Fact]
    public async Task Cancelling_Or_Finishing_A_Reminder_Cancels_Its_Notification()
    {
        var (host, notifications) = NewHost();
        await using var _ = host;

        await host.Get<IAlarm>().SetWithAlert("a", TimeSpan.FromHours(1), null, "A");
        await host.Get<IAlarm>().SetWithAlert("b", TimeSpan.FromMinutes(5), null, "B");
        Assert.Equal(2, notifications.Pending.Count);

        await host.Get<IAlarm>().Cancel("a");
        await host.AdvanceAsync(TimeSpan.FromMinutes(5), step: TimeSpan.FromMinutes(1)); // "b" fires once and is done

        Assert.Empty(notifications.Pending);
    }


    [Fact]
    public void Notification_Ids_Are_Stable_And_Positive()
    {
        var a = new ActorReminder("alarm", "x", "tick", DateTimeOffset.UnixEpoch, null);
        Assert.Equal(ReminderNotificationObserver.NotificationId(a), ReminderNotificationObserver.NotificationId(a with { DueAt = DateTimeOffset.MaxValue }));
        Assert.NotEqual(ReminderNotificationObserver.NotificationId(a), ReminderNotificationObserver.NotificationId(a with { Name = "tock" }));
        Assert.True(ReminderNotificationObserver.NotificationId(a) > 0);
    }


    /// <summary>Behaves like the OS scheduler: one pending notification per id.</summary>
    sealed class FakeNotifications : INotificationManager
    {
        readonly Dictionary<int, Notification> pending = [];

        public IReadOnlyList<Notification> Pending
        {
            get { lock (this.pending) return [.. this.pending.Values]; }
        }

        public Task Send(Notification notification)
        {
            lock (this.pending) this.pending[notification.Id] = notification;
            return Task.CompletedTask;
        }

        public Task Cancel(int id)
        {
            lock (this.pending) this.pending.Remove(id);
            return Task.CompletedTask;
        }

        public Task Cancel(CancelScope cancelScope = CancelScope.All)
        {
            lock (this.pending) this.pending.Clear();
            return Task.CompletedTask;
        }

        public Task<IList<Notification>> GetPendingNotifications() => Task.FromResult<IList<Notification>>([.. this.Pending]);
        public Task<Notification>? GetNotification(int notificationId) => null;
        public Task<AccessState> RequestAccess(AccessRequestFlags flags = AccessRequestFlags.Notification) => Task.FromResult(AccessState.Available);
        public Task<AccessState> GetCurrentAccess(AccessRequestFlags flags = AccessRequestFlags.Notification) => Task.FromResult(AccessState.Available);
        public void AddChannel(Channel channel) { }
        public void RemoveChannel(string channelId) { }
        public void ClearChannels() { }
        public Channel? GetChannel(string channelId) => null;
        public IList<Channel> GetChannels() => [];
    }
}
