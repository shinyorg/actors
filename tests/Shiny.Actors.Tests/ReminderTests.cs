using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Shiny.Actors.Jobs;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class ReminderTests
{
    static readonly DateTimeOffset Start = new(2026, 10, 1, 9, 0, 0, TimeSpan.Zero);


    [Fact]
    public async Task A_Periodic_Reminder_Keeps_Firing()
    {
        var time = new FakeTimeProvider(Start);
        var (actors, _, _) = Create(o => o.TimeProvider = time);
        var alarm = actors.Get<IAlarm>("a");

        await alarm.Set("tick", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        Assert.Empty(await alarm.Rings());

        time.Advance(TimeSpan.FromMinutes(1));
        await Eventually(() => alarm.Rings(), r => r.Length == 1, "the first tick");

        time.Advance(TimeSpan.FromMinutes(1));
        await Eventually(() => alarm.Rings(), r => r.Length == 2, "the second tick");
        Assert.Equal(["tick"], await alarm.Reminders());
    }


    [Fact]
    public async Task A_One_Shot_Reminder_Fires_Once_Then_Disappears()
    {
        var time = new FakeTimeProvider(Start);
        var (actors, _, _) = Create(o => o.TimeProvider = time);
        var alarm = actors.Get<IAlarm>("once");

        await alarm.Set("wake", TimeSpan.FromMinutes(5), null);
        time.Advance(TimeSpan.FromMinutes(5));

        await Eventually(() => alarm.Rings(), r => r.Length == 1, "the reminder fires");
        await Eventually(() => alarm.Reminders(), r => r.Length == 0, "the one-shot is removed");

        time.Advance(TimeSpan.FromHours(1));
        await Task.Delay(50, Ct);
        Assert.Single(await alarm.Rings());
    }


    [Fact]
    public async Task Missed_Ticks_Are_Coalesced_Into_One()
    {
        var time = new FakeTimeProvider(Start);
        var (actors, _, _) = Create(o => o.TimeProvider = time);
        var alarm = actors.Get<IAlarm>("late");

        await alarm.Set("tick", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        time.Advance(TimeSpan.FromMinutes(10)); // ten periods pass in one jump

        await Eventually(() => alarm.Rings(), r => r.Length == 1, "one coalesced tick");
        await Task.Delay(50, Ct);
        Assert.Single(await alarm.Rings());
    }


    [Fact]
    public async Task An_Unregistered_Reminder_Stops()
    {
        var time = new FakeTimeProvider(Start);
        var (actors, _, _) = Create(o => o.TimeProvider = time);
        var alarm = actors.Get<IAlarm>("stop");

        await alarm.Set("tick", TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));
        Assert.True(await alarm.Cancel("tick"));
        Assert.False(await alarm.Cancel("tick"));

        time.Advance(TimeSpan.FromMinutes(5));
        await Task.Delay(50, Ct);
        Assert.Empty(await alarm.Rings());
    }


    [Fact]
    public async Task A_Reminder_Activates_An_Idle_Actor()
    {
        var time = new FakeTimeProvider(Start);
        var (actors, probe, _) = Create(o => o.TimeProvider = time);

        await actors.Get<IAlarm>("sleepy").Set("wake", TimeSpan.FromMinutes(30), null);
        await actors.DeactivateAllAsync(Ct);
        Assert.Equal(0, actors.ActivationCount);

        time.Advance(TimeSpan.FromMinutes(30));
        await Eventually(() => Task.FromResult(probe.Events.Contains("ring:sleepy:wake")), x => x, "the reminder wakes the actor");
    }


    [Fact]
    public async Task Reminders_Survive_A_Restart_And_The_Job_Fires_Them()
    {
        var dir = Path.Combine(Path.GetTempPath(), "shiny-actors-" + Guid.NewGuid().ToString("N"));
        try
        {
            var time = new FakeTimeProvider(Start);
            var (first, _, _) = Create(o =>
            {
                o.TimeProvider = time;
                o.UseFileStateProvider(dir);
            });
            await first.Get<IAlarm>("persisted").Set("daily", TimeSpan.FromHours(1), TimeSpan.FromDays(1));
            await first.DisposeAsync();
            Assert.True(File.Exists(Path.Combine(dir, "reminders.json")));

            // the app was killed; the OS wakes it two hours later to run the background job
            var later = new FakeTimeProvider(Start.AddHours(2));
            var (second, probe, _) = Create(o =>
            {
                o.TimeProvider = later;
                o.UseFileStateProvider(dir);
            });

            await new ActorReminderJob(second, NullLogger<ActorReminderJob>.Instance).Run(Ct);

            Assert.Contains("ring:persisted:daily", probe.Events);
            Assert.Equal(0, second.ActivationCount); // woken, handled, written, deactivated
            Assert.Equal(["daily"], await second.Get<IAlarm>("persisted").Rings());
            Assert.Equal(0, await second.RunDueRemindersAsync(cancellationToken: Ct)); // next tick is tomorrow
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }


    [Fact]
    public async Task Only_Remindable_Actors_Can_Register()
    {
        var (actors, _, _) = Create();
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => actors.Get<IRemindless>("x").TryRegister());
        Assert.Contains("IRemindable", ex.Message);
    }
}


public interface IRemindless : IActor
{
    Task TryRegister();
}


public class RemindlessActor : Actor, IRemindless
{
    public Task TryRegister() => this.RegisterReminderAsync("nope", TimeSpan.FromMinutes(1)).AsTask();
}
