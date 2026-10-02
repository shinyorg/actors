using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors.Testing;

namespace Shiny.Actors.Tests;


/// <summary>The testing kit, used the way an app's own tests would use it.</summary>
public class TestingKitTests
{
    static ActorTestHost NewHost(Action<ShinyActorBuilder>? actors = null)
        => new(services => services.AddSingleton(new Probe()), actors);


    [Fact]
    public async Task Seeded_State_Is_What_The_Actor_Starts_With()
    {
        await using var host = NewHost();
        await host.SetStateAsync<CounterActor, CounterState>(new CounterState { Count = 41 });

        Assert.Equal(42, await host.Get<ICounter>().Increment());
    }


    [Fact]
    public async Task Stored_State_Can_Be_Inspected()
    {
        await using var host = NewHost();
        await host.Get<ICounter>("x").Increment(7);
        Assert.Null(await host.GetStateAsync<CounterActor, CounterState>("x")); // CounterActor writes on deactivation

        await host.DeactivateAllAsync();
        Assert.Equal(7, (await host.GetStateAsync<CounterActor, CounterState>("x"))!.Count);
    }


    [Fact]
    public async Task Advancing_Time_Runs_Reminders_Without_Waiting_Around()
    {
        await using var host = NewHost();
        await host.Get<IAlarm>().Set("tick", TimeSpan.FromMinutes(5), TimeSpan.FromMinutes(5));
        Assert.Single(await host.GetRemindersAsync<AlarmActor>());

        await host.AdvanceAsync(TimeSpan.FromMinutes(5));
        Assert.Equal(["tick"], await host.Get<IAlarm>().Rings());

        await host.AdvanceAsync(TimeSpan.FromMinutes(10));
        Assert.Equal(3, (await host.Get<IAlarm>().Rings()).Length);
    }


    [Fact]
    public async Task Advancing_Time_Deactivates_Idle_Actors()
    {
        await using var host = NewHost(actors => actors.Configure(o => o.IdleTimeout = TimeSpan.FromMinutes(2)));
        await host.Get<ICounter>().Increment();
        Assert.Equal(1, host.System.ActivationCount);

        await host.AdvanceAsync(TimeSpan.FromMinutes(3));
        Assert.Equal(0, host.System.ActivationCount);
    }


    [Fact]
    public async Task Every_Call_Is_Recorded_With_Its_Outcome()
    {
        await using var host = NewHost();
        await host.Get<ICounter>().Increment(2);
        await Assert.ThrowsAsync<InvalidOperationException>(() => host.Get<ICounter>().Fail("bad"));

        Assert.Collection(host.Calls,
            c => Assert.Equal(("Increment", 2, (object?)2), (c.Method, (int)c.Arguments[0]!, c.Result)),
            c => Assert.True(c.Failed && c.Exception is InvalidOperationException && c.Method == "Fail")
        );
    }


    [Fact]
    public async Task Streams_Can_Be_Recorded()
    {
        await using var host = NewHost();
        using var recorder = host.Record<Note>("room");

        await host.GetStream<Note>("room").PublishAsync(new Note("hi"), TestContext.Current.CancellationToken);
        await host.GetStream<Note>("room").PublishAsync(new Note("there"), TestContext.Current.CancellationToken);

        Assert.Equal(["hi", "there"], (await recorder.WaitForAsync(2)).Select(n => n.Text));
    }


    [Fact]
    public async Task Event_Sourced_History_Can_Be_Inspected()
    {
        await using var host = NewHost();
        await host.Get<IAccount>().Deposit(10m);
        await host.Get<IAccount>().Withdraw(4m);

        Assert.Equal([new Deposited(10m), new Withdrawn(4m)], await host.GetEventsAsync<AccountActor, AccountEvent>());
    }
}
