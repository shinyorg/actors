using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace Shiny.Actors.Tests;


public class ActorTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;


    static (ActorSystem Actors, Probe Probe, ServiceProvider Services) Create(Action<ActorSystemOptions>? configure = null)
    {
        var probe = new Probe();
        var services = new ServiceCollection()
            .AddSingleton(probe)
            .AddShinyActors(actors => { if (configure is not null) actors.Configure(configure); })
            .BuildServiceProvider();

        return (services.GetRequiredService<ActorSystem>(), probe, services);
    }


    static async Task WaitUntil(Func<bool> condition, string because)
    {
        var timeout = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow > timeout)
                Assert.Fail("Timed out waiting until " + because);
            await Task.Delay(10, Ct);
        }
    }


    [Fact]
    public async Task Calls_Reach_The_Actor_And_State_Is_Kept_Between_Calls()
    {
        var (actors, _, _) = Create();
        var counter = actors.Get<ICounter>("a");

        Assert.Equal(1, await counter.Increment());     // interface default parameter
        Assert.Equal(6, await counter.Increment(5));
        Assert.Equal(6, await actors.Get<ICounter>("a").GetCount());
        Assert.Equal(0, await actors.Get<ICounter>("b").GetCount());
        Assert.Equal(2, actors.ActivationCount);
    }


    [Fact]
    public async Task One_Call_At_A_Time_Per_Actor()
    {
        var (actors, probe, _) = Create();
        var counter = actors.Get<ICounter>("busy");

        await Task.WhenAll(Enumerable.Range(0, 500).Select(_ => Task.Run(() => counter.Increment().AsTask(), Ct)));

        Assert.Equal(500, await counter.GetCount());
        Assert.Equal(1, probe.MaxConcurrent);
    }


    [Fact]
    public async Task Different_Actors_Run_In_Parallel()
    {
        var (actors, _, _) = Create();
        var started = DateTime.UtcNow;

        await Task.WhenAll(Enumerable.Range(0, 10).Select(i => actors.Get<ICounter>("p" + i).Slow(200, Ct).AsTask()));

        Assert.True(DateTime.UtcNow - started < TimeSpan.FromSeconds(1.5), "ten 200ms calls on ten actors should overlap");
    }


    [Fact]
    public async Task OneWay_Calls_Are_Queued_In_Order()
    {
        var (actors, _, _) = Create();
        var counter = actors.Get<ICounter>("tell");

        for (var i = 0; i < 100; i++)
            await counter.Bump();

        Assert.Equal(100, await counter.GetCount()); // queued behind every bump
    }


    [Fact]
    public async Task Exceptions_Reach_The_Caller_And_The_Actor_Survives()
    {
        var (actors, probe, _) = Create();
        var counter = actors.Get<ICounter>("boom");
        await counter.Increment();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => counter.Fail("nope"));
        Assert.Equal("nope", ex.Message);

        Assert.Equal(2, await counter.Increment());
        Assert.Single(probe.Events, x => x.StartsWith("activate:boom"));
    }


    [Fact]
    public async Task Cancelling_A_Queued_Call_Releases_The_Caller_Immediately()
    {
        var (actors, _, _) = Create();
        var counter = actors.Get<ICounter>("queue");

        var blocker = counter.Slow(1000, Ct);
        using var cts = new CancellationTokenSource(50);
        var started = DateTime.UtcNow;

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => counter.Slow(10, cts.Token).AsTask());
        Assert.True(DateTime.UtcNow - started < TimeSpan.FromMilliseconds(800), "should not wait for the blocking call");

        Assert.Equal(1, await blocker); // the cancelled call never ran
        Assert.Equal(1, await counter.GetCount());
    }


    [Fact]
    public async Task One_Class_With_Two_Interfaces_Is_One_Activation()
    {
        var (actors, _, _) = Create();

        await actors.Get<ICounter>("x").Increment(3);
        await actors.Get<IResettable>("x").Reset();

        Assert.Equal(0, await actors.Get<ICounter>("x").GetCount());
        Assert.Equal(1, actors.ActivationCount);
    }


    [Fact]
    public async Task Proxies_Compare_By_Identity()
    {
        var (actors, _, _) = Create();

        Assert.Equal(actors.Get<ICounter>("same"), actors.Get<ICounter>("same"));
        Assert.NotEqual(actors.Get<ICounter>("same"), actors.Get<ICounter>("other"));
        Assert.Equal("same", actors.Get<ICounter>("same").GetActorId());
        Assert.Equal("counter/same", actors.Get<ICounter>("same").ToString());
        await Task.CompletedTask;
    }


    [Fact]
    public async Task Idle_Actors_Deactivate_And_Come_Back_With_Their_State()
    {
        var time = new FakeTimeProvider();
        var (actors, probe, _) = Create(o =>
        {
            o.TimeProvider = time;
            o.IdleTimeout = TimeSpan.FromMinutes(1);
        });

        await actors.Get<ICounter>("idle").Increment(7);
        time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(1, actors.ActivationCount);

        time.Advance(TimeSpan.FromSeconds(31));
        await WaitUntil(() => actors.ActivationCount == 0, "the idle actor deactivates");
        Assert.Contains("deactivate:idle:Idle", probe.Events);

        Assert.Equal(8, await actors.Get<ICounter>("idle").Increment()); // written in OnDeactivateAsync, read back on activation
        Assert.Contains("activate:idle:7", probe.Events);
    }


    [Fact]
    public async Task Calls_Keep_An_Actor_Alive()
    {
        var time = new FakeTimeProvider();
        var (actors, _, _) = Create(o =>
        {
            o.TimeProvider = time;
            o.IdleTimeout = TimeSpan.FromMinutes(1);
        });

        for (var i = 0; i < 5; i++)
        {
            await actors.Get<ICounter>("busy").Increment();
            time.Advance(TimeSpan.FromSeconds(40));
        }
        await Task.Delay(50, Ct);
        Assert.Equal(1, actors.ActivationCount);
    }


    [Fact]
    public async Task DeactivateOnIdle_Deactivates_After_The_Current_Call()
    {
        var (actors, probe, _) = Create();
        await actors.Get<ICounter>("bye").Increment(2);
        await actors.Get<ICounter>("bye").DeactivateSoon();

        await WaitUntil(() => actors.ActivationCount == 0, "the actor deactivates itself");
        Assert.Contains("deactivate:bye:Requested", probe.Events);
        Assert.Equal(2, await actors.Get<ICounter>("bye").GetCount());
    }


    [Fact]
    public async Task DeactivateAll_Flushes_Every_Actor()
    {
        var (actors, probe, _) = Create();
        await actors.Get<ICounter>("one").Increment();
        await actors.Get<ICounter>("two").Increment();

        await actors.DeactivateAllAsync(Ct);

        Assert.Equal(0, actors.ActivationCount);
        Assert.Contains("deactivate:one:Shutdown", probe.Events);
        Assert.Contains("deactivate:two:Shutdown", probe.Events);
    }


    [Fact]
    public async Task Calls_Racing_A_Deactivation_Are_Not_Lost()
    {
        var (actors, _, _) = Create();
        var counter = actors.Get<ICounter>("race");

        for (var round = 0; round < 20; round++)
        {
            var calls = Enumerable.Range(0, 50).Select(_ => Task.Run(() => counter.Increment().AsTask(), Ct)).ToList();
            var stop = actors.DeactivateAsync<ICounter>("race");
            await Task.WhenAll(calls);
            await stop;
        }

        Assert.Equal(1000, await counter.GetCount());
    }


    [Fact]
    public async Task A_Failed_Activation_Fails_Its_Callers_And_Is_Retried_Next_Call()
    {
        var (actors, probe, _) = Create();
        probe.ActivationFailuresLeft = 1;

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => actors.Get<IFlaky>("f").Hello());
        Assert.IsType<IOException>(ex.InnerException);

        await WaitUntil(() => actors.ActivationCount == 0, "the failed activation is removed");
        Assert.Equal("hi f", await actors.Get<IFlaky>("f").Hello());
    }


    [Fact]
    public async Task Calling_Yourself_Throws_Instead_Of_Hanging()
    {
        var (actors, _, _) = Create();

        var ex = await Assert.ThrowsAsync<ActorDeadlockException>(() => actors.Get<IPingPong>("me").CallSelf());
        Assert.Contains("PingPongActor/me -> Shiny.Actors.Tests.PingPongActor/me", ex.Message);
    }


    [Fact]
    public async Task A_Call_Cycle_Throws_Instead_Of_Hanging()
    {
        var (actors, _, _) = Create();

        var ex = await Assert.ThrowsAsync<ActorDeadlockException>(() => actors.Get<IPingPong>("a").CallOther("b"));
        Assert.Contains("/a -> Shiny.Actors.Tests.PingPongActor/b -> Shiny.Actors.Tests.PingPongActor/a", ex.Message);

        // and an actor calling a different actor is fine
        Assert.Equal("pong b", await actors.Get<IPingPong>("a").CallBack("b"));
    }


    [Fact]
    public async Task Timers_Tick_As_Turns_Of_The_Actor()
    {
        var time = new FakeTimeProvider();
        var (actors, _, _) = Create(o => o.TimeProvider = time);
        var ticker = actors.Get<ITicker>("t");
        Assert.Equal(0, await ticker.Ticks());

        for (var i = 1; i <= 3; i++)
        {
            time.Advance(TimeSpan.FromSeconds(1));
            var expected = i;
            await WaitUntil(() => ticker.Ticks().Result == expected, $"tick {expected}");
        }
    }


    [Fact]
    public async Task File_State_Survives_A_New_ActorSystem()
    {
        var dir = Path.Combine(Path.GetTempPath(), "shiny-actors-" + Guid.NewGuid().ToString("N"));
        try
        {
            var (first, _, _) = Create(o => o.UseFileStateProvider(dir));
            await first.Get<ICounter>("Some/Weird:Id*").Increment(41);
            await first.DisposeAsync();

            var (second, _, _) = Create(o => o.UseFileStateProvider(dir));
            Assert.Equal(42, await second.Get<ICounter>("Some/Weird:Id*").Increment());
            Assert.Single(Directory.GetFiles(Path.Combine(dir, "counter")));
        }
        finally
        {
            Directory.Delete(dir, true);
        }
    }


    [Fact]
    public async Task Works_Without_A_Container()
    {
        await using var actors = new ActorSystem();
        Assert.Equal(0, await actors.Get<ITicker>("solo").Ticks());

        // CounterActor needs a Probe from DI - the failure says which service is missing
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => actors.Get<ICounter>("x").GetCount());
        Assert.Contains("Probe", ex.InnerException!.Message);
    }


    [Fact]
    public async Task A_Disposed_System_Refuses_Calls()
    {
        var (actors, probe, _) = Create();
        await actors.Get<ICounter>("d").Increment();
        await actors.DisposeAsync();

        Assert.Contains("deactivate:d:Shutdown", probe.Events);
        Assert.Throws<ObjectDisposedException>(() => actors.Get<ICounter>("d"));
    }
}
