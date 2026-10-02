using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class ReentrancyTests
{
    [Fact]
    public async Task A_Waiting_Call_Does_Not_Block_The_Next_One()
    {
        var (actors, _, _) = Create();
        var gate = actors.Get<IGate>("g");

        var waiting = gate.WaitForOpen();
        await gate.Open("go"); // would queue behind WaitForOpen forever without [Reentrant]

        Assert.Equal("go", await waiting.WaitAsync(TimeSpan.FromSeconds(5), Ct));
    }


    [Fact]
    public async Task Calls_Interleave_But_Code_Between_Awaits_Never_Overlaps()
    {
        var (actors, probe, _) = Create();
        var gate = actors.Get<IGate>("work");
        var started = DateTime.UtcNow;

        await Task.WhenAll(Enumerable.Range(0, 10).Select(_ => Task.Run(() => gate.Work(), Ct)));

        Assert.True(DateTime.UtcNow - started < TimeSpan.FromMilliseconds(400), "ten 50ms waits should overlap");
        Assert.Equal(1, probe.MaxConcurrent);
    }


    [Fact]
    public async Task A_Reentrant_Actor_Can_Call_Itself()
    {
        var (actors, _, _) = Create();
        Assert.Equal("pong", await actors.Get<IGate>("self").CallSelf());
    }


    [Fact]
    public async Task Deactivation_Waits_For_Interleaved_Calls()
    {
        var (actors, _, _) = Create();
        var gate = actors.Get<IGate>("d");
        await gate.Ping();

        var working = gate.Work(); // ~60ms, mostly awaiting
        await Task.Delay(15, Ct);
        var stopping = actors.DeactivateAsync<IGate>("d");

        await Task.Delay(15, Ct);
        Assert.False(stopping.IsCompleted, "deactivation must wait for the call in flight");

        await stopping.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        // the call finished inside the activation; the caller's task completes via a continuation, a moment later
        await working.WaitAsync(TimeSpan.FromSeconds(5), Ct);
        Assert.Equal(0, actors.ActivationCount);
    }
}
