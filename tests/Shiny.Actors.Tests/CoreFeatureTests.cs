using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors.Remoting;
using Shiny.Net.HttpServer;
using static Shiny.Actors.Tests.TestHelpers;

namespace Shiny.Actors.Tests;


public class CoreFeatureTests
{
    [Fact]
    public async Task A_Reentrant_Actor_Shutting_Down_Still_Takes_Its_Own_Calls()
    {
        var (actors, _, _) = Create();
        var gate = actors.Get<IGate>("closing");
        await gate.Ping();

        var working = gate.WorkThenCallSelf(); // calls itself ~100ms from now
        await Task.Delay(20, Ct);
        var stopping = actors.DeactivateAsync<IGate>("closing");

        // this used to hang: the self-call was refused, then waited on a shutdown that waited on it
        Assert.Equal("pong", await working.WaitAsync(TimeSpan.FromSeconds(5), Ct));
        await stopping.WaitAsync(TimeSpan.FromSeconds(5), Ct);
    }


    [Fact]
    public async Task Calls_Arriving_During_Shutdown_Go_To_The_Next_Activation_In_Order()
    {
        var (actors, probe, _) = Create();
        var counter = actors.Get<ICounter>("handover");
        await counter.Increment();

        var slow = counter.Slow(100, Ct);            // keeps the activation busy
        var stopping = actors.DeactivateAsync<ICounter>("handover");
        await Task.Delay(20, Ct);
        var late = Enumerable.Range(0, 5).Select(_ => counter.Increment().AsTask()).ToArray(); // arrive while stopping

        await slow;
        await stopping;
        var results = await Task.WhenAll(late);
        Assert.Equal(new[] { 3, 4, 5, 6, 7 }, results); // all handled, in order, by the next activation
        Assert.Equal(2, probe.Events.Count(x => x.StartsWith("activate:handover")));
    }


    [Fact]
    public async Task AlwaysInterleave_Methods_Run_While_A_Call_Is_Waiting()
    {
        var (actors, _, _) = Create();
        var workflow = actors.Get<IWorkflow>("w");

        var run = workflow.Run(10_000);
        await Task.Delay(30, Ct);
        Assert.Equal("running", await workflow.Status().WaitAsync(TimeSpan.FromSeconds(2), Ct));

        await workflow.Cancel().WaitAsync(TimeSpan.FromSeconds(2), Ct);
        Assert.Equal("cancelled", await run.WaitAsync(TimeSpan.FromSeconds(2), Ct));
    }


    [Fact]
    public async Task Calling_Your_Own_AlwaysInterleave_Method_Is_Not_A_Deadlock()
    {
        var (actors, _, _) = Create();
        Assert.Equal("checking", await actors.Get<IWorkflow>("self").RunAndCheckOwnStatus());
    }


    [Fact]
    public async Task ReadOnly_Calls_Share_But_Never_With_A_Write()
    {
        var (actors, probe, _) = Create();
        var library = actors.Get<ILibrary>("l");

        var reads = Enumerable.Range(0, 5).Select(_ => library.Read(80)).ToList();
        var write = library.Write(30);
        var after = Enumerable.Range(0, 3).Select(_ => library.Read(20)).ToList();

        await Task.WhenAll(reads.Concat(after).Append(write));

        Assert.True(probe.MaxConcurrent > 1, "read-only calls should overlap");
        Assert.DoesNotContain("read during write!", probe.Events);
        Assert.DoesNotContain("write during read!", probe.Events);
    }


    [Fact]
    public async Task Stateless_Workers_Run_In_Parallel_Up_To_Their_Limit()
    {
        var (actors, probe, _) = Create();
        var resizer = actors.Get<IResizer>("any");

        // issued back to back, so every pick happens before any call finishes - the pool alone decides placement
        var calls = new List<Task<int>>();
        for (var i = 0; i < 8; i++)
            calls.Add(resizer.Resize(500));
        var instances = await Task.WhenAll(calls);

        Assert.Equal(4, probe.MaxConcurrent); // four busy at once - one id, four activations - never five
        Assert.Equal(4, instances.Distinct().Count());
        Assert.True(actors.ActivationCount <= 4);
    }


    [Fact]
    public async Task Request_Context_Flows_In_And_Onward_But_Not_Back()
    {
        var (actors, _, _) = Create();

        ActorRequestContext.Set("tenant", "acme");
        try
        {
            Assert.Equal("acme", await actors.Get<IContextual>("a").Read("tenant"));
            Assert.Equal("acme", await actors.Get<IContextual>("a").ReadVia("b", "tenant"));

            Assert.Equal("changed", await actors.Get<IContextual>("a").SetAndReturn("tenant", "changed"));
            Assert.Equal("acme", ActorRequestContext.Get("tenant"));
        }
        finally
        {
            ActorRequestContext.Clear();
        }
    }


    [Fact]
    public async Task Filters_See_Every_Call_And_Can_Refuse_Or_Rewrite_It()
    {
        var log = new ConcurrentQueue<string>();
        var (actors, _, _) = Create(o => o
            .AddCallFilter(new LambdaFilter(async (ctx, next) =>
            {
                log.Enqueue($"{ctx.ActorName}.{ctx.Method.Name}({string.Join(",", ctx.Arguments)})");
                await next(ctx);
            }))
            .AddCallFilter(new LambdaFilter(async (ctx, next) =>
            {
                if (ctx.Method.Name == nameof(ICounter.GetCount) && ActorRequestContext.Get("user") is null)
                    throw new UnauthorizedAccessException("sign in");

                await next(ctx);
                if (ctx.Method.Name == nameof(ICounter.Increment))
                    ctx.Result = (int)ctx.Result! * 100; // rewrite what the caller sees
            }))
        );

        Assert.Equal(500, await actors.Get<ICounter>("f").Increment(5));
        await Assert.ThrowsAsync<UnauthorizedAccessException>(() => actors.Get<ICounter>("f").GetCount());
        await actors.Get<ICounter>("f").Bump(); // one-way calls are filtered too

        await Eventually(() => Task.FromResult(log.Count), c => c == 3, "three filtered calls");
        Assert.Equal(["counter.Increment(5)", "counter.GetCount()", "counter.Bump()"], log);
    }


    [Fact]
    public async Task Filters_See_Methods_That_Return_Nothing()
    {
        var seen = new ConcurrentQueue<string>();
        var (actors, _, _) = Create(o => o.AddCallFilter(new LambdaFilter(async (ctx, next) =>
        {
            seen.Enqueue(ctx.Method.Name);
            await next(ctx);
        })));

        await actors.Get<IResettable>("v").Reset();                                                     // Task
        await actors.Get<ICounter>("v").DeactivateSoon();                                               // ValueTask
        await Assert.ThrowsAsync<InvalidOperationException>(() => actors.Get<ICounter>("w").Fail("x")); // Task that throws

        Assert.Equal(["Reset", "DeactivateSoon", "Fail"], seen);
    }


    [Fact]
    public async Task An_Actor_Can_Filter_Its_Own_Calls()
    {
        var (actors, _, _) = Create();
        Assert.Equal(9, await actors.Get<IGuarded>("g").Square(3));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => actors.Get<IGuarded>("g").Square(-3));
    }


    [Fact]
    public async Task Calls_Are_Traced_With_Parent_Child_Spans()
    {
        var spans = new ConcurrentQueue<Activity>();
        using var listener = Listen(spans);
        var (actors, _, _) = Create();

        using (var root = new ActivitySource("test").StartActivity("request"))
        {
            await actors.Get<IContextual>("a").ReadVia("b", "x");
        }

        // listeners are process-wide: look only at this trace, and give the inner span a moment to end
        var trace = spans.First(s => s.DisplayName == "request").TraceId;
        var mine = await Eventually(() => Task.FromResult(spans.Where(s => s.TraceId == trace && s.Source.Name == ActorTelemetry.Name).ToList()), x => x.Count >= 2, "both spans end");
        var outer = Assert.Single(mine, s => s.DisplayName == "Shiny.Actors.Tests.IContextual/ReadVia");
        var inner = Assert.Single(mine, s => s.DisplayName == "Shiny.Actors.Tests.IContextual/Read");
        Assert.Equal(outer.SpanId, inner.ParentSpanId);
        Assert.Equal(outer.TraceId, inner.TraceId);
        Assert.Equal("a", outer.GetTagItem("actor.id"));
        Assert.Equal("b", inner.GetTagItem("actor.id"));
    }


    [Fact]
    public async Task Calls_Are_Measured()
    {
        var measurements = new ConcurrentQueue<(string Name, long Value, string? Method, string? Actor)>();
        using var meters = new MeterListener();
        meters.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == ActorTelemetry.Name)
                l.EnableMeasurementEvents(instrument);
        };
        meters.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            string? method = null, actor = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "method")
                    method = tag.Value as string;
                else if (tag.Key == "actor")
                    actor = tag.Value as string;
            }
            measurements.Enqueue((instrument.Name, value, method, actor));
        });
        meters.Start();

        var (actors, _, _) = Create();
        await actors.Get<IMetered>("m").Tick();
        await actors.Get<IMetered>("m").Tick();

        const string actorName = "Shiny.Actors.Tests.MeteredActor";
        await Eventually(
            () => Task.FromResult(measurements.Count(m => m.Name == "shiny.actors.calls" && m.Actor == actorName && m.Method == "Tick")),
            c => c == 2,
            "both calls are counted"
        );
        Assert.Contains(measurements, m => m.Name == "shiny.actors.activations" && m.Actor == actorName);
    }


    [Fact]
    public async Task Traces_Context_And_Filters_Cross_A_Remote_Call()
    {
        var spans = new ConcurrentQueue<Activity>();
        using var listener = Listen(spans);
        var filtered = new ConcurrentQueue<string>();

        var builder = HttpServer.CreateBuilder();
        builder.Configure((HttpServerOptions o) => o.Port = 0);
        builder.Services.AddSingleton(new Probe()).AddShinyActors(actors => actors.AddCallFilter(new LambdaFilter(async (ctx, next) =>
        {
            filtered.Enqueue($"{ctx.Method.Name}:{ActorRequestContext.Get("tenant")}");
            await next(ctx);
        })));
        var server = builder.Build();
        await using var _ = server;
        server.MapActors(o => o.Expose<IContextual>());
        await server.StartAsync(Ct);
        var remote = new RemoteActorSystem(new HttpClient { BaseAddress = new Uri(server.ListenUrl!.TrimEnd('/') + "/actors/") });

        ActorRequestContext.Set("tenant", "acme");
        string? seen;
        ActivityTraceId traceId;
        try
        {
            using var root = new ActivitySource("test").StartActivity("request");
            traceId = root!.TraceId;
            seen = await remote.Get<IContextual>("r").ReadVia("s", "tenant");
        }
        finally
        {
            ActorRequestContext.Clear();
        }

        Assert.Equal("acme", seen);
        Assert.Contains("ReadVia:acme", filtered);
        Assert.Contains("Read:acme", filtered);

        // client -> server -> actor -> actor, all in the caller's trace
        var ours = await Eventually(
            () => Task.FromResult(spans.Where(s => s.Source.Name == ActorTelemetry.Name && s.TraceId == traceId).ToList()),
            x => x.Count >= 4,
            "client, server and both actor spans end"
        );
        Assert.Contains(ours, s => s.Kind == ActivityKind.Client);
        Assert.Contains(ours, s => s.Kind == ActivityKind.Server);
        Assert.Contains(ours, s => s.DisplayName.EndsWith("/Read") && s.Kind == ActivityKind.Internal);
    }


    static ActivityListener Listen(ConcurrentQueue<Activity> spans)
    {
        var listener = new ActivityListener
        {
            ShouldListenTo = s => s.Name is ActorTelemetry.Name or "test",
            Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
            ActivityStopped = spans.Enqueue
        };
        ActivitySource.AddActivityListener(listener);
        return listener;
    }


    sealed class LambdaFilter(Func<ActorCallContext, ActorCallDelegate, ValueTask> filter) : IActorCallFilter
    {
        public ValueTask InvokeAsync(ActorCallContext context, ActorCallDelegate next) => filter(context, next);
    }
}
