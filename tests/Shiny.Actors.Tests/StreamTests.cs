using Microsoft.Extensions.DependencyInjection;

namespace Shiny.Actors.Tests;


public class StreamTests
{
    static CancellationToken Ct => TestContext.Current.CancellationToken;


    static (ActorSystem Actors, Probe Probe) Create()
    {
        var probe = new Probe();
        var services = new ServiceCollection().AddSingleton(probe).AddShinyActors().BuildServiceProvider();
        return (services.GetRequiredService<ActorSystem>(), probe);
    }


    static async Task<T> Eventually<T>(Func<Task<T>> read, Func<T, bool> done)
    {
        var timeout = DateTime.UtcNow.AddSeconds(5);
        while (true)
        {
            var value = await read();
            if (done(value) || DateTime.UtcNow > timeout)
                return value;
            await Task.Delay(10, Ct);
        }
    }


    [Fact]
    public async Task ReadAll_Sees_Events_In_Publish_Order()
    {
        var (actors, _) = Create();
        var stream = actors.GetStream<Note>("room");

        await using var reader = stream.ReadAllAsync(Ct).GetAsyncEnumerator(Ct);
        var first = reader.MoveNextAsync(); // subscribes

        await stream.PublishAsync(new Note("1"), Ct);
        await stream.PublishAsync(new Note("2"), Ct);
        await stream.PublishAsync(new Note("3"), Ct);

        var received = new List<string>();
        Assert.True(await first);
        received.Add(reader.Current.Text);
        while (received.Count < 3 && await reader.MoveNextAsync())
            received.Add(reader.Current.Text);

        Assert.Equal(["1", "2", "3"], received);
    }


    [Fact]
    public async Task Streams_Are_Separated_By_Key_And_Type()
    {
        var (actors, _) = Create();
        var heard = new List<string>();
        using var sub = actors.GetStream<Note>("a").Subscribe((n, _) => { lock (heard) heard.Add(n.Text); return default; });

        await actors.GetStream<Note>("b").PublishAsync(new Note("wrong key"), Ct);
        await actors.GetStream<string>("a").PublishAsync("wrong type", Ct);
        await actors.GetStream<Note>("a").PublishAsync(new Note("right"), Ct);

        await Eventually(() => Task.FromResult(heard.Count), c => c > 0);
        await Task.Delay(50, Ct);
        Assert.Equal(["right"], heard);
    }


    [Fact]
    public async Task Disposing_A_Subscription_Stops_Delivery()
    {
        var (actors, _) = Create();
        var count = 0;
        var sub = actors.GetStream<Note>("s").Subscribe((_, _) => { Interlocked.Increment(ref count); return default; });

        await actors.GetStream<Note>("s").PublishAsync(new Note("1"), Ct);
        await Eventually(() => Task.FromResult(Volatile.Read(ref count)), c => c == 1);
        sub.Dispose();
        await actors.GetStream<Note>("s").PublishAsync(new Note("2"), Ct);
        await Task.Delay(50, Ct);

        Assert.Equal(1, count);
    }


    [Fact]
    public async Task Implicit_Consumers_Are_Activated_By_Their_Key()
    {
        var (actors, probe) = Create();

        await Task.WhenAll(Enumerable.Range(0, 100).Select(i => actors.GetStream<Note>("bob").PublishAsync(new Note(i.ToString()), Ct).AsTask()));
        var bob = await Eventually(() => actors.Get<IInbox>("bob").Received(), r => r.Length == 100);
        Assert.Equal(100, bob.Length);
        Assert.Equal(1, probe.MaxConcurrent); // 100 events, one actor: one at a time

        await actors.GetStream<Note>("alice").PublishAsync(new Note("for alice"), Ct);
        Assert.Equal(["for alice"], await Eventually(() => actors.Get<IInbox>("alice").Received(), r => r.Length == 1));
    }


    [Fact]
    public async Task An_Actor_Subscription_Delivers_As_Turns_And_Ends_With_The_Actor()
    {
        var (actors, _) = Create();
        var listener = actors.Get<IListener>("l");
        await listener.Listen("news");

        await actors.GetStream<Note>("news").PublishAsync(new Note("first"), Ct);
        Assert.Equal(["first"], await Eventually(() => listener.Heard(), h => h.Length == 1));

        await actors.DeactivateAsync<IListener>("l");
        await actors.GetStream<Note>("news").PublishAsync(new Note("after"), Ct);

        Assert.Empty(await actors.Get<IListener>("l").Heard()); // a fresh activation, no subscription
    }


    [Fact]
    public void Subscribing_Outside_An_Actor_Needs_No_Actor()
    {
        var (actors, _) = Create();
        using var sub = actors.GetStream<int>("numbers").Subscribe((_, _) => default);
        Assert.Equal(0, actors.ActivationCount);
    }
}
