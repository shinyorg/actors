using Microsoft.Extensions.DependencyInjection;

namespace Shiny.Actors.Tests;


static class TestHelpers
{
    public static CancellationToken Ct => TestContext.Current.CancellationToken;


    public static (ActorSystem Actors, Probe Probe, ServiceProvider Services) Create(Action<ActorSystemOptions>? configure = null)
    {
        var probe = new Probe();
        var services = new ServiceCollection()
            .AddSingleton(probe)
            .AddShinyActors(actors => { if (configure is not null) actors.Configure(configure); })
            .BuildServiceProvider();

        return (services.GetRequiredService<ActorSystem>(), probe, services);
    }


    public static async Task<T> Eventually<T>(Func<Task<T>> read, Func<T, bool> done, string because)
    {
        var timeout = DateTime.UtcNow.AddSeconds(5);
        while (true)
        {
            var value = await read();
            if (done(value))
                return value;
            if (DateTime.UtcNow > timeout)
                Assert.Fail("Timed out waiting until " + because);
            await Task.Delay(10, Ct);
        }
    }
}
