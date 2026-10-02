using System.Text.Json.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Shiny.Actors;
using Shiny.Actors.Remoting;
using Microsoft.Extensions.Logging;
using Shiny.DocumentDb.Sqlite;
using Shiny.Net.HttpServer;

// the "device": an actor system served over HTTP
var builder = HttpServer.CreateBuilder();
builder.Configure((HttpServerOptions o) => o.Port = 0);
// state and reminders in one SQLite file - what a phone would use
builder.Services.AddShinyActors(actors => actors.UseDocumentDb(new SqliteDatabaseProvider($"Data Source={Path.Combine(AppContext.BaseDirectory, "actors.db")}")));

var server = builder.Build();
server.MapActors(o => o.Expose<ICart>().Expose<IStoreSales>().ExposeStream<OrderPlaced>());
await server.StartAsync();

// the "phone": the same IActorSystem, remotely - nothing below knows the difference
IActorSystem actors = new RemoteActorSystem(new HttpClient { BaseAddress = new Uri(server.ListenUrl!.TrimEnd('/') + "/actors/") });
Console.WriteLine($"serving actors at {server.ListenUrl}/actors (calls below go over HTTP)");

// a UI (or anything else) can watch a stream with await foreach
var feed = actors.GetStream<OrderPlaced>("store-1");
var watching = Task.Run(async () =>
{
    await foreach (var order in feed.ReadAllAsync())
        Console.WriteLine($"  [feed] {order.Sku} x{order.Quantity}");
});

var cart = actors.Get<ICart>("allan");
await cart.Add("coffee", 2);
await cart.Add("donut", 1);
Console.WriteLine($"cart: {string.Join(", ", (await cart.Items()).Select(x => $"{x.Key} x{x.Value}"))}");

await cart.Checkout("store-1");
await Task.Delay(100);

var sales = actors.Get<IStoreSales>("store-1");
Console.WriteLine($"store-1 has sold {await sales.UnitsSold()} units in total, best seller: {await sales.BestSeller()} (persisted across runs)");

var local = server.Services!.GetRequiredService<ActorSystem>();
await local.DisposeAsync();
await server.StopAsync();


// --- contracts --------------------------------------------------------------

public interface ICart : IActor
{
    Task Add(string sku, int quantity);
    Task<IReadOnlyDictionary<string, int>> Items();
    Task Checkout(string storeId);
}

public interface IStoreSales : IActor
{
    Task<int> UnitsSold();
    Task<string?> BestSeller();
}

public record OrderPlaced(string Sku, int Quantity);


// --- actors -----------------------------------------------------------------

public class CartState
{
    public Dictionary<string, int> Items { get; set; } = [];
}

public class CartActor : Actor<CartState>, ICart
{
    public async Task Add(string sku, int quantity)
    {
        this.State.Items[sku] = this.State.Items.GetValueOrDefault(sku) + quantity;
        await this.WriteStateAsync();
    }

    public Task<IReadOnlyDictionary<string, int>> Items() => Task.FromResult<IReadOnlyDictionary<string, int>>(this.State.Items);

    public async Task Checkout(string storeId)
    {
        var stream = this.Actors.GetStream<OrderPlaced>(storeId);
        foreach (var (sku, quantity) in this.State.Items)
            await stream.PublishAsync(new OrderPlaced(sku, quantity));

        await this.ClearStateAsync();
    }
}


public class SalesState
{
    public int Units { get; set; }
}

/// <summary>Implicitly subscribed: every OrderPlaced on stream "store-1" lands on the actor with id "store-1".</summary>
public class SkuTally
{
    public Dictionary<string, int> Units { get; set; } = [];
}

/// <summary>Two states - its own and a named tally - saved automatically after every call.</summary>
[AutoSave]
public class StoreSalesActor([ActorState("by-sku")] IActorState<SkuTally> tally) : Actor<SalesState>, IStoreSales, IActorStreamConsumer<OrderPlaced>, IRemindable
{
    public Task<string?> BestSeller() => Task.FromResult(tally.State.Units.OrderByDescending(x => x.Value).Select(x => x.Key).FirstOrDefault());

    public async ValueTask OnNextAsync(OrderPlaced item, CancellationToken cancellationToken)
    {
        this.State.Units += item.Quantity;
        tally.State.Units[item.Sku] = tally.State.Units.GetValueOrDefault(item.Sku) + item.Quantity;

        // a persistent nightly report - survives restarts, and Shiny.Actors.Jobs fires it from the background on a phone
        await this.RegisterReminderAsync("nightly-report", TimeSpan.FromHours(12), TimeSpan.FromDays(1), cancellationToken: cancellationToken);
    }

    public ValueTask ReceiveReminderAsync(ReminderTick tick, CancellationToken cancellationToken)
    {
        this.Logger.LogInformation("{Store} sold {Units} units", this.Id, this.State.Units);
        return default;
    }

    public Task<int> UnitsSold() => Task.FromResult(this.State.Units);
}


[JsonSerializable(typeof(CartState))]
[JsonSerializable(typeof(SalesState))]
[JsonSerializable(typeof(OrderPlaced))]
[JsonSerializable(typeof(SkuTally))]
partial class SampleJson : JsonSerializerContext;
