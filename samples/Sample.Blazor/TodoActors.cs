using System.Text.Json.Serialization;
using Shiny.Actors;

namespace Sample.Blazor;


public interface ITodoList : IActor
{
    Task<TodoItem[]> Items();
    Task Add(string text);
    Task Toggle(int id);
    Task Remove(int id);
    Task<string[]> History();
    Task RemindMe(int seconds);
}


public record TodoItem(int Id, string Text, bool Done);

public class TodoState
{
    public List<TodoItem> Items { get; set; } = [];
    public int NextId { get; set; } = 1;
}


[JsonPolymorphic]
[JsonDerivedType(typeof(ItemAdded), "added")]
[JsonDerivedType(typeof(ItemToggled), "toggled")]
[JsonDerivedType(typeof(ItemRemoved), "removed")]
public abstract record TodoEvent;
public record ItemAdded(int Id, string Text) : TodoEvent;
public record ItemToggled(int Id) : TodoEvent;
public record ItemRemoved(int Id) : TodoEvent;


/// <summary>Published after every change, so any component showing the list refreshes.</summary>
public record TodoChanged(string ListId, string What);


/// <summary>
/// Event-sourced: IndexedDB holds the events, never the list itself. [AutoSave] appends the raised events after
/// every call, and a reminder works while the tab is open.
/// </summary>
[AutoSave]
public class TodoListActor : JournaledActor<TodoState, TodoEvent>, ITodoList, IRemindable
{
    protected override void Apply(TodoState state, TodoEvent @event)
    {
        switch (@event)
        {
            case ItemAdded added:
                state.Items.Add(new TodoItem(added.Id, added.Text, false));
                state.NextId = Math.Max(state.NextId, added.Id + 1);
                break;

            case ItemToggled toggled:
                var index = state.Items.FindIndex(x => x.Id == toggled.Id);
                if (index >= 0)
                    state.Items[index] = state.Items[index] with { Done = !state.Items[index].Done };
                break;

            case ItemRemoved removed:
                state.Items.RemoveAll(x => x.Id == removed.Id);
                break;
        }
    }


    public Task<TodoItem[]> Items() => Task.FromResult(this.State.Items.ToArray());


    public Task Add(string text) => this.Change(new ItemAdded(this.State.NextId, text), $"added \"{text}\"");

    public Task Toggle(int id) => this.Change(new ItemToggled(id), $"toggled #{id}");

    public Task Remove(int id) => this.Change(new ItemRemoved(id), $"removed #{id}");


    public async Task<string[]> History()
    {
        var lines = new List<string>();
        await foreach (var e in this.ReadEventsAsync())
            lines.Add($"v{e.Version}  {e.Timestamp.LocalDateTime:T}  {e.Event}");
        return [.. lines];
    }


    public Task RemindMe(int seconds)
        => this.RegisterReminderAsync("nudge", TimeSpan.FromSeconds(seconds)).AsTask();


    public async ValueTask ReceiveReminderAsync(ReminderTick tick, CancellationToken cancellationToken)
    {
        var open = this.State.Items.Count(x => !x.Done);
        await this.Actors.GetStream<TodoChanged>(this.Id).PublishAsync(new TodoChanged(this.Id, $"⏰ reminder: {open} item(s) still open"), cancellationToken);
    }


    async Task Change(TodoEvent @event, string what)
    {
        this.RaiseEvent(@event);
        await this.Actors.GetStream<TodoChanged>(this.Id).PublishAsync(new TodoChanged(this.Id, what));
    }
}


[JsonSerializable(typeof(TodoState))]
[JsonSerializable(typeof(TodoEvent))]
[JsonSerializable(typeof(TodoItem[]))]
[JsonSerializable(typeof(TodoChanged))]
public partial class SampleJson : JsonSerializerContext;
