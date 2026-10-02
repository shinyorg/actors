using System.Text.Json.Serialization;
using Shiny.Actors;

namespace Sample.Maui.Actors;


public record ChatMessage(string Room, string User, string Text, DateTimeOffset At, bool IsSystem = false)
{
    [JsonIgnore]
    public string Display => this.IsSystem ? this.Text : $"{this.User}: {this.Text}";
}

public record RoomSummary(string Id, string Name, string? LastMessage, DateTimeOffset? LastAt);

/// <summary>Published on the "directory" stream whenever a room is created or gets a message.</summary>
public record DirectoryChanged(string RoomId);


public interface IChatRoom : IActor
{
    Task<ChatMessage[]> Recent(int take);
    Task Post(string user, string text);
    Task Join(string user);

    /// <summary>Read-only, so several can run at once - and never beside a post.</summary>
    [ReadOnly] Task<string[]> Members();

    /// <summary>Posts a reminder into the room later - and notifies the phone even if the app is closed.</summary>
    Task RemindIn(string user, int minutes);
}


public interface IRoomDirectory : IActor
{
    [ReadOnly] Task<RoomSummary[]> Rooms();
    Task<string> Create(string name);

    /// <summary>One-way: a room tells the directory about a new message without waiting for it.</summary>
    [OneWay] Task Touch(string roomId, string preview, DateTimeOffset at);
}


public class RoomState
{
    public List<ChatMessage> Messages { get; set; } = [];
}

public class RoomMembers
{
    public List<string> Users { get; set; } = [];
}

public class DirectoryState
{
    public List<RoomSummary> Rooms { get; set; } = [];
}


/// <summary>
/// One chat room. Messages are its own state; members are a second, named state. [AutoSave] writes whichever changed
/// after every call - on a phone the OS can kill the app at any moment, so nothing lives only in memory.
/// </summary>
[AutoSave]
public class ChatRoomActor([ActorState("members")] IActorState<RoomMembers> members) : Actor<RoomState>, IChatRoom, IRemindable
{
    const int Keep = 200;

    public Task<ChatMessage[]> Recent(int take) => Task.FromResult(this.State.Messages.TakeLast(take).ToArray());

    public Task<string[]> Members() => Task.FromResult(members.State.Users.ToArray());


    public async Task Join(string user)
    {
        if (members.State.Users.Contains(user))
            return;

        members.State.Users.Add(user);
        await this.Say(new ChatMessage(this.Id, "", $"{user} joined", DateTimeOffset.UtcNow, IsSystem: true));
    }


    public Task Post(string user, string text) => this.Say(new ChatMessage(this.Id, user, text.Trim(), DateTimeOffset.UtcNow));


    public Task RemindIn(string user, int minutes) => this.RegisterReminderAsync(
        $"remind:{user}:{Guid.NewGuid():N}",
        TimeSpan.FromMinutes(minutes),
        notification: new ReminderNotification($"#{this.Id}", $"{user} asked to be reminded about this room")
    ).AsTask();


    public ValueTask ReceiveReminderAsync(ReminderTick tick, CancellationToken cancellationToken)
    {
        var user = tick.Name.Split(':')[1];
        return new(this.Say(new ChatMessage(this.Id, "", $"⏰ reminder for {user}", DateTimeOffset.UtcNow, IsSystem: true)));
    }


    async Task Say(ChatMessage message)
    {
        this.State.Messages.Add(message);
        if (this.State.Messages.Count > Keep)
            this.State.Messages.RemoveRange(0, this.State.Messages.Count - Keep);

        // a durable stream: a phone that drops off Wi-Fi resumes from where it was, without gaps
        await this.Actors.GetStream<ChatMessage>(this.Id).PublishAsync(message);
        await this.Actors.Get<IRoomDirectory>(RoomDirectoryActor.Id_).Touch(this.Id, message.IsSystem ? message.Text : $"{message.User}: {message.Text}", message.At);
    }
}


[AutoSave]
public class RoomDirectoryActor : Actor<DirectoryState>, IRoomDirectory
{
    public const string Id_ = "all";

    protected override ValueTask OnActivateAsync(CancellationToken cancellationToken)
    {
        if (this.State.Rooms.Count == 0)
            this.State.Rooms.Add(new RoomSummary("general", "General", null, null));
        return default;
    }


    public Task<RoomSummary[]> Rooms() => Task.FromResult(this.State.Rooms.OrderByDescending(x => x.LastAt ?? DateTimeOffset.MinValue).ToArray());


    public async Task<string> Create(string name)
    {
        var id = new string([.. name.Trim().ToLowerInvariant().Select(c => char.IsLetterOrDigit(c) ? c : '-')]).Trim('-');
        if (id.Length == 0)
            throw new ArgumentException("A room needs a name with at least one letter or digit.");

        if (this.State.Rooms.All(x => x.Id != id))
        {
            this.State.Rooms.Add(new RoomSummary(id, name.Trim(), null, null));
            await this.Actors.GetStream<DirectoryChanged>(Id_).PublishAsync(new DirectoryChanged(id));
        }
        return id;
    }


    public async Task Touch(string roomId, string preview, DateTimeOffset at)
    {
        var index = this.State.Rooms.FindIndex(x => x.Id == roomId);
        var room = index >= 0 ? this.State.Rooms[index] : new RoomSummary(roomId, roomId, null, null);
        room = room with { LastMessage = preview, LastAt = at };

        if (index >= 0)
            this.State.Rooms[index] = room;
        else
            this.State.Rooms.Add(room);

        await this.Actors.GetStream<DirectoryChanged>(Id_).PublishAsync(new DirectoryChanged(roomId));
    }
}


[JsonSerializable(typeof(ChatMessage))]
[JsonSerializable(typeof(ChatMessage[]))]
[JsonSerializable(typeof(RoomSummary[]))]
[JsonSerializable(typeof(DirectoryChanged))]
[JsonSerializable(typeof(RoomState))]
[JsonSerializable(typeof(RoomMembers))]
[JsonSerializable(typeof(DirectoryState))]
public partial class ChatJson : JsonSerializerContext;
