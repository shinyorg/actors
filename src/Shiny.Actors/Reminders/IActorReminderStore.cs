using System.Collections.Concurrent;
using System.Text.Json;

namespace Shiny.Actors;


/// <summary>Where reminders live. Reminders are few and small, so the whole set is read once at startup.</summary>
public interface IActorReminderStore
{
    ValueTask<IReadOnlyList<ActorReminder>> GetAllAsync(CancellationToken cancellationToken);

    /// <summary>Adds or replaces the reminder with the same actor name, actor id and reminder name.</summary>
    ValueTask SaveAsync(ActorReminder reminder, CancellationToken cancellationToken);

    ValueTask RemoveAsync(ActorReminder reminder, CancellationToken cancellationToken);
}


/// <summary>The default. Reminders fire while the process lives but do not survive a restart.</summary>
public sealed class InMemoryActorReminderStore : IActorReminderStore
{
    readonly ConcurrentDictionary<(string, string, string), ActorReminder> reminders = new();

    public ValueTask<IReadOnlyList<ActorReminder>> GetAllAsync(CancellationToken cancellationToken)
        => new([.. this.reminders.Values]);

    public ValueTask SaveAsync(ActorReminder reminder, CancellationToken cancellationToken)
    {
        this.reminders[reminder.Key] = reminder;
        return default;
    }

    public ValueTask RemoveAsync(ActorReminder reminder, CancellationToken cancellationToken)
    {
        this.reminders.TryRemove(reminder.Key, out _);
        return default;
    }
}


/// <summary>
/// All reminders in one JSON file, rewritten atomically (temp file + move) on every change.
/// <see cref="ActorSystemOptions.UseFileStateProvider"/> puts one next to the state files.
/// </summary>
public sealed class FileActorReminderStore(string filePath) : IActorReminderStore
{
    readonly SemaphoreSlim gate = new(1, 1);
    Dictionary<(string, string, string), ActorReminder>? reminders;

    public string FilePath => filePath;


    public async ValueTask<IReadOnlyList<ActorReminder>> GetAllAsync(CancellationToken cancellationToken)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            return [.. (await this.LoadAsync(cancellationToken).ConfigureAwait(false)).Values];
        }
        finally
        {
            this.gate.Release();
        }
    }


    public ValueTask SaveAsync(ActorReminder reminder, CancellationToken cancellationToken)
        => this.ChangeAsync(all => all[reminder.Key] = reminder, cancellationToken);

    public ValueTask RemoveAsync(ActorReminder reminder, CancellationToken cancellationToken)
        => this.ChangeAsync(all => all.Remove(reminder.Key), cancellationToken);


    async ValueTask ChangeAsync(Action<Dictionary<(string, string, string), ActorReminder>> change, CancellationToken cancellationToken)
    {
        await this.gate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var all = await this.LoadAsync(cancellationToken).ConfigureAwait(false);
            change(all);

            Directory.CreateDirectory(Path.GetDirectoryName(Path.GetFullPath(filePath))!);
            var temp = $"{filePath}.{Guid.NewGuid():N}.tmp";
            await using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
            {
                await JsonSerializer.SerializeAsync(stream, [.. all.Values], ActorsJsonContext.Default.ListActorReminder, cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }
            File.Move(temp, filePath, overwrite: true);
        }
        finally
        {
            this.gate.Release();
        }
    }


    async ValueTask<Dictionary<(string, string, string), ActorReminder>> LoadAsync(CancellationToken cancellationToken)
    {
        if (this.reminders is not null)
            return this.reminders;

        List<ActorReminder>? stored = null;
        if (File.Exists(filePath))
        {
            await using var stream = new FileStream(filePath, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.Asynchronous);
            stored = await JsonSerializer.DeserializeAsync(stream, ActorsJsonContext.Default.ListActorReminder, cancellationToken).ConfigureAwait(false);
        }
        return this.reminders = (stored ?? []).ToDictionary(x => x.Key);
    }
}
