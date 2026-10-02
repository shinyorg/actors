using System.Globalization;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;

namespace Shiny.Actors;


/// <summary>
/// Each log is a folder of batch files, <c>{first}-{last}.json</c> - one per append, so a batch is written (and
/// survives a crash) as a unit. Appends take a lock file, so separate processes sharing the folder are safe; reads
/// never need it because a batch file appears atomically.
/// </summary>
public sealed class FileActorEventStore(string rootDirectory) : IActorEventStore
{
    static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(30);

    public string RootDirectory => rootDirectory;


    public async IAsyncEnumerable<StoredEvent> ReadAsync(ActorStateKey key, long afterVersion, [EnumeratorCancellation] CancellationToken cancellationToken)
    {
        foreach (var (first, last, path) in Batches(this.GetFolder(key)))
        {
            if (last <= afterVersion)
                continue;

            byte[] bytes;
            try
            {
                bytes = await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
            }
            catch (FileNotFoundException)
            {
                continue; // trimmed while we read
            }

            using var document = JsonDocument.Parse(bytes);
            var timestamp = document.RootElement.GetProperty("t").GetDateTimeOffset();
            var version = first;
            foreach (var e in document.RootElement.GetProperty("e").EnumerateArray())
            {
                if (version > afterVersion)
                    yield return new StoredEvent(version, timestamp, Encoding.UTF8.GetBytes(e.GetRawText()));
                version++;
            }
        }
    }


    public async ValueTask<long> AppendAsync(ActorStateKey key, long expectedVersion, IReadOnlyList<ReadOnlyMemory<byte>> events, DateTimeOffset timestamp, CancellationToken cancellationToken)
    {
        var folder = this.GetFolder(key);
        Directory.CreateDirectory(folder);

        await using var _ = await AcquireLockAsync(folder, cancellationToken).ConfigureAwait(false);
        var current = CurrentVersion(folder);
        if (current != expectedVersion)
            throw new ActorStateConflictException(key, expectedVersion.ToString(CultureInfo.InvariantCulture), current.ToString(CultureInfo.InvariantCulture));

        if (events.Count == 0)
            return current;

        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartObject();
            writer.WriteString("t", timestamp);
            writer.WriteStartArray("e");
            foreach (var e in events)
                writer.WriteRawValue(e.Span, skipInputValidation: false);
            writer.WriteEndArray();
            writer.WriteEndObject();
        }

        var last = current + events.Count;
        var path = Path.Combine(folder, $"{current + 1:D12}-{last:D12}.json");
        var temp = path + ".tmp";
        await using (var stream = new FileStream(temp, FileMode.Create, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
        {
            await stream.WriteAsync(buffer.GetBuffer().AsMemory(0, (int)buffer.Length), cancellationToken).ConfigureAwait(false);
            stream.Flush(flushToDisk: true);
        }
        File.Move(temp, path);
        WriteVersionFloor(folder, last);
        return last;
    }


    public async ValueTask<long> GetVersionAsync(ActorStateKey key, CancellationToken cancellationToken)
    {
        await Task.CompletedTask.ConfigureAwait(false);
        return CurrentVersion(this.GetFolder(key));
    }


    public async ValueTask TrimAsync(ActorStateKey key, long throughVersion, CancellationToken cancellationToken)
    {
        var folder = this.GetFolder(key);
        if (!Directory.Exists(folder))
            return;

        await using var _ = await AcquireLockAsync(folder, cancellationToken).ConfigureAwait(false);
        foreach (var (_, last, path) in Batches(folder))
            if (last <= throughVersion)
                File.Delete(path); // a batch straddling the line stays whole; readers skip by version
    }


    string GetFolder(ActorStateKey key)
        => Path.Combine(rootDirectory, Safe(key.ActorName), $"{Hash(key.ActorId)}.{Safe(key.StateName)}.events");


    static IEnumerable<(long First, long Last, string Path)> Batches(string folder)
    {
        if (!Directory.Exists(folder))
            return [];

        return Directory.EnumerateFiles(folder, "*.json")
            .Select(path =>
            {
                var name = Path.GetFileNameWithoutExtension(path).Split('-');
                return (First: long.Parse(name[0], CultureInfo.InvariantCulture), Last: long.Parse(name[1], CultureInfo.InvariantCulture), Path: path);
            })
            .OrderBy(x => x.First);
    }


    // the newest batch names the current version; once trimmed to nothing, a floor file remembers it
    static long CurrentVersion(string folder)
    {
        var last = Batches(folder).Select(x => x.Last).DefaultIfEmpty(0).Max();
        var floor = Path.Combine(folder, "version");
        return File.Exists(floor) && long.TryParse(File.ReadAllText(floor), CultureInfo.InvariantCulture, out var stored) ? Math.Max(last, stored) : last;
    }


    static void WriteVersionFloor(string folder, long version)
    {
        var path = Path.Combine(folder, "version");
        var temp = path + ".tmp";
        File.WriteAllText(temp, version.ToString(CultureInfo.InvariantCulture));
        File.Move(temp, path, overwrite: true);
    }


    static async ValueTask<FileStream> AcquireLockAsync(string folder, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + LockTimeout;
        while (true)
        {
            try
            {
                return new FileStream(Path.Combine(folder, ".lock"), FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.DeleteOnClose);
            }
            catch (IOException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }
        }
    }


    static string Safe(string name)
    {
        var sb = new StringBuilder(name.Length);
        foreach (var c in name)
            sb.Append(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.' ? c : '_');
        var safe = sb.ToString();
        return safe == name && safe.Trim('.').Length > 0 ? safe : $"{safe}-{Hash(name)[..8]}";
    }


    static string Hash(string value) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
}
