using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.Actors;


/// <summary>
/// One JSON file per state: <c>{root}/{actor name}/{hash(id)}.json</c>, and <c>{hash(id)}.{state name}.json</c>
/// for named states. The files hold plain state JSON; the ETag is a hash of the content.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>Writes go to a temp file that is moved into place - a crash mid-write leaves the previous state intact.</item>
/// <item>The ETag check and the write happen under a lock file, so separate processes sharing the directory are safe.</item>
/// <item>Ids are hashed for file names - any id is safe, including on case-insensitive file systems.</item>
/// </list>
/// In a MAUI app, put the root under <c>FileSystem.AppDataDirectory</c>.
/// </remarks>
public sealed class FileActorStateProvider(string rootDirectory) : IActorStateProvider
{
    static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(30);

    public string RootDirectory => rootDirectory;


    public async ValueTask<StoredState<TState>> ReadAsync<TState>(ActorStateKey key, JsonTypeInfo<TState> typeInfo, CancellationToken cancellationToken) where TState : class
    {
        // a move into place is atomic, so a read never sees half a file and needs no lock
        var bytes = await TryReadAsync(this.GetPath(key), cancellationToken).ConfigureAwait(false);
        return bytes is null ? default : new(JsonSerializer.Deserialize(bytes, typeInfo), ETag(bytes));
    }


    public async ValueTask<string> WriteAsync<TState>(ActorStateKey key, TState state, JsonTypeInfo<TState> typeInfo, string? etag, CancellationToken cancellationToken) where TState : class
    {
        var path = this.GetPath(key);
        var json = JsonSerializer.SerializeToUtf8Bytes(state, typeInfo);
        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        await using var _ = await AcquireLockAsync(path, cancellationToken).ConfigureAwait(false);
        await CheckAsync(key, path, etag, cancellationToken).ConfigureAwait(false);

        var temp = $"{path}.{Guid.NewGuid():N}.tmp";
        try
        {
            await using (var stream = new FileStream(temp, FileMode.CreateNew, FileAccess.Write, FileShare.None, 4096, FileOptions.Asynchronous))
            {
                await stream.WriteAsync(json, cancellationToken).ConfigureAwait(false);
                stream.Flush(flushToDisk: true);
            }
            File.Move(temp, path, overwrite: true);
        }
        catch
        {
            try { File.Delete(temp); } catch { /* best effort */ }
            throw;
        }
        return ETag(json);
    }


    public async ValueTask<string?> ClearAsync(ActorStateKey key, string? etag, CancellationToken cancellationToken)
    {
        var path = this.GetPath(key);
        if (!Directory.Exists(Path.GetDirectoryName(path)))
            return etag is null ? null : throw new ActorStateConflictException(key, etag, null);

        await using var _ = await AcquireLockAsync(path, cancellationToken).ConfigureAwait(false);
        await CheckAsync(key, path, etag, cancellationToken).ConfigureAwait(false);
        File.Delete(path);
        return null;
    }


    internal string GetPath(ActorStateKey key)
    {
        var file = Hash(key.ActorId);
        if (!key.IsDefault)
            file += "." + SafeName(key.StateName, allowDot: false);

        return Path.Combine(rootDirectory, SafeName(key.ActorName, allowDot: true), file + ".json");
    }


    static async ValueTask CheckAsync(ActorStateKey key, string path, string? etag, CancellationToken cancellationToken)
    {
        var current = await TryReadAsync(path, cancellationToken).ConfigureAwait(false) is { } bytes ? ETag(bytes) : null;
        if (current != etag)
            throw new ActorStateConflictException(key, etag, current);
    }


    static async ValueTask<byte[]?> TryReadAsync(string path, CancellationToken cancellationToken)
    {
        try
        {
            return await File.ReadAllBytesAsync(path, cancellationToken).ConfigureAwait(false);
        }
        catch (FileNotFoundException)
        {
            return null;
        }
        catch (DirectoryNotFoundException)
        {
            return null;
        }
    }


    // FileShare.None is an OS-level exclusive lock on every platform .NET runs on, so this also excludes other processes
    static async ValueTask<FileStream> AcquireLockAsync(string path, CancellationToken cancellationToken)
    {
        var deadline = DateTime.UtcNow + LockTimeout;
        while (true)
        {
            try
            {
                return new FileStream(path + ".lock", FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None, 1, FileOptions.DeleteOnClose);
            }
            catch (IOException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(10, cancellationToken).ConfigureAwait(false);
            }
        }
    }


    static string ETag(byte[] json) => Convert.ToHexStringLower(SHA256.HashData(json))[..32];


    static string SafeName(string name, bool allowDot)
    {
        var sb = new StringBuilder(name.Length);
        foreach (var c in name)
            sb.Append(char.IsAsciiLetterOrDigit(c) || c is '-' or '_' || (allowDot && c == '.') ? c : '_');

        var safe = sb.ToString();
        // anything rewritten (or all dots) gets a hash suffix so two names can't collide
        return safe == name && safe.Trim('.').Length > 0 ? safe : $"{safe}-{Hash(name)[..8]}";
    }


    static string Hash(string value) => Convert.ToHexStringLower(SHA256.HashData(Encoding.UTF8.GetBytes(value)));
}
