using System.Text.Json;
using System.Text.Json.Serialization;
using System.Text.Json.Serialization.Metadata;

namespace Shiny.Actors.Remoting;


/// <summary>
/// The JSON metadata remote calls use: every <see cref="JsonSerializerContext"/> in a project with actors
/// (registered by the generator), plus primitives. Web defaults (camelCase) so both ends agree.
/// </summary>
public static class ActorJson
{
    static readonly Lock sync = new();
    static readonly List<IJsonTypeInfoResolver> resolvers = [];
    static JsonSerializerOptions? options;


    public static JsonSerializerOptions Options
    {
        get
        {
            var current = Volatile.Read(ref options);
            if (current is not null)
                return current;

            lock (sync)
            {
                if (options is null)
                {
                    var built = new JsonSerializerOptions(JsonSerializerDefaults.Web)
                    {
                        TypeInfoResolver = JsonTypeInfoResolver.Combine([.. resolvers, RemotingJsonContext.Default])
                    };
                    built.MakeReadOnly();
                    options = built;
                }
                return options;
            }
        }
    }


    /// <summary>Adds metadata the generator could not see - a context from a library without actors, for instance.</summary>
    public static void AddResolver(IJsonTypeInfoResolver resolver)
    {
        lock (sync)
        {
            if (resolvers.Contains(resolver))
                return;

            resolvers.Add(resolver);
            options = null; // options are frozen once used - rebuild with the new resolver
        }
    }


    /// <summary>
    /// Metadata from the first registered <see cref="JsonSerializerContext"/> that declares <paramref name="type"/>, with that
    /// context's own options - the same metadata the generator wires up, so a state's stored format never depends on
    /// which path found it.
    /// </summary>
    public static JsonTypeInfo? FindContextTypeInfo(Type type)
    {
        IJsonTypeInfoResolver[] snapshot;
        lock (sync)
            snapshot = [.. resolvers];

        foreach (var resolver in snapshot)
            if (resolver is JsonSerializerContext context && context.GetTypeInfo(type) is { } info)
                return info;

        return null;
    }


    public static bool CanSerialize(Type type) => Options.TryGetTypeInfo(type, out _);


    public static JsonTypeInfo GetTypeInfo(Type type)
        => Options.TryGetTypeInfo(type, out var info)
            ? info
            : throw new NotSupportedException(
                $"No JSON metadata for '{type.FullName}'. Add [JsonSerializable(typeof({type.Name}))] to a JsonSerializerContext " +
                "in the project that declares the actor interface, or call ActorJson.AddResolver(...)."
            );


    public static JsonTypeInfo<T> GetTypeInfo<T>() => (JsonTypeInfo<T>)GetTypeInfo(typeof(T));
}


/// <summary>What a failed remote call answers with.</summary>
public sealed record RemoteError(string Error, string Message);


[JsonSerializable(typeof(RemoteError))]
[JsonSerializable(typeof(string))]
[JsonSerializable(typeof(bool))]
[JsonSerializable(typeof(byte))]
[JsonSerializable(typeof(short))]
[JsonSerializable(typeof(int))]
[JsonSerializable(typeof(long))]
[JsonSerializable(typeof(float))]
[JsonSerializable(typeof(double))]
[JsonSerializable(typeof(decimal))]
[JsonSerializable(typeof(char))]
[JsonSerializable(typeof(Guid))]
[JsonSerializable(typeof(DateTime))]
[JsonSerializable(typeof(DateTimeOffset))]
[JsonSerializable(typeof(DateOnly))]
[JsonSerializable(typeof(TimeOnly))]
[JsonSerializable(typeof(TimeSpan))]
[JsonSerializable(typeof(bool?))]
[JsonSerializable(typeof(int?))]
[JsonSerializable(typeof(long?))]
[JsonSerializable(typeof(double?))]
[JsonSerializable(typeof(decimal?))]
[JsonSerializable(typeof(Guid?))]
[JsonSerializable(typeof(DateTimeOffset?))]
[JsonSerializable(typeof(TimeSpan?))]
[JsonSerializable(typeof(byte[]))]
[JsonSerializable(typeof(string[]))]
[JsonSerializable(typeof(int[]))]
[JsonSerializable(typeof(long[]))]
[JsonSerializable(typeof(List<string>))]
[JsonSerializable(typeof(List<int>))]
[JsonSerializable(typeof(IReadOnlyList<string>))]
[JsonSerializable(typeof(Dictionary<string, string>))]
[JsonSerializable(typeof(Dictionary<string, int>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, string>))]
[JsonSerializable(typeof(IReadOnlyDictionary<string, int>))]
[JsonSerializable(typeof(JsonElement))]
[JsonSerializable(typeof(System.Text.Json.Nodes.JsonObject))]
partial class RemotingJsonContext : JsonSerializerContext;
