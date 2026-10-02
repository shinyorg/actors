using System.Text.Json;

namespace Shiny.Actors.Remoting;


/// <summary>The wire format: arguments travel as a JSON array, results as a JSON value.</summary>
public static class ActorWire
{
    public static byte[] WriteArguments(ActorMethod method, object?[] arguments)
    {
        if (arguments.Length != method.ParameterTypes.Count)
            throw new ArgumentException($"'{method.Key}' takes {method.ParameterTypes.Count} arguments, not {arguments.Length}.", nameof(arguments));

        using var buffer = new MemoryStream();
        using (var writer = new Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            for (var i = 0; i < arguments.Length; i++)
                JsonSerializer.Serialize(writer, arguments[i], ActorJson.GetTypeInfo(method.ParameterTypes[i]));
            writer.WriteEndArray();
        }
        return buffer.ToArray();
    }


    /// <exception cref="RemoteActorException">400 - the body does not match the method.</exception>
    public static object?[] ReadArguments(ActorMethod method, ReadOnlyMemory<byte> json)
    {
        var types = method.ParameterTypes;
        if (json.IsEmpty)
            return types.Count == 0 ? [] : throw BadArguments(method, $"expected a JSON array of {types.Count} arguments, got no body");

        try
        {
            using var document = JsonDocument.Parse(json);
            var root = document.RootElement;
            if (root.ValueKind != JsonValueKind.Array || root.GetArrayLength() != types.Count)
                throw BadArguments(method, $"expected a JSON array of {types.Count} arguments");

            var values = new object?[types.Count];
            var i = 0;
            foreach (var element in root.EnumerateArray())
            {
                var type = types[i];
                values[i] = element.Deserialize(ActorJson.GetTypeInfo(type));
                if (values[i] is null && type.IsValueType && Nullable.GetUnderlyingType(type) is null)
                    throw BadArguments(method, $"argument {i} cannot be null");
                i++;
            }
            return values;
        }
        catch (JsonException ex)
        {
            throw BadArguments(method, ex.Message);
        }
    }


    /// <summary>The header that carries <see cref="ActorRequestContext"/> across a remote call.</summary>
    public const string RequestContextHeader = "x-actor-context";


    public static string? EncodeRequestContext(IReadOnlyDictionary<string, string> context)
    {
        if (context.Count == 0)
            return null;

        var json = JsonSerializer.SerializeToUtf8Bytes(new Dictionary<string, string>(context), RemotingJsonContext.Default.DictionaryStringString);
        return Convert.ToBase64String(json).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }


    /// <summary>Null for a missing or malformed header - a bad context is dropped, never fatal.</summary>
    public static IReadOnlyDictionary<string, string>? DecodeRequestContext(string? header)
    {
        if (string.IsNullOrEmpty(header))
            return null;

        try
        {
            var base64 = header.Replace('-', '+').Replace('_', '/');
            base64 = base64.PadRight(base64.Length + (4 - base64.Length % 4) % 4, '=');
            return JsonSerializer.Deserialize(Convert.FromBase64String(base64), RemotingJsonContext.Default.DictionaryStringString);
        }
        catch (Exception ex) when (ex is FormatException or JsonException)
        {
            return null;
        }
    }


    /// <summary>The wire name of a stream's event type - stable across versions, unlike an assembly-qualified name.</summary>
    public static string StreamName(Type eventType)
    {
        if (!eventType.IsGenericType)
            return eventType.FullName ?? eventType.Name;

        var name = eventType.GetGenericTypeDefinition().FullName ?? eventType.Name;
        return $"{name}[{string.Join(",", eventType.GetGenericArguments().Select(StreamName))}]";
    }


    /// <summary>
    /// Makes an id, key or name safe as one URL path segment through any proxy or router: plain names
    /// (letters, digits, <c>-</c>, <c>_</c>, <c>.</c>) travel as-is, anything else as <c>~</c> + base64url. Percent-encoding
    /// is not enough - some hops decode <c>%2F</c> before routing, and <c>..</c> is normalized away.
    /// </summary>
    public static string EncodeSegment(string value)
    {
        var plain = value.Length > 0 && value[0] != '.' && value.All(c => char.IsAsciiLetterOrDigit(c) || c is '-' or '_' or '.');
        return plain ? value : "~" + Convert.ToBase64String(System.Text.Encoding.UTF8.GetBytes(value)).TrimEnd('=').Replace('+', '-').Replace('/', '_');
    }


    /// <exception cref="RemoteActorException">400 - a <c>~</c> segment that is not valid base64url.</exception>
    public static string DecodeSegment(string segment)
    {
        if (!segment.StartsWith('~'))
            return segment;

        var base64 = segment[1..].Replace('-', '+').Replace('_', '/');
        base64 = base64.PadRight(base64.Length + (4 - base64.Length % 4) % 4, '=');
        try
        {
            return System.Text.Encoding.UTF8.GetString(Convert.FromBase64String(base64));
        }
        catch (FormatException)
        {
            throw new RemoteActorException(400, "BadSegment", $"'{segment}' is not a valid encoded path segment.");
        }
    }


    static RemoteActorException BadArguments(ActorMethod method, string reason)
        => new(400, "BadArguments", $"Bad arguments for '{method.Key}': {reason}.");
}
