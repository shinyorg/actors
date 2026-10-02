using System.Collections.Immutable;

namespace Shiny.Actors;


/// <summary>
/// Values that travel with a call - to the actor, to every actor it calls, and across a remote call (as a header).
/// For correlation ids, tenant, culture, the user a call is made on behalf of. Changes made inside an actor flow on
/// to the calls it makes, never back to its caller.
/// </summary>
public static class ActorRequestContext
{
    static readonly AsyncLocal<ImmutableDictionary<string, string>?> values = new();


    public static IReadOnlyDictionary<string, string> Current => values.Value ?? ImmutableDictionary<string, string>.Empty;

    public static string? Get(string key) => values.Value?.GetValueOrDefault(key);

    public static void Set(string key, string value)
        => values.Value = (values.Value ?? ImmutableDictionary.Create<string, string>(StringComparer.Ordinal)).SetItem(key, value);

    public static bool Remove(string key)
    {
        var current = values.Value;
        if (current is null || !current.ContainsKey(key))
            return false;

        values.Value = current.Remove(key);
        return true;
    }

    public static void Clear() => values.Value = null;


    internal static ImmutableDictionary<string, string>? Snapshot => values.Value;

    internal static void Restore(ImmutableDictionary<string, string>? snapshot) => values.Value = snapshot;

    internal static void Restore(IReadOnlyDictionary<string, string>? incoming)
        => values.Value = incoming is null || incoming.Count == 0 ? null : incoming.ToImmutableDictionary(StringComparer.Ordinal);
}
