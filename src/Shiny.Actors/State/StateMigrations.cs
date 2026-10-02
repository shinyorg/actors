using System.Collections.Concurrent;
using System.Reflection;
using System.Text.Json.Nodes;

namespace Shiny.Actors;


/// <summary>
/// The current shape of a state (or event) type. Raise it when the type changes incompatibly, and register a
/// migration for each step with <c>options.AddStateMigration&lt;T&gt;(fromVersion, json =&gt; ...)</c>. Unversioned
/// types are version 1. Stored JSON carries <c>"$v"</c> only once a type is versioned.
/// </summary>
[AttributeUsage(AttributeTargets.Class | AttributeTargets.Struct, Inherited = false)]
public sealed class StateVersionAttribute(int version) : Attribute
{
    public int Version => version;
}


sealed class StateSchema
{
    public const string VersionProperty = "$v";

    readonly SortedDictionary<int, Action<JsonObject>> migrations;

    StateSchema(Type type, int current, SortedDictionary<int, Action<JsonObject>> migrations)
    {
        this.Type = type;
        this.Current = current;
        this.migrations = migrations;
    }


    public Type Type { get; }
    public int Current { get; }

    /// <summary>Whether stored JSON is stamped and migrated - otherwise it is plain state JSON.</summary>
    public bool IsVersioned => this.Current > 1 || this.migrations.Count > 0;


    public static StateSchema Create(Type type, IReadOnlyDictionary<int, Action<JsonObject>>? migrations)
    {
        var current = type.GetCustomAttribute<StateVersionAttribute>(false)?.Version ?? 1;
        var steps = new SortedDictionary<int, Action<JsonObject>>(migrations?.ToDictionary() ?? []);

        var stray = steps.Keys.Where(v => v < 1 || v >= current).ToList();
        if (stray.Count > 0)
            throw new InvalidOperationException(
                $"'{type.Name}' is [StateVersion({current})] but has migrations from version(s) {string.Join(", ", stray)} - a migration upgrades from N to N+1, for N below the current version."
            );

        return new StateSchema(type, current, steps);
    }


    /// <summary>Brings stored JSON up to <see cref="Current"/> and removes the stamp.</summary>
    public JsonObject Upgrade(JsonObject json)
    {
        var stored = json[VersionProperty]?.GetValue<int>() ?? 1;
        json.Remove(VersionProperty);

        if (stored > this.Current)
            throw new InvalidOperationException(
                $"Stored '{this.Type.Name}' is version {stored}, but this app knows only up to {this.Current} - it was written by a newer version of the app."
            );

        for (var version = stored; version < this.Current; version++)
        {
            if (!this.migrations.TryGetValue(version, out var migrate))
                throw new InvalidOperationException(
                    $"Stored '{this.Type.Name}' is version {stored}, and there is no migration from version {version} to {version + 1}. " +
                    $"Add options.AddStateMigration<{this.Type.Name}>({version}, json => ...)."
                );
            migrate(json);
        }
        return json;
    }


    public JsonObject Stamp(JsonObject json)
    {
        json[VersionProperty] = this.Current;
        return json;
    }
}


sealed class StateSchemas(IReadOnlyDictionary<Type, Dictionary<int, Action<JsonObject>>> migrations)
{
    readonly ConcurrentDictionary<Type, StateSchema> schemas = new();

    public StateSchema For(Type type) => this.schemas.GetOrAdd(type, t => StateSchema.Create(t, migrations.GetValueOrDefault(t)));
}
