using System.ComponentModel;

namespace Shiny.Actors.Remoting;


/// <summary>
/// An actor interface described for the wire: what remote callers name it, and how to call each method
/// from JSON arguments. Generated for every actor interface - nothing is reflected over.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Advanced)]
public sealed class ActorContract
{
    public ActorContract(Type interfaceType, string name, IEnumerable<ActorMethod> methods)
    {
        this.InterfaceType = interfaceType;
        this.Name = name;
        this.Methods = methods.ToDictionary(x => x.Key, StringComparer.Ordinal);
        foreach (var method in this.Methods.Values)
            method.Contract = this;
    }

    public Type InterfaceType { get; }

    /// <summary>The wire name - the interface's full name unless pinned with <see cref="ActorNameAttribute"/>.</summary>
    public string Name { get; }

    public IReadOnlyDictionary<string, ActorMethod> Methods { get; }


    /// <summary>Parameter and return types that have no JSON metadata - each would fail at call time.</summary>
    public IEnumerable<Type> GetTypesMissingJsonMetadata()
        => this.Methods.Values
            .SelectMany(m => m.ReturnType is null ? m.ParameterTypes : [.. m.ParameterTypes, m.ReturnType])
            .Distinct()
            .Where(t => !ActorJson.CanSerialize(t));

    public override string ToString() => this.Name;
}


[Flags]
public enum ActorMethodFlags
{
    None = 0,
    OneWay = 1,
    AlwaysInterleave = 2,
    ReadOnly = 4
}


[EditorBrowsable(EditorBrowsableState.Advanced)]
public sealed class ActorMethod(
    string key,
    string name,
    Type[] parameterTypes,
    Type? returnType,
    ActorMethodFlags flags,
    Func<object, object?[], CancellationToken, ValueTask<object?>> invoke
)
{
    /// <summary>The contract this method belongs to - set when the contract is built.</summary>
    public ActorContract Contract { get; internal set; } = null!;

    public ActorMethodFlags Flags => flags;

    /// <summary>The method name, or the name plus its parameter types when the interface overloads it.</summary>
    public string Key => key;
    public string Name => name;

    /// <summary>The parameters sent over the wire - a <see cref="CancellationToken"/> parameter is not one of them.</summary>
    public IReadOnlyList<Type> ParameterTypes => parameterTypes;

    /// <summary>Null for <c>Task</c>/<c>ValueTask</c>.</summary>
    public Type? ReturnType => returnType;

    public bool OneWay => (flags & ActorMethodFlags.OneWay) != 0;

    /// <summary>Calls the method on an actor instance with already-deserialized arguments.</summary>
    public ValueTask<object?> InvokeAsync(object actor, object?[] arguments, CancellationToken cancellationToken)
        => invoke(actor, arguments, cancellationToken);

    public override string ToString() => this.Key;
}
