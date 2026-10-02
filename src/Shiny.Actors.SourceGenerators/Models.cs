using System;
using System.Collections;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using Microsoft.CodeAnalysis;

namespace Shiny.Actors.SourceGenerators;


enum ReturnKind { Task, TaskOfT, ValueTask, ValueTaskOfT }


sealed record ParameterModel(string Type, string TypeOf, string Name, bool IsCancellationToken);


sealed record MethodModel(
    string DeclaringInterface,
    string Name,
    string Key,
    string ReturnType,
    ReturnKind Kind,
    string? ResultType,
    string? ResultTypeOf,
    EquatableArray<ParameterModel> Parameters,
    bool OneWay,
    bool AlwaysInterleave = false,
    bool ReadOnly = false
);


sealed record InterfaceModel(
    string FullName,
    string WireName,
    string ProxyName,
    EquatableArray<MethodModel> Methods,
    EquatableArray<DiagnosticInfo> Diagnostics
);


sealed record CtorParameterModel(string Type, string? ServiceKey, bool Optional, string? DefaultLiteral, string? StateName = null, string? StateProvider = null, string? StateType = null);


sealed record ActorModel(
    string FullName,
    string ActorName,
    EquatableArray<string> Interfaces,
    EquatableArray<string> ImplicitStreams,
    string? StateType,
    bool OverridesStateTypeInfo,
    string? StateProvider,
    EquatableArray<CtorParameterModel> CtorParameters,
    EquatableArray<DiagnosticInfo> Diagnostics,
    Location? Location,
    bool IsValid,
    bool IsReentrant,
    int? AutoSave,
    int MaxWorkers = 0,
    string? EventType = null,
    bool OverridesEventTypeInfo = false
);


sealed record ContextModel(string FullName, EquatableArray<string> Types);


sealed record DiagnosticInfo(DiagnosticDescriptor Descriptor, Location? Location, EquatableArray<string> Arguments)
{
    public static DiagnosticInfo Create(DiagnosticDescriptor descriptor, Location? location, params string[] args)
        => new(descriptor, location, new EquatableArray<string>(args.ToImmutableArray()));

    public Diagnostic ToDiagnostic() => Diagnostic.Create(this.Descriptor, this.Location, this.Arguments.ToArray<object>());
}


/// <summary>An immutable array with value equality, so incremental steps can cache.</summary>
readonly struct EquatableArray<T> : IEquatable<EquatableArray<T>>, IEnumerable<T>
{
    readonly ImmutableArray<T> items;

    public EquatableArray(ImmutableArray<T> items) => this.items = items;

    public ImmutableArray<T> Items => this.items.IsDefault ? ImmutableArray<T>.Empty : this.items;
    public int Length => this.Items.Length;

    public bool Equals(EquatableArray<T> other) => this.Items.SequenceEqual(other.Items);
    public override bool Equals(object? obj) => obj is EquatableArray<T> other && this.Equals(other);

    public override int GetHashCode()
    {
        var hash = 17;
        foreach (var item in this.Items)
            hash = unchecked(hash * 31 + (item?.GetHashCode() ?? 0));
        return hash;
    }

    public U[] ToArray<U>() => this.Items.Cast<U>().ToArray();
    public IEnumerator<T> GetEnumerator() => ((IEnumerable<T>)this.Items).GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => this.GetEnumerator();
}


static class EquatableArrayExtensions
{
    public static EquatableArray<T> ToEquatable<T>(this IEnumerable<T> source) => new(source.ToImmutableArray());
}
