using Microsoft.CodeAnalysis;

namespace Shiny.Actors.SourceGenerators;


static class Diagnostics
{
    const string Category = "Shiny.Actors";

    public static readonly DiagnosticDescriptor UnsupportedMember = new(
        "SACT001", "Unsupported actor interface member",
        "'{0}' cannot be part of an actor interface: {1}",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor UnsupportedReturnType = new(
        "SACT002", "Actor methods must be asynchronous",
        "'{0}' returns '{1}'; actor methods must return Task, Task<T>, ValueTask or ValueTask<T>",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor OneWayReturnsValue = new(
        "SACT003", "[OneWay] methods cannot return a value",
        "'{0}' is [OneWay] so its caller never sees a result; return Task or ValueTask",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor NotAnActor = new(
        "SACT004", "Actor implementations must derive from Actor",
        "'{0}' implements actor interface '{1}' but does not derive from Shiny.Actors.Actor",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor CannotConstruct = new(
        "SACT005", "Actor cannot be constructed by generated code",
        "Actor '{0}' cannot be created: {1}",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor DuplicateImplementation = new(
        "SACT006", "Several actors implement the same interface",
        "'{0}' is implemented by {1}; choose one with options.AddActor<{2}, TImplementation>() or Get<{2}> will throw",
        Category, DiagnosticSeverity.Warning, true
    );

    public static readonly DiagnosticDescriptor MissingStateMetadata = new(
        "SACT007", "Actor state type has no JSON metadata",
        "State type '{0}' of actor '{1}' is not declared on any JsonSerializerContext in this project; add [JsonSerializable(typeof({0}))] or override StateTypeInfo",
        Category, DiagnosticSeverity.Warning, true
    );

    public static readonly DiagnosticDescriptor UnsupportedInterface = new(
        "SACT008", "Unsupported actor interface",
        "Actor interface '{0}' {1}",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor InvalidStateParameter = new(
        "SACT010", "[ActorState] needs an IActorState<T> parameter",
        "Parameter '{0}' of '{1}' is marked [ActorState] but is not an IActorState<T>",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor StatefulStatelessWorker = new(
        "SACT011", "A [StatelessWorker] cannot have persistent state",
        "'{0}' is a [StatelessWorker] - several activations may share an id, so it cannot derive from Actor<TState> or take [ActorState] parameters",
        Category, DiagnosticSeverity.Error, true
    );

    public static readonly DiagnosticDescriptor DuplicateActorName = new(
        "SACT009", "Duplicate actor name",
        "Actors '{0}' and '{1}' are both named '{2}'; names key persisted state and must be unique",
        Category, DiagnosticSeverity.Error, true
    );
}
