using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Text;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Shiny.Actors.SourceGenerators;


/// <summary>
/// Writes a proxy for every actor interface and a registration for every actor class, wired up by a
/// module initializer - so nothing is discovered by reflection and nothing needs listing by hand.
/// </summary>
[Generator(LanguageNames.CSharp)]
public sealed class ActorGenerator : IIncrementalGenerator
{
    const string IActorName = "Shiny.Actors.IActor";
    const string ActorBaseName = "Shiny.Actors.Actor";
    const string StatefulActorName = "Shiny.Actors.Actor<TState>";
    const string JournaledActorName = "Shiny.Actors.JournaledActor<TState, TEvent>";
    const string StreamConsumerName = "Shiny.Actors.IActorStreamConsumer<T>";

    static readonly SymbolDisplayFormat TypeFormat = SymbolDisplayFormat.FullyQualifiedFormat
        .AddMiscellaneousOptions(SymbolDisplayMiscellaneousOptions.IncludeNullableReferenceTypeModifier);

    static readonly SymbolDisplayFormat TypeOfFormat = SymbolDisplayFormat.FullyQualifiedFormat;


    public void Initialize(IncrementalGeneratorInitializationContext context)
    {
        var interfaces = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is InterfaceDeclarationSyntax { BaseList: not null },
                static (ctx, ct) => GetInterface(ctx, ct)
            )
            .Where(static x => x is not null)
            .Select(static (x, _) => x!);

        var actors = context.SyntaxProvider
            .CreateSyntaxProvider(
                static (node, _) => node is ClassDeclarationSyntax { BaseList: not null },
                static (ctx, ct) => GetActor(ctx, ct)
            )
            .Where(static x => x is not null)
            .Select(static (x, _) => x!);

        var contexts = context.SyntaxProvider
            .ForAttributeWithMetadataName(
                "System.Text.Json.Serialization.JsonSerializableAttribute",
                static (node, _) => node is ClassDeclarationSyntax,
                static (ctx, _) => GetContext(ctx)
            )
            .Where(static x => x is not null)
            .Select(static (x, _) => x!);

        var all = interfaces.Collect()
            .Combine(actors.Collect())
            .Combine(contexts.Collect());

        context.RegisterSourceOutput(all, static (spc, source) =>
        {
            var ((ifaces, classes), ctxs) = source;
            Emit(spc, ifaces, classes, ctxs);
        });
    }


    #region Discovery

    static InterfaceModel? GetInterface(GeneratorSyntaxContext ctx, System.Threading.CancellationToken ct)
    {
        if (ctx.SemanticModel.GetDeclaredSymbol(ctx.Node, ct) is not INamedTypeSymbol symbol || !IsActorInterface(symbol))
            return null;

        var location = ((InterfaceDeclarationSyntax)ctx.Node).Identifier.GetLocation();
        var fullName = symbol.ToDisplayString(TypeOfFormat);
        var diagnostics = new List<DiagnosticInfo>();

        if (symbol.IsGenericType)
            diagnostics.Add(DiagnosticInfo.Create(Diagnostics.UnsupportedInterface, location, symbol.Name, "cannot be generic"));
        if (!IsReachable(symbol))
            diagnostics.Add(DiagnosticInfo.Create(Diagnostics.UnsupportedInterface, location, symbol.Name, "must be public or internal, and not nested in a private type"));

        var methods = new List<MethodModel>();
        var contracts = new[] { symbol }.Concat(symbol.AllInterfaces.Where(IsActorInterface));
        foreach (var contract in contracts)
        {
            var declaring = contract.ToDisplayString(TypeOfFormat);
            foreach (var member in contract.GetMembers())
            {
                if (!member.IsAbstract)
                    continue; // default implementations and non-abstract statics need nothing from the proxy

                var memberLocation = member.Locations.FirstOrDefault(x => x.IsInSource) ?? location;
                if (member is not IMethodSymbol { MethodKind: MethodKind.Ordinary } method)
                {
                    if (member is IMethodSymbol { AssociatedSymbol: not null })
                        continue; // accessors - reported on the property/event itself

                    diagnostics.Add(DiagnosticInfo.Create(Diagnostics.UnsupportedMember, memberLocation, member.Name, "only methods are supported"));
                    continue;
                }
                if (method.IsStatic)
                {
                    diagnostics.Add(DiagnosticInfo.Create(Diagnostics.UnsupportedMember, memberLocation, method.Name, "static abstract members are not supported"));
                    continue;
                }
                if (method.IsGenericMethod)
                {
                    diagnostics.Add(DiagnosticInfo.Create(Diagnostics.UnsupportedMember, memberLocation, method.Name, "generic methods are not supported"));
                    continue;
                }
                if (method.Parameters.Any(p => p.RefKind != RefKind.None))
                {
                    diagnostics.Add(DiagnosticInfo.Create(Diagnostics.UnsupportedMember, memberLocation, method.Name, "ref, out and in parameters cannot cross an actor boundary"));
                    continue;
                }

                var kind = GetReturnKind(method.ReturnType, out var resultType, out var resultTypeOf);
                if (kind is null)
                {
                    diagnostics.Add(DiagnosticInfo.Create(Diagnostics.UnsupportedReturnType, memberLocation, method.Name, method.ReturnType.ToDisplayString()));
                    continue;
                }

                var oneWay = method.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == "Shiny.Actors.OneWayAttribute");
                if (oneWay && kind is ReturnKind.TaskOfT or ReturnKind.ValueTaskOfT)
                {
                    diagnostics.Add(DiagnosticInfo.Create(Diagnostics.OneWayReturnsValue, memberLocation, method.Name));
                    continue;
                }

                var parameters = method.Parameters
                    .Select(p => new ParameterModel(
                        p.Type.ToDisplayString(TypeFormat),
                        p.Type.ToDisplayString(TypeOfFormat),
                        "@" + p.Name,
                        p.Type.ToDisplayString() == "System.Threading.CancellationToken"
                    ))
                    .ToEquatable();

                methods.Add(new MethodModel(
                    declaring,
                    method.Name,
                    method.Name,
                    method.ReturnType.ToDisplayString(TypeFormat),
                    kind.Value,
                    resultType,
                    resultTypeOf,
                    parameters,
                    oneWay,
                    HasAttribute(method, "Shiny.Actors.AlwaysInterleaveAttribute"),
                    HasAttribute(method, "Shiny.Actors.ReadOnlyAttribute")
                ));
            }
        }

        // overloads need a key that tells them apart on the wire
        var keyed = methods
            .Select(m => methods.Count(x => x.Name == m.Name) == 1
                ? m
                : m with { Key = $"{m.Name}({string.Join(",", m.Parameters.Where(p => !p.IsCancellationToken).Select(p => p.TypeOf.Replace("global::", "")))})" })
            .ToEquatable();

        var wireName = GetAttributeString(symbol, "Shiny.Actors.ActorNameAttribute") ?? symbol.ToDisplayString();
        return new InterfaceModel(fullName, wireName, SafeName(fullName) + "_ActorProxy", keyed, diagnostics.ToEquatable());
    }


    static ActorModel? GetActor(GeneratorSyntaxContext ctx, System.Threading.CancellationToken ct)
    {
        if (ctx.SemanticModel.GetDeclaredSymbol(ctx.Node, ct) is not INamedTypeSymbol symbol || symbol.IsAbstract || symbol.IsStatic)
            return null;

        var actorInterfaces = symbol.AllInterfaces.Where(IsActorInterface).ToList();
        var streams = symbol.AllInterfaces
            .Where(i => i.OriginalDefinition.ToDisplayString() == StreamConsumerName)
            .Select(i => i.TypeArguments[0].ToDisplayString(TypeOfFormat))
            .ToList();

        if (actorInterfaces.Count == 0 && streams.Count == 0)
            return null;

        var location = ((ClassDeclarationSyntax)ctx.Node).Identifier.GetLocation();
        var fullName = symbol.ToDisplayString(TypeOfFormat);
        var diagnostics = new List<DiagnosticInfo>();

        if (!DerivesFrom(symbol, ActorBaseName))
        {
            if (actorInterfaces.Count > 0)
                diagnostics.Add(DiagnosticInfo.Create(Diagnostics.NotAnActor, location, symbol.Name, actorInterfaces[0].Name));
            return Invalid(fullName, diagnostics, location);
        }
        if (symbol.IsGenericType)
            diagnostics.Add(DiagnosticInfo.Create(Diagnostics.CannotConstruct, location, symbol.Name, "actor classes cannot be generic"));
        if (!IsReachable(symbol))
            diagnostics.Add(DiagnosticInfo.Create(Diagnostics.CannotConstruct, location, symbol.Name, "it must be public or internal, and not nested in a private type"));

        var ctors = symbol.InstanceConstructors
            .Where(c => c.DeclaredAccessibility is Accessibility.Public or Accessibility.Internal or Accessibility.ProtectedOrInternal)
            .OrderByDescending(c => c.Parameters.Length)
            .ToList();

        IMethodSymbol? ctor = null;
        if (ctors.Count == 0)
            diagnostics.Add(DiagnosticInfo.Create(Diagnostics.CannotConstruct, location, symbol.Name, "it has no public or internal constructor"));
        else if (ctors.Count > 1 && ctors[0].Parameters.Length == ctors[1].Parameters.Length)
            diagnostics.Add(DiagnosticInfo.Create(Diagnostics.CannotConstruct, location, symbol.Name, "more than one constructor has the most parameters - remove the ambiguity"));
        else
            ctor = ctors[0];

        if (diagnostics.Count > 0 || ctor is null)
            return Invalid(fullName, diagnostics, location);

        foreach (var p in ctor.Parameters.Where(p => HasStateAttribute(p) && GetStateType(p) is null))
            diagnostics.Add(DiagnosticInfo.Create(Diagnostics.InvalidStateParameter, p.Locations.FirstOrDefault() ?? location, p.Name, symbol.Name));
        if (diagnostics.Count > 0)
            return Invalid(fullName, diagnostics, location);

        var ctorParameters = ctor.Parameters.Select(p =>
        {
            if (HasStateAttribute(p))
            {
                var attribute = p.GetAttributes().First(a => a.AttributeClass?.ToDisplayString() == "Shiny.Actors.ActorStateAttribute");
                var provider = attribute.NamedArguments.FirstOrDefault(x => x.Key == "Provider").Value.Value as string;
                var stateName = attribute.ConstructorArguments.FirstOrDefault().Value as string ?? p.Name;
                return new CtorParameterModel(p.Type.ToDisplayString(TypeOfFormat), null, false, null, stateName, provider, GetStateType(p)!.ToDisplayString(TypeOfFormat));
            }

            var keyed = p.GetAttributes().FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == "Microsoft.Extensions.DependencyInjection.FromKeyedServicesAttribute");
            var key = keyed is { ConstructorArguments.Length: > 0 } ? keyed.ConstructorArguments[0].ToCSharpString() : null;
            string? literal = null;
            if (p.HasExplicitDefaultValue && p.Type.IsValueType)
                literal = p.ExplicitDefaultValue is null ? "default" : SymbolDisplay.FormatPrimitive(p.ExplicitDefaultValue, quoteStrings: true, useHexadecimalNumbers: false);

            var optional = p.HasExplicitDefaultValue || p.NullableAnnotation == NullableAnnotation.Annotated;
            return new CtorParameterModel(p.Type.ToDisplayString(TypeOfFormat), key, optional, literal);
        }).ToEquatable();

        string? stateType = null;
        string? eventType = null;
        for (var t = symbol.BaseType; t is not null; t = t.BaseType)
        {
            var definition = t.OriginalDefinition.ToDisplayString();
            if (definition == StatefulActorName)
            {
                stateType = t.TypeArguments[0].ToDisplayString(TypeOfFormat);
                break;
            }
            if (definition == JournaledActorName)
            {
                stateType = t.TypeArguments[0].ToDisplayString(TypeOfFormat);
                eventType = t.TypeArguments[1].ToDisplayString(TypeOfFormat);
                break;
            }
        }

        var worker = symbol.GetAttributes().FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == "Shiny.Actors.StatelessWorkerAttribute");
        var maxWorkers = 0;
        if (worker is not null)
        {
            maxWorkers = worker.NamedArguments.FirstOrDefault(x => x.Key == "MaxLocalWorkers").Value.Value is int max && max > 0 ? max : -1;
            if (stateType is not null || ctorParameters.Any(p => p.StateType is not null))
            {
                diagnostics.Add(DiagnosticInfo.Create(Diagnostics.StatefulStatelessWorker, location, symbol.Name));
                return Invalid(fullName, diagnostics, location);
            }
        }

        var overridesTypeInfo = false;
        var overridesEventTypeInfo = false;
        for (var t = symbol; t is not null && t.OriginalDefinition.ToDisplayString() is not (StatefulActorName or JournaledActorName); t = t.BaseType)
        {
            overridesTypeInfo |= t.GetMembers("StateTypeInfo").Any(m => m.IsOverride);
            overridesEventTypeInfo |= t.GetMembers("EventTypeInfo").Any(m => m.IsOverride);
        }

        return new ActorModel(
            fullName,
            GetAttributeString(symbol, "Shiny.Actors.ActorNameAttribute") ?? symbol.ToDisplayString(),
            actorInterfaces.Select(i => i.ToDisplayString(TypeOfFormat)).ToEquatable(),
            streams.ToEquatable(),
            stateType,
            overridesTypeInfo,
            GetAttributeString(symbol, "Shiny.Actors.StateProviderAttribute"),
            ctorParameters,
            diagnostics.ToEquatable(),
            location,
            true,
            symbol.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == "Shiny.Actors.ReentrantAttribute"),
            symbol.GetAttributes().FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == "Shiny.Actors.AutoSaveAttribute") is { } autoSave
                ? autoSave.ConstructorArguments.Length > 0 && autoSave.ConstructorArguments[0].Value is int mode ? mode : 2
                : null,
            maxWorkers,
            eventType,
            overridesEventTypeInfo
        );
    }


    static ActorModel Invalid(string fullName, List<DiagnosticInfo> diagnostics, Location location)
        => new(fullName, fullName, default, default, null, false, null, default, diagnostics.ToEquatable(), location, false, false, null);


    static ContextModel? GetContext(GeneratorAttributeSyntaxContext ctx)
    {
        if (ctx.TargetSymbol is not INamedTypeSymbol symbol || !IsReachable(symbol) || symbol.IsGenericType || symbol.IsAbstract)
            return null;
        if (!DerivesFrom(symbol, "System.Text.Json.Serialization.JsonSerializerContext"))
            return null;

        var types = ctx.Attributes
            .Where(a => a.ConstructorArguments.Length > 0 && a.ConstructorArguments[0].Value is ITypeSymbol)
            .Select(a => ((ITypeSymbol)a.ConstructorArguments[0].Value!).ToDisplayString(TypeOfFormat))
            .ToEquatable();

        return new ContextModel(symbol.ToDisplayString(TypeOfFormat), types);
    }

    #endregion


    #region Emit

    static void Emit(SourceProductionContext spc, ImmutableArray<InterfaceModel> interfaces, ImmutableArray<ActorModel> actors, ImmutableArray<ContextModel> contexts)
    {
        // partial declarations surface once per declaration
        var ifaces = interfaces.GroupBy(x => x.FullName).Select(g => g.First()).OrderBy(x => x.FullName).ToList();
        var classes = actors.GroupBy(x => x.FullName).Select(g => g.First()).OrderBy(x => x.FullName).ToList();

        foreach (var diagnostic in ifaces.SelectMany(x => x.Diagnostics).Concat(classes.SelectMany(x => x.Diagnostics)))
            spc.ReportDiagnostic(diagnostic.ToDiagnostic());

        var proxies = ifaces.Where(x => x.Diagnostics.Length == 0).ToList();
        var valid = classes.Where(x => x.IsValid).ToList();

        foreach (var group in valid.SelectMany(a => a.Interfaces.Select(i => (Interface: i, Actor: a))).GroupBy(x => x.Interface).Where(g => g.Count() > 1))
        {
            var names = string.Join(", ", group.Select(x => x.Actor.FullName.Replace("global::", "")));
            var display = group.Key.Replace("global::", "");
            var shortName = display.Substring(display.LastIndexOf('.') + 1);
            foreach (var (_, actor) in group)
                spc.ReportDiagnostic(Diagnostic.Create(Diagnostics.DuplicateImplementation, actor.Location, group.Key.Replace("global::", ""), names, shortName));
        }

        foreach (var group in valid.GroupBy(x => x.ActorName).Where(g => g.Count() > 1))
        {
            var list = group.ToList();
            spc.ReportDiagnostic(Diagnostic.Create(Diagnostics.DuplicateActorName, list[1].Location, list[0].FullName.Replace("global::", ""), list[1].FullName.Replace("global::", ""), group.Key));
        }

        var stateContexts = new Dictionary<string, string>();
        foreach (var context in contexts.GroupBy(x => x.FullName).OrderBy(x => x.Key))
            foreach (var type in context.SelectMany(x => x.Types))
                if (!stateContexts.ContainsKey(type))
                    stateContexts[type] = context.Key;

        foreach (var actor in valid.Where(a => a.StateType is not null && !a.OverridesStateTypeInfo && !stateContexts.ContainsKey(a.StateType!)))
            spc.ReportDiagnostic(Diagnostic.Create(Diagnostics.MissingStateMetadata, actor.Location, actor.StateType!.Replace("global::", ""), actor.FullName.Replace("global::", "")));

        foreach (var actor in valid.Where(a => a.EventType is not null && !a.OverridesEventTypeInfo && !stateContexts.ContainsKey(a.EventType!)))
            spc.ReportDiagnostic(Diagnostic.Create(Diagnostics.MissingStateMetadata, actor.Location, actor.EventType!.Replace("global::", ""), actor.FullName.Replace("global::", "")));

        foreach (var actor in valid)
            foreach (var state in actor.CtorParameters.Where(p => p.StateType is not null && !stateContexts.ContainsKey(p.StateType!)))
                spc.ReportDiagnostic(Diagnostic.Create(Diagnostics.MissingStateMetadata, actor.Location, state.StateType!.Replace("global::", ""), actor.FullName.Replace("global::", "")));

        if (proxies.Count == 0 && valid.Count == 0)
            return;

        var sb = new StringBuilder();
        sb.AppendLine("// <auto-generated/>");
        sb.AppendLine("#nullable enable");
        sb.AppendLine("#pragma warning disable CA2255 // module initializers are how the registrations reach the runtime without reflection");
        sb.AppendLine();
        sb.AppendLine("namespace Shiny.Actors.Generated");
        sb.AppendLine("{");

        var proxyNames = new HashSet<string>();
        var proxyNameFor = new Dictionary<string, string>();
        foreach (var proxy in proxies)
        {
            var name = proxy.ProxyName;
            for (var i = 2; !proxyNames.Add(name); i++)
                name = proxy.ProxyName + i;

            proxyNameFor[proxy.FullName] = name;
            EmitProxy(sb, proxy, name);
            EmitRemoteProxy(sb, proxy, "Remote" + name, name);
        }

        var contextNames = contexts.Select(x => x.FullName).Distinct().OrderBy(x => x).ToList();
        EmitRegistration(sb, proxies, proxyNameFor, valid, stateContexts, contextNames);
        sb.AppendLine("}");

        spc.AddSource("ShinyActors.g.cs", sb.ToString());
    }


    static void EmitProxy(StringBuilder sb, InterfaceModel model, string name)
    {
        sb.AppendLine("    [global::System.CodeDom.Compiler.GeneratedCode(\"Shiny.Actors.SourceGenerators\", \"1.0.0\")]");
        sb.AppendLine("    [global::System.ComponentModel.EditorBrowsable(global::System.ComponentModel.EditorBrowsableState.Never)]");
        sb.AppendLine($"    internal sealed class {name} : {model.FullName}, global::Shiny.Actors.IActorProxy, global::System.IEquatable<{name}>");
        sb.AppendLine("    {");

        // one descriptor per method, shared by this proxy, the remote proxy and the wire contract
        var index = 0;
        foreach (var method in model.Methods)
            EmitMethodDescriptor(sb, method, index++);

        sb.AppendLine();
        sb.AppendLine($"        internal static readonly global::Shiny.Actors.Remoting.ActorContract Contract = new(typeof({model.FullName}), {SymbolDisplay.FormatLiteral(model.WireName, true)}, new global::Shiny.Actors.Remoting.ActorMethod[] {{ {string.Join(", ", model.Methods.Select((_, i) => $"M{i}"))} }});");
        sb.AppendLine();
        sb.AppendLine("        readonly global::Shiny.Actors.ActorReference reference;");
        sb.AppendLine();
        sb.AppendLine($"        public {name}(global::Shiny.Actors.ActorReference reference) => this.reference = reference;");
        sb.AppendLine();
        sb.AppendLine("        global::Shiny.Actors.ActorReference global::Shiny.Actors.IActorProxy.Reference => this.reference;");
        sb.AppendLine();
        sb.AppendLine($"        public bool Equals({name}? other) => other is not null && other.reference.Registration.ImplementationType == this.reference.Registration.ImplementationType && other.reference.Id == this.reference.Id;");
        sb.AppendLine($"        public override bool Equals(object? obj) => this.Equals(obj as {name});");
        sb.AppendLine("        public override int GetHashCode() => global::System.HashCode.Combine(this.reference.Registration.ImplementationType, this.reference.Id);");
        sb.AppendLine("        public override string ToString() => this.reference.ToString();");

        index = 0;
        foreach (var method in model.Methods)
        {
            var descriptor = $"M{index++}";
            var signature = string.Join(", ", method.Parameters.Select(p => $"{p.Type} {p.Name}"));
            var token = method.Parameters.FirstOrDefault(p => p.IsCancellationToken)?.Name ?? "default";
            var args = string.Join(", ", method.Parameters.Select(p => p.IsCancellationToken ? "__ct" : p.Name));
            var wireArgs = $"new object?[] {{ {string.Join(", ", method.Parameters.Where(p => !p.IsCancellationToken).Select(p => $"(object?){p.Name}"))} }}";
            var call = $"__actor.{method.Name}({args})";
            var actorType = method.DeclaringInterface;

            string body;
            if (method.OneWay)
            {
                var tell = $"this.reference.Tell<{actorType}>({descriptor}, {wireArgs}, (__actor, __ct) => {Wrap(call, method)})";
                body = method.Kind == ReturnKind.Task ? $"{tell}.AsTask()" : tell;
            }
            else
            {
                var ask = method.Kind is ReturnKind.TaskOfT or ReturnKind.ValueTaskOfT
                    ? $"this.reference.Ask<{actorType}, {method.ResultType}>({descriptor}, {wireArgs}, (__actor, __ct) => {Wrap(call, method)}, {token})"
                    : $"this.reference.Ask<{actorType}>({descriptor}, {wireArgs}, (__actor, __ct) => {Wrap(call, method)}, {token})";

                body = method.Kind is ReturnKind.ValueTask or ReturnKind.ValueTaskOfT ? $"new({ask})" : ask;
            }

            sb.AppendLine();
            sb.AppendLine($"        {method.ReturnType} {method.DeclaringInterface}.{method.Name}({signature})");
            sb.AppendLine($"            => {body};");
        }
        sb.AppendLine("    }");
        sb.AppendLine();
    }


    static void EmitMethodDescriptor(StringBuilder sb, MethodModel method, int index)
    {
        var wire = method.Parameters.Where(p => !p.IsCancellationToken).ToList();
        var types = string.Join(", ", wire.Select(p => $"typeof({p.TypeOf})"));
        var argIndex = 0;
        var args = string.Join(", ", method.Parameters.Select(p => p.IsCancellationToken ? "__ct" : $"({p.Type})__args[{argIndex++}]!"));
        var call = $"(({method.DeclaringInterface})__actor).{method.Name}({args})";
        var returns = method.ResultTypeOf is null ? "null" : $"typeof({method.ResultTypeOf})";

        var flags = new List<string>();
        if (method.OneWay) flags.Add("OneWay");
        if (method.AlwaysInterleave) flags.Add("AlwaysInterleave");
        if (method.ReadOnly) flags.Add("ReadOnly");
        var flagExpr = flags.Count == 0
            ? "global::Shiny.Actors.Remoting.ActorMethodFlags.None"
            : string.Join(" | ", flags.Select(f => $"global::Shiny.Actors.Remoting.ActorMethodFlags.{f}"));

        var invoker = method.Kind is ReturnKind.TaskOfT or ReturnKind.ValueTaskOfT
            ? $"static async (__actor, __args, __ct) => (object?)await {call}"
            : $"static async (__actor, __args, __ct) => {{ await {call}; return null; }}";

        sb.AppendLine($"        internal static readonly global::Shiny.Actors.Remoting.ActorMethod M{index} = new({SymbolDisplay.FormatLiteral(method.Key, true)}, {SymbolDisplay.FormatLiteral(method.Name, true)}, new global::System.Type[] {{ {types} }}, {returns}, {flagExpr}, {invoker});");
    }


    // the mailbox speaks ValueTask; adapt a Task-returning method to it
    static string Wrap(string call, MethodModel method) => method.Kind switch
    {
        ReturnKind.Task => $"new global::System.Threading.Tasks.ValueTask({call})",
        ReturnKind.TaskOfT => $"new global::System.Threading.Tasks.ValueTask<{method.ResultType}>({call})",
        _ => call
    };


    static void EmitRemoteProxy(StringBuilder sb, InterfaceModel model, string name, string localName)
    {
        sb.AppendLine("    [global::System.CodeDom.Compiler.GeneratedCode(\"Shiny.Actors.SourceGenerators\", \"1.0.0\")]");
        sb.AppendLine("    [global::System.ComponentModel.EditorBrowsable(global::System.ComponentModel.EditorBrowsableState.Never)]");
        sb.AppendLine($"    internal sealed class {name} : {model.FullName}, global::Shiny.Actors.Remoting.IRemoteActorProxy");
        sb.AppendLine("    {");
        sb.AppendLine("        readonly global::Shiny.Actors.Remoting.RemoteActorReference reference;");
        sb.AppendLine();
        sb.AppendLine($"        public {name}(global::Shiny.Actors.Remoting.RemoteActorReference reference) => this.reference = reference;");
        sb.AppendLine();
        sb.AppendLine("        global::Shiny.Actors.Remoting.RemoteActorReference global::Shiny.Actors.Remoting.IRemoteActorProxy.Reference => this.reference;");
        sb.AppendLine("        public override string ToString() => this.reference.ToString();");

        var index = 0;
        foreach (var method in model.Methods)
        {
            var descriptor = $"{localName}.M{index++}";
            var signature = string.Join(", ", method.Parameters.Select(p => $"{p.Type} {p.Name}"));
            var token = method.Parameters.FirstOrDefault(p => p.IsCancellationToken)?.Name ?? "default";
            var args = string.Join(", ", method.Parameters.Where(p => !p.IsCancellationToken).Select(p => $"(object?){p.Name}"));

            var invoke = method.Kind is ReturnKind.TaskOfT or ReturnKind.ValueTaskOfT
                ? $"this.reference.Invoke<{method.ResultType}>({descriptor}, new object?[] {{ {args} }}, {token})"
                : $"this.reference.Invoke({descriptor}, new object?[] {{ {args} }}, {token})";

            var body = method.Kind is ReturnKind.ValueTask or ReturnKind.ValueTaskOfT ? $"new({invoke})" : invoke;

            sb.AppendLine();
            sb.AppendLine($"        {method.ReturnType} {method.DeclaringInterface}.{method.Name}({signature})");
            sb.AppendLine($"            => {body};");
        }
        sb.AppendLine("    }");
        sb.AppendLine();
    }


    static void EmitRegistration(StringBuilder sb, List<InterfaceModel> proxies, Dictionary<string, string> proxyNames, List<ActorModel> actors, Dictionary<string, string> stateContexts, List<string> contexts)
    {
        const string sp = "global::Microsoft.Extensions.DependencyInjection.ServiceProviderServiceExtensions";
        const string keyedSp = "global::Microsoft.Extensions.DependencyInjection.ServiceProviderKeyedServiceExtensions";

        sb.AppendLine("    [global::System.CodeDom.Compiler.GeneratedCode(\"Shiny.Actors.SourceGenerators\", \"1.0.0\")]");
        sb.AppendLine("    internal static class ShinyActorsRegistration");
        sb.AppendLine("    {");
        sb.AppendLine("        [global::System.Runtime.CompilerServices.ModuleInitializer]");
        sb.AppendLine("        internal static void Initialize()");
        sb.AppendLine("        {");

        foreach (var context in contexts)
            sb.AppendLine($"            global::Shiny.Actors.ActorRegistry.RegisterJsonContext({context}.Default);");

        foreach (var proxy in proxies)
        {
            sb.AppendLine($"            global::Shiny.Actors.ActorRegistry.RegisterProxy<{proxy.FullName}>(static r => new {proxyNames[proxy.FullName]}(r));");
            sb.AppendLine($"            global::Shiny.Actors.ActorRegistry.RegisterRemoteProxy<{proxy.FullName}>(static r => new Remote{proxyNames[proxy.FullName]}(r));");
            sb.AppendLine($"            global::Shiny.Actors.ActorRegistry.RegisterContract({proxyNames[proxy.FullName]}.Contract);");
        }

        foreach (var actor in actors)
        {
            var args = string.Join(", ", actor.CtorParameters.Select(p =>
            {
                if (p.StateType is not null)
                {
                    var info = stateContexts.TryGetValue(p.StateType, out var stateContext)
                        ? $"{stateContext}.Default.GetTypeInfo(typeof({p.StateType}))"
                        : "null";
                    var provider = p.StateProvider is null ? "null" : SymbolDisplay.FormatLiteral(p.StateProvider, true);
                    return $"global::Shiny.Actors.ActorState.Create<{p.StateType}>({SymbolDisplay.FormatLiteral(p.StateName!, true)}, {info}, {provider})";
                }

                if (p.ServiceKey is not null)
                    return p.Optional
                        ? $"{keyedSp}.GetKeyedService<{p.Type}>(sp, {p.ServiceKey})"
                        : $"{keyedSp}.GetRequiredKeyedService<{p.Type}>(sp, {p.ServiceKey})";

                if (p.DefaultLiteral is not null)
                    return p.DefaultLiteral;

                return p.Optional
                    ? $"{sp}.GetService<{p.Type}>(sp)"
                    : $"{sp}.GetRequiredService<{p.Type}>(sp)";
            }));

            var stateTypeInfo = actor.StateType is not null && stateContexts.TryGetValue(actor.StateType, out var context)
                ? $"{context}.Default.GetTypeInfo(typeof({actor.StateType}))"
                : "null";

            sb.AppendLine("            global::Shiny.Actors.ActorRegistry.RegisterActor(new global::Shiny.Actors.ActorRegistration(");
            sb.AppendLine($"                typeof({actor.FullName}),");
            sb.AppendLine($"                {SymbolDisplay.FormatLiteral(actor.ActorName, true)},");
            sb.AppendLine($"                static sp => new {actor.FullName}({args}),");
            sb.AppendLine($"                new global::System.Type[] {{ {string.Join(", ", actor.Interfaces.Select(i => $"typeof({i})"))} }},");
            sb.AppendLine($"                stateTypeInfo: {stateTypeInfo},");
            sb.AppendLine($"                stateProviderName: {(actor.StateProvider is null ? "null" : SymbolDisplay.FormatLiteral(actor.StateProvider, true))},");
            sb.AppendLine($"                implicitStreams: new global::System.Type[] {{ {string.Join(", ", actor.ImplicitStreams.Select(i => $"typeof({i})"))} }},");
            sb.AppendLine($"                isReentrant: {(actor.IsReentrant ? "true" : "false")},");
            sb.AppendLine($"                autoSave: {(actor.AutoSave is { } mode ? $"(global::Shiny.Actors.AutoSaveMode){mode}" : "null")},");
            sb.AppendLine($"                maxWorkers: {actor.MaxWorkers}");
            sb.AppendLine("            ));");
        }

        sb.AppendLine("        }");
        sb.AppendLine("    }");
    }

    #endregion


    #region Helpers

    static bool IsActorInterface(INamedTypeSymbol symbol)
        => symbol.TypeKind == TypeKind.Interface
           && symbol.ToDisplayString() != IActorName
           && symbol.AllInterfaces.Any(i => i.ToDisplayString() == IActorName);


    static bool DerivesFrom(INamedTypeSymbol symbol, string baseName)
    {
        for (var t = symbol.BaseType; t is not null; t = t.BaseType)
            if (t.ToDisplayString() == baseName)
                return true;
        return false;
    }


    static bool IsReachable(INamedTypeSymbol symbol)
    {
        for (ISymbol? s = symbol; s is INamedTypeSymbol t; s = t.ContainingType)
            if (t.DeclaredAccessibility is not (Accessibility.Public or Accessibility.Internal or Accessibility.ProtectedOrInternal))
                return false;
        return true;
    }


    static ReturnKind? GetReturnKind(ITypeSymbol type, out string? resultType, out string? resultTypeOf)
    {
        resultType = null;
        resultTypeOf = null;
        if (type is not INamedTypeSymbol named || named.ContainingNamespace?.ToDisplayString() != "System.Threading.Tasks")
            return null;

        if (named.Name == "Task" && named.Arity == 0) return ReturnKind.Task;
        if (named.Name == "ValueTask" && named.Arity == 0) return ReturnKind.ValueTask;

        if (named.Arity == 1 && named.Name is "Task" or "ValueTask")
        {
            resultType = named.TypeArguments[0].ToDisplayString(TypeFormat);
            resultTypeOf = named.TypeArguments[0].ToDisplayString(TypeOfFormat);
            return named.Name == "Task" ? ReturnKind.TaskOfT : ReturnKind.ValueTaskOfT;
        }
        return null;
    }


    static bool HasAttribute(ISymbol symbol, string attribute)
        => symbol.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == attribute);


    static bool HasStateAttribute(IParameterSymbol parameter)
        => parameter.GetAttributes().Any(a => a.AttributeClass?.ToDisplayString() == "Shiny.Actors.ActorStateAttribute");


    static ITypeSymbol? GetStateType(IParameterSymbol parameter)
        => parameter.Type is INamedTypeSymbol { IsGenericType: true } named && named.OriginalDefinition.ToDisplayString() == "Shiny.Actors.IActorState<TState>"
            ? named.TypeArguments[0]
            : null;


    static string? GetAttributeString(INamedTypeSymbol symbol, string attribute)
        => symbol.GetAttributes()
            .FirstOrDefault(a => a.AttributeClass?.ToDisplayString() == attribute)
            ?.ConstructorArguments.FirstOrDefault().Value as string;


    static string SafeName(string fullName)
    {
        var sb = new StringBuilder();
        foreach (var c in fullName.Replace("global::", ""))
            sb.Append(char.IsLetterOrDigit(c) ? c : '_');
        return sb.ToString();
    }

    #endregion
}
