using System.Collections.Concurrent;
using System.ComponentModel;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text.Json.Serialization.Metadata;
using Shiny.Actors.Remoting;

namespace Shiny.Actors;


/// <summary>
/// Where generated code registers proxies and actors (from a module initializer). Not for direct use.
/// </summary>
[EditorBrowsable(EditorBrowsableState.Never)]
public static class ActorRegistry
{
    static readonly ConcurrentDictionary<Type, Func<ActorReference, object>> proxies = new();
    static readonly ConcurrentDictionary<Type, ActorRegistration> byImplementation = new();
    static readonly ConcurrentDictionary<Type, ActorRegistration> byInterface = new();
    static readonly ConcurrentDictionary<Type, ConcurrentBag<Type>> conflicts = new();
    static readonly ConcurrentDictionary<Type, Func<RemoteActorReference, object>> remoteProxies = new();
    static readonly ConcurrentDictionary<Type, ActorContract> contracts = new();
    static int version;


    public static void RegisterRemoteProxy<TActor>(Func<RemoteActorReference, TActor> factory) where TActor : class, IActor
        => remoteProxies[typeof(TActor)] = factory;


    public static void RegisterContract(ActorContract contract) => contracts[contract.InterfaceType] = contract;


    /// <summary>Every JsonSerializerContext in a project with actors - the metadata remote calls serialize with.</summary>
    public static void RegisterJsonContext(IJsonTypeInfoResolver resolver) => ActorJson.AddResolver(resolver);


    public static ActorContract? FindContract(Type contract) => contracts.GetValueOrDefault(contract);

    public static IEnumerable<ActorContract> Contracts => contracts.Values;

    internal static Func<RemoteActorReference, object>? FindRemoteProxy(Type contract) => remoteProxies.GetValueOrDefault(contract);



    public static void RegisterProxy<TActor>(Func<ActorReference, TActor> factory) where TActor : class, IActor
        => proxies[typeof(TActor)] = factory;


    public static void RegisterActor(ActorRegistration registration)
    {
        byImplementation[registration.ImplementationType] = registration;
        foreach (var contract in registration.Interfaces)
        {
            var winner = byInterface.GetOrAdd(contract, registration);
            if (winner.ImplementationType != registration.ImplementationType)
            {
                var bag = conflicts.GetOrAdd(contract, _ => [winner.ImplementationType]);
                bag.Add(registration.ImplementationType);
            }
        }
        Interlocked.Increment(ref version);
    }


    internal static int Version => Volatile.Read(ref version);

    internal static IEnumerable<ActorRegistration> All => byImplementation.Values;

    internal static ActorRegistration? FindByImplementation(Type type) => byImplementation.GetValueOrDefault(type);


    internal static ActorRegistration? FindByInterface(Type contract, out IReadOnlyCollection<Type>? conflicting)
    {
        conflicting = conflicts.TryGetValue(contract, out var bag) ? [.. bag.Distinct()] : null;
        return byInterface.GetValueOrDefault(contract);
    }


    internal static Func<ActorReference, object>? FindProxy(Type contract) => proxies.GetValueOrDefault(contract);


    /// <summary>
    /// Module initializers run when an assembly is first used, which under JIT can be later than
    /// the first <c>Get</c>. Running it here closes that gap; under NativeAOT they have already run.
    /// </summary>
    public static void EnsureInitialized(Assembly assembly)
        => RuntimeHelpers.RunModuleConstructor(assembly.ManifestModule.ModuleHandle);
}
