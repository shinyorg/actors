using Microsoft.Extensions.DependencyInjection.Extensions;
using Shiny.Actors;

namespace Microsoft.Extensions.DependencyInjection;


public static class ShinyActorServiceCollectionExtensions
{
    /// <summary>
    /// Adds Shiny.Actors - <see cref="ActorSystem"/> and <see cref="IActorSystem"/> as singletons - and configures it,
    /// add-ons included, through one <see cref="ShinyActorBuilder"/>. Every actor in the app's assemblies is already known
    /// through the generator. Call it again (from a library, say) to add to the same configuration.
    /// </summary>
    public static IServiceCollection AddShinyActors(this IServiceCollection services, Action<ShinyActorBuilder>? configure = null)
    {
        var configuration = services
            .FirstOrDefault(x => x.ServiceType == typeof(ShinyActorConfiguration))
            ?.ImplementationInstance as ShinyActorConfiguration;

        if (configuration is null)
        {
            configuration = new ShinyActorConfiguration();
            services.AddSingleton(configuration);
            services.TryAddSingleton(sp => new ActorSystem(configuration.Build(sp), sp));
            services.TryAddSingleton<IActorSystem>(sp => sp.GetRequiredService<ActorSystem>());
        }

        configure?.Invoke(new ShinyActorBuilder(services, configuration));
        return services;
    }
}
