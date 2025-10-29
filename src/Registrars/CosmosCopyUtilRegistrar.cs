using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Soenneker.Cosmos.Copy.Abstract;
using Soenneker.Cosmos.Suite.Registrars;

namespace Soenneker.Cosmos.Copy.Registrars;

/// <summary>
/// A utility to copy to and from Cosmos databases and containers
/// </summary>
public static class CosmosCopyUtilRegistrar
{
    /// <summary>
    /// Adds <see cref="ICosmosCopyUtil"/> as a singleton service. <para/>
    /// </summary>
    public static IServiceCollection AddCosmosCopyUtilAsSingleton(this IServiceCollection services)
    {
        services.AddCosmosSuiteAsSingleton().TryAddSingleton<ICosmosCopyUtil, CosmosCopyUtil>();

        return services;
    }

    /// <summary>
    /// Adds <see cref="ICosmosCopyUtil"/> as a scoped service. <para/>
    /// </summary>
    public static IServiceCollection AddCosmosCopyUtilAsScoped(this IServiceCollection services)
    {
        services.AddCosmosSuiteAsSingleton().TryAddScoped<ICosmosCopyUtil, CosmosCopyUtil>();

        return services;
    }
}