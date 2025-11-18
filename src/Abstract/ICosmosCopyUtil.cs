using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.Cosmos.Copy.Dtos;

namespace Soenneker.Cosmos.Copy.Abstract;

/// <summary>
/// A utility to copy to and from Cosmos databases and containers
/// </summary>
public interface ICosmosCopyUtil
{
    /// <summary>
    /// Copies all containers and their items from a source database to a destination database.
    /// Prior to copying, all existing containers in the destination database are deleted, then recreated to match the source.
    /// Optionally filters items by createdAt >= cutoffUtc (global default, can be overridden per container).
    /// Optionally configures per-container cutoff times and exclusion via containerConfigs.
    /// </summary>
    ValueTask CopyDatabase(string sourceEndpoint, string sourceAccountKey, string sourceDatabaseName, string destinationEndpoint, string destinationAccountKey,
        string destinationDatabaseName, DateTime? cutoffUtc = null, int numTasks = 50, IEnumerable<ContainerCopyConfig>? containerConfigs = null, CancellationToken cancellationToken = default);

    /// <summary>
    /// Copies items from a source container to a destination container. Optionally filters items by createdAt >= cutoffUtc.
    /// Containers are created in the destination if they do not exist.
    /// </summary>
    ValueTask CopyContainer(string sourceEndpoint, string sourceAccountKey, string sourceDatabaseName, string sourceContainerName, string destinationEndpoint,
        string destinationAccountKey, string destinationDatabaseName, string destinationContainerName, DateTime? cutoffUtc = null, int numTasks = 50,
        CancellationToken cancellationToken = default);
}