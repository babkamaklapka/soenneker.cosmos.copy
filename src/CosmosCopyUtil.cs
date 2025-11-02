using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Soenneker.Cosmos.Client.Abstract;
using Soenneker.Cosmos.Copy.Abstract;
using Soenneker.Cosmos.Container.Abstract;
using Soenneker.Cosmos.Database.Abstract;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;

namespace Soenneker.Cosmos.Copy;

/// <inheritdoc cref="ICosmosCopyUtil"/>
public sealed class CosmosCopyUtil : ICosmosCopyUtil
{
    private readonly ILogger<CosmosCopyUtil> _logger;
    private readonly ICosmosClientUtil _clientUtil;
    private readonly ICosmosDatabaseUtil _databaseUtil;
    private readonly ICosmosContainerUtil _containerUtil;

    public CosmosCopyUtil(ILogger<CosmosCopyUtil> logger, ICosmosClientUtil clientUtil, ICosmosDatabaseUtil databaseUtil, ICosmosContainerUtil containerUtil)
    {
        _logger = logger;
        _clientUtil = clientUtil;
        _databaseUtil = databaseUtil;
        _containerUtil = containerUtil;
    }

    public async ValueTask CopyDatabase(string sourceEndpoint, string sourceAccountKey, string sourceDatabaseName, 
        string destinationEndpoint, string destinationAccountKey, string destinationDatabaseName, 
        DateTime? cutoffUtc = null, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting CopyDatabase from {sourceDb} to {destDb}. Cutoff: {cutoff}", sourceDatabaseName, destinationDatabaseName, cutoffUtc);
        CosmosClient sourceClient = await _clientUtil.Get(sourceEndpoint, sourceAccountKey, cancellationToken).NoSync();
        CosmosClient destClient = await _clientUtil.Get(destinationEndpoint, destinationAccountKey, cancellationToken).NoSync();

        Microsoft.Azure.Cosmos.Database sourceDb = await _databaseUtil.Get(sourceDatabaseName, sourceClient, cancellationToken).NoSync();
        Microsoft.Azure.Cosmos.Database destDb = await _databaseUtil.Get(destinationDatabaseName, destClient, cancellationToken).NoSync();

        // Delete all containers in destination
        _logger.LogWarning("Deleting all containers in destination database {destDb} ...", destinationDatabaseName);
        await _containerUtil.DeleteAll(destinationEndpoint, destinationAccountKey, destinationDatabaseName, cancellationToken).NoSync();
        _logger.LogInformation("Finished deleting containers in destination database {destDb}", destinationDatabaseName);

        // Enumerate source containers, create in dest, then copy contents
        IReadOnlyList<ContainerProperties> sourceContainers = await _containerUtil.GetAll(sourceEndpoint, sourceAccountKey, sourceDatabaseName, cancellationToken).NoSync();
        var containerIndex = 0;
        foreach (ContainerProperties props in sourceContainers)
        {
            containerIndex++;
            _logger.LogInformation("[{index}] Ensuring destination container {container} (PK: {pk}) exists ...", containerIndex, props.Id,
                props.PartitionKeyPath);
            // Create destination container with same properties
            await destDb.CreateContainerIfNotExistsAsync(new ContainerProperties
            {
                Id = props.Id,
                PartitionKeyPath = props.PartitionKeyPath,
                UniqueKeyPolicy = props.UniqueKeyPolicy
            }, throughput: null, cancellationToken: cancellationToken).NoSync();

            _logger.LogInformation("[{index}] Copying container {container} ...", containerIndex, props.Id);
            await CopyContainer(sourceEndpoint, sourceAccountKey, sourceDatabaseName, props.Id, destinationEndpoint, destinationAccountKey, destinationDatabaseName, props.Id,
                cutoffUtc, cancellationToken).NoSync();
            _logger.LogInformation("[{index}] Finished copying container {container}", containerIndex, props.Id);
        }

        _logger.LogInformation("Completed CopyDatabase from {sourceDb} to {destDb}", sourceDatabaseName, destinationDatabaseName);
    }

    public async ValueTask CopyContainer(string sourceEndpoint, string sourceAccountKey, string sourceDatabaseName, string sourceContainerName,
        string destinationEndpoint, string destinationAccountKey, string destinationDatabaseName, string destinationContainerName, 
        DateTime? cutoffUtc = null, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting CopyContainer from {sourceDb}/{sourceContainer} to {destDb}/{destContainer}. Cutoff: {cutoff}", sourceDatabaseName,
            sourceContainerName, destinationDatabaseName, destinationContainerName, cutoffUtc);
        CosmosClient sourceClient = await _clientUtil.Get(sourceEndpoint, sourceAccountKey, cancellationToken).NoSync();
        CosmosClient destClient = await _clientUtil.Get(destinationEndpoint, destinationAccountKey, cancellationToken).NoSync();

        Microsoft.Azure.Cosmos.Container sourceContainer = await _containerUtil.Get(sourceEndpoint, sourceAccountKey, sourceDatabaseName, sourceContainerName, sourceClient, cancellationToken).NoSync();
        Microsoft.Azure.Cosmos.Container destContainer =
            await _containerUtil.Get(destinationEndpoint, destinationAccountKey, destinationDatabaseName, destinationContainerName, destClient , cancellationToken).NoSync();

        // Ensure destination container exists (in case CopyContainer is called directly)
        ContainerResponse sourceContainerResponse = await sourceContainer.ReadContainerAsync(cancellationToken: cancellationToken).NoSync();
        ContainerProperties sourceProps = sourceContainerResponse.Resource;
        _logger.LogDebug("Source container {container} properties: PK: {pk}", sourceContainerName, sourceProps.PartitionKeyPath);
        Microsoft.Azure.Cosmos.Database destDb = await _databaseUtil.Get(destinationDatabaseName, destClient, cancellationToken).NoSync();
        await destDb.CreateContainerIfNotExistsAsync(new ContainerProperties
        {
            Id = destinationContainerName,
            PartitionKeyPath = sourceProps.PartitionKeyPath,
            UniqueKeyPolicy = sourceProps.UniqueKeyPolicy
        }, throughput: null, cancellationToken: cancellationToken).NoSync();
        _logger.LogInformation("Ensured destination container {container} exists (PK: {pk})", destinationContainerName, sourceProps.PartitionKeyPath);

        string queryText = cutoffUtc.HasValue ? "SELECT * FROM c WHERE c.createdAt >= @cutoff" : "SELECT * FROM c";
        _logger.LogDebug("Querying source with: {query}", queryText);

        var queryDef = new QueryDefinition(queryText);
        if (cutoffUtc.HasValue)
        {
            queryDef.WithParameter("@cutoff", cutoffUtc.Value);
            _logger.LogDebug("Applied cutoff parameter: {cutoff}", cutoffUtc);
        }

        FeedIterator<JsonElement>? feedIterator = sourceContainer.GetItemQueryIterator<JsonElement>(queryDef);

        string? partitionKeyPath = NormalizePartitionKeyPath(sourceProps.PartitionKeyPath);
        _logger.LogDebug("Normalized partition key path: {pk}", partitionKeyPath);

        var tasks = new List<Task>();
        long copied = 0;
        var pageIndex = 0;
        DateTimeOffset startedAt = DateTimeOffset.UtcNow;
        while (feedIterator.HasMoreResults)
        {
            pageIndex++;
            FeedResponse<JsonElement> page = await feedIterator.ReadNextAsync(cancellationToken).NoSync();
            _logger.LogInformation("Processing page {pageIndex} with {count} items from {sourceContainer}", pageIndex, page.Count, sourceContainerName);
            foreach (JsonElement doc in page)
            {
                // Let SDK infer the partition key from the document
                tasks.Add(destContainer.UpsertItemAsync(doc, cancellationToken: cancellationToken));

                if (tasks.Count >= 100)
                {
                    await Task.WhenAll(tasks).NoSync();
                    tasks.Clear();
                    _logger.LogDebug("Flushed a batch of 100 upserts to {destContainer}", destinationContainerName);
                }

                copied++;
            }
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks).NoSync();
            _logger.LogDebug("Flushed final batch of {count} upserts to {destContainer}", tasks.Count, destinationContainerName);
        }

        TimeSpan duration = DateTimeOffset.UtcNow - startedAt;
        _logger.LogInformation("Completed CopyContainer to {destDb}/{destContainer}. Total items copied: {copied}. Duration: {duration}",
            destinationDatabaseName, destinationContainerName, copied, duration);
    }

    private static string? NormalizePartitionKeyPath(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return null;
        if (path![0] == '/')
            return path.Substring(1);
        return path;
    }
}