using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Logging;
using Soenneker.Cosmos.Copy.Abstract;
using Soenneker.Cosmos.Container.Abstract;
using Soenneker.Cosmos.Container.Setup.Abstract;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using Soenneker.Extensions.String;

namespace Soenneker.Cosmos.Copy;

/// <inheritdoc cref="ICosmosCopyUtil"/>
public sealed class CosmosCopyUtil : ICosmosCopyUtil
{
    private readonly ILogger<CosmosCopyUtil> _logger;
    private readonly ICosmosContainerUtil _containerUtil;
    private readonly ICosmosContainerSetupUtil _containerSetupUtil;

    public CosmosCopyUtil(ILogger<CosmosCopyUtil> logger, ICosmosContainerUtil containerUtil,
        ICosmosContainerSetupUtil containerSetupUtil)
    {
        _logger = logger;
        _containerUtil = containerUtil;
        _containerSetupUtil = containerSetupUtil;
    }

    public async ValueTask CopyDatabase(string sourceEndpoint, string sourceAccountKey, string sourceDatabaseName, string destinationEndpoint,
        string destinationAccountKey, string destinationDatabaseName, DateTime? cutoffUtc = null, CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting CopyDatabase from {sourceDb} to {destDb}. Cutoff: {cutoff}", sourceDatabaseName, destinationDatabaseName, cutoffUtc);

        await _containerUtil.DeleteAll(destinationEndpoint, destinationAccountKey, destinationDatabaseName, cancellationToken)
                            .NoSync();

        _logger.LogInformation("Finished deleting containers in destination database {destDb}", destinationDatabaseName);

        // Enumerate source containers, create in dest, then copy contents
        IReadOnlyList<ContainerProperties> sourceContainers = await _containerUtil
                                                                    .GetAll(sourceEndpoint, sourceAccountKey, sourceDatabaseName, cancellationToken)
                                                                    .NoSync();

        foreach (ContainerProperties props in sourceContainers)
        {
            await CopyContainer(sourceEndpoint, sourceAccountKey, sourceDatabaseName, props.Id, destinationEndpoint, destinationAccountKey,
                    destinationDatabaseName, props.Id, cutoffUtc, cancellationToken)
                .NoSync();
        }

        _logger.LogInformation("Completed CopyDatabase from {sourceDb} to {destDb}", sourceDatabaseName, destinationDatabaseName);
    }

    public async ValueTask CopyContainer(string sourceEndpoint, string sourceAccountKey, string sourceDatabaseName, string sourceContainerName,
        string destinationEndpoint, string destinationAccountKey, string destinationDatabaseName, string destinationContainerName, DateTime? cutoffUtc = null,
        CancellationToken cancellationToken = default)
    {
        _logger.LogInformation("Starting CopyContainer from {sourceDb}/{sourceContainer} to {destDb}/{destContainer}. Cutoff: {cutoff}", sourceDatabaseName,
            sourceContainerName, destinationDatabaseName, destinationContainerName, cutoffUtc);


        Microsoft.Azure.Cosmos.Container sourceContainer = await _containerUtil
                                                                 .Get(sourceEndpoint, sourceAccountKey, sourceDatabaseName, sourceContainerName,
                                                                     cancellationToken)
                                                                 .NoSync();

        ContainerResponse? containerResponse = await _containerSetupUtil
                                                     .Ensure(destinationEndpoint, destinationAccountKey, destinationDatabaseName, destinationContainerName,
                                                         cancellationToken)
                                                     .NoSync();

        Microsoft.Azure.Cosmos.Container destContainer = containerResponse.Container;

        // Ensure destination container exists (in case CopyContainer is called directly)
        ContainerResponse sourceContainerResponse = await sourceContainer.ReadContainerAsync(cancellationToken: cancellationToken)
                                                                         .NoSync();

        ContainerProperties sourceProps = sourceContainerResponse.Resource;

        _logger.LogDebug("Source container {container} properties: PK: {pk}", sourceContainerName, sourceProps.PartitionKeyPath);

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
            FeedResponse<JsonElement> page = await feedIterator.ReadNextAsync(cancellationToken)
                                                               .NoSync();

            _logger.LogInformation("Processing page {pageIndex} with {count} items from {sourceContainer}", pageIndex, page.Count, sourceContainerName);
            foreach (JsonElement doc in page)
            {
                // Let SDK infer the partition key from the document
                tasks.Add(destContainer.UpsertItemAsync(doc, cancellationToken: cancellationToken));

                if (tasks.Count >= 100)
                {
                    await Task.WhenAll(tasks)
                              .NoSync();
                    tasks.Clear();
                    _logger.LogDebug("Flushed a batch of 100 upserts to {destContainer}", destinationContainerName);
                }

                copied++;
            }
        }

        if (tasks.Count > 0)
        {
            await Task.WhenAll(tasks)
                      .NoSync();
            _logger.LogDebug("Flushed final batch of {count} upserts to {destContainer}", tasks.Count, destinationContainerName);
        }

        TimeSpan duration = DateTimeOffset.UtcNow - startedAt;
        _logger.LogInformation("Completed CopyContainer to {destDb}/{destContainer}. Total items copied: {copied}. Duration: {duration}",
            destinationDatabaseName, destinationContainerName, copied, duration);
    }

    private static string? NormalizePartitionKeyPath(string? path)
    {
        if (path.IsNullOrWhiteSpace())
            return null;

        if (path[0] == '/')
            return path[1..];

        return path;
    }
}