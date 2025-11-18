using System;

namespace Soenneker.Cosmos.Copy.Dtos;

/// <summary>
/// Configuration for copying a specific container, including optional cutoff time and exclusion flag.
/// </summary>
public sealed class ContainerCopyConfig
{
    /// <summary>
    /// The name of the container to configure.
    /// </summary>
    public string ContainerName { get; set; } = default!;

    /// <summary>
    /// Optional cutoff time for filtering items by createdAt. If null, uses the global cutoff time or no filter.
    /// </summary>
    public DateTime? CutoffUtc { get; set; }

    /// <summary>
    /// If true, this container will be excluded from the copy operation.
    /// </summary>
    public bool Exclude { get; set; }
}

