using System.Text.Json.Nodes;

namespace KopiQueue;

/// <summary>A job handed to a worker: the stored row and its deserialised args (design §6.1).</summary>
/// <typeparam name="TArgs">The job args type.</typeparam>
/// <remarks>
/// The properties reflect the row as it was fetched for this attempt: <see cref="Attempt"/> already counts it, and
/// <see cref="AttemptedAt"/> is when it started.
/// </remarks>
public sealed class Job<TArgs>
    where TArgs : IJobArgs
{
    private JsonObject? _metadata;

    /// <summary>Creates a job from a stored row and its args, for example to call a worker in a unit test.</summary>
    /// <param name="row">The stored job.</param>
    /// <param name="args">The job's args.</param>
    /// <exception cref="ArgumentException"><paramref name="row"/> has a different kind from <typeparamref name="TArgs"/>.</exception>
    /// <exception cref="KopiQueueConfigurationException"><typeparamref name="TArgs"/> has no valid <see cref="JobKindAttribute"/>.</exception>
    public Job(JobRow row, TArgs args)
    {
        ArgumentNullException.ThrowIfNull(row);
        ArgumentNullException.ThrowIfNull(args);
        var kind = JobKindRegistry.GetKind<TArgs>();
        if (!string.Equals(row.Kind, kind, StringComparison.Ordinal))
        {
            throw new ArgumentException($"Job {row.Id} has kind '{row.Kind}', but {typeof(TArgs).FullName} has kind '{kind}'.", nameof(row));
        }

        Row = row;
        Args = args;
    }

    /// <summary>The stored job, including the fields this class does not repeat, such as <see cref="JobRow.State"/>.</summary>
    public JobRow Row { get; }

    /// <summary>The job's args.</summary>
    public TArgs Args { get; }

    /// <summary>The job's ID.</summary>
    public long Id => Row.Id;

    /// <summary>The job's kind.</summary>
    public string Kind => Row.Kind;

    /// <summary>The queue the job runs on.</summary>
    public string Queue => Row.Queue;

    /// <summary>The current attempt, from 1.</summary>
    public int Attempt => Row.Attempt;

    /// <summary>How many attempts the job gets before it is discarded.</summary>
    public int MaxAttempts => Row.MaxAttempts;

    /// <summary>Priority from 1 (highest) to 4 (lowest).</summary>
    public int Priority => Row.Priority;

    /// <summary>The job's tags.</summary>
    public IReadOnlyList<string> Tags => Row.Tags;

    /// <summary>
    /// The job's metadata, parsed from <see cref="JobRow.MetadataJson"/> on first use. Changing the object does not change
    /// the stored job.
    /// </summary>
    public JsonObject Metadata => _metadata ??= ParseMetadata();

    /// <summary>When the job was inserted.</summary>
    public DateTimeOffset CreatedAt => Row.CreatedAt;

    /// <summary>When the job was due to run.</summary>
    public DateTimeOffset ScheduledAt => Row.ScheduledAt;

    /// <summary>When the current attempt started, or <c>null</c> for a job that has not been fetched.</summary>
    public DateTimeOffset? AttemptedAt => Row.AttemptedAt;

    /// <summary>One entry per earlier failed attempt, oldest first.</summary>
    public IReadOnlyList<AttemptError> Errors => Row.Errors;

    private JsonObject ParseMetadata() =>
        JsonNode.Parse(Row.MetadataJson) as JsonObject
        ?? throw new InvalidOperationException($"Job {Row.Id} has metadata that is not a JSON object.");
}
