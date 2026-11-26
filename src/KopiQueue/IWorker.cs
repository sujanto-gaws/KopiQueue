namespace KopiQueue;

/// <summary>
/// Works jobs whose args are <typeparamref name="TArgs"/> (FR-DEF-02). Most workers derive from
/// <see cref="Worker{TArgs}"/>, which implements this interface with overridable defaults.
/// </summary>
/// <typeparam name="TArgs">The job args type. Its <see cref="JobKindAttribute"/> selects the jobs this worker gets.</typeparam>
/// <remarks>
/// <para>
/// A worker is created in a new dependency-injection scope for each job (FR-DEF-03, ADR-005), so it can take scoped
/// services such as a <c>DbContext</c> in its constructor. A worker registered as an instance
/// (<see cref="WorkerRegistry.Add{TArgs}(IWorker{TArgs})"/>) is shared by every job instead, and must be thread-safe.
/// </para>
/// <para>
/// A job completes when <see cref="WorkAsync"/> returns. It fails, and is retried or discarded, when <c>WorkAsync</c>
/// throws. Workers cancel or snooze a job by throwing the exceptions that <c>JobControl</c> creates (ADR-004).
/// </para>
/// <para>
/// <c>WorkAsync</c> returns <see cref="Task"/>, not <see cref="ValueTask"/> (DQ-1): a synchronous worker returns the
/// cached <see cref="Task.CompletedTask"/>, which allocates nothing.
/// </para>
/// </remarks>
public interface IWorker<TArgs>
    where TArgs : IJobArgs
{
    /// <summary>Works <paramref name="job"/>.</summary>
    /// <param name="job">The job, with its deserialised args.</param>
    /// <param name="cancellationToken">
    /// Cancelled when the job's timeout passes (<see cref="Timeout"/>), when the job is cancelled from outside, or when
    /// the client stops hard. Pass it to every asynchronous call so the job can unwind.
    /// </param>
    /// <returns>A task that completes when the job is done.</returns>
    Task WorkAsync(Job<TArgs> job, CancellationToken cancellationToken);

    /// <summary>
    /// How long <paramref name="job"/> may run before its <c>cancellationToken</c> is cancelled and the attempt fails
    /// (FR-DEF-04).
    /// </summary>
    /// <param name="job">The job about to run.</param>
    /// <returns>
    /// A positive timeout, <see cref="System.Threading.Timeout.InfiniteTimeSpan"/> for none, or <c>null</c> (the default)
    /// for the client's <c>JobTimeout</c>.
    /// </returns>
    TimeSpan? Timeout(Job<TArgs> job) => null;

    /// <summary>How long to wait before retrying <paramref name="job"/> after a failed attempt (FR-DEF-04).</summary>
    /// <param name="job">The job whose attempt failed. <see cref="Job{TArgs}.Attempt"/> is the attempt that failed.</param>
    /// <returns>
    /// The delay from the failure until the retry, or <c>null</c> (the default) for the client's retry policy.
    /// Not used when the failed attempt was the job's last one.
    /// </returns>
    TimeSpan? NextRetry(Job<TArgs> job) => null;
}

/// <summary>
/// A base class for workers (FR-DEF-02): override <see cref="WorkAsync"/>, and <see cref="Timeout"/> or
/// <see cref="NextRetry"/> to change the client's defaults for this kind of job (FR-DEF-04).
/// </summary>
/// <example>
/// <code>
/// public sealed class SendEmailWorker(IEmailSender sender) : Worker&lt;SendEmailArgs&gt;
/// {
///     public override TimeSpan? Timeout(Job&lt;SendEmailArgs&gt; job) =&gt; TimeSpan.FromSeconds(30);
///
///     public override Task WorkAsync(Job&lt;SendEmailArgs&gt; job, CancellationToken cancellationToken) =&gt;
///         sender.SendAsync(job.Args.To, job.Args.TemplateId, cancellationToken);
/// }
/// </code>
/// </example>
/// <typeparam name="TArgs">The job args type.</typeparam>
/// <remarks>See <see cref="IWorker{TArgs}"/> for the worker's lifetime and outcomes.</remarks>
public abstract class Worker<TArgs> : IWorker<TArgs>
    where TArgs : IJobArgs
{
    /// <inheritdoc />
    public abstract Task WorkAsync(Job<TArgs> job, CancellationToken cancellationToken);

    /// <inheritdoc />
    public virtual TimeSpan? Timeout(Job<TArgs> job) => null;

    /// <inheritdoc />
    public virtual TimeSpan? NextRetry(Job<TArgs> job) => null;
}
