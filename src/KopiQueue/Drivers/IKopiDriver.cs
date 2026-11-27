using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using KopiQueue.Migrations;
using Microsoft.Extensions.Logging;

namespace KopiQueue.Drivers;

/// <summary>
/// A database driver: everything database-specific that the rest of KopiQueue needs (design §8).
/// The packages <c>KopiQueue.Npgsql</c> and <c>KopiQueue.SqlServer</c> provide one each.
/// </summary>
/// <remarks>
/// Experimental (<c>KQ0001</c>) until 2.0: operations are added as features land, so implementing this interface
/// outside KopiQueue is not supported yet. Applications pass a driver to KopiQueue and normally never call it.
/// </remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public interface IKopiDriver
{
    /// <summary>What the database supports, so the core can adapt without branching on the driver type.</summary>
    DriverCapabilities Capabilities { get; }

    /// <summary>An executor that runs each operation on its own pooled connection.</summary>
    IKopiExecutor GetExecutor();

    /// <summary>
    /// An executor that runs operations on the connection and inside the transaction of <paramref name="transaction"/>,
    /// so they commit or roll back with it (FR-INS-02).
    /// </summary>
    /// <remarks>
    /// The caller owns the transaction: the executor never commits, rolls back or disposes it. Once it has completed,
    /// the executor's operations throw <see cref="InvalidOperationException"/>.
    /// </remarks>
    /// <exception cref="ArgumentException">The transaction belongs to another database client, for example a <c>SqlTransaction</c> passed to the PostgreSQL driver.</exception>
    IKopiExecutor GetExecutor(DbTransaction transaction);

    /// <summary>Creates a migrator for the driver's schema (FR-MIG-02).</summary>
    /// <param name="loggerFactory">Logs each migration. <c>null</c> disables logging.</param>
    KopiMigrator CreateMigrator(ILoggerFactory? loggerFactory = null);

    /// <summary>
    /// Creates the listener that the client's notifier connects through (design §3, FR-EXE-03), or returns <c>null</c>
    /// when the database cannot notify other processes, so the client polls (FR-DB-04). Returns <c>null</c> exactly
    /// when <see cref="DriverCapabilities.ListenNotify"/> is <c>false</c>.
    /// </summary>
    IKopiListener? CreateListener();
}

/// <summary>Database operations, typed: no SQL reaches the core (design §8).</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public interface IKopiExecutor
{
    /// <summary>
    /// Inserts jobs and returns them as stored, in the order given. All jobs are inserted or none: in the caller's
    /// transaction, or in the driver's own (FR-INS-04). A driver with <see cref="DriverCapabilities.MaxRowsPerStatement"/>
    /// splits larger batches into several statements itself.
    /// </summary>
    /// <remarks>
    /// On a driver with <see cref="DriverCapabilities.ListenNotify"/>, the insert also notifies each queue that received
    /// <see cref="JobState.Available"/> jobs, once, in the same transaction, so the notification is delivered only if the
    /// insert commits (FR-INS-07).
    /// </remarks>
    /// <exception cref="ArgumentException">A job is malformed in a way the database cannot represent, for example a tag containing a comma.</exception>
    /// <exception cref="NotSupportedException">A job has a unique key: unique jobs are not supported yet.</exception>
    Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken);

    /// <summary>
    /// Inserts jobs on the fastest path the database has and returns how many were inserted, without reading them back
    /// (FR-INS-04): binary <c>COPY</c> when <see cref="DriverCapabilities.BulkCopy"/> is set, batched inserts otherwise.
    /// All jobs are inserted or none, and notifications follow the same rules as <see cref="JobInsertManyAsync"/>.
    /// </summary>
    /// <exception cref="ArgumentException">A job is malformed in a way the database cannot represent.</exception>
    /// <exception cref="NotSupportedException">A job has a unique key: unique jobs are not supported yet.</exception>
    Task<long> JobInsertFastAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken);

    /// <summary>Reads a job by ID, or returns <c>null</c> if there is none.</summary>
    Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken);

    /// <summary>
    /// Locks and starts up to <see cref="JobGetAvailableParams.Max"/> jobs of one queue for one client (design §5.1):
    /// jobs that are <see cref="JobState.Available"/> and due by the database's clock, in the order
    /// <c>priority, scheduled_at, id</c> (FR-EXE-02). Each returned job is <see cref="JobState.Running"/>, its
    /// <see cref="JobRow.Attempt"/> is one higher, <see cref="JobRow.AttemptedAt"/> is the database's now, and the client
    /// ID is appended to <see cref="JobRow.AttemptedBy"/>, which keeps the newest
    /// <see cref="JobGetAvailableParams.MaxAttemptedBy"/> entries. Jobs locked by another transaction are skipped, so
    /// concurrent fetchers never get the same job (FR-EXE-01).
    /// </summary>
    /// <returns>The started jobs, in fetch order. Empty when nothing is due.</returns>
    Task<IReadOnlyList<JobRow>> JobGetAvailableAsync(JobGetAvailableParams parameters, CancellationToken cancellationToken);

    /// <summary>
    /// Records the outcomes of finished attempts in one statement (design §5.3), as River's
    /// <c>JobSetStateIfRunningMany</c>. Only a job that is still <see cref="JobState.Running"/> changes; a job that was
    /// rescued, cancelled or deleted meanwhile keeps its row, except that <see cref="JobSetStateParams.MetadataUpdates"/>
    /// is merged either way. A running job whose cancellation was asked for (<c>cancel_attempted_at</c> in its metadata)
    /// and whose outcome would run it again is <see cref="JobState.Cancelled"/> instead, finalised now, keeping its
    /// attempt and schedule, with the error still appended.
    /// </summary>
    /// <returns>Every given job that exists, changed or not, in ID order. A job that does not exist is left out.</returns>
    Task<IReadOnlyList<JobRow>> JobSetStateIfRunningManyAsync(IReadOnlyList<JobSetStateParams> jobs, CancellationToken cancellationToken);

    /// <summary>
    /// Tries to win the leader lease (design §5.5, FR-MNT-01): takes it when nobody holds it, when its holder's lease has
    /// expired by the database's clock, or when <see cref="LeaderElectParams.LeaderId"/> already holds it, in which case
    /// the lease is extended and its election time kept. A won lease expires <see cref="LeaderElectParams.Ttl"/> after the
    /// database's now. Winning sends no notification.
    /// </summary>
    /// <returns><c>true</c> when the client holds the lease afterwards.</returns>
    Task<bool> LeaderAttemptElectAsync(LeaderElectParams parameters, CancellationToken cancellationToken);

    /// <summary>
    /// Extends the lease of a leader (design §5.5) to <see cref="LeaderElectParams.Ttl"/> after the database's now, if
    /// <see cref="LeaderElectParams.LeaderId"/> still holds it.
    /// </summary>
    /// <returns><c>true</c> when the lease was extended; <c>false</c> when another client holds it or nobody does.</returns>
    Task<bool> LeaderAttemptReelectAsync(LeaderElectParams parameters, CancellationToken cancellationToken);

    /// <summary>
    /// Gives up the lease of <paramref name="leaderId"/>, if it holds it (design §5.5, FR-MNT-01), so that another client
    /// can win at once. On a driver with <see cref="DriverCapabilities.ListenNotify"/> it also announces the resignation on
    /// the leadership topic, in the same statement, as River does: <c>{"leader_id": "...", "action": "resigned"}</c>.
    /// </summary>
    /// <returns><c>true</c> when the client held the lease; nothing is announced otherwise.</returns>
    Task<bool> LeaderResignAsync(string leaderId, CancellationToken cancellationToken);

    /// <summary>
    /// Deletes up to <see cref="QueueDeleteExpiredParams.Max"/> queues whose row was not updated within
    /// <see cref="QueueDeleteExpiredParams.Retention"/>, by the database's clock (FR-MNT-05). Rows locked by another
    /// transaction are skipped. Jobs are not touched.
    /// </summary>
    /// <returns>The names of the deleted queues.</returns>
    Task<IReadOnlyList<string>> QueueDeleteExpiredAsync(QueueDeleteExpiredParams parameters, CancellationToken cancellationToken);
}

/// <summary>What a database supports. The core reads these instead of checking the driver type (FR-DB-03).</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed class DriverCapabilities
{
    /// <summary>
    /// Whether the database can notify other processes of inserts and control messages (PostgreSQL LISTEN/NOTIFY).
    /// When <c>false</c>, other processes find work by polling and the client wakes only its own producers (FR-DB-04).
    /// </summary>
    public required bool ListenNotify { get; init; }

    /// <summary>
    /// Whether the database has a bulk-load path for <c>InsertManyFastAsync</c> that skips returning rows
    /// (PostgreSQL <c>COPY</c>). When <c>false</c>, the fast path uses batched inserts.
    /// </summary>
    public required bool BulkCopy { get; init; }

    /// <summary>
    /// The most rows one statement may touch, or <c>null</c> for no limit. Executors split larger batches themselves, in
    /// one transaction, so a batch stays all-or-nothing; the core uses the value to cap its own batch sizes, such as
    /// <c>FetchBatchMax</c>. SQL Server uses 1,000: its batched insert statement size (spike 0.10), safely below the
    /// 2,000-row lock-escalation cap (spike 0.9).
    /// </summary>
    public int? MaxRowsPerStatement { get; init; }
}

/// <summary>One outcome for <see cref="IKopiExecutor.JobSetStateIfRunningManyAsync"/>; a <c>null</c> field leaves its column as it is.</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed class JobSetStateParams
{
    /// <summary>The job.</summary>
    public required long Id { get; init; }

    /// <summary>
    /// The new state: any but <see cref="JobState.Running"/> and <see cref="JobState.Pending"/>.
    /// </summary>
    public required JobState State { get; init; }

    /// <summary>The attempt number to store, for an attempt that does not count.</summary>
    public int? Attempt { get; init; }

    /// <summary>An error to append to the job's errors.</summary>
    public AttemptError? Error { get; init; }

    /// <summary>When the job was finalised.</summary>
    public DateTimeOffset? FinalizedAt { get; init; }

    /// <summary>A JSON object merged into the job's metadata at the top level.</summary>
    public string? MetadataUpdates { get; init; }

    /// <summary>When the job is due again.</summary>
    public DateTimeOffset? ScheduledAt { get; init; }
}

/// <summary>What <see cref="IKopiExecutor.JobGetAvailableAsync"/> fetches.</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed class JobGetAvailableParams
{
    /// <summary>The queue to fetch from.</summary>
    public required string Queue { get; init; }

    /// <summary>The most jobs to fetch: at least 1, and no more than <see cref="DriverCapabilities.MaxRowsPerStatement"/>.</summary>
    public required int Max { get; init; }

    /// <summary>The fetching client, recorded in <see cref="JobRow.AttemptedBy"/>.</summary>
    public required string ClientId { get; init; }

    /// <summary>How many entries <see cref="JobRow.AttemptedBy"/> keeps, newest last. Default: 100, as River.</summary>
    public int MaxAttemptedBy { get; init; } = 100;
}

/// <summary>A client asking for the leader lease (<see cref="IKopiExecutor.LeaderAttemptElectAsync"/>, <see cref="IKopiExecutor.LeaderAttemptReelectAsync"/>).</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed class LeaderElectParams
{
    /// <summary>The client's ID: 1 to 127 characters.</summary>
    public required string LeaderId { get; init; }

    /// <summary>How long the lease lasts after it is won or extended. Positive.</summary>
    public required TimeSpan Ttl { get; init; }
}

/// <summary>Which queues <see cref="IKopiExecutor.QueueDeleteExpiredAsync"/> deletes.</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed class QueueDeleteExpiredParams
{
    /// <summary>How long a queue is kept after its row was last updated. Positive.</summary>
    public required TimeSpan Retention { get; init; }

    /// <summary>The most queues to delete: at least 1.</summary>
    public required int Max { get; init; }
}

/// <summary>A job to insert, already serialised and validated by the core.</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed class JobInsertParams
{
    /// <summary>The job's kind.</summary>
    public required string Kind { get; init; }

    /// <summary>The job's arguments as a JSON value.</summary>
    public required string ArgsJson { get; init; }

    /// <summary>The queue to run on.</summary>
    public required string Queue { get; init; }

    /// <summary>The initial state: usually <see cref="JobState.Available"/>, or <see cref="JobState.Scheduled"/> for a future <see cref="ScheduledAt"/>.</summary>
    public required JobState State { get; init; }

    /// <summary>Priority from 1 (highest) to 4.</summary>
    public required int Priority { get; init; }

    /// <summary>How many attempts the job gets.</summary>
    public required int MaxAttempts { get; init; }

    /// <summary>When the job is due, or <c>null</c> for the database's current time.</summary>
    public DateTimeOffset? ScheduledAt { get; init; }

    /// <summary>The job's metadata as a JSON object. Default: <c>{}</c>.</summary>
    public string MetadataJson { get; init; } = "{}";

    /// <summary>The job's tags. Default: none.</summary>
    public IReadOnlyList<string> Tags { get; init; } = [];

    /// <summary>The job's unique key (design §5.6), or <c>null</c> if the job is not unique.</summary>
    public byte[]? UniqueKey { get; init; }

    /// <summary>The states in which <see cref="UniqueKey"/> is enforced, as a bitmask (design §5.6).</summary>
    public byte? UniqueStates { get; init; }
}

/// <summary>A job as inserted.</summary>
/// <param name="Job">The stored row.</param>
/// <param name="UniqueSkippedAsDuplicate">
/// Whether no row was inserted because a job with the same unique key exists; <see cref="Job"/> is then that job.
/// </param>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public sealed record JobInsertResult(JobRow Job, bool UniqueSkippedAsDuplicate);

/// <summary>Diagnostic IDs used by KopiQueue.</summary>
public static class DiagnosticIds
{
    /// <summary>Use of the experimental driver API (<see cref="IKopiDriver"/> and related types).</summary>
    public const string ExperimentalDriver = "KQ0001";
}
