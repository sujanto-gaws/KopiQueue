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
}

/// <summary>Database operations, typed: no SQL reaches the core (design §8).</summary>
/// <remarks>Experimental (<c>KQ0001</c>): see <see cref="IKopiDriver"/>.</remarks>
[Experimental(DiagnosticIds.ExperimentalDriver)]
public interface IKopiExecutor
{
    /// <summary>
    /// Inserts jobs in one statement and returns them as stored, in the order given.
    /// The core splits larger batches using <see cref="DriverCapabilities.MaxRowsPerStatement"/>.
    /// </summary>
    /// <remarks>
    /// On a driver with <see cref="DriverCapabilities.ListenNotify"/>, the insert also notifies each queue that received
    /// <see cref="JobState.Available"/> jobs, once, in the same transaction, so the notification is delivered only if the
    /// insert commits (FR-INS-07).
    /// </remarks>
    /// <exception cref="ArgumentException">A job is malformed in a way the database cannot represent, for example a tag containing a comma.</exception>
    /// <exception cref="NotSupportedException">A job has a unique key: unique jobs are not supported yet.</exception>
    Task<IReadOnlyList<JobInsertResult>> JobInsertManyAsync(IReadOnlyList<JobInsertParams> jobs, CancellationToken cancellationToken);

    /// <summary>Reads a job by ID, or returns <c>null</c> if there is none.</summary>
    Task<JobRow?> JobGetByIdAsync(long id, CancellationToken cancellationToken);
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
    /// The most jobs the core passes to one batched executor call, such as <see cref="IKopiExecutor.JobInsertManyAsync"/>,
    /// or <c>null</c> for no limit. The core splits larger batches. SQL Server uses 1,000: its batched insert
    /// statement size (spike 0.10), safely below the 2,000-row lock-escalation cap (spike 0.9).
    /// </summary>
    public int? MaxRowsPerStatement { get; init; }
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
