using System.Data.Common;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace KopiQueue.Migrations;

/// <summary>What a run should do.</summary>
/// <param name="Migrations">All migrations of <paramref name="Line"/>, versions 1..N.</param>
/// <param name="Line">The migration line, for example <c>main</c>.</param>
/// <param name="Direction">Up or down.</param>
/// <param name="TargetVersion">See <see cref="MigrationPlanner.Plan"/>.</param>
/// <param name="LockTimeout">How long to wait for another process that holds the migration lock.</param>
/// <param name="DryRun">Plan and render the steps, but execute nothing.</param>
internal sealed record MigrationRequest(
    IReadOnlyList<Migration> Migrations,
    string Line,
    MigrationDirection Direction,
    int? TargetVersion = null,
    TimeSpan? LockTimeout = null,
    bool DryRun = false)
{
    /// <summary>Default for <see cref="LockTimeout"/>.</summary>
    public static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(30);
}

/// <summary>Outcome of a run.</summary>
/// <param name="Line">The migration line.</param>
/// <param name="Direction">Up or down.</param>
/// <param name="VersionBefore">Version of the line before the run (0 = none).</param>
/// <param name="VersionAfter">Version after the run. For a dry run, the version the run would reach.</param>
/// <param name="LatestVersion">Latest version this library knows for the line.</param>
/// <param name="DryRun">Whether nothing was executed.</param>
/// <param name="Steps">Steps executed (or, for a dry run, planned), in order.</param>
internal sealed record MigrationResult(
    string Line,
    MigrationDirection Direction,
    int VersionBefore,
    int VersionAfter,
    int LatestVersion,
    bool DryRun,
    IReadOnlyList<MigrationStepResult> Steps);

/// <summary>One executed step.</summary>
internal sealed record MigrationStepResult(MigrationStep Step, TimeSpan Duration);

/// <summary>
/// Runs migrations for one line: takes the driver's lock, reads the applied versions, plans, then executes each step
/// in its own transaction (FR-MIG-01, FR-MIG-06). Concurrent runners serialise on the lock; a runner that gets the lock
/// after another has finished finds nothing left to do.
/// </summary>
internal sealed partial class MigrationRunner
{
    internal static readonly TimeSpan LockPollInterval = TimeSpan.FromMilliseconds(200);

    private readonly IMigrationDriver _driver;
    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;

    public MigrationRunner(IMigrationDriver driver, ILogger? logger = null, TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(driver);
        _driver = driver;
        _logger = logger ?? NullLogger.Instance;
        _timeProvider = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Executes <paramref name="request"/>.</summary>
    /// <exception cref="KopiQueueMigrationException">
    /// The lock could not be taken in time, the database state does not allow the run, or a step failed
    /// (the inner exception is the database error). Steps before the failed one stay applied.
    /// </exception>
    public async Task<MigrationResult> RunAsync(MigrationRequest request, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(request);
        var session = await _driver.OpenSessionAsync(cancellationToken).ConfigureAwait(false);
        await using (session.ConfigureAwait(false))
        {
            await AcquireLockAsync(session, request.LockTimeout ?? MigrationRequest.DefaultLockTimeout, cancellationToken)
                .ConfigureAwait(false);
            try
            {
                return await RunLockedAsync(session, request, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                await ReleaseLockAsync(session).ConfigureAwait(false);
            }
        }
    }

    private async Task<MigrationResult> RunLockedAsync(IMigrationSession session, MigrationRequest request, CancellationToken cancellationToken)
    {
        var applied = await session.GetAppliedVersionsAsync(request.Line, cancellationToken).ConfigureAwait(false);
        var before = MigrationPlanner.CurrentVersion(applied);
        var latest = request.Migrations.Count;
        var plan = MigrationPlanner.Plan(request.Migrations, applied, request.Direction, request.TargetVersion);

        if (plan.Count == 0)
        {
            if (before > latest)
            {
                LogDatabaseNewer(_logger, request.Line, before, latest);
            }
            else
            {
                LogNothingToDo(_logger, request.Line, before);
            }

            return new MigrationResult(request.Line, request.Direction, before, before, latest, request.DryRun, []);
        }

        var results = new List<MigrationStepResult>(plan.Count);
        foreach (var migration in plan)
        {
            var script = migration.For(request.Direction);
            var step = new MigrationStep(
                migration.Line, migration.Version, migration.Name, request.Direction,
                MigrationTemplate.Render(script.Sql, _driver.SchemaQualifier, _driver.TablePrefix), script.InTransaction);

            if (request.DryRun)
            {
                results.Add(new MigrationStepResult(step, TimeSpan.Zero));
                continue;
            }

            LogStepStarting(_logger, step.Line, step.Version, step.Name, step.Direction);
            var started = _timeProvider.GetTimestamp();
            try
            {
                await session.ExecuteAsync(step, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                LogStepFailed(_logger, ex, step.Line, step.Version, step.Name, step.Direction);
                var partial = step.InTransaction
                    ? "It was rolled back."
                    : "It ran without a transaction, so the database may be partially migrated.";
                throw new KopiQueueMigrationException(
                    $"Migration {step.Line} {step.Version} ({step.Name}) failed while migrating {step.Direction.ToString().ToLowerInvariant()}. " +
                    $"{partial} {ex.Message}", ex);
            }

            var elapsed = _timeProvider.GetElapsedTime(started);
            LogStepCompleted(_logger, step.Line, step.Version, step.Name, step.Direction, elapsed.TotalMilliseconds);
            results.Add(new MigrationStepResult(step, elapsed));
        }

        var after = request.Direction == MigrationDirection.Up ? plan[^1].Version : plan[^1].Version - 1;
        return new MigrationResult(request.Line, request.Direction, before, after, latest, request.DryRun, results);
    }

    private async Task AcquireLockAsync(IMigrationSession session, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var started = _timeProvider.GetTimestamp();
        var waiting = false;
        while (!await session.TryAcquireLockAsync(cancellationToken).ConfigureAwait(false))
        {
            if (_timeProvider.GetElapsedTime(started) >= timeout)
            {
                throw new KopiQueueMigrationException(
                    $"Timed out after {timeout.TotalSeconds:0.#} s waiting for the migration lock: another process is migrating this schema.");
            }

            if (!waiting)
            {
                LogWaitingForLock(_logger);
                waiting = true;
            }

            await Task.Delay(LockPollInterval, _timeProvider, cancellationToken).ConfigureAwait(false);
        }

        var waitedMs = _timeProvider.GetElapsedTime(started).TotalMilliseconds;
        LogLockAcquired(_logger, waitedMs);
    }

    private async Task ReleaseLockAsync(IMigrationSession session)
    {
        try
        {
            await session.ReleaseLockAsync(CancellationToken.None).ConfigureAwait(false);
        }
        catch (DbException ex)
        {
            // The connection is probably broken, which releases a session lock on the server anyway.
            // Do not hide the exception that is already propagating.
            LogLockReleaseFailed(_logger, ex);
        }
    }

    [LoggerMessage(EventId = 4000, Level = LogLevel.Information, Message = "Waiting for the migration lock held by another process")]
    private static partial void LogWaitingForLock(ILogger logger);

    [LoggerMessage(EventId = 4001, Level = LogLevel.Debug, Message = "Migration lock acquired after {ElapsedMs:0} ms")]
    private static partial void LogLockAcquired(ILogger logger, double elapsedMs);

    [LoggerMessage(EventId = 4002, Level = LogLevel.Information, Message = "Migration line {Line} is at version {Version}; nothing to do")]
    private static partial void LogNothingToDo(ILogger logger, string line, int version);

    [LoggerMessage(EventId = 4003, Level = LogLevel.Warning,
        Message = "Migration line {Line} is at version {Version}, newer than the latest version {Latest} this library knows")]
    private static partial void LogDatabaseNewer(ILogger logger, string line, int version, int latest);

    [LoggerMessage(EventId = 4004, Level = LogLevel.Information, Message = "Migrating {Line} {Version} ({Name}) {Direction}")]
    private static partial void LogStepStarting(ILogger logger, string line, int version, string name, MigrationDirection direction);

    [LoggerMessage(EventId = 4005, Level = LogLevel.Information, Message = "Migrated {Line} {Version} ({Name}) {Direction} in {ElapsedMs:0} ms")]
    private static partial void LogStepCompleted(ILogger logger, string line, int version, string name, MigrationDirection direction, double elapsedMs);

    [LoggerMessage(EventId = 4006, Level = LogLevel.Error, Message = "Migration {Line} {Version} ({Name}) {Direction} failed")]
    private static partial void LogStepFailed(ILogger logger, Exception exception, string line, int version, string name, MigrationDirection direction);

    [LoggerMessage(EventId = 4007, Level = LogLevel.Warning, Message = "Could not release the migration lock")]
    private static partial void LogLockReleaseFailed(ILogger logger, Exception exception);
}
