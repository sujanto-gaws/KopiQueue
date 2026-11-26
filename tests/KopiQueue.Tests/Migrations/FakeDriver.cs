using System.Data.Common;
using KopiQueue.Migrations;

namespace KopiQueue.Tests.Migrations;

/// <summary>An in-memory <see cref="IMigrationDriver"/> that records every call.</summary>
internal sealed class FakeDriver : IMigrationDriver
{
    public List<string> Log { get; } = [];

    public List<int> Applied { get; set; } = [];

    public int? FailOn { get; init; }

    public bool FailUnlock { get; init; }

    public int LockBusyAttempts { get; init; }

    public int LockAttempts { get; private set; }

    public string SchemaQualifier => string.Empty;

    public string TablePrefix => "p_";

    /// <summary>The fake's timestamp for <paramref name="version"/>: one day per version from 2020-01-01.</summary>
    public static DateTimeOffset AppliedAt(int version) => new DateTimeOffset(2020, 1, 1, 0, 0, 0, TimeSpan.Zero).AddDays(version);

    public Task<IMigrationSession> OpenSessionAsync(CancellationToken cancellationToken) =>
        Task.FromResult<IMigrationSession>(new Session(this));

    private sealed class Session(FakeDriver driver) : IMigrationSession
    {
        public Task<bool> TryAcquireLockAsync(CancellationToken cancellationToken)
        {
            driver.LockAttempts++;
            if (driver.LockAttempts <= driver.LockBusyAttempts)
            {
                return Task.FromResult(false);
            }

            driver.Log.Add("lock");
            return Task.FromResult(true);
        }

        public Task ReleaseLockAsync(CancellationToken cancellationToken)
        {
            driver.Log.Add("unlock");
            return driver.FailUnlock ? throw new FakeDbException("connection broken") : Task.CompletedTask;
        }

        public Task<IReadOnlyList<AppliedVersion>> GetAppliedVersionsAsync(string line, CancellationToken cancellationToken)
        {
            driver.Log.Add($"applied {line}");
            return Task.FromResult<IReadOnlyList<AppliedVersion>>([.. driver.Applied.Order().Select(v => new AppliedVersion(v, AppliedAt(v)))]);
        }

        public Task ExecuteAsync(MigrationStep step, CancellationToken cancellationToken)
        {
            var direction = step.Direction.ToString().ToLowerInvariant();
            driver.Log.Add($"exec {direction} {step.Version}: {step.Sql}");
            if (step.Version == driver.FailOn)
            {
                throw new FakeDbException("syntax error");
            }

            if (step.Direction == MigrationDirection.Up)
            {
                driver.Applied.Add(step.Version);
            }
            else
            {
                driver.Applied.Remove(step.Version);
            }

            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            driver.Log.Add("dispose");
            return ValueTask.CompletedTask;
        }
    }
}

internal sealed class FakeDbException(string message) : DbException(message);
