using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Conformance;

/// <summary>
/// The driver conformance suite (FR-DB-06, ADR-012, design §11). Every driver runs these cases through a concrete
/// subclass; a feature is done only when they pass on every driver. Cases use only <see cref="IKopiDriver"/>,
/// <see cref="KopiClient"/> and the fixture, so they state the semantics every database must share (FR-DB-03).
/// </summary>
/// <remarks>
/// Most cases run inside a caller transaction that is rolled back, so they leave nothing behind. Cases about
/// committing use a kind of their own, so they never see each other's jobs.
/// </remarks>
/// <typeparam name="TFixture">The driver's fixture.</typeparam>
public abstract partial class DriverConformanceTests<TFixture>(TFixture fixture)
    where TFixture : DriverConformanceFixture
{
    /// <summary>The driver's fixture.</summary>
    protected TFixture Fixture { get; } = fixture;

    /// <summary>The current test's cancellation token.</summary>
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    /// <summary>Skips when the server is unavailable, then returns the shared migrated schema.</summary>
    protected async Task<ConformanceSchema> MigratedAsync()
    {
        Fixture.SkipIfUnavailable();
        return await Fixture.GetMigratedSchemaAsync(Ct);
    }

    /// <summary>A kind no other test uses.</summary>
    protected static string UniqueKind() => "conf_" + Guid.NewGuid().ToString("N");

    /// <summary>A time every supported database stores exactly: whole microseconds, in UTC.</summary>
    protected static DateTimeOffset Microseconds(DateTimeOffset value) =>
        new DateTimeOffset(value.UtcTicks - (value.UtcTicks % 10), TimeSpan.Zero);

    /// <summary>A minimal valid job.</summary>
    protected static JobInsertParams Job(string kind, string queue = "default", JobState state = JobState.Available) => new()
    {
        Kind = kind,
        ArgsJson = "{\"n\":1}",
        Queue = queue,
        State = state,
        Priority = 1,
        MaxAttempts = 25,
    };
}
