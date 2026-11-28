using KopiQueue.TestInfrastructure.Integration;

namespace KopiQueue.Npgsql.Tests;

// The shared client integration suites (plan task 5.8: FR-DB-06) on PostgreSQL. Each class keeps the name its tests had
// before they were shared.

/// <summary>Timeouts, retries, several clients, stopping under load and rolling deploys (plan task 2.9).</summary>
public sealed class NpgsqlClientIntegrationTests(NpgsqlIntegrationFixture db)
    : ClientIntegrationTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>Start, pick-up, soft and hard stop (plan task 2.7).</summary>
public sealed class NpgsqlClientLifecycleTests(NpgsqlIntegrationFixture db)
    : ClientLifecycleTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>Fetch, run and complete through the executor and completer (plan tasks 2.4-2.6).</summary>
public sealed class NpgsqlExecutionTests(NpgsqlIntegrationFixture db)
    : ExecutionTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>Producers woken by notifications, their own client or the poll timer (plan task 2.3).</summary>
public sealed class NpgsqlProducerTests(NpgsqlIntegrationFixture db)
    : ProducerIntegrationTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>The queue cleaner (plan task 3.5).</summary>
public sealed class NpgsqlQueueCleanerTests(NpgsqlIntegrationFixture db)
    : QueueCleanerTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>The job cleaner (plan task 3.5).</summary>
public sealed class NpgsqlJobCleanerTests(NpgsqlIntegrationFixture db)
    : JobCleanerTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>Periodic jobs across a change of leader (plan task 4.3).</summary>
public sealed class NpgsqlPeriodicTests(NpgsqlIntegrationFixture db)
    : PeriodicTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>Leader election between working clients (plan task 3.1).</summary>
public sealed class NpgsqlLeadershipTests(NpgsqlIntegrationFixture db)
    : LeadershipTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>External cancel (plan task 3.7).</summary>
public sealed class NpgsqlCancelTests(NpgsqlIntegrationFixture db)
    : CancelTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>Queue rows, pause and resume (plan task 3.6).</summary>
public sealed class NpgsqlQueuePauseTests(NpgsqlIntegrationFixture db)
    : QueuePauseTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;

/// <summary>The admin API (plan task 4.4).</summary>
public sealed class NpgsqlAdminTests(NpgsqlIntegrationFixture db)
    : AdminTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>;
