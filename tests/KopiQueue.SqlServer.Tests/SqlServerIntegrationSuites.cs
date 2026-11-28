using KopiQueue.TestInfrastructure.Integration;

namespace KopiQueue.SqlServer.Tests;

// The shared client integration suites (plan task 5.8: FR-DB-06) on SQL Server.

/// <summary>Timeouts, retries, several clients, stopping under load and rolling deploys (plan task 2.9).</summary>
public sealed class SqlServerClientIntegrationTests(SqlServerIntegrationFixture db)
    : ClientIntegrationTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>Start, pick-up, soft and hard stop (plan task 2.7).</summary>
public sealed class SqlServerClientLifecycleTests(SqlServerIntegrationFixture db)
    : ClientLifecycleTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>Fetch, run and complete through the executor and completer (plan tasks 2.4-2.6).</summary>
public sealed class SqlServerExecutionTests(SqlServerIntegrationFixture db)
    : ExecutionTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>Producers woken by notifications, their own client or the poll timer (plan task 2.3).</summary>
public sealed class SqlServerProducerTests(SqlServerIntegrationFixture db)
    : ProducerIntegrationTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>The queue cleaner (plan task 3.5).</summary>
public sealed class SqlServerQueueCleanerTests(SqlServerIntegrationFixture db)
    : QueueCleanerTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>The job cleaner (plan task 3.5).</summary>
public sealed class SqlServerJobCleanerTests(SqlServerIntegrationFixture db)
    : JobCleanerTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>Periodic jobs across a change of leader (plan task 4.3).</summary>
public sealed class SqlServerPeriodicTests(SqlServerIntegrationFixture db)
    : PeriodicTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>Leader election between working clients (plan task 3.1).</summary>
public sealed class SqlServerLeadershipTests(SqlServerIntegrationFixture db)
    : LeadershipTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>External cancel (plan task 3.7).</summary>
public sealed class SqlServerCancelTests(SqlServerIntegrationFixture db)
    : CancelTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>Queue rows, pause and resume (plan task 3.6).</summary>
public sealed class SqlServerQueuePauseTests(SqlServerIntegrationFixture db)
    : QueuePauseTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>The admin API (plan task 4.4).</summary>
public sealed class SqlServerAdminTests(SqlServerIntegrationFixture db)
    : AdminTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>The scheduler (plan task 3.3).</summary>
public sealed class SqlServerSchedulerTests(SqlServerIntegrationFixture db)
    : SchedulerTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>The rescuer (plan task 3.4).</summary>
public sealed class SqlServerRescuerTests(SqlServerIntegrationFixture db)
    : RescuerTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;

/// <summary>Unique inserts through the client (plan task 4.2); <see cref="SqlServerUniqueTests"/> covers the driver.</summary>
public sealed class SqlServerClientUniqueTests(SqlServerIntegrationFixture db)
    : UniqueTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>;
