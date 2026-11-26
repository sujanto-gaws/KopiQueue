using System.Collections.Concurrent;
using KopiQueue.Drivers;
using KopiQueue.Migrations;
using KopiQueue.TestInfrastructure;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;

namespace KopiQueue.Npgsql.Tests;

/// <summary>
/// A client hosted with <c>AddKopiQueue</c> works jobs on PostgreSQL, configured from appsettings-style configuration
/// (plan task 2.8: FR-HST-01, -02, -04; M2 exit criterion, including <c>PollOnly</c>).
/// </summary>
public sealed class NpgsqlHostingTests(PostgresFixture db) : IClassFixture<PostgresFixture>
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("Auto", "00:05:00")] // only LISTEN can pick the job up in time
    [InlineData("PollOnly", "00:00:00.200")]
    public async Task A_hosted_client_works_jobs_inserted_elsewhere_with_dependencies_from_a_scope_per_job(string mode, string poll)
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await MigratedSchemaAsync(ds);
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KopiQueue:Queues:hosted:MaxWorkers"] = "4",
            ["KopiQueue:NotificationMode"] = mode,
            ["KopiQueue:FetchPollInterval"] = poll,
            ["KopiQueue:FetchCooldown"] = "00:00:00.005",
        });
        var seen = new Seen();
        builder.Services.AddSingleton(ds);
        builder.Services.AddSingleton(seen);
        builder.Services.AddScoped<PerJob>();
        builder.Services
            .AddKopiQueue(kq => kq.UseNpgsql(new NpgsqlDriverOptions { Schema = schema }).AddWorker<HostedArgs, HostedWorker>())
            .BindConfiguration("KopiQueue");
        using var host = builder.Build();
        await host.StartAsync(Ct);
        await Task.Delay(300, Ct); // the initial fetch has run and, with Auto, the listener is up

        var elsewhere = new KopiClient(new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema }));
        var ids = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            ids.Add((await elsewhere.InsertAsync(new HostedArgs(i), new InsertOptions { Queue = "hosted" }, Ct)).Job.Id);
        }

        var driver = new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema });
        foreach (var id in ids)
        {
            await WaitForCompletedAsync(driver, id);
        }

        await host.StopAsync(Ct);

        Assert.Equal(3, seen.Scopes.Distinct().Count()); // a new scope, and so a new PerJob, for each job
        Assert.False(host.Services.GetRequiredService<KopiClient>().IsWorking);
    }

    [Fact]
    public async Task UseKeyedNpgsql_takes_the_data_source_registered_under_its_key()
    {
        db.SkipIfUnavailable();
        await using var ds = db.CreateDataSource();
        var schema = await MigratedSchemaAsync(ds);
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddKeyedSingleton("jobs", ds);
        builder.Services.AddSingleton(new Seen());
        builder.Services.AddScoped<PerJob>();
        builder.Services.AddKopiQueue(kq => kq
            .UseKeyedNpgsql("jobs", new NpgsqlDriverOptions { Schema = schema })
            .AddQueue("hosted", 1)
            .AddWorker<HostedArgs, HostedWorker>()
            .Configure(o => o.FetchPollInterval = TimeSpan.FromMilliseconds(100)));
        using var host = builder.Build();
        await host.StartAsync(Ct);

        var id = (await host.Services.GetRequiredService<IKopiClient>().InsertAsync(new HostedArgs(1), new InsertOptions { Queue = "hosted" }, Ct)).Job.Id;
        await WaitForCompletedAsync(new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema }), id);
        await host.StopAsync(Ct);
    }

    private static async Task WaitForCompletedAsync(IKopiDriver driver, long id)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (true)
        {
            var job = await driver.GetExecutor().JobGetByIdAsync(id, Ct) ?? throw new InvalidOperationException($"Job {id} is missing.");
            if (job.State == JobState.Completed)
            {
                return;
            }

            Assert.True(DateTime.UtcNow < deadline, $"Job {id} is {job.State} after 15 s.");
            await Task.Delay(20, Ct);
        }
    }

    private static async Task<string> MigratedSchemaAsync(NpgsqlDataSource ds)
    {
        var schema = "x_" + Guid.NewGuid().ToString("N")[..12];
        await using (var cmd = ds.CreateCommand($"CREATE SCHEMA {schema}"))
        {
            await cmd.ExecuteNonQueryAsync(Ct);
        }

        await new NpgsqlDriver(ds, new NpgsqlDriverOptions { Schema = schema }).CreateMigrator().MigrateAsync(MigrationDirection.Up, cancellationToken: Ct);
        return schema;
    }

    [JobKind("hosted")]
    public sealed record HostedArgs(int N) : IJobArgs;

    private sealed class Seen
    {
        public ConcurrentBag<Guid> Scopes { get; } = [];
    }

    private sealed class PerJob
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    private sealed class HostedWorker(PerJob perJob, Seen seen) : Worker<HostedArgs>
    {
        public override Task WorkAsync(Job<HostedArgs> job, CancellationToken cancellationToken)
        {
            seen.Scopes.Add(perJob.Id);
            return Task.CompletedTask;
        }
    }
}
