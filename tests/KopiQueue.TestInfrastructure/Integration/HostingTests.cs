using System.Collections.Concurrent;
using KopiQueue.Drivers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// A client hosted with <c>AddKopiQueue</c> works jobs on a real database, configured from appsettings-style
/// configuration and registered with the driver's <c>Use…</c> method (plan tasks 2.8 and 5.8: FR-HST-01, -02, -04,
/// FR-DB-03; M2 exit criterion, including <c>PollOnly</c>).
/// </summary>
public abstract class HostingTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    /// <summary>The driver's fixture.</summary>
    protected TFixture Fixture => db;

    /// <summary>The current test's cancellation token.</summary>
    protected static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("Auto", "00:05:00")] // only a notification can pick the job up in time
    [InlineData("PollOnly", "00:00:00.200")]
    public async Task A_hosted_client_works_jobs_inserted_elsewhere_with_dependencies_from_a_scope_per_job(string mode, string poll)
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        Assert.SkipWhen(mode == "Auto" && !db.HasNotifications, "The driver has no notifications: the PollOnly case covers it.");
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KopiQueue:Queues:hosted:MaxWorkers"] = "4",
            ["KopiQueue:NotificationMode"] = mode,
            ["KopiQueue:FetchPollInterval"] = poll,
            ["KopiQueue:FetchCooldown"] = "00:00:00.005",
        });
        var seen = new Seen();
        builder.Services.AddSingleton(seen);
        builder.Services.AddScoped<PerJob>();
        builder.Services
            .AddKopiQueue(kq => schema.UseIn(kq).AddWorker<HostedArgs, HostedWorker>())
            .BindConfiguration("KopiQueue");
        using var host = builder.Build();
        await host.StartAsync(Ct);
        await Task.Delay(300, Ct); // the initial fetch has run and, with Auto, the listener is up

        var elsewhere = new KopiClient(schema.Driver);
        var ids = new List<long>();
        for (var i = 0; i < 3; i++)
        {
            ids.Add((await elsewhere.InsertAsync(new HostedArgs(i), new InsertOptions { Queue = "hosted" }, Ct)).Job.Id);
        }

        foreach (var id in ids)
        {
            await schema.WaitForStateAsync(id, JobState.Completed, Ct);
        }

        await host.StopAsync(Ct);

        Assert.Equal(3, seen.Scopes.Distinct().Count()); // a new scope, and so a new PerJob, for each job
        Assert.False(host.Services.GetRequiredService<KopiClient>().IsWorking);
    }

    [Fact]
    public async Task A_hosted_client_works_the_jobs_it_inserts_at_once_whatever_its_poll_interval()
    {
        // The hosted client's own inserts wake its producers in-process, with or without notifications (FR-DB-04).
        await using var schema = await db.CreateSchemaAsync(Ct);
        using var host = Hosted(services => services.AddKopiQueue(kq => schema.UseIn(kq)
            .AddQueue("hosted", 1)
            .AddWorker<HostedArgs, HostedWorker>()
            .Configure(o => o.FetchPollInterval = TimeSpan.FromMinutes(5))));
        await host.StartAsync(Ct);
        await Task.Delay(300, Ct); // past the initial fetch

        var id = (await host.Services.GetRequiredService<IKopiClient>().InsertAsync(new HostedArgs(1), new InsertOptions { Queue = "hosted" }, Ct)).Job.Id;
        await schema.WaitForStateAsync(id, JobState.Completed, Ct);
        await host.StopAsync(Ct);
    }

    /// <summary>A host with the services <see cref="HostedWorker"/> needs, plus <paramref name="configure"/>'s.</summary>
    protected static IHost Hosted(Action<IServiceCollection> configure)
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Services.AddSingleton(new Seen());
        builder.Services.AddScoped<PerJob>();
        configure(builder.Services);
        return builder.Build();
    }

    [JobKind("hosted")]
    public sealed record HostedArgs(int N) : IJobArgs;

    /// <summary>The scopes jobs ran in.</summary>
    protected sealed class Seen
    {
        public ConcurrentBag<Guid> Scopes { get; } = [];
    }

    /// <summary>A scoped dependency: one per job.</summary>
    protected sealed class PerJob
    {
        public Guid Id { get; } = Guid.NewGuid();
    }

    /// <summary>Records the scope each job ran in.</summary>
    protected sealed class HostedWorker(PerJob perJob, Seen seen) : Worker<HostedArgs>
    {
        public override Task WorkAsync(Job<HostedArgs> job, CancellationToken cancellationToken)
        {
            seen.Scopes.Add(perJob.Id);
            return Task.CompletedTask;
        }
    }
}
