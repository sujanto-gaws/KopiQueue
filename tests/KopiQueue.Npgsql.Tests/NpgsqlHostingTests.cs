using KopiQueue.TestInfrastructure.Integration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KopiQueue.Npgsql.Tests;

/// <summary>The shared hosting suite on PostgreSQL, and <c>UseKeyedNpgsql</c> (plan task 2.8: FR-HST-04).</summary>
public sealed class NpgsqlHostingTests(NpgsqlIntegrationFixture db)
    : HostingTests<NpgsqlIntegrationFixture>(db), IClassFixture<NpgsqlIntegrationFixture>
{
    [Fact]
    public async Task UseKeyedNpgsql_takes_the_data_source_registered_under_its_key()
    {
        await using var schema = await Fixture.CreateSchemaAsync(Ct);
        using var host = Hosted(services => services
            .AddKeyedSingleton("jobs", Fixture.DataSource)
            .AddKopiQueue(kq => kq
                .UseKeyedNpgsql("jobs", new NpgsqlDriverOptions { Schema = schema.Name })
                .AddQueue("hosted", 1)
                .AddWorker<HostedArgs, HostedWorker>()
                .Configure(o => o.FetchPollInterval = TimeSpan.FromMilliseconds(100))));
        await host.StartAsync(Ct);

        var id = (await host.Services.GetRequiredService<IKopiClient>().InsertAsync(new HostedArgs(1), new InsertOptions { Queue = "hosted" }, Ct)).Job.Id;
        await schema.WaitForStateAsync(id, JobState.Completed, Ct);
        await host.StopAsync(Ct);
    }
}
