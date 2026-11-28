using KopiQueue.TestInfrastructure.Integration;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KopiQueue.SqlServer.Tests;

/// <summary>
/// The shared hosting suite on SQL Server through <c>UseSqlServer</c>, and its configuration overload (plan task 5.8:
/// FR-HST-04, FR-DB-03).
/// </summary>
public sealed class SqlServerHostingTests(SqlServerIntegrationFixture db)
    : HostingTests<SqlServerIntegrationFixture>(db), IClassFixture<SqlServerIntegrationFixture>
{
    [Fact]
    public async Task UseSqlServer_can_read_its_connection_string_from_configuration_when_the_client_is_created()
    {
        await using var schema = await Fixture.CreateSchemaAsync(Ct);
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["ConnectionStrings:Jobs"] = Fixture.ConnectionString })
            .Build();
        using var host = Hosted(services => services
            .AddSingleton<IConfiguration>(configuration)
            .AddKopiQueue(kq => kq
                .UseSqlServer(sp => sp.GetRequiredService<IConfiguration>().GetConnectionString("Jobs")!, new SqlServerDriverOptions { Schema = schema.Name })
                .AddQueue("hosted", 1)
                .AddWorker<HostedArgs, HostedWorker>()));
        await host.StartAsync(Ct);

        var id = (await host.Services.GetRequiredService<IKopiClient>().InsertAsync(new HostedArgs(1), new InsertOptions { Queue = "hosted" }, Ct)).Job.Id;
        await schema.WaitForStateAsync(id, JobState.Completed, Ct);
        await host.StopAsync(Ct);
    }

    [Fact]
    public void UseSqlServer_refuses_a_missing_builder_or_connection_string()
    {
        var builder = new ServiceCollection().AddKopiQueue(_ => { });

        Assert.Throws<ArgumentNullException>(() => SqlServerKopiQueueBuilderExtensions.UseSqlServer(null!, "Server=x"));
        Assert.Throws<ArgumentNullException>(() => builder.UseSqlServer((string)null!));
        Assert.Throws<ArgumentException>(() => builder.UseSqlServer(" "));
        Assert.Throws<ArgumentNullException>(() => builder.UseSqlServer((Func<IServiceProvider, string>)null!));
    }
}
