using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace KopiQueue.Tests;

/// <summary><c>AddKopiQueue</c>: registration, configuration binding and validation, and the hosted service (plan task 2.8: FR-HST-01, -02).</summary>
public sealed class HostingTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void The_client_is_registered_once_as_itself_and_as_IKopiClient_with_a_hosted_service()
    {
        using var host = Host(kq => kq.UseDriver(_ => new KopiClientLifecycleTests.IdleDriver()));

        var client = host.Services.GetRequiredService<KopiClient>();

        Assert.Same(client, host.Services.GetRequiredService<IKopiClient>());
        Assert.Same(client, host.Services.GetRequiredService<KopiClient>());
        Assert.Single(host.Services.GetServices<IHostedService>(), s => s.GetType().Name == "KopiQueueHostedService");
    }

    [Fact]
    public void Configuration_is_bound_and_overrides_what_code_set()
    {
        using var host = Host(
            kq => kq.UseDriver(_ => new KopiClientLifecycleTests.IdleDriver()).AddQueue("default", 50).AddQueue("email", 10)
                .AddWorker(new KopiClientLifecycleTests.NoopWorker()),
            new()
            {
                ["KopiQueue:Queues:default:MaxWorkers"] = "5",
                ["KopiQueue:Queues:reports:MaxWorkers"] = "2",
                ["KopiQueue:ClientId"] = "worker-1",
                ["KopiQueue:JobTimeout"] = "00:05:00",
                ["KopiQueue:SoftStopTimeout"] = "00:00:10",
                ["KopiQueue:NotificationMode"] = "PollOnly",
                ["KopiQueue:CompleterMode"] = "Inline",
                ["KopiQueue:FetchBatchMax"] = "200",
            });

        var options = host.Services.GetRequiredService<IOptions<KopiQueueOptions>>().Value;

        Assert.Equal(
            [("default", 5), ("email", 10), ("reports", 2)],
            options.Queues.OrderBy(q => q.Key, StringComparer.Ordinal).Select(q => (q.Key, q.Value.MaxWorkers)));
        Assert.Equal(
            ("worker-1", TimeSpan.FromMinutes(5), (TimeSpan?)TimeSpan.FromSeconds(10), NotificationMode.PollOnly, CompleterMode.Inline, 200),
            (options.ClientId, options.JobTimeout, options.SoftStopTimeout, options.NotificationMode, options.CompleterMode, options.FetchBatchMax));
        Assert.Equal("worker-1", host.Services.GetRequiredService<KopiClient>().ClientId);
    }

    [Fact]
    public async Task An_invalid_configuration_fails_host_startup_listing_every_problem()
    {
        using var host = Host(
            kq => kq.UseDriver(_ => new KopiClientLifecycleTests.IdleDriver()).AddQueue("default", 0),
            new() { ["KopiQueue:FetchPollInterval"] = "00:00:00" });

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(Ct));

        Assert.Equal(3, error.Failures.Count());
        Assert.Contains(error.Failures, f => f.Contains("MaxWorkers", StringComparison.Ordinal));
        Assert.Contains(error.Failures, f => f.Contains("FetchPollInterval", StringComparison.Ordinal));
        Assert.Contains(error.Failures, f => f.Contains("no workers", StringComparison.Ordinal));
    }

    [Fact]
    public async Task A_missing_driver_fails_host_startup()
    {
        using var host = Host(_ => { });

        var error = await Assert.ThrowsAsync<OptionsValidationException>(() => host.StartAsync(Ct));

        Assert.Contains("UseNpgsql", Assert.Single(error.Failures), StringComparison.Ordinal);
    }

    [Fact]
    public async Task An_insert_only_host_starts_and_stops_without_touching_the_database()
    {
        var driver = new KopiClientLifecycleTests.IdleDriver();
        using var host = Host(kq => kq.UseDriver(_ => driver));

        await host.StartAsync(Ct);
        await Task.Delay(100, Ct);
        await host.StopAsync(Ct);

        Assert.Equal((0, 0), (driver.Fetches, driver.ListenerRequests));
    }

    [Fact]
    public async Task The_host_starts_and_stops_a_working_client()
    {
        var driver = new KopiClientLifecycleTests.IdleDriver();
        using var host = Host(
            kq => kq.UseDriver(_ => driver).AddQueue("default", 2).AddWorker(new KopiClientLifecycleTests.NoopWorker()),
            new() { ["KopiQueue:FetchPollInterval"] = "00:00:00.020" });
        var client = host.Services.GetRequiredService<KopiClient>();

        await host.StartAsync(Ct);
        var limit = DateTime.UtcNow.AddSeconds(10);
        while (driver.Fetches == 0)
        {
            Assert.True(DateTime.UtcNow < limit, "The client did not fetch.");
            await Task.Delay(10, Ct);
        }

        var working = client.IsWorking;
        await host.StopAsync(Ct);

        Assert.True(working);
        Assert.False(client.IsWorking);
    }

    [Fact]
    public void Adding_KopiQueue_twice_is_refused()
    {
        var services = new ServiceCollection();
        services.AddKopiQueue(_ => { });

        Assert.Throws<InvalidOperationException>(() => services.AddKopiQueue(_ => { }));
    }

    private static IHost Host(Action<KopiQueueBuilder> configure, Dictionary<string, string?>? settings = null)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(settings ?? []);
        builder.Services.AddKopiQueue(configure).BindConfiguration("KopiQueue");
        return builder.Build();
    }
}
