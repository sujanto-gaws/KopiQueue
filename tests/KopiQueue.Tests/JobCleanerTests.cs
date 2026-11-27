using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KopiQueue.Tests;

/// <summary>
/// The job cleaner's service and options (plan task 3.5: FR-MNT-04; design §5.7): its name, interval and batch size,
/// what it asks the driver for, and the three retentions' defaults, validation and mapping.
/// </summary>
public sealed class JobCleanerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void The_job_cleaner_runs_every_30_seconds_in_batches_of_1_000()
    {
        var cleaner = Assert.IsType<JobCleaner>(
            Assert.Single(KopiClient.MaintenanceServices(new KopiClientLifecycleTests.IdleDriver(), new KopiClientOptions()), s => s.Name == "job_cleaner"));

        Assert.Equal((TimeSpan.FromSeconds(30), 1_000), (cleaner.Interval, cleaner.BatchSize));
    }

    [Fact]
    public async Task A_batch_asks_for_each_states_retention_and_counts_the_deleted_jobs()
    {
        var driver = new KopiClientLifecycleTests.IdleDriver { DeletedFinalized = [3, 5, 8] };
        var cleaner = Assert.IsType<JobCleaner>(Assert.Single(
            KopiClient.MaintenanceServices(driver, new KopiClientOptions { CompletedJobRetention = TimeSpan.FromHours(2), CancelledJobRetention = null }),
            s => s.Name == "job_cleaner"));

        Assert.Equal(3, await cleaner.RunBatchAsync(Ct));

        var asked = driver.LastDeleteFinalized!;
        Assert.Equal(
            (TimeSpan.FromHours(2), (TimeSpan?)null, TimeSpan.FromDays(7), 1_000),
            (asked.CompletedRetention, asked.CancelledRetention, asked.DiscardedRetention, asked.Max));
    }

    [Fact]
    public void With_no_retention_at_all_the_job_cleaner_is_not_run()
    {
        var options = new KopiClientOptions { CompletedJobRetention = null, CancelledJobRetention = null, DiscardedJobRetention = null };

        var names = KopiClient.MaintenanceServices(new KopiClientLifecycleTests.IdleDriver(), options).Select(s => s.Name);

        Assert.DoesNotContain("job_cleaner", names);
        Assert.Contains("job_cleaner", KopiClient.MaintenanceServices(
            new KopiClientLifecycleTests.IdleDriver(),
            new KopiClientOptions { CompletedJobRetention = null, CancelledJobRetention = null, DiscardedJobRetention = TimeSpan.FromDays(1) }).Select(s => s.Name));
    }

    [Fact]
    public void The_retentions_default_to_24_hours_24_hours_and_7_days()
    {
        var client = new KopiClientOptions();
        var hosted = new KopiQueueOptions();

        Assert.Equal((TimeSpan.FromHours(24), TimeSpan.FromHours(24), TimeSpan.FromDays(7)), (client.CompletedJobRetention, client.CancelledJobRetention, client.DiscardedJobRetention));
        Assert.Equal((TimeSpan.FromHours(24), TimeSpan.FromHours(24), TimeSpan.FromDays(7)), (hosted.CompletedJobRetention, hosted.CancelledJobRetention, hosted.DiscardedJobRetention));
    }

    [Theory]
    [InlineData("CompletedJobRetention")]
    [InlineData("CancelledJobRetention")]
    [InlineData("DiscardedJobRetention")]
    public void Each_retention_must_be_positive_or_null(string option)
    {
        KopiClientOptions With(TimeSpan? value) => option switch
        {
            "CompletedJobRetention" => new KopiClientOptions { CompletedJobRetention = value },
            "CancelledJobRetention" => new KopiClientOptions { CancelledJobRetention = value },
            _ => new KopiClientOptions { DiscardedJobRetention = value },
        };

        bool Invalid(TimeSpan? value) => KopiClient.ConfigurationErrors(With(value)).Any(e => e.Contains(option, StringComparison.Ordinal));

        Assert.True(Invalid(TimeSpan.Zero));
        Assert.True(Invalid(TimeSpan.FromSeconds(-1)));
        Assert.False(Invalid(TimeSpan.FromTicks(1))); // no minimum: finalised jobs are not refreshed
        Assert.False(Invalid(null));
    }

    [Fact]
    public void The_hosted_client_takes_the_retentions_from_configuration_and_an_empty_value_keeps_jobs()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KopiQueue:CompletedJobRetention"] = "01:00:00",
            ["KopiQueue:CancelledJobRetention"] = "2.00:00:00",
            ["KopiQueue:DiscardedJobRetention"] = "30.00:00:00",
        });
        builder.Services.AddKopiQueue(kq => kq.UseDriver(_ => new KopiClientLifecycleTests.IdleDriver())).BindConfiguration("KopiQueue");
        using var host = builder.Build();

        var options = host.Services.GetRequiredService<KopiClient>().Options;

        Assert.Equal(
            (TimeSpan.FromHours(1), TimeSpan.FromDays(2), TimeSpan.FromDays(30)),
            (options.CompletedJobRetention, options.CancelledJobRetention, options.DiscardedJobRetention));
    }

    [Fact]
    public void The_hosted_client_maps_a_null_retention()
    {
        var services = new ServiceCollection();
        services.AddKopiQueue(kq =>
        {
            kq.UseDriver(_ => new KopiClientLifecycleTests.IdleDriver());
            kq.Configure(o => o.CompletedJobRetention = null);
        });
        using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetRequiredService<KopiClient>().Options.CompletedJobRetention);
    }
}
