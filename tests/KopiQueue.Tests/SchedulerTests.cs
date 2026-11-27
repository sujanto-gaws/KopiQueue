using KopiQueue.Drivers;
using KopiQueue.Execution;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace KopiQueue.Tests;

/// <summary>
/// The scheduler service and its option (plan task 3.3: FR-SCH-01, FR-SCH-02, FR-MNT-02; design §5.7): its name, interval
/// and batch size, what one batch asks of the driver, and how <c>SchedulerInterval</c> is validated, mapped and shared
/// with the executor.
/// </summary>
public sealed class SchedulerTests
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void The_scheduler_runs_every_scheduler_interval_in_batches_of_10_000()
    {
        var scheduler = new Scheduler(new KopiClientLifecycleTests.IdleDriver(), TimeSpan.FromSeconds(3));

        Assert.Equal(("scheduler", TimeSpan.FromSeconds(3), 10_000), (scheduler.Name, scheduler.Interval, scheduler.BatchSize));
    }

    [Fact]
    public async Task A_batch_asks_for_the_batch_size_and_counts_promoted_and_discarded_jobs()
    {
        var driver = new KopiClientLifecycleTests.IdleDriver();
        var scheduler = new Scheduler(driver, TimeSpan.FromSeconds(5), batchSize: 250);

        var handled = await scheduler.RunBatchAsync(Ct);

        Assert.Equal(1, handled);
        Assert.Equal(250, driver.LastSchedule!.Max);
        Assert.Equal(JobScheduleParams.DefaultUniqueConflictError, driver.LastSchedule.UniqueConflictError);
    }

    [Fact]
    public void The_scheduler_interval_defaults_to_5_seconds_everywhere()
    {
        Assert.Equal(TimeSpan.FromSeconds(5), new KopiClientOptions().SchedulerInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), new KopiQueueOptions().SchedulerInterval);
        Assert.Equal(TimeSpan.FromSeconds(5), new JobExecutorOptions().SchedulerInterval);
    }

    [Fact]
    public void The_leader_runs_the_scheduler_at_the_clients_interval_with_the_rescuer_and_cleaners()
    {
        var driver = new KopiClientLifecycleTests.IdleDriver();
        var interval = TimeSpan.FromSeconds(2);

        var withCleaner = KopiClient.MaintenanceServices(driver, new KopiClientOptions { SchedulerInterval = interval });
        var withoutCleaner = KopiClient.MaintenanceServices(driver, new KopiClientOptions { SchedulerInterval = interval, QueueRetention = null });

        Assert.Equal(["scheduler", "rescuer", "job_cleaner", "queue_cleaner"], withCleaner.Select(s => s.Name));
        Assert.Equal(["scheduler", "rescuer", "job_cleaner"], withoutCleaner.Select(s => s.Name));
        Assert.Equal(interval, withoutCleaner[0].Interval);
        Assert.Equal(interval, withCleaner[0].Interval);
    }

    [Fact]
    public void The_executor_takes_the_schedulers_interval_so_short_retries_never_wait_for_a_slower_scheduler()
    {
        var options = KopiClient.ExecutorOptions(new KopiClientOptions { SchedulerInterval = TimeSpan.FromSeconds(12), JobTimeout = TimeSpan.FromMinutes(3) });

        Assert.Equal((TimeSpan.FromSeconds(12), TimeSpan.FromMinutes(3)), (options.SchedulerInterval, options.JobTimeout));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void A_scheduler_interval_that_is_not_positive_is_a_configuration_error(int seconds)
    {
        var errors = KopiClient.ConfigurationErrors(new KopiClientOptions { SchedulerInterval = TimeSpan.FromSeconds(seconds) });

        Assert.Contains("KopiClientOptions.SchedulerInterval must be positive.", errors);
    }

    [Fact]
    public void The_hosted_client_takes_the_scheduler_interval_from_configuration()
    {
        var builder = Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?> { ["KopiQueue:SchedulerInterval"] = "00:00:02.500" });
        builder.Services.AddKopiQueue(kq => kq.UseDriver(_ => new KopiClientLifecycleTests.IdleDriver())).BindConfiguration("KopiQueue");
        using var host = builder.Build();

        var client = host.Services.GetRequiredService<KopiClient>();

        Assert.Equal(TimeSpan.FromMilliseconds(2500), client.Options.SchedulerInterval);
    }
}
