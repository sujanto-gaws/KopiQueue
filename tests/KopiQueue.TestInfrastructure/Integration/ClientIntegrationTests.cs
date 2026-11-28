using System.Collections.Concurrent;
using KopiQueue.Drivers;
using Xunit;

namespace KopiQueue.TestInfrastructure.Integration;

/// <summary>
/// Working clients on a real database: timeouts, retries, several clients on one queue, stopping under load and
/// rolling deploys (plan task 2.9: FR-EXE-04, -05, FR-ERR-01..04, FR-HST-03, NFR-REL-01, DQ-3).
/// </summary>
public abstract class ClientIntegrationTests<TFixture>(TFixture db)
    where TFixture : IntegrationFixture
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_job_that_outlives_its_timeout_fails_with_a_timeout_error_and_is_retried()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var worker = new ScriptedWorker { TimeoutAfter = TimeSpan.FromMilliseconds(200), RetryIn = TimeSpan.FromMinutes(10) };
        var id = await InsertAsync(driver, "hang");

        await using (await StartedAsync(driver, worker))
        {
            var job = await WaitForAsync(driver, id, j => j.State == JobState.Retryable);

            Assert.Equal(1, job.Attempt);
            Assert.Contains("did not finish within its timeout of 00:00:00.2", Assert.Single(job.Errors).Error, StringComparison.Ordinal);
            Assert.True(worker.SawCancellation);
        }
    }

    [Fact]
    public async Task A_failing_job_is_retried_until_it_succeeds_with_each_error_recorded()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var id = await InsertAsync(driver, "fail-twice");

        await using (await StartedAsync(driver, new ScriptedWorker { RetryIn = TimeSpan.FromMilliseconds(100) }))
        {
            var job = await WaitForAsync(driver, id, j => j.State == JobState.Completed);

            Assert.Equal(3, job.Attempt);
            Assert.Equal([(1, "attempt 1 failed"), (2, "attempt 2 failed")], job.Errors.Select(e => (e.Attempt, e.Error)));
            Assert.Equal(["test-client", "test-client", "test-client"], job.AttemptedBy);
        }
    }

    [Fact]
    public async Task A_job_that_keeps_failing_is_discarded_after_its_last_attempt()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var id = await InsertAsync(driver, "fail", maxAttempts: 3);

        await using (await StartedAsync(driver, new ScriptedWorker { RetryIn = TimeSpan.FromMilliseconds(100) }))
        {
            var job = await WaitForAsync(driver, id, j => j.State == JobState.Discarded);

            Assert.Equal((3, 3), (job.Attempt, job.Errors.Count));
            Assert.NotNull(job.FinalizedAt);
        }
    }

    [Fact]
    public async Task A_short_snooze_runs_the_job_again_without_using_an_attempt()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var id = await InsertAsync(driver, "snooze-once");

        await using (await StartedAsync(driver, new ScriptedWorker()))
        {
            var job = await WaitForAsync(driver, id, j => j.State == JobState.Completed);

            Assert.Equal(1, job.Attempt); // the snoozed attempt was given back
            Assert.Empty(job.Errors);
            using var metadata = System.Text.Json.JsonDocument.Parse(job.MetadataJson);
            Assert.Equal(1, metadata.RootElement.GetProperty("snoozes").GetInt32());
        }
    }

    [Fact]
    public async Task Several_clients_share_a_queue_run_each_job_once_and_respect_their_concurrency()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        const int Jobs = 300;
        var workers = Enumerable.Range(0, 3).Select(_ => new ScriptedWorker { WorkFor = (2, 6) }).ToArray();
        var clients = new List<KopiClient>();
        foreach (var (worker, i) in workers.Select((w, i) => (w, i)))
        {
            clients.Add(await StartedClientAsync(driver, worker, clientId: $"client-{i}", maxWorkers: 4));
        }

        try
        {
            await driver.GetExecutor().JobInsertFastAsync(
                [.. Enumerable.Range(0, Jobs).Select(_ => Params("ok"))],
                Ct);
            await schema.WaitForCountAsync(j => j.State == JobState.Completed, Jobs, TimeSpan.FromSeconds(60), Ct);
        }
        finally
        {
            foreach (var client in clients)
            {
                await client.StopAsync(Ct);
            }
        }

        var ran = workers.SelectMany(w => w.Ran).ToArray();
        Assert.Equal(Jobs, ran.Length);
        Assert.Equal(Jobs, ran.Distinct().Count());
        Assert.All(workers, w => Assert.InRange(w.Peak, 0, 4));
        Assert.True(workers.Count(w => !w.Ran.IsEmpty) > 1, "One client ran every job.");
        Assert.Equal(Jobs, await schema.CountAsync(j => j.State == JobState.Completed && j.Attempt == 1 && j.AttemptedBy.Count == 1, Ct));
    }

    [Fact]
    public async Task Stopping_under_load_loses_no_job_and_leaves_none_running()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        const int Jobs = 200;
        await driver.GetExecutor().JobInsertFastAsync([.. Enumerable.Range(0, Jobs).Select(_ => Params("ok"))], Ct);
        const int Finishing = 30;
        var worker = new ScriptedWorker { WorkFor = (5, 20), HangAfter = Finishing }; // later jobs run until cancelled
        var client = await StartedClientAsync(driver, worker, maxWorkers: 10);

        await schema.WaitForCountAsync(j => j.State == JobState.Completed, Finishing, TimeSpan.FromSeconds(30), Ct);
        using var shutdown = new CancellationTokenSource(TimeSpan.FromMilliseconds(100));
        await client.StopAsync(shutdown.Token).WaitAsync(TimeSpan.FromSeconds(30), Ct);

        Assert.Equal(Finishing, await schema.CountAsync(j => j.State == JobState.Completed && j.Attempt == 1, Ct));
        Assert.Equal(0, await schema.CountAsync(j => j.State == JobState.Running, Ct));
        Assert.Equal(Jobs - Finishing, await schema.CountAsync(j => j.State == JobState.Available && j.Attempt == 0 && j.Errors.Count == 0, Ct));
        Assert.Equal(worker.Ran.Count - Finishing, worker.Interrupted); // the hard stop interrupted every job still running
        Assert.InRange(worker.Interrupted, 1, 10);
    }

    [Fact]
    public async Task During_a_rolling_deploy_a_job_of_an_unknown_kind_is_retried_until_a_client_with_its_worker_takes_it()
    {
        await using var schema = await db.CreateSchemaAsync(Ct);
        var driver = schema.Driver;
        var id = (await new KopiClient(driver).InsertAsync(new NewKindArgs(), new InsertOptions { Queue = "it" }, Ct)).Job.Id;

        // The old version has no worker for the new kind: it fetches the job once (poll-only, long poll) and fails it.
        var old = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["it"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(new ScriptedWorker()),
            NotificationMode = NotificationMode.PollOnly,
            FetchPollInterval = TimeSpan.FromMinutes(5),
            ClientId = "old",
        });
        await old.StartAsync(Ct);
        var failed = await WaitForAsync(driver, id, j => j.Errors.Count == 1);
        await old.StopAsync(Ct);

        // The new version has the worker and takes the job once its retry is due, about a second later.
        var upgraded = new NewKindWorker();
        var current = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["it"] = new() { MaxWorkers = 1 } },
            Workers = new WorkerRegistry().Add(upgraded),
            FetchPollInterval = TimeSpan.FromMilliseconds(100),
            ClientId = "new",
        });
        await current.StartAsync(Ct);
        try
        {
            var job = await WaitForAsync(driver, id, j => j.State == JobState.Completed);

            Assert.Equal(JobState.Available, failed.State); // the first retry is due within 5 s
            Assert.Equal("No worker is registered for kind 'it_new_kind' on this client.", failed.Errors[0].Error);
            Assert.Equal(2, job.Attempt);
            Assert.Equal(["old", "new"], job.AttemptedBy);
            Assert.True(upgraded.Ran);
        }
        finally
        {
            await current.StopAsync(Ct);
        }
    }

    private static async Task<long> InsertAsync(IKopiDriver driver, string script, int maxAttempts = 25) =>
        (await new KopiClient(driver).InsertAsync(new ScriptArgs(script), new InsertOptions { Queue = "it", MaxAttempts = maxAttempts }, Ct)).Job.Id;

    private static JobInsertParams Params(string script) => new()
    {
        Kind = "it_script", ArgsJson = $$"""{"script":"{{script}}"}""", Queue = "it", State = JobState.Available, Priority = 1, MaxAttempts = 25,
    };

    private static async Task<StoppingClient> StartedAsync(IKopiDriver driver, ScriptedWorker worker) =>
        new(await StartedClientAsync(driver, worker));

    private static async Task<KopiClient> StartedClientAsync(IKopiDriver driver, ScriptedWorker worker, string clientId = "test-client", int maxWorkers = 5)
    {
        var client = new KopiClient(driver, new KopiClientOptions
        {
            Queues = new Dictionary<string, QueueOptions> { ["it"] = new() { MaxWorkers = maxWorkers } },
            Workers = new WorkerRegistry().Add(worker),
            ClientId = clientId,
            FetchPollInterval = TimeSpan.FromMilliseconds(100),
            FetchCooldown = TimeSpan.FromMilliseconds(5),
        });
        await client.StartAsync(Ct);
        return client;
    }

    private static async Task<JobRow> WaitForAsync(IKopiDriver driver, long id, Func<JobRow, bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(20);
        while (true)
        {
            var job = await driver.GetExecutor().JobGetByIdAsync(id, Ct) ?? throw new InvalidOperationException($"Job {id} is missing.");
            if (condition(job))
            {
                return job;
            }

            Assert.True(DateTime.UtcNow < deadline, $"Job {id} is {job.State} at attempt {job.Attempt} with {job.Errors.Count} errors after 20 s.");
            await Task.Delay(20, Ct);
        }
    }

    private sealed class StoppingClient(KopiClient client) : IAsyncDisposable
    {
        public ValueTask DisposeAsync() => new(client.StopAsync(Ct));
    }

    [JobKind("it_script")]
    public sealed record ScriptArgs(string Script) : IJobArgs;

    [JobKind("it_new_kind")]
    public sealed record NewKindArgs : IJobArgs;

    private sealed class NewKindWorker : Worker<NewKindArgs>
    {
        public bool Ran { get; private set; }

        public override Task WorkAsync(Job<NewKindArgs> job, CancellationToken cancellationToken)
        {
            Ran = true;
            return Task.CompletedTask;
        }
    }

    /// <summary>Does what each job's script says, and records what ran.</summary>
    private sealed class ScriptedWorker : Worker<ScriptArgs>
    {
        private int _running;
        private int _peak;
        private int _interrupted;
        private int _started;

        public TimeSpan? TimeoutAfter { get; init; }

        public TimeSpan? RetryIn { get; init; }

        public (int Min, int Max) WorkFor { get; init; }

        /// <summary>Jobs started after this many run until they are cancelled.</summary>
        public int? HangAfter { get; init; }

        public ConcurrentBag<long> Ran { get; } = [];

        public int Peak => Volatile.Read(ref _peak);

        public int Interrupted => Volatile.Read(ref _interrupted);

        public bool SawCancellation { get; private set; }

        public override TimeSpan? Timeout(Job<ScriptArgs> job) => TimeoutAfter;

        public override TimeSpan? NextRetry(Job<ScriptArgs> job) => RetryIn;

        public override async Task WorkAsync(Job<ScriptArgs> job, CancellationToken cancellationToken)
        {
            var now = Interlocked.Increment(ref _running);
            for (var peak = Volatile.Read(ref _peak); now > peak && Interlocked.CompareExchange(ref _peak, now, peak) != peak; peak = Volatile.Read(ref _peak))
            {
            }

            try
            {
                Ran.Add(job.Id);
                var order = Interlocked.Increment(ref _started);
                switch (job.Args.Script)
                {
                    case "hang":
                        try
                        {
                            await Task.Delay(System.Threading.Timeout.Infinite, cancellationToken);
                        }
                        catch (OperationCanceledException)
                        {
                            SawCancellation = true;
                            throw;
                        }

                        break;
                    case "fail":
                    case "fail-twice" when job.Attempt <= 2:
                        throw new InvalidOperationException($"attempt {job.Attempt} failed");
                    case "snooze-once" when !job.Row.MetadataJson.Contains("snoozes", StringComparison.Ordinal):
                        throw JobControl.Snooze(TimeSpan.FromMilliseconds(200));
                    default:
                        if (WorkFor.Max > 0)
                        {
                            try
                            {
                                var hang = order > HangAfter;
                                await Task.Delay(hang ? System.Threading.Timeout.Infinite : Random.Shared.Next(WorkFor.Min, WorkFor.Max), cancellationToken);
                            }
                            catch (OperationCanceledException)
                            {
                                Interlocked.Increment(ref _interrupted);
                                throw;
                            }
                        }

                        break;
                }
            }
            finally
            {
                Interlocked.Decrement(ref _running);
            }
        }
    }
}
