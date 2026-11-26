using System.Collections.Concurrent;
using System.Text.Json.Serialization;
using KopiQueue.Drivers;
using KopiQueue.Execution;
using KopiQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;

namespace KopiQueue.Tests;

/// <summary>
/// The executor: DI scope per job, deserialisation, timeout and outcome classification (plan task 2.4: FR-DEF-03,
/// FR-EXE-05, FR-EXE-07, FR-EXE-08; DQ-3).
/// </summary>
public sealed class JobExecutorTests
{
    private static readonly IJobArgsSerializer Serializer = new SystemTextJsonArgsSerializer(ExecutorJsonContext.Default);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task A_worker_that_returns_completes_the_job()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        Job<PingArgs>? seen = null;
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((job, _) =>
        {
            seen = job;
            time.Advance(TimeSpan.FromSeconds(2));
            return Task.CompletedTask;
        })), completer, time);
        var row = Row("ping", """{"Text":"hi"}""", attempt: 3, metadata: """{"tenant":"a"}""");

        await executor.ExecuteAsync(row, Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal(new JobCompletion(row, JobState.Completed, time.GetUtcNow(), null, null, TimeSpan.FromSeconds(2)), completion);
        Assert.NotNull(seen);
        Assert.Equal(("hi", 7L, 3), (seen.Args.Text, seen.Id, seen.Attempt));
        Assert.Equal("a", seen.Metadata["tenant"]!.GetValue<string>());
    }

    [Fact]
    public async Task An_exception_is_recorded_and_the_job_retried_after_the_default_backoff()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(Throwing(new InvalidOperationException("boom")), completer, time, random: () => 0.5);

        await executor.ExecuteAsync(Row("ping", attempt: 2), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Retryable, (DateTimeOffset?)null), (completion.State, completion.FinalizedAt));
        Assert.Equal(time.GetUtcNow() + TimeSpan.FromSeconds(16), completion.ScheduledAt);
        Assert.NotNull(completion.Error);
        Assert.Equal((time.GetUtcNow(), 2, "boom"), (completion.Error.At, completion.Error.Attempt, completion.Error.Error));
        Assert.Contains("System.InvalidOperationException: boom", completion.Error.Trace, StringComparison.Ordinal);
        Assert.Contains(nameof(Throwing), completion.Error.Trace, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_last_attempt_failing_discards_the_job()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(Throwing(new InvalidOperationException("boom")), completer, time);

        await executor.ExecuteAsync(Row("ping", attempt: 5, maxAttempts: 5), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Discarded, time.GetUtcNow(), (DateTimeOffset?)null), (completion.State, completion.FinalizedAt, completion.ScheduledAt));
        Assert.Equal("boom", completion.Error!.Error);
    }

    [Fact]
    public async Task The_workers_next_retry_wins_and_a_throwing_or_negative_one_falls_back_to_the_backoff()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        TimeSpan? next = TimeSpan.FromMinutes(7);
        var throwNext = false;
        var worker = new LambdaWorker<PingArgs>((_, _) => throw new InvalidOperationException("x"))
        {
            NextRetryOverride = _ => throwNext ? throw new InvalidOperationException("bad override") : next,
        };
        var executor = Executor(new WorkerRegistry().Add(worker), completer, time, random: () => 0.5);

        await executor.ExecuteAsync(Row("ping", attempt: 1), Ct);
        next = TimeSpan.FromSeconds(-1);
        await executor.ExecuteAsync(Row("ping", attempt: 1), Ct);
        throwNext = true;
        await executor.ExecuteAsync(Row("ping", attempt: 1), Ct);

        Assert.Equal(
            [TimeSpan.FromMinutes(7), TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(1)],
            completer.Completions.Select(c => c.ScheduledAt!.Value - time.GetUtcNow()));
    }

    [Fact]
    public async Task A_job_that_exceeds_the_client_timeout_has_its_token_cancelled_and_counts_as_an_error()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var observed = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>(async (_, ct) =>
        {
            try
            {
                await Task.Delay(Timeout.Infinite, ct);
            }
            finally
            {
                observed.TrySetResult(ct.IsCancellationRequested);
            }
        })), completer, time, timeout: TimeSpan.FromSeconds(30));

        var run = executor.ExecuteAsync(Row("ping"), Ct);
        time.Advance(TimeSpan.FromSeconds(29));
        await Task.Delay(50, Ct);
        var early = run.IsCompleted;
        time.Advance(TimeSpan.FromSeconds(1));
        await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.False(early);
        Assert.True(await observed.Task);
        var completion = Assert.Single(completer.Completions);
        Assert.Equal(JobState.Retryable, completion.State);
        Assert.Equal("The job did not finish within its timeout of 00:00:30.", completion.Error!.Error);
    }

    [Fact]
    public async Task The_workers_timeout_overrides_the_clients_and_infinite_means_none()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var timeouts = new Queue<TimeSpan?>([TimeSpan.FromSeconds(5), Timeout.InfiniteTimeSpan]);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = new LambdaWorker<PingArgs>(async (_, ct) => await release.Task.WaitAsync(ct))
        {
            TimeoutOverride = _ => timeouts.Dequeue(),
        };
        var executor = Executor(new WorkerRegistry().Add(worker), completer, time, timeout: TimeSpan.FromHours(1));

        var five = executor.ExecuteAsync(Row("ping"), Ct);
        time.Advance(TimeSpan.FromSeconds(5));
        await five.WaitAsync(TimeSpan.FromSeconds(10), Ct);
        var infinite = executor.ExecuteAsync(Row("ping"), Ct);
        time.Advance(TimeSpan.FromDays(30));
        await Task.Delay(50, Ct);
        var stillRunning = !infinite.IsCompleted;
        release.SetResult();
        await infinite.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        Assert.True(stillRunning);
        Assert.Equal([JobState.Retryable, JobState.Completed], completer.Completions.Select(c => c.State));
        Assert.Contains("00:00:05", completer.Completions[0].Error!.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_hard_stop_cancels_the_token_and_the_attempt_is_retried()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, ct) => Task.Delay(Timeout.Infinite, ct))), completer, time);
        using var hardStop = new CancellationTokenSource();

        var run = executor.ExecuteAsync(Row("ping"), hardStop.Token);
        await hardStop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal(JobState.Retryable, completion.State);
        Assert.Equal("The client stopped before the job finished.", completion.Error!.Error);
    }

    [Fact]
    public async Task A_cancellation_the_worker_raises_itself_is_an_ordinary_error()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(Throwing(new OperationCanceledException("gave up")), completer, time);

        await executor.ExecuteAsync(Row("ping"), Ct);

        Assert.Equal("gave up", Assert.Single(completer.Completions).Error!.Error);
    }

    [Fact]
    public async Task An_unknown_kind_is_an_error_that_is_retried_so_another_deployment_can_take_it()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, _) => Task.CompletedTask)), completer, time, random: () => 0.5);

        await executor.ExecuteAsync(Row("from_the_future", attempt: 1), Ct);
        await executor.ExecuteAsync(Row("from_the_future", attempt: 25, maxAttempts: 25), Ct);

        Assert.Equal([JobState.Retryable, JobState.Discarded], completer.Completions.Select(c => c.State));
        Assert.Equal(time.GetUtcNow() + TimeSpan.FromSeconds(1), completer.Completions[0].ScheduledAt);
        Assert.Equal("No worker is registered for kind 'from_the_future' on this client.", completer.Completions[0].Error!.Error);
        Assert.Null(completer.Completions[0].Error!.Trace);
    }

    [Fact]
    public async Task Args_that_do_not_deserialise_fail_the_attempt_without_running_the_worker()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var ran = false;
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, _) =>
        {
            ran = true;
            return Task.CompletedTask;
        })), completer, time);

        await executor.ExecuteAsync(Row("ping", args: "not json"), Ct);

        Assert.False(ran);
        var completion = Assert.Single(completer.Completions);
        Assert.Equal(JobState.Retryable, completion.State);
        Assert.Contains("JsonException", completion.Error!.Trace, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Each_job_gets_its_own_scope_which_is_disposed_after_the_job()
    {
        var completer = new RecordingCompleter();
        await using var services = new ServiceCollection().AddScoped<ScopedThing>().BuildServiceProvider();
        var executor = new JobExecutor(new WorkerRegistry().Add<PingArgs, ScopedWorker>(), Serializer, completer, services);

        await executor.ExecuteAsync(Row("ping"), Ct);
        await executor.ExecuteAsync(Row("ping"), Ct);

        Assert.Equal([JobState.Completed, JobState.Completed], completer.Completions.Select(c => c.State));
        Assert.Equal(2, ScopedWorker.Seen.Count);
        Assert.NotSame(ScopedWorker.Seen[0], ScopedWorker.Seen[1]);
        Assert.All(ScopedWorker.Seen, thing => Assert.True(thing.Disposed));
    }

    [Fact]
    public async Task Without_services_a_worker_with_a_public_constructor_is_created_and_disposed_per_job()
    {
        var completer = new RecordingCompleter();
        var executor = new JobExecutor(new WorkerRegistry().Add<PingArgs, DisposableWorker>(), Serializer, completer);
        DisposableWorker.Created.Clear();

        await executor.ExecuteAsync(Row("ping"), Ct);

        Assert.Equal(JobState.Completed, Assert.Single(completer.Completions).State);
        Assert.True(Assert.Single(DisposableWorker.Created).Disposed);
    }

    [Fact]
    public async Task A_throwing_timeout_override_fails_the_attempt()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var worker = new LambdaWorker<PingArgs>((_, _) => Task.CompletedTask) { TimeoutOverride = _ => throw new InvalidOperationException("no timeout") };
        var executor = Executor(new WorkerRegistry().Add(worker), completer, time);

        await executor.ExecuteAsync(Row("ping"), Ct);

        Assert.Equal("no timeout", Assert.Single(completer.Completions).Error!.Error);
    }

    [Theory]
    [InlineData(1, 1)]
    [InlineData(2, 16)]
    [InlineData(3, 81)]
    [InlineData(5, 625)]
    [InlineData(10, 10_000)]
    public void The_default_backoff_is_attempt_to_the_fourth_seconds_with_ten_percent_jitter(int attempt, double seconds)
    {
        Assert.Equal(seconds, RetryBackoff.Delay(attempt, 0.5).TotalSeconds, 6);
        Assert.Equal(seconds * 0.9, RetryBackoff.Delay(attempt, 0).TotalSeconds, 6);
        Assert.InRange(RetryBackoff.Delay(attempt, 0.999999).TotalSeconds, seconds * 1.0999, seconds * 1.1);
    }

    [Fact]
    public void The_default_backoff_is_capped_at_24_hours() =>
        Assert.Equal([TimeSpan.FromHours(24), TimeSpan.FromHours(24)], new[] { RetryBackoff.Delay(25, 0), RetryBackoff.Delay(1000, 0.9) });

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void A_client_timeout_that_is_not_positive_or_infinite_is_rejected(int seconds) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new JobExecutor(
            new WorkerRegistry(), Serializer, new RecordingCompleter(), options: new JobExecutorOptions { JobTimeout = TimeSpan.FromSeconds(seconds) }));

    private static JobExecutor Executor(
        WorkerRegistry workers, RecordingCompleter completer, FakeTimeProvider time, TimeSpan? timeout = null, Func<double>? random = null) =>
        new(workers, Serializer, completer, options: new JobExecutorOptions { JobTimeout = timeout ?? TimeSpan.FromMinutes(1), Random = random }, timeProvider: time);

    private static WorkerRegistry Throwing(Exception exception) =>
        new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, _) => Throw(exception)));

    private static Task Throw(Exception exception) => throw exception;

    private static JobRow Row(string kind, string args = "{}", int attempt = 1, int maxAttempts = 25, string metadata = "{}") => new()
    {
        Id = 7, Kind = kind, Queue = "default", State = JobState.Running, Priority = 1, ArgsJson = args, MetadataJson = metadata,
        Tags = [], Attempt = attempt, MaxAttempts = maxAttempts, CreatedAt = DateTimeOffset.UnixEpoch, ScheduledAt = DateTimeOffset.UnixEpoch,
        AttemptedAt = DateTimeOffset.UnixEpoch, AttemptedBy = ["c"],
    };

    [JobKind("ping")]
    public sealed record PingArgs(string? Text = null) : IJobArgs;

    private sealed class RecordingCompleter : IJobCompleter
    {
        private readonly ConcurrentQueue<JobCompletion> _completions = new();

        public IReadOnlyList<JobCompletion> Completions => [.. _completions];

        public ValueTask CompleteAsync(JobCompletion completion, CancellationToken cancellationToken)
        {
            _completions.Enqueue(completion);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class LambdaWorker<TArgs>(Func<Job<TArgs>, CancellationToken, Task> work) : Worker<TArgs>
        where TArgs : IJobArgs
    {
        public Func<Job<TArgs>, TimeSpan?>? TimeoutOverride { get; init; }

        public Func<Job<TArgs>, TimeSpan?>? NextRetryOverride { get; init; }

        public override Task WorkAsync(Job<TArgs> job, CancellationToken cancellationToken) => work(job, cancellationToken);

        public override TimeSpan? Timeout(Job<TArgs> job) => TimeoutOverride is null ? null : TimeoutOverride(job);

        public override TimeSpan? NextRetry(Job<TArgs> job) => NextRetryOverride is null ? null : NextRetryOverride(job);
    }

    public sealed class ScopedThing : IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }

    public sealed class ScopedWorker(ScopedThing thing) : Worker<PingArgs>
    {
        public static List<ScopedThing> Seen { get; } = [];

        public override Task WorkAsync(Job<PingArgs> job, CancellationToken cancellationToken)
        {
            Assert.False(thing.Disposed);
            Seen.Add(thing);
            return Task.CompletedTask;
        }
    }

    public sealed class DisposableWorker : Worker<PingArgs>, IDisposable
    {
        public DisposableWorker() => Created.Add(this);

        public static ConcurrentBag<DisposableWorker> Created { get; } = [];

        public bool Disposed { get; private set; }

        public override Task WorkAsync(Job<PingArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;

        public void Dispose() => Disposed = true;
    }
}

[JsonSerializable(typeof(JobExecutorTests.PingArgs))]
internal sealed partial class ExecutorJsonContext : JsonSerializerContext;
