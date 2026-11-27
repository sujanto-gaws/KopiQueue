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
        var startedAt = time.GetUtcNow();
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, _) =>
        {
            time.Advance(TimeSpan.FromSeconds(3));
            return Throw(new InvalidOperationException("boom"));
        })), completer, time, random: () => 0.5);

        await executor.ExecuteAsync(Row("ping", attempt: 2, errors: 1), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Retryable, (DateTimeOffset?)null, (int?)null), (completion.State, completion.FinalizedAt, completion.Attempt));
        Assert.Equal(time.GetUtcNow() + TimeSpan.FromSeconds(16), completion.ScheduledAt);
        Assert.NotNull(completion.Error);
        Assert.Equal((startedAt, 2, "boom"), (completion.Error.At, completion.Error.Attempt, completion.Error.Error));
        Assert.Contains("System.InvalidOperationException: boom", completion.Error.Trace, StringComparison.Ordinal);
        Assert.Contains(nameof(Throw), completion.Error.Trace, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(0, 1, JobState.Available)]
    [InlineData(1, 16, JobState.Retryable)]
    [InlineData(4, 625, JobState.Retryable)]
    public async Task The_backoff_counts_errors_and_a_retry_within_the_scheduler_interval_is_available_at_once(
        int previousErrors, int seconds, JobState state)
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(Throwing(new InvalidOperationException("boom")), completer, time, random: () => 0.5);

        // The attempt number does not matter: snoozes and interruptions use no attempt, but errors are what count.
        await executor.ExecuteAsync(Row("ping", attempt: 9, errors: previousErrors), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((state, time.GetUtcNow() + TimeSpan.FromSeconds(seconds)), (completion.State, completion.ScheduledAt!.Value));
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

        var delays = completer.Completions.Select(c => c.ScheduledAt!.Value - time.GetUtcNow()).ToArray();
        Assert.Equal(TimeSpan.FromMinutes(7), delays[0]);
        Assert.InRange(delays[1], TimeSpan.FromSeconds(0.9), TimeSpan.FromSeconds(1.1)); // the default policy
        Assert.Equal(TimeSpan.FromSeconds(1), delays[2]); // the client's policy
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
        Assert.Equal(JobState.Available, completion.State);
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
        Assert.Equal([JobState.Available, JobState.Completed], completer.Completions.Select(c => c.State));
        Assert.Contains("00:00:05", completer.Completions[0].Error!.Error, StringComparison.Ordinal);
    }

    [Fact]
    public async Task After_an_external_cancel_a_worker_that_throws_cancels_the_job_whatever_attempts_are_left()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, ct) => Task.Delay(Timeout.Infinite, ct))), completer, time);
        using var cancel = new CancellationTokenSource();

        var run = executor.ExecuteAsync(Row("ping", attempt: 1, maxAttempts: 25), Ct, cancel.Token);
        await cancel.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Cancelled, time.GetUtcNow(), (int?)null), (completion.State, completion.FinalizedAt!.Value, completion.Attempt));
        Assert.Equal(1, completion.Error!.Attempt);
        Assert.Contains(nameof(TaskCanceledException), completion.Error.Trace, StringComparison.Ordinal); // the worker's own exception
    }

    [Fact]
    public async Task After_an_external_cancel_any_error_of_the_worker_cancels_the_job_with_that_error()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>(async (_, ct) =>
        {
            await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { }, TaskScheduler.Default);
            throw new InvalidOperationException("the cleanup failed");
        })), completer, time);
        using var cancel = new CancellationTokenSource();

        var run = executor.ExecuteAsync(Row("ping", attempt: 1), Ct, cancel.Token);
        await cancel.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Cancelled, "the cleanup failed"), (completion.State, completion.Error!.Error));
    }

    [Fact]
    public async Task After_an_external_cancel_a_worker_that_returns_completes_the_job()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>(async (_, ct) =>
            await Task.Delay(Timeout.Infinite, ct).ContinueWith(_ => { }, TaskScheduler.Default))), completer, time);
        using var cancel = new CancellationTokenSource();

        var run = executor.ExecuteAsync(Row("ping"), Ct, cancel.Token);
        await cancel.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal(JobState.Completed, completion.State);
        Assert.Null(completion.Error);
    }

    [Fact]
    public async Task A_hard_stop_still_interrupts_a_job_that_was_also_cancelled_and_the_completer_settles_it()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, ct) => Task.Delay(Timeout.Infinite, ct))), completer, time);
        using var hardStop = new CancellationTokenSource();
        using var cancel = new CancellationTokenSource();

        await cancel.CancelAsync();
        await hardStop.CancelAsync();
        await executor.ExecuteAsync(Row("ping", attempt: 2), hardStop.Token, cancel.Token).WaitAsync(TimeSpan.FromSeconds(10), Ct);

        // Available without using the attempt; on a row with cancel_attempted_at the completer's guard writes it as cancelled.
        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Available, (int?)1), (completion.State, completion.Attempt));
    }

    [Fact]
    public async Task A_timeout_is_still_an_error_when_the_job_was_not_cancelled()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(
            new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, ct) => Task.Delay(Timeout.Infinite, ct))), completer, time, timeout: TimeSpan.FromSeconds(30));
        using var cancel = new CancellationTokenSource();

        var run = executor.ExecuteAsync(Row("ping"), Ct, cancel.Token);
        await Task.Delay(50, Ct);
        time.Advance(TimeSpan.FromSeconds(30));
        await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal(JobState.Available, completion.State); // retried: the default policy's first delay is within the scheduler interval
        Assert.Equal("The job did not finish within its timeout of 00:00:30.", completion.Error!.Error);
    }

    [Fact]
    public async Task A_hard_stop_interrupts_the_job_which_runs_again_without_using_an_attempt()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, ct) => Task.Delay(Timeout.Infinite, ct))), completer, time);
        using var hardStop = new CancellationTokenSource();

        var run = executor.ExecuteAsync(Row("ping", attempt: 3, maxAttempts: 3), hardStop.Token);
        await hardStop.CancelAsync();
        await run.WaitAsync(TimeSpan.FromSeconds(10), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Available, (int?)2, time.GetUtcNow()), (completion.State, completion.Attempt, completion.ScheduledAt!.Value));
        Assert.Null(completion.Error);
        Assert.Null(completion.FinalizedAt);
    }

    [Fact]
    public async Task An_error_other_than_a_cancellation_during_a_hard_stop_is_still_an_error()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        using var hardStop = new CancellationTokenSource();
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>(async (_, ct) =>
        {
            await hardStop.CancelAsync();
            throw new InvalidOperationException("cleanup failed");
        })), completer, time);

        await executor.ExecuteAsync(Row("ping"), hardStop.Token);

        Assert.Equal("cleanup failed", Assert.Single(completer.Completions).Error!.Error);
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

        Assert.Equal([JobState.Available, JobState.Discarded], completer.Completions.Select(c => c.State));
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
        Assert.Equal(JobState.Available, completion.State);
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
    public void The_default_backoff_is_the_error_count_to_the_fourth_seconds_with_ten_percent_jitter(int errorCount, double seconds)
    {
        var policy = new DefaultRetryPolicy();
        Assert.Equal(seconds, policy.Delay(errorCount, 0.5).TotalSeconds, 6);
        Assert.Equal(seconds * 0.9, policy.Delay(errorCount, 0).TotalSeconds, 6);
        Assert.InRange(policy.Delay(errorCount, 0.999999).TotalSeconds, seconds * 1.0999, seconds * 1.1);
    }

    [Fact]
    public void The_default_backoff_is_capped_at_24_hours_or_the_configured_maximum()
    {
        var policy = new DefaultRetryPolicy();
        Assert.Equal([TimeSpan.FromHours(24), TimeSpan.FromHours(24)], new[] { policy.Delay(25, 0), policy.Delay(1000, 0.9) });
        Assert.Equal(TimeSpan.FromMinutes(1), new DefaultRetryPolicy { MaxDelay = TimeSpan.FromMinutes(1) }.Delay(5, 0.5));
        Assert.Throws<ArgumentOutOfRangeException>(() => new DefaultRetryPolicy { MaxDelay = TimeSpan.Zero });
    }

    [Fact]
    public void The_default_policy_counts_the_jobs_errors_including_this_one()
    {
        var now = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var policy = new DefaultRetryPolicy(() => 0.5);

        Assert.Equal(now.AddSeconds(81), policy.NextRetry(Row("ping", errors: 2), now));
    }

    [Fact]
    public async Task Cancel_finalises_the_job_as_cancelled_with_its_reason_whatever_attempts_are_left()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var startedAt = time.GetUtcNow();
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>(async (_, _) =>
        {
            await Task.Yield();
            time.Advance(TimeSpan.FromSeconds(1));
            throw JobControl.Cancel("customer deleted");
        })), completer, time);

        await executor.ExecuteAsync(Row("ping", attempt: 1, maxAttempts: 25), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal((JobState.Cancelled, time.GetUtcNow(), (DateTimeOffset?)null), (completion.State, completion.FinalizedAt!.Value, completion.ScheduledAt));
        Assert.Equal(new AttemptError(startedAt, 1, "customer deleted", null), completion.Error);
        Assert.Null(completion.Attempt);
    }

    [Theory]
    [InlineData(0, JobState.Available)]
    [InlineData(5, JobState.Available)]
    [InlineData(6, JobState.Scheduled)]
    [InlineData(300, JobState.Scheduled)]
    public async Task Snooze_reschedules_without_using_an_attempt_and_counts_the_snoozes(int seconds, JobState state)
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var executor = Executor(Throwing(JobControl.Snooze(TimeSpan.FromSeconds(seconds))), completer, time);

        await executor.ExecuteAsync(Row("ping", attempt: 3, maxAttempts: 3, metadata: """{"snoozes": 2, "tenant": "a"}"""), Ct);

        var completion = Assert.Single(completer.Completions);
        Assert.Equal(
            (state, (int?)2, time.GetUtcNow() + TimeSpan.FromSeconds(seconds), "{\"snoozes\":3}"),
            (completion.State, completion.Attempt, completion.ScheduledAt!.Value, completion.MetadataUpdates));
        Assert.Null(completion.Error);
        Assert.Null(completion.FinalizedAt);
    }

    [Theory]
    [InlineData("{}", 1)]
    [InlineData("""{"snoozes": 4}""", 5)]
    [InlineData("""{"snoozes": 4.7}""", 5)]
    [InlineData("""{"snoozes": "7"}""", 8)]
    [InlineData("""{"snoozes": true}""", 1)]
    [InlineData("[]", 1)]
    [InlineData("not json", 1)]
    public void The_snooze_count_is_read_leniently(string metadata, long next) =>
        Assert.Equal(next, JobExecutor.NextSnoozeCount(metadata));

    [Fact]
    public async Task Cancel_and_snooze_are_recognised_when_wrapped()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var throws = new Queue<Exception>(
        [
            new AggregateException(JobControl.Cancel("inside an aggregate")),
            new InvalidOperationException("outer", JobControl.Snooze(TimeSpan.FromMinutes(1))),
            new AggregateException(new InvalidOperationException("first"), JobControl.Cancel("second of two")),
        ]);
        var executor = Executor(new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, _) => Throw(throws.Dequeue()))), completer, time);

        for (var i = 0; i < 3; i++)
        {
            await executor.ExecuteAsync(Row("ping"), Ct);
        }

        Assert.Equal([JobState.Cancelled, JobState.Scheduled, JobState.Cancelled], completer.Completions.Select(c => c.State));
        Assert.Equal("second of two", completer.Completions[2].Error!.Error);
    }

    [Fact]
    public void Snoozing_for_a_negative_time_and_cancelling_without_a_reason_are_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => JobControl.Snooze(TimeSpan.FromSeconds(-1)));
        Assert.Throws<ArgumentException>(() => JobControl.Cancel(" "));
        Assert.Equal("why", JobControl.Cancel("why").Reason);
        Assert.Equal(TimeSpan.FromSeconds(3), JobControl.Snooze(TimeSpan.FromSeconds(3)).Delay);
    }

    [Fact]
    public async Task The_clients_retry_policy_is_used_and_one_in_the_past_or_throwing_falls_back_to_the_default()
    {
        var (completer, time) = (new RecordingCompleter(), new FakeTimeProvider());
        var policy = new ScriptedPolicy();
        var executor = Executor(Throwing(new InvalidOperationException("boom")), completer, time, policy: policy);

        policy.Next = now => now.AddHours(2);
        await executor.ExecuteAsync(Row("ping"), Ct);
        policy.Next = now => now.AddSeconds(-1);
        await executor.ExecuteAsync(Row("ping"), Ct);
        policy.Next = _ => throw new InvalidOperationException("policy bug");
        await executor.ExecuteAsync(Row("ping"), Ct);

        var delays = completer.Completions.Select(c => c.ScheduledAt!.Value - time.GetUtcNow()).ToArray();
        Assert.Equal(TimeSpan.FromHours(2), delays[0]);
        Assert.Equal(JobState.Retryable, completer.Completions[0].State);
        Assert.All(delays[1..], d => Assert.InRange(d, TimeSpan.FromSeconds(0.9), TimeSpan.FromSeconds(1.1)));
        Assert.Equal(7, Assert.IsType<JobRow>(policy.LastJob).Id);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-5)]
    public void A_client_timeout_that_is_not_positive_or_infinite_is_rejected(int seconds) =>
        Assert.Throws<ArgumentOutOfRangeException>(() => new JobExecutor(
            new WorkerRegistry(), Serializer, new RecordingCompleter(), options: new JobExecutorOptions { JobTimeout = TimeSpan.FromSeconds(seconds) }));

    private static JobExecutor Executor(
        WorkerRegistry workers, RecordingCompleter completer, FakeTimeProvider time, TimeSpan? timeout = null, Func<double>? random = null,
        IRetryPolicy? policy = null) =>
        new(
            workers,
            Serializer,
            completer,
            options: new JobExecutorOptions
            {
                JobTimeout = timeout ?? TimeSpan.FromMinutes(1),
                RetryPolicy = policy ?? (random is null ? null : new DefaultRetryPolicy(random)),
            },
            timeProvider: time);

    private static WorkerRegistry Throwing(Exception exception) =>
        new WorkerRegistry().Add(new LambdaWorker<PingArgs>((_, _) => Throw(exception)));

    private static Task Throw(Exception exception) => throw exception;

    private static JobRow Row(string kind, string args = "{}", int attempt = 1, int maxAttempts = 25, string metadata = "{}", int errors = 0) => new()
    {
        Id = 7, Kind = kind, Queue = "default", State = JobState.Running, Priority = 1, ArgsJson = args, MetadataJson = metadata,
        Tags = [], Attempt = attempt, MaxAttempts = maxAttempts, CreatedAt = DateTimeOffset.UnixEpoch, ScheduledAt = DateTimeOffset.UnixEpoch,
        AttemptedAt = DateTimeOffset.UnixEpoch, AttemptedBy = ["c"],
        Errors = [.. Enumerable.Range(1, errors).Select(i => new AttemptError(DateTimeOffset.UnixEpoch, i, "earlier", null))],
    };

    [JobKind("ping")]
    public sealed record PingArgs(string? Text = null) : IJobArgs;

    private sealed class ScriptedPolicy : IRetryPolicy
    {
        public Func<DateTimeOffset, DateTimeOffset> Next { get; set; } = now => now;

        public JobRow? LastJob { get; private set; }

        public DateTimeOffset NextRetry(JobRow job, DateTimeOffset now)
        {
            LastJob = job;
            return Next(now);
        }
    }

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
