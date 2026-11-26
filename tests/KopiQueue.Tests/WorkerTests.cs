using System.Text.Json.Nodes;
using KopiQueue.Serialization;
using Microsoft.Extensions.DependencyInjection;

namespace KopiQueue.Tests;

/// <summary>Worker interfaces, the job view and the worker registry (plan task 2.1: FR-DEF-02, FR-DEF-04, FR-DEF-06).</summary>
public sealed class WorkerTests
{
    private static readonly IJobArgsSerializer Serializer = new SystemTextJsonArgsSerializer(WebJsonContext.Default);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void Job_exposes_the_row_and_the_args()
    {
        var row = Row("send_email", metadata: """{"source":"test"}""");
        var args = new SendEmailArgs("a@example.com", "welcome", [1, 2]);

        var job = new Job<SendEmailArgs>(row, args);

        Assert.Same(row, job.Row);
        Assert.Same(args, job.Args);
        Assert.Equal((row.Id, row.Kind, row.Queue, row.Attempt, row.MaxAttempts, row.Priority), (job.Id, job.Kind, job.Queue, job.Attempt, job.MaxAttempts, job.Priority));
        Assert.Equal(row.Tags, job.Tags);
        Assert.Equal((row.CreatedAt, row.ScheduledAt, row.AttemptedAt), (job.CreatedAt, job.ScheduledAt, job.AttemptedAt));
        Assert.Same(row.Errors, job.Errors);
        Assert.Equal("test", job.Metadata["source"]!.GetValue<string>());
        Assert.Same(job.Metadata, job.Metadata);
    }

    [Fact]
    public void Changing_the_metadata_object_does_not_change_the_row()
    {
        var job = new Job<SendEmailArgs>(Row("send_email"), new SendEmailArgs("a", "b", []));

        job.Metadata["x"] = 1;

        Assert.Equal("{}", job.Row.MetadataJson);
    }

    [Fact]
    public void Job_rejects_a_row_of_another_kind()
    {
        var ex = Assert.Throws<ArgumentException>(() => new Job<SendEmailArgs>(Row("resize_image"), new SendEmailArgs("a", "b", [])));

        Assert.Contains("'resize_image'", ex.Message, StringComparison.Ordinal);
        Assert.Contains("'send_email'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Timeout_and_NextRetry_default_to_the_client_settings()
    {
        var job = new Job<SendEmailArgs>(Row("send_email"), new SendEmailArgs("a", "b", []));
        IWorker<SendEmailArgs> plain = new InterfaceOnlyWorker();
        var derived = new EmailWorker();

        Assert.Null(plain.Timeout(job));
        Assert.Null(plain.NextRetry(job));
        Assert.Null(derived.Timeout(job));
        Assert.Null(derived.NextRetry(job));
    }

    [Fact]
    public void Add_registers_the_kind_of_the_args_type()
    {
        var workers = new WorkerRegistry()
            .Add<SendEmailArgs, EmailWorker>()
            .Add(new ResizeWorker());

        Assert.Equal(["resize_image", "send_email"], workers.Kinds.Order(StringComparer.Ordinal));
        Assert.True(workers.KindRegistry.TryGetType("send_email", out var type));
        Assert.Equal(typeof(SendEmailArgs), type);
        Assert.True(workers.TryGet("resize_image", out var registration));
        Assert.Equal((typeof(ResizeArgs), typeof(ResizeWorker)), (registration.ArgsType, registration.WorkerType));
        Assert.False(workers.TryGet("unlisted", out _));
    }

    [Fact]
    public void Add_with_only_the_worker_type_finds_its_args_type()
    {
        var workers = new WorkerRegistry().Add<EmailWorker>();

        Assert.True(workers.TryGet("send_email", out var registration));
        Assert.Equal((typeof(SendEmailArgs), typeof(EmailWorker)), (registration.ArgsType, registration.WorkerType));
    }

    [Theory]
    [InlineData(typeof(NotAWorker), "does not implement IWorker<TArgs>")]
    [InlineData(typeof(TwoKindWorker), "for KopiQueue.Tests.SendEmailArgs, KopiQueue.Tests.ResizeArgs")]
    public void Add_with_only_the_worker_type_needs_exactly_one_args_type(Type workerType, string message)
    {
        var workers = new WorkerRegistry();
        Action add = workerType == typeof(NotAWorker) ? () => workers.Add<NotAWorker>() : () => workers.Add<TwoKindWorker>();

        var ex = Assert.Throws<KopiQueueConfigurationException>(add);

        Assert.Contains(workerType.FullName!, ex.Message, StringComparison.Ordinal);
        Assert.Contains(message, ex.Message, StringComparison.Ordinal);
        Assert.Empty(workers.Kinds);
    }

    [Fact]
    public void Add_with_only_the_worker_type_rethrows_registration_errors_unwrapped()
    {
        var workers = new WorkerRegistry().Add<EmailWorker>();

        Assert.Throws<KopiQueueConfigurationException>(() => workers.Add<OtherEmailWorker>());
    }

    [Fact]
    public void A_second_worker_for_a_kind_is_a_configuration_error()
    {
        var workers = new WorkerRegistry().Add<SendEmailArgs, EmailWorker>();

        var ex = Assert.Throws<KopiQueueConfigurationException>(() => workers.Add<SendEmailArgs, OtherEmailWorker>());

        Assert.Contains("'send_email'", ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(EmailWorker).FullName!, ex.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(OtherEmailWorker).FullName!, ex.Message, StringComparison.Ordinal);
        Assert.Throws<KopiQueueConfigurationException>(() => workers.Add<SendEmailArgs, EmailWorker>());
        Assert.Throws<KopiQueueConfigurationException>(() => workers.Add(new EmailWorker()));
        Assert.True(workers.TryGet("send_email", out var registration));
        Assert.Equal(typeof(EmailWorker), registration.WorkerType);
    }

    [Fact]
    public void Two_args_types_with_one_kind_are_a_configuration_error()
    {
        var workers = new WorkerRegistry().Add<SendEmailArgs, EmailWorker>();

        var ex = Assert.Throws<KopiQueueConfigurationException>(() => workers.Add(new DuplicateKindWorker()));

        Assert.Contains(nameof(DuplicateKindArgs), ex.Message, StringComparison.Ordinal);
        Assert.Equal(["send_email"], workers.Kinds);
    }

    [Fact]
    public void Args_without_a_kind_are_a_configuration_error()
    {
        var ex = Assert.Throws<KopiQueueConfigurationException>(() => new WorkerRegistry().Add(new NoKindWorker()));

        Assert.Contains("[JobKind]", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void An_abstract_worker_type_is_a_configuration_error()
    {
        var workers = new WorkerRegistry();

        var ex = Assert.Throws<KopiQueueConfigurationException>(() => workers.Add<SendEmailArgs, AbstractEmailWorker>());

        Assert.Contains("abstract", ex.Message, StringComparison.Ordinal);
        Assert.Empty(workers.Kinds);
    }

    [Fact]
    public void A_client_that_works_jobs_needs_a_worker()
    {
        var ex = Assert.Throws<KopiQueueConfigurationException>(() => new WorkerRegistry().ValidateForWork());
        Assert.Contains("No workers are registered", ex.Message, StringComparison.Ordinal);

        new WorkerRegistry().Add(new ResizeWorker()).ValidateForWork();
    }

    [Fact]
    public async Task A_work_unit_deserialises_the_args_and_runs_the_worker()
    {
        var worker = new ResizeWorker();
        var registration = Registration(new WorkerRegistry().Add(worker), "resize_image");
        using var cts = new CancellationTokenSource();

        await using var unit = registration.CreateWorkUnit(Row("resize_image", args: """{"width":640}"""), Serializer, EmptyServices());
        await unit.WorkAsync(cts.Token);

        var (job, token) = Assert.Single(worker.Calls);
        Assert.Equal(new ResizeArgs(640), job.Args);
        Assert.Equal(cts.Token, token);
        Assert.Equal(TimeSpan.FromSeconds(30), unit.Timeout());
        Assert.Equal(TimeSpan.FromSeconds(640), unit.NextRetry());
    }

    [Fact]
    public async Task An_instance_worker_is_shared_and_never_disposed()
    {
        var worker = new DisposableResizeWorker();
        var registration = Registration(new WorkerRegistry().Add(worker), "resize_image");

        for (var i = 0; i < 2; i++)
        {
            await using var unit = registration.CreateWorkUnit(Row("resize_image", args: """{"width":1}"""), Serializer, EmptyServices());
            await unit.WorkAsync(Ct);
        }

        Assert.Equal(2, worker.Calls.Count);
        Assert.False(worker.Disposed);
    }

    [Fact]
    public async Task A_worker_type_is_constructed_per_job_with_injected_services_and_disposed_after_it()
    {
        var log = new List<string>();
        using var services = new ServiceCollection().AddSingleton(log).BuildServiceProvider();
        var registration = Registration(new WorkerRegistry().Add<SendEmailArgs, LoggingEmailWorker>(), "send_email");
        var row = Row("send_email", args: """{"to":"a@example.com","templateId":"t","ids":[]}""");

        for (var i = 0; i < 2; i++)
        {
            await using var scope = services.CreateAsyncScope();
            await using var unit = registration.CreateWorkUnit(row, Serializer, scope.ServiceProvider);
            await unit.WorkAsync(Ct);
        }

        Assert.Equal(["created 1", "work 1 a@example.com", "disposed 1", "created 2", "work 2 a@example.com", "disposed 2"], log);
    }

    [Fact]
    public async Task A_worker_type_registered_in_the_container_is_resolved_and_left_to_its_scope()
    {
        var log = new List<string>();
        using var services = new ServiceCollection().AddSingleton(log).AddScoped<LoggingEmailWorker>().BuildServiceProvider();
        var registration = Registration(new WorkerRegistry().Add<SendEmailArgs, LoggingEmailWorker>(), "send_email");
        var row = Row("send_email", args: """{"to":"b@example.com","templateId":"t","ids":[]}""");

        await using (var scope = services.CreateAsyncScope())
        {
            var fromScope = scope.ServiceProvider.GetRequiredService<LoggingEmailWorker>();
            await using (var unit = registration.CreateWorkUnit(row, Serializer, scope.ServiceProvider))
            {
                await unit.WorkAsync(Ct);
            }

            Assert.Equal(["created 1", "work 1 b@example.com"], log);
            Assert.Equal(1, fromScope.Number);
        }

        Assert.Equal(["created 1", "work 1 b@example.com", "disposed 1"], log);
    }

    [Fact]
    public void A_work_unit_rejects_a_row_of_another_kind()
    {
        var registration = Registration(new WorkerRegistry().Add(new ResizeWorker()), "resize_image");

        Assert.Throws<ArgumentException>(() => registration.CreateWorkUnit(Row("send_email"), Serializer, EmptyServices()));
    }

    [Fact]
    public void Args_that_do_not_deserialise_fail_before_the_worker_is_created()
    {
        var log = new List<string>();
        using var services = new ServiceCollection().AddSingleton(log).BuildServiceProvider();
        var registration = Registration(new WorkerRegistry().Add<SendEmailArgs, LoggingEmailWorker>(), "send_email");

        Assert.ThrowsAny<Exception>(() => registration.CreateWorkUnit(Row("send_email", args: "not json"), Serializer, services));
        Assert.Empty(log);
    }

    private static WorkerRegistration Registration(WorkerRegistry workers, string kind) =>
        workers.TryGet(kind, out var registration) ? registration : throw new InvalidOperationException(kind);

    private static ServiceProvider EmptyServices() => new ServiceCollection().BuildServiceProvider();

    private static JobRow Row(string kind, string args = "{}", string metadata = "{}") => new()
    {
        Id = 42,
        Kind = kind,
        Queue = "default",
        State = JobState.Running,
        Priority = 2,
        ArgsJson = args,
        MetadataJson = metadata,
        Tags = ["a-tag"],
        Attempt = 3,
        MaxAttempts = 25,
        AttemptedAt = new DateTimeOffset(2026, 11, 26, 12, 0, 5, TimeSpan.Zero),
        AttemptedBy = ["client-1"],
        Errors = [new AttemptError(new DateTimeOffset(2026, 11, 26, 11, 0, 0, TimeSpan.Zero), 2, "boom", null)],
        CreatedAt = new DateTimeOffset(2026, 11, 26, 10, 0, 0, TimeSpan.Zero),
        ScheduledAt = new DateTimeOffset(2026, 11, 26, 12, 0, 0, TimeSpan.Zero),
    };

    private sealed class InterfaceOnlyWorker : IWorker<SendEmailArgs>
    {
        public Task WorkAsync(Job<SendEmailArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class EmailWorker : Worker<SendEmailArgs>
    {
        public override Task WorkAsync(Job<SendEmailArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class OtherEmailWorker : Worker<SendEmailArgs>
    {
        public override Task WorkAsync(Job<SendEmailArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private abstract class AbstractEmailWorker : Worker<SendEmailArgs>;

    private sealed class NotAWorker;

    private sealed class TwoKindWorker : IWorker<SendEmailArgs>, IWorker<ResizeArgs>
    {
        public Task WorkAsync(Job<SendEmailArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;

        public Task WorkAsync(Job<ResizeArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private class ResizeWorker : Worker<ResizeArgs>
    {
        public List<(Job<ResizeArgs> Job, CancellationToken Token)> Calls { get; } = [];

        public override TimeSpan? Timeout(Job<ResizeArgs> job) => TimeSpan.FromSeconds(30);

        public override TimeSpan? NextRetry(Job<ResizeArgs> job) => TimeSpan.FromSeconds(job.Args.Width);

        public override Task WorkAsync(Job<ResizeArgs> job, CancellationToken cancellationToken)
        {
            lock (Calls)
            {
                Calls.Add((job, cancellationToken));
            }

            return Task.CompletedTask;
        }
    }

    private sealed class DisposableResizeWorker : ResizeWorker, IDisposable
    {
        public bool Disposed { get; private set; }

        public void Dispose() => Disposed = true;
    }

    private sealed class LoggingEmailWorker : Worker<SendEmailArgs>, IAsyncDisposable
    {
        private readonly List<string> _log;

        public LoggingEmailWorker(List<string> log)
        {
            _log = log;
            Number = log.Count(l => l.StartsWith("created", StringComparison.Ordinal)) + 1;
            _log.Add($"created {Number}");
        }

        public int Number { get; }

        public override Task WorkAsync(Job<SendEmailArgs> job, CancellationToken cancellationToken)
        {
            _log.Add($"work {Number} {job.Args.To}");
            return Task.CompletedTask;
        }

        public ValueTask DisposeAsync()
        {
            _log.Add($"disposed {Number}");
            return ValueTask.CompletedTask;
        }
    }

    [JobKind("send_email")]
    private sealed record DuplicateKindArgs : IJobArgs;

    private sealed class DuplicateKindWorker : Worker<DuplicateKindArgs>
    {
        public override Task WorkAsync(Job<DuplicateKindArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed record NoKindArgs : IJobArgs;

    private sealed class NoKindWorker : Worker<NoKindArgs>
    {
        public override Task WorkAsync(Job<NoKindArgs> job, CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
