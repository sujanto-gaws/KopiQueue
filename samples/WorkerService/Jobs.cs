using System.Text.Json.Serialization;

namespace KopiQueue.Samples.WorkerService;

/// <summary>Sends an email. Completes on its first attempt.</summary>
[JobKind("send_email")]
public sealed record SendEmailArgs(string To, string Template) : IJobArgs;

/// <summary>Fails its first attempt, so the retry policy runs it again about a second later.</summary>
[JobKind("flaky")]
public sealed record FlakyArgs(int Number) : IJobArgs;

/// <summary>Every args type, for source-generated (reflection-free) serialisation.</summary>
[JsonSerializable(typeof(SendEmailArgs))]
[JsonSerializable(typeof(FlakyArgs))]
public sealed partial class SampleJsonContext : JsonSerializerContext;

/// <summary>Stands in for an email service: a dependency the worker gets from DI.</summary>
public sealed partial class Outbox(ILogger<Outbox> logger)
{
    public void Send(string to, string template) => LogSent(logger, template, to);

    [LoggerMessage(Level = LogLevel.Information, Message = "Sent {Template} to {To}")]
    private static partial void LogSent(ILogger logger, string template, string to);
}

/// <summary>Created in a new DI scope for each job.</summary>
public sealed class SendEmailWorker(Outbox outbox) : Worker<SendEmailArgs>
{
    public override async Task WorkAsync(Job<SendEmailArgs> job, CancellationToken cancellationToken)
    {
        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken); // honour the token: it fires on timeout and hard stop
        outbox.Send(job.Args.To, job.Args.Template);
    }
}

public sealed partial class FlakyWorker(ILogger<FlakyWorker> logger) : Worker<FlakyArgs>
{
    public override Task WorkAsync(Job<FlakyArgs> job, CancellationToken cancellationToken)
    {
        if (job.Attempt == 1)
        {
            throw new InvalidOperationException($"Flaky job {job.Args.Number} fails its first attempt.");
        }

        LogSucceeded(logger, job.Args.Number, job.Attempt);
        return Task.CompletedTask;
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Flaky job {Number} succeeded on attempt {Attempt}")]
    private static partial void LogSucceeded(ILogger logger, int number, int attempt);
}

/// <summary>Inserts a few jobs every few seconds, as an application would.</summary>
public sealed partial class DemoInserter(IKopiClient client, ILogger<DemoInserter> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        for (var round = 1; !stoppingToken.IsCancellationRequested; round++)
        {
            await client.InsertAsync(new SendEmailArgs($"user{round}@example.com", "welcome"), new InsertOptions { Queue = "email" }, stoppingToken);
            await client.InsertAsync(new FlakyArgs(round), cancellationToken: stoppingToken);
            LogInserted(logger, round);
            await Task.Delay(TimeSpan.FromSeconds(3), stoppingToken);
        }
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Inserted round {Round}")]
    private static partial void LogInserted(ILogger logger, int round);
}
