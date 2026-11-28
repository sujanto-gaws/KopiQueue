using Microsoft.EntityFrameworkCore;

namespace KopiQueue.Samples.MinimalApi;

/// <summary>Sends an order's confirmation. Only the order's ID travels in the job: the worker reads the rest.</summary>
[JobKind("send_order_confirmation")]
public sealed record SendConfirmationArgs(long OrderId) : IJobArgs;

/// <summary>
/// Runs in a DI scope of its own, so it gets its own <see cref="ShopDb"/>. A job runs at least once, so the worker
/// checks whether the confirmation was already sent before sending it.
/// </summary>
public sealed partial class SendConfirmationWorker(ShopDb db, ILogger<SendConfirmationWorker> logger) : Worker<SendConfirmationArgs>
{
    public override async Task WorkAsync(Job<SendConfirmationArgs> job, CancellationToken cancellationToken)
    {
        // The job committed with its order, so the order is there.
        var order = await db.Orders.SingleAsync(o => o.Id == job.Args.OrderId, cancellationToken);
        if (order.ConfirmationSentAt is not null)
        {
            LogAlreadySent(logger, order.Id);
            return;
        }

        await Task.Delay(TimeSpan.FromMilliseconds(50), cancellationToken); // the email provider
        LogSent(logger, order.Id, order.Email);
        order.ConfirmationSentAt = DateTimeOffset.UtcNow;
        await db.SaveChangesAsync(cancellationToken);
    }

    [LoggerMessage(Level = LogLevel.Information, Message = "Sent the confirmation of order {OrderId} to {Email}")]
    private static partial void LogSent(ILogger logger, long orderId, string email);

    [LoggerMessage(Level = LogLevel.Information, Message = "Order {OrderId}'s confirmation was already sent")]
    private static partial void LogAlreadySent(ILogger logger, long orderId);
}
