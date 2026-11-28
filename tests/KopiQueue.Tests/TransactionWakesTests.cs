using System.Data;
using System.Data.Common;
using KopiQueue.Execution;

namespace KopiQueue.Tests;

/// <summary>Wake-ups that wait for a caller's transaction to commit (plan task 5.7: FR-DB-04, design §3.3).</summary>
public sealed class TransactionWakesTests
{
    [Fact]
    public void A_commit_wakes_each_recorded_queue_of_each_client_once()
    {
        var (one, two) = (new LocalWaker(), new LocalWaker());
        var woken = new List<string>();
        using var a = one.Subscribe("a", () => woken.Add("one:a"));
        using var b = one.Subscribe("b", () => woken.Add("one:b"));
        using var c = one.Subscribe("c", () => woken.Add("one:c"));
        using var other = two.Subscribe("a", () => woken.Add("two:a"));
        var tx = new FakeTransaction();

        TransactionWakes.Add(tx, one, ["a", "b"]);
        TransactionWakes.Add(tx, one, ["a"]);
        TransactionWakes.Add(tx, two, ["a"]);
        Assert.Empty(woken); // nothing before the commit

        TransactionWakes.Committed(tx);
        TransactionWakes.Committed(tx);

        Assert.Equal(["one:a", "one:b", "two:a"], woken.Order(StringComparer.Ordinal));
        Assert.False(TransactionWakes.IsPending(tx));
    }

    [Fact]
    public void A_rollback_forgets_the_wake_ups()
    {
        var waker = new LocalWaker();
        var woken = 0;
        using var subscription = waker.Subscribe("a", () => woken++);
        var tx = new FakeTransaction();
        TransactionWakes.Add(tx, waker, ["a"]);

        TransactionWakes.RolledBack(tx);
        TransactionWakes.Committed(tx);

        Assert.Equal(0, woken);
        Assert.False(TransactionWakes.IsPending(tx));
    }

    [Fact]
    public void Nothing_is_recorded_for_a_client_without_producers_or_without_queues()
    {
        var waker = new LocalWaker();
        var tx = new FakeTransaction();

        TransactionWakes.Add(tx, waker, ["a"]);
        Assert.False(TransactionWakes.IsPending(tx));

        using var subscription = waker.Subscribe("a", () => { });
        TransactionWakes.Add(tx, waker, []);
        Assert.False(TransactionWakes.IsPending(tx));

        TransactionWakes.Add(tx, waker, ["a"]);
        Assert.True(TransactionWakes.IsPending(tx));
    }

    private sealed class FakeTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }
}
