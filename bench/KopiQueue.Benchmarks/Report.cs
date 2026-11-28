using System.Globalization;

namespace KopiQueue.Benchmarks;

internal sealed class BenchSettings
{
    /// <summary>READ_COMMITTED_SNAPSHOT on the benchmark database: on is Azure SQL Database's default.</summary>
    public bool Rcsi { get; init; } = true;

    /// <summary>Fewer rows and repetitions, for a smoke run.</summary>
    public bool Quick { get; init; }

    /// <summary>Keep the database afterwards, to look at it.</summary>
    public bool Keep { get; init; }

    /// <summary>The steps to run (fetch, insert, e2e, dq6, dq7, latency); empty runs them all.</summary>
    public IReadOnlyList<string> Only { get; init; } = [];

    public int Scale(int full) => Quick ? Math.Max(1, full / 10) : full;

    public bool Runs(string step) => Only.Count == 0 || Only.Contains(step);
}

/// <summary>Writes the Markdown report.</summary>
internal static class Report
{
    private static readonly CultureInfo Invariant = CultureInfo.InvariantCulture;

    public static void Heading(string text) => Console.WriteLine($"\n## {text}\n");

    public static void Line(string text) => Console.WriteLine(text);

    public static void Table(IReadOnlyList<string> header, IEnumerable<IReadOnlyList<string>> rows)
    {
        Console.WriteLine($"| {string.Join(" | ", header)} |");
        Console.WriteLine($"|{string.Concat(header.Select(_ => "---|"))}");
        foreach (var row in rows)
        {
            Console.WriteLine($"| {string.Join(" | ", row)} |");
        }

        Console.WriteLine();
    }

    public static string N(double value, int decimals = 0) => value.ToString("N" + decimals.ToString(Invariant), Invariant);

    /// <summary>p50, p95, p99 and max of times in milliseconds.</summary>
    public static string Distribution(IReadOnlyCollection<double> milliseconds)
    {
        if (milliseconds.Count == 0)
        {
            return "no samples";
        }

        var sorted = milliseconds.Order().ToArray();
        double At(double p) => sorted[Math.Min(sorted.Length - 1, (int)Math.Ceiling(p * sorted.Length) - 1)];
        return $"p50 {N(At(0.50), 1)} ms, p95 {N(At(0.95), 1)} ms, p99 {N(At(0.99), 1)} ms, max {N(sorted[^1], 1)} ms ({sorted.Length} samples)";
    }
}
