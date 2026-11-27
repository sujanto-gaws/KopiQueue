using System.Reflection;
using KopiQueue.Drivers;

namespace KopiQueue.Tests;

/// <summary>
/// M4's exit criterion "the conformance suite covers every <see cref="IKopiExecutor"/> operation" (plan task 4.6), kept
/// true: a new executor operation fails this test until a conformance case calls it.
/// </summary>
public sealed class ConformanceCoverageTests
{
    [Fact]
    public void Every_executor_operation_is_called_by_a_conformance_case()
    {
        var suite = FindConformanceSuite();
        var source = string.Concat(Directory.EnumerateFiles(suite, "DriverConformanceTests*.cs").Select(File.ReadAllText));
        var operations = typeof(IKopiExecutor).GetMethods(BindingFlags.Public | BindingFlags.Instance).Select(m => m.Name).Distinct().Order().ToList();

        var uncovered = operations.Where(name => !source.Contains("." + name + "(", StringComparison.Ordinal)).ToList();

        Assert.NotEmpty(operations);
        Assert.True(uncovered.Count == 0, $"No conformance case calls: {string.Join(", ", uncovered)}. Add cases to tests/KopiQueue.TestInfrastructure/Conformance.");
    }

    /// <summary>The conformance suite's folder, found from the test's output folder upwards.</summary>
    private static string FindConformanceSuite()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            var candidate = Path.Combine(dir.FullName, "tests", "KopiQueue.TestInfrastructure", "Conformance");
            if (Directory.Exists(candidate))
            {
                return candidate;
            }
        }

        throw new DirectoryNotFoundException($"tests/KopiQueue.TestInfrastructure/Conformance was not found above {AppContext.BaseDirectory}.");
    }
}
