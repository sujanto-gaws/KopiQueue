using System.Globalization;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;

namespace KopiQueue.Migrations;

/// <summary>
/// Loads migrations embedded in an assembly. Each migration is a pair of resources named
/// <c>kopi-migrations/&lt;line&gt;/&lt;NNN&gt;_&lt;name&gt;.up.sql</c> and <c>...down.sql</c>.
/// </summary>
/// <remarks>
/// Drivers embed their SQL with:
/// <code>&lt;EmbeddedResource Include="Migrations\**\*.sql" LogicalName="kopi-migrations/%(RecursiveDir)%(Filename)%(Extension)" /&gt;</code>
/// A malformed set of resources is a packaging bug, so it throws <see cref="InvalidOperationException"/>.
/// </remarks>
internal static partial class MigrationLoader
{
    /// <summary>Logical-name prefix of migration resources.</summary>
    public const string ResourceRoot = "kopi-migrations/";

    /// <summary>A leading comment line that makes a script run outside a transaction.</summary>
    public const string NoTransactionDirective = "-- kopi:no-transaction";

    /// <summary>Loads every migration line embedded in <paramref name="assembly"/>, keyed by line.</summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<Migration>> FromAssembly(Assembly assembly)
    {
        ArgumentNullException.ThrowIfNull(assembly);
        var files = new List<(string Path, string Content)>();
        foreach (var resource in assembly.GetManifestResourceNames())
        {
            // MSBuild's %(RecursiveDir) uses '\' on Windows.
            var path = resource.Replace('\\', '/');
            if (!path.StartsWith(ResourceRoot, StringComparison.Ordinal))
            {
                continue;
            }

            using var stream = assembly.GetManifestResourceStream(resource)
                ?? throw new InvalidOperationException($"Resource '{resource}' could not be opened.");
            using var reader = new StreamReader(stream, Encoding.UTF8);
            files.Add((path[ResourceRoot.Length..], reader.ReadToEnd()));
        }

        return Parse(files);
    }

    /// <summary>
    /// Builds migration lines from <c>(path, content)</c> pairs, where a path looks like <c>main/001_create_job.up.sql</c>.
    /// </summary>
    public static IReadOnlyDictionary<string, IReadOnlyList<Migration>> Parse(IEnumerable<(string Path, string Content)> files)
    {
        ArgumentNullException.ThrowIfNull(files);
        var entries = new Dictionary<(string Line, int Version), Entry>();
        foreach (var (path, content) in files)
        {
            var match = FileNamePattern().Match(path);
            if (!match.Success)
            {
                throw new InvalidOperationException(
                    $"Migration resource '{path}' does not match '<line>/<NNN>_<name>.<up|down>.sql'.");
            }

            var line = match.Groups["line"].Value;
            var version = int.Parse(match.Groups["version"].Value, NumberStyles.None, CultureInfo.InvariantCulture);
            var name = match.Groups["name"].Value.Replace('_', ' ');
            if (version < 1)
            {
                throw new InvalidOperationException($"Migration '{path}': versions start at 1.");
            }

            var entry = entries.GetValueOrDefault((line, version)) ?? new Entry(name);
            if (entry.Name != name)
            {
                throw new InvalidOperationException(
                    $"Migration {line} {version} has two names: '{entry.Name}' and '{name}'.");
            }

            var script = ParseScript(content);
            if (match.Groups["direction"].Value == "up")
            {
                entry = entry.Up is null ? entry with { Up = script } : throw Duplicate(path);
            }
            else
            {
                entry = entry.Down is null ? entry with { Down = script } : throw Duplicate(path);
            }

            entries[(line, version)] = entry;
        }

        var lines = new Dictionary<string, IReadOnlyList<Migration>>(StringComparer.Ordinal);
        foreach (var group in entries.GroupBy(e => e.Key.Line, StringComparer.Ordinal))
        {
            var migrations = new List<Migration>();
            foreach (var ((line, version), entry) in group.OrderBy(e => e.Key.Version))
            {
                if (version != migrations.Count + 1)
                {
                    throw new InvalidOperationException(
                        $"Migration line '{line}' has a gap: version {migrations.Count + 1} is missing.");
                }

                if (entry.Up is null || entry.Down is null)
                {
                    throw new InvalidOperationException(
                        $"Migration {line} {version} ({entry.Name}) needs both an .up.sql and a .down.sql script.");
                }

                migrations.Add(new Migration(line, version, entry.Name, entry.Up, entry.Down));
            }

            lines[group.Key] = migrations;
        }

        return lines;
    }

    /// <summary>Reads the directives in the leading comment block of a script.</summary>
    internal static MigrationScript ParseScript(string content)
    {
        var inTransaction = true;
        using var reader = new StringReader(content);
        while (reader.ReadLine() is { } raw)
        {
            var line = raw.Trim();
            if (line.Length == 0)
            {
                continue;
            }

            if (!line.StartsWith("--", StringComparison.Ordinal))
            {
                break;
            }

            if (string.Equals(line, NoTransactionDirective, StringComparison.Ordinal))
            {
                inTransaction = false;
            }
        }

        return new MigrationScript(content, inTransaction);
    }

    private static InvalidOperationException Duplicate(string path) => new($"Migration resource '{path}' is duplicated.");

    [GeneratedRegex(@"^(?<line>[a-z][a-z0-9_]*)/(?<version>[0-9]{3,})_(?<name>[a-z0-9_]+)\.(?<direction>up|down)\.sql$", RegexOptions.CultureInvariant)]
    private static partial Regex FileNamePattern();

    private sealed record Entry(string Name, MigrationScript? Up = null, MigrationScript? Down = null);
}
