using System.Text.RegularExpressions;

namespace KopiQueue.Migrations;

/// <summary>
/// Substitutes placeholders in migration SQL:
/// <list type="bullet">
///   <item><c>{{schema}}</c>: the quoted schema followed by a dot (<c>"app".</c>), or nothing to use the default schema.</item>
///   <item><c>{{prefix}}</c>: the table prefix, for example <c>river_</c>.</item>
/// </list>
/// Write table names as <c>{{schema}}{{prefix}}job</c>. Both values are validated by <see cref="SchemaIdentifiers"/>
/// before they get here (NFR-SEC-01), so substitution cannot inject SQL.
/// </summary>
internal static partial class MigrationTemplate
{
    /// <summary>Renders <paramref name="sql"/>.</summary>
    /// <exception cref="InvalidOperationException">The SQL contains an unknown placeholder (a typo in a migration).</exception>
    public static string Render(string sql, string schemaQualifier, string tablePrefix)
    {
        ArgumentNullException.ThrowIfNull(sql);
        return Placeholder().Replace(sql, m => m.Groups["name"].Value switch
        {
            "schema" => schemaQualifier,
            "prefix" => tablePrefix,
            var unknown => throw new InvalidOperationException($"Unknown placeholder '{{{{{unknown}}}}}' in migration SQL."),
        });
    }

    [GeneratedRegex(@"\{\{\s*(?<name>[A-Za-z_]+)\s*\}\}", RegexOptions.CultureInvariant)]
    private static partial Regex Placeholder();
}

/// <summary>Validation of the identifiers that are substituted into SQL (NFR-SEC-01).</summary>
internal static partial class SchemaIdentifiers
{
    /// <summary>Returns <paramref name="schema"/> if it is a safe schema name: a letter or underscore, then letters, digits or underscores, at most 63 characters.</summary>
    /// <exception cref="ArgumentException">The name is not allowed.</exception>
    public static string? ValidateSchema(string? schema) =>
        schema is null || SchemaPattern().IsMatch(schema)
            ? schema
            : throw new ArgumentException(
                $"Schema name '{schema}' is not allowed: use 1-63 letters, digits or underscores, not starting with a digit.", nameof(schema));

    /// <summary>Returns <paramref name="prefix"/> if it is a safe table prefix: lower-case letters, digits or underscores, at most 32 characters.</summary>
    /// <exception cref="ArgumentException">The prefix is not allowed.</exception>
    public static string ValidatePrefix(string prefix) =>
        prefix is not null && PrefixPattern().IsMatch(prefix)
            ? prefix
            : throw new ArgumentException(
                $"Table prefix '{prefix}' is not allowed: use 1-32 lower-case letters, digits or underscores, not starting with a digit.", nameof(prefix));

    [GeneratedRegex("^[A-Za-z_][A-Za-z0-9_]{0,62}$", RegexOptions.CultureInvariant)]
    private static partial Regex SchemaPattern();

    [GeneratedRegex("^[a-z_][a-z0-9_]{0,31}$", RegexOptions.CultureInvariant)]
    private static partial Regex PrefixPattern();
}
