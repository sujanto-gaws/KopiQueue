using System.Data;
using System.Globalization;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer;

/// <summary>
/// Compiles a <see cref="JobFilter.MetadataContains"/> object into a T-SQL predicate with PostgreSQL's <c>jsonb @&gt;</c>
/// semantics (FR-ADM-02, design §8.1). SQL Server has no containment operator, but the filter is known before the query
/// runs, so each key and array element becomes an <c>EXISTS</c> over <c>OPENJSON</c>, and every value a parameter.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>An object contains another when it has each of its keys, with a value that contains the other's value.</item>
/// <item>An array contains another when each of the other's elements is contained in one of its elements, in any order.</item>
/// <item>A scalar contains an equal scalar of the same JSON type: strings compare by their characters, exactly; numbers by
/// value, so <c>1500</c> equals <c>1.5e3</c>; <c>true</c>, <c>false</c> and <c>null</c> as themselves.</item>
/// </list>
/// Keys compare exactly, and case-sensitively. Numbers are compared as doubles and, where both fit, as
/// <c>decimal(38, 10)</c>, which is exact for every number a job is likely to carry.
/// </remarks>
internal sealed class SqlServerJsonContainment
{
    /// <summary>The most parameters one filter may use, well inside SQL Server's 2,100 per request.</summary>
    public const int MaxParameters = 2000;

    private readonly List<SqlParameter> _parameters = [];
    private int _aliases;

    private SqlServerJsonContainment()
    {
    }

    /// <summary>The predicate that <paramref name="column"/> contains <paramref name="filter"/>, and its parameters.</summary>
    /// <exception cref="ArgumentException">The filter needs more than <see cref="MaxParameters"/> parameters.</exception>
    public static (string Sql, IReadOnlyList<SqlParameter> Parameters) Compile(JsonObject filter, string column)
    {
        ArgumentNullException.ThrowIfNull(filter);
        var compiler = new SqlServerJsonContainment();
        var sql = compiler.ObjectMembers(filter, column);
        return (sql, compiler._parameters);
    }

    /// <summary>That the JSON object <paramref name="document"/> has every member of <paramref name="filter"/>.</summary>
    private string ObjectMembers(JsonObject filter, string document)
    {
        if (filter.Count == 0)
        {
            return "1 = 1";
        }

        var parts = new List<string>(filter.Count);
        foreach (var (key, value) in filter)
        {
            var member = Alias();
            var keyParameter = Parameter(SqlDbType.NVarChar, 4000, key);
            parts.Add(
                $"EXISTS (SELECT 1 FROM OPENJSON({document}) AS {member} " +
                $"WHERE CAST({member}.[key] AS varbinary(8000)) = CAST({keyParameter} AS varbinary(8000)) AND {Contains(value, member)})");
        }

        return string.Join(" AND ", parts);
    }

    /// <summary>That the <c>OPENJSON</c> row <paramref name="row"/> (its <c>[type]</c> and <c>[value]</c>) contains <paramref name="filter"/>.</summary>
    private string Contains(JsonNode? filter, string row)
    {
        switch (filter)
        {
            case null:
                return $"{row}.[type] = 0";

            case JsonObject obj:
                return obj.Count == 0
                    ? $"{row}.[type] = 5"
                    : $"{row}.[type] = 5 AND {ObjectMembers(obj, $"{row}.[value]")}";

            case JsonArray array:
                var parts = new List<string>(array.Count + 1) { $"{row}.[type] = 4" };
                foreach (var element in array)
                {
                    var item = Alias();
                    parts.Add($"EXISTS (SELECT 1 FROM OPENJSON({row}.[value]) AS {item} WHERE {Contains(element, item)})");
                }

                return string.Join(" AND ", parts);

            case JsonValue value:
                return Scalar(value, row);

            default:
                throw new ArgumentException($"Unsupported JSON node {filter.GetType().Name} in MetadataContains.", nameof(filter));
        }
    }

    private string Scalar(JsonValue value, string row)
    {
        switch (value.GetValueKind())
        {
            case JsonValueKind.String:
                var text = Parameter(SqlDbType.NVarChar, -1, value.GetValue<string>());
                return $"{row}.[type] = 1 AND CAST({row}.[value] AS varbinary(max)) = CAST({text} AS varbinary(max))";

            case JsonValueKind.True:
                return $"{row}.[type] = 3 AND {row}.[value] = N'true'";

            case JsonValueKind.False:
                return $"{row}.[type] = 3 AND {row}.[value] = N'false'";

            case JsonValueKind.Null:
                return $"{row}.[type] = 0";

            case JsonValueKind.Number:
                var raw = value.ToJsonString();
                var asDouble = Parameter(SqlDbType.Float, 0, double.Parse(raw, NumberStyles.Float, CultureInfo.InvariantCulture));
                object asDecimal = decimal.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var d) && Math.Abs(d) < 1e28m
                    ? Math.Round(d, 10)
                    : DBNull.Value;
                var exact = Parameter(SqlDbType.Decimal, 0, asDecimal);
                return $"{row}.[type] = 2 AND TRY_CAST({row}.[value] AS float) = {asDouble} " +
                       $"AND (TRY_CAST({row}.[value] AS decimal(38, 10)) IS NULL OR {exact} IS NULL " +
                       $"OR TRY_CAST({row}.[value] AS decimal(38, 10)) = {exact})";

            default:
                throw new ArgumentException($"Unsupported JSON value kind {value.GetValueKind()} in MetadataContains.", nameof(value));
        }
    }

    private string Alias() => "m" + (_aliases++).ToString(CultureInfo.InvariantCulture);

    private string Parameter(SqlDbType type, int size, object value)
    {
        if (_parameters.Count >= MaxParameters)
        {
            throw new ArgumentException($"MetadataContains is too large for SQL Server: it needs more than {MaxParameters} parameters.");
        }

        var name = new StringBuilder("@meta").Append(_parameters.Count.ToString(CultureInfo.InvariantCulture)).ToString();
        var parameter = new SqlParameter(name, type) { Value = value };
        if (size != 0)
        {
            parameter.Size = size;
        }

        if (type == SqlDbType.Decimal)
        {
            parameter.Precision = 38;
            parameter.Scale = 10;
        }

        _parameters.Add(parameter);
        return name;
    }
}
