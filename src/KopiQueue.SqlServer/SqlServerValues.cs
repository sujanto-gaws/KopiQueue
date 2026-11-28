using System.Data;
using System.Text.Json;
using KopiQueue.Drivers;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer;

/// <summary>
/// Conversions between KopiQueue's values and the SQL Server columns of design §4.4: times are <c>datetime2(6)</c>
/// holding UTC, and <c>tags</c>, <c>attempted_by</c> and <c>errors</c> are <c>nvarchar(max)</c> holding a JSON array.
/// </summary>
internal static class SqlServerValues
{
    /// <summary>
    /// A stored time as a <see cref="DateTimeOffset"/> in UTC. SqlClient reads <c>datetime2</c> with an unspecified kind;
    /// the column holds UTC, so the offset is zero.
    /// </summary>
    public static DateTimeOffset FromDatabase(DateTime value) =>
        new(DateTime.SpecifyKind(value, DateTimeKind.Utc));

    /// <summary>
    /// The value to store for <paramref name="value"/>: the same instant in UTC, whatever its offset. SQL Server rounds
    /// it to whole microseconds when it stores it in a <c>datetime2(6)</c> column.
    /// </summary>
    public static DateTime ToDatabase(DateTimeOffset value) => value.UtcDateTime;

    /// <summary>A <c>datetime2</c> parameter holding <paramref name="value"/> in UTC, or <c>NULL</c>.</summary>
    public static SqlParameter Time(string name, DateTimeOffset? value) => new(name, SqlDbType.DateTime2)
    {
        Value = value is { } v ? ToDatabase(v) : DBNull.Value,
    };

    /// <summary>Reads a stored JSON array of strings; <c>NULL</c> reads as empty.</summary>
    /// <exception cref="InvalidOperationException">The value is not a JSON array of strings.</exception>
    public static string[] ReadStrings(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return [];
        }

        using var document = ParseArray(reader.GetString(ordinal), reader.GetName(ordinal));
        var values = new string[document.RootElement.GetArrayLength()];
        var i = 0;
        foreach (var element in document.RootElement.EnumerateArray())
        {
            values[i++] = element.ValueKind == JsonValueKind.String
                ? element.GetString()!
                : throw new InvalidOperationException($"Column {reader.GetName(ordinal)} holds a JSON array with a {element.ValueKind} in it, not only strings.");
        }

        return values;
    }

    /// <summary>Reads a stored JSON array of River's error objects; <c>NULL</c> reads as empty.</summary>
    /// <exception cref="InvalidOperationException">The value is not a JSON array.</exception>
    public static AttemptError[] ReadErrors(SqlDataReader reader, int ordinal)
    {
        if (reader.IsDBNull(ordinal))
        {
            return [];
        }

        using var document = ParseArray(reader.GetString(ordinal), reader.GetName(ordinal));
        return [.. document.RootElement.EnumerateArray().Select(StoredJob.ParseError)];
    }

    private static JsonDocument ParseArray(string json, string column)
    {
        JsonDocument document;
        try
        {
            document = JsonDocument.Parse(json);
        }
        catch (JsonException e)
        {
            throw new InvalidOperationException($"Column {column} does not hold valid JSON.", e);
        }

        if (document.RootElement.ValueKind != JsonValueKind.Array)
        {
            var kind = document.RootElement.ValueKind;
            document.Dispose();
            throw new InvalidOperationException($"Column {column} holds a JSON {kind}, not an array.");
        }

        return document;
    }
}
