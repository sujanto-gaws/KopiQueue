using System.Globalization;
using Microsoft.Data.SqlClient;

namespace KopiQueue.SqlServer;

/// <summary>
/// The session settings KopiQueue's tables need (design §4.4). Filtered indexes and the indexed computed column
/// <c>unique_slot</c> can only be written by a session with <c>ANSI_NULLS</c>, <c>ANSI_PADDING</c>,
/// <c>ANSI_WARNINGS</c>, <c>ARITHABORT</c>, <c>CONCAT_NULL_YIELDS_NULL</c> and <c>QUOTED_IDENTIFIER</c> on and
/// <c>NUMERIC_ROUNDABORT</c> off, and KopiQueue needs database compatibility level 150 or later (NFR-COMP-02a).
/// </summary>
/// <remarks>
/// SqlClient's defaults satisfy this, with one subtlety: SqlClient leaves <c>ARITHABORT</c> off, but at compatibility
/// level 90 and later <c>ANSI_WARNINGS</c> on implies it. So <c>ARITHABORT</c> itself is not checked.
/// A session fails the check when other code changed its settings, for example with <c>SET ANSI_WARNINGS OFF</c>.
/// </remarks>
internal static class SqlServerSessionCheck
{
    /// <summary>The lowest database compatibility level KopiQueue supports: SQL Server 2019's.</summary>
    public const int MinimumCompatibilityLevel = 150;

    private const string Query =
        """
        SELECT CAST(SESSIONPROPERTY('ANSI_NULLS') AS int),
               CAST(SESSIONPROPERTY('ANSI_PADDING') AS int),
               CAST(SESSIONPROPERTY('ANSI_WARNINGS') AS int),
               CAST(SESSIONPROPERTY('CONCAT_NULL_YIELDS_NULL') AS int),
               CAST(SESSIONPROPERTY('QUOTED_IDENTIFIER') AS int),
               CAST(SESSIONPROPERTY('NUMERIC_ROUNDABORT') AS int),
               CAST((SELECT compatibility_level FROM sys.databases WHERE database_id = DB_ID()) AS int)
        """;

    /// <summary>Reads the settings of <paramref name="connection"/>'s session and throws if KopiQueue cannot work in it.</summary>
    /// <param name="connection">The open connection.</param>
    /// <param name="transaction">The transaction open on it, if any: SqlClient requires every command to enlist in it.</param>
    /// <param name="cancellationToken">Cancels the query.</param>
    /// <exception cref="KopiQueueConfigurationException">A setting is wrong; the message names each one.</exception>
    public static async Task EnsureAsync(SqlConnection connection, SqlTransaction? transaction, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var settings = await ReadAsync(connection, transaction, cancellationToken).ConfigureAwait(false);
        var problems = Evaluate(settings);
        if (problems.Count > 0)
        {
            throw new KopiQueueConfigurationException(
                "This SQL Server session cannot write KopiQueue's tables: " + string.Join("; ", problems) + ". " +
                "SqlClient's defaults are correct, so look for code that changes these settings on the connection.");
        }
    }

    /// <summary>Reads the settings of <paramref name="connection"/>'s session.</summary>
    public static async Task<SessionSettings> ReadAsync(SqlConnection connection, SqlTransaction? transaction, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(connection);
        var command = new SqlCommand(Query, connection, transaction);
        await using (command.ConfigureAwait(false))
        {
            var reader = await command.ExecuteReaderAsync(cancellationToken).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                if (!await reader.ReadAsync(cancellationToken).ConfigureAwait(false))
                {
                    throw new InvalidOperationException("The session settings query returned no row.");
                }

                return new SessionSettings(
                    AnsiNulls: reader.GetInt32(0) == 1,
                    AnsiPadding: reader.GetInt32(1) == 1,
                    AnsiWarnings: reader.GetInt32(2) == 1,
                    ConcatNullYieldsNull: reader.GetInt32(3) == 1,
                    QuotedIdentifier: reader.GetInt32(4) == 1,
                    NumericRoundAbort: reader.GetInt32(5) == 1,
                    CompatibilityLevel: reader.IsDBNull(6) ? null : reader.GetInt32(6));
            }
        }
    }

    /// <summary>What is wrong with <paramref name="settings"/>, one entry per problem; empty when nothing is.</summary>
    public static IReadOnlyList<string> Evaluate(SessionSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        var problems = new List<string>();
        RequireOn(settings.AnsiNulls, "ANSI_NULLS");
        RequireOn(settings.AnsiPadding, "ANSI_PADDING");
        RequireOn(settings.AnsiWarnings, "ANSI_WARNINGS");
        RequireOn(settings.ConcatNullYieldsNull, "CONCAT_NULL_YIELDS_NULL");
        RequireOn(settings.QuotedIdentifier, "QUOTED_IDENTIFIER");
        if (settings.NumericRoundAbort)
        {
            problems.Add("NUMERIC_ROUNDABORT is ON but must be OFF");
        }

        // Null when the session cannot see its database's row in sys.databases; the level is then not checked.
        if (settings.CompatibilityLevel < MinimumCompatibilityLevel)
        {
            problems.Add(string.Create(
                CultureInfo.InvariantCulture,
                $"the database compatibility level is {settings.CompatibilityLevel}, but KopiQueue needs {MinimumCompatibilityLevel} or later"));
        }

        return problems;

        void RequireOn(bool value, string name)
        {
            if (!value)
            {
                problems.Add(name + " is OFF but must be ON");
            }
        }
    }

    /// <summary>The settings of one session that the check looks at.</summary>
    internal sealed record SessionSettings(
        bool AnsiNulls,
        bool AnsiPadding,
        bool AnsiWarnings,
        bool ConcatNullYieldsNull,
        bool QuotedIdentifier,
        bool NumericRoundAbort,
        int? CompatibilityLevel);
}

/// <summary>
/// Runs <see cref="SqlServerSessionCheck"/> on the driver's pooled sessions until one passes, then never again.
/// </summary>
/// <remarks>
/// The pool is KopiQueue's own (<c>Application Name=KopiQueue</c>), and SqlClient resets a session's SET options when
/// it reuses it, so every pooled session has the settings the first one had. Once a check passes, later operations skip
/// the round trip. A failed check is not remembered, so a fixed database passes on the next operation.
/// </remarks>
internal sealed class SqlServerSessionGate
{
    private volatile bool _passed;

    /// <summary>Whether a check has passed.</summary>
    public bool Passed => _passed;

    /// <summary>Checks <paramref name="connection"/>'s session unless a check has already passed.</summary>
    /// <exception cref="KopiQueueConfigurationException">A setting is wrong.</exception>
    public async Task EnsureAsync(SqlConnection connection, CancellationToken cancellationToken)
    {
        if (_passed)
        {
            return;
        }

        await SqlServerSessionCheck.EnsureAsync(connection, transaction: null, cancellationToken).ConfigureAwait(false);
        _passed = true;
    }
}
