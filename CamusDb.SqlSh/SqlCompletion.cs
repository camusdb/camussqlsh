
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using RadLine;

/// <summary>
/// Provides Tab-completion for the SQL shell. Completion is context-aware: when the
/// word being typed follows a keyword that expects a table or view name (from, into,
/// update, join, table, view, describe, or the FOR of SHOW STATISTICS FOR), the cached
/// list of table, view and materialized-view names is offered instead of the static SQL
/// vocabulary. Names are
/// loaded lazily via "show tables" / "show views" / "show materialized views" and
/// refreshed whenever the active database changes. Configuration keys are cached the
/// same way, from "show variables", for the cluster-settings statements.
/// </summary>
internal sealed class SqlCompletion : ITextCompletion
{
    // Keywords that, when they immediately precede the word being typed, mean the user
    // is most likely referring to a table or view.
    private static readonly HashSet<string> TableContextKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "from",
        "into",
        "update",
        "join",
        "table",
        "view",
        "desc",
        "describe",
    };

    // The word after SETTING is a configuration key: SET CLUSTER SETTING <key> = <value> and
    // RESET CLUSTER SETTING <key>. This is where a typo is most expensive — the server can only
    // answer "unknown key" after a round trip, and a near-miss on a real key is worse still.
    private static readonly HashSet<string> SettingContextKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "setting",
    };

    private readonly string[] _staticWords;
    private volatile string[] _tables = [];
    private volatile string[] _settings = [];

    public SqlCompletion(IEnumerable<string> staticWords)
    {
        _staticWords = staticWords.Distinct(StringComparer.OrdinalIgnoreCase)
                                  .OrderBy(w => w, StringComparer.OrdinalIgnoreCase)
                                  .ToArray();
    }

    public IEnumerable<string>? GetCompletions(string prefix, string word, string suffix)
    {
        if (IsSettingContext(prefix) && _settings.Length > 0)
            return _settings;

        if (IsTableContext(prefix) && _tables.Length > 0)
            return _tables;

        return _staticWords;
    }

    private static bool IsTableContext(string prefix)
    {
        string lastToken = LastToken(prefix);

        if (lastToken.Length == 0)
            return false;

        if (TableContextKeywords.Contains(lastToken))
            return true;

        // SHOW STATISTICS FOR <table>. FOR on its own says nothing about what comes next — SHOW
        // GRANTS FOR wants a user name — so the word before it is what decides. The TABLE noise word
        // of SHOW STATISTICS FOR TABLE <table> already lands in the branch above.
        return string.Equals(lastToken, "for", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(TokenFromEnd(prefix, 1), "statistics", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsSettingContext(string prefix)
    {
        string lastToken = LastToken(prefix);
        return lastToken.Length > 0 && SettingContextKeywords.Contains(lastToken);
    }

    private static string LastToken(string prefix) => TokenFromEnd(prefix, 0);

    // The token `skip` positions back from the end of `prefix`: 0 is the word immediately before the
    // one being typed, 1 the word before that. Empty when the prefix holds no token that far back.
    private static string TokenFromEnd(string prefix, int skip)
    {
        int end = prefix.Length;

        for (int i = 0; ; i++)
        {
            while (end > 0 && IsWordBoundary(prefix[end - 1]))
                end--;

            int start = end;
            while (start > 0 && !IsWordBoundary(prefix[start - 1]))
                start--;

            if (i == skip)
                return prefix[start..end];

            if (start == 0)
                return "";

            end = start;
        }
    }

    private static bool IsWordBoundary(char c) => char.IsWhiteSpace(c) || c is ',' or '(' or ';';

    /// <summary>
    /// Loads the table and view names for the active database into the completion cache.
    /// Each command fails independently (no database selected, connection issues, a server
    /// that predates views), so completion degrades gracefully to whatever could be loaded,
    /// and ultimately to the static vocabulary.
    /// </summary>
    public async Task RefreshTablesAsync(CamusConnection connection)
    {
        List<string> names = new();

        bool loadedAny = await LoadNamesAsync(connection, "show tables", names);
        loadedAny |= await LoadNamesAsync(connection, "show views", names);
        loadedAny |= await LoadNamesAsync(connection, "show materialized views", names);

        if (!loadedAny)
            return; // Keep whatever we had; completion falls back to keywords.

        _tables = names.Distinct(StringComparer.OrdinalIgnoreCase)
                       .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                       .ToArray();
    }

    /// <summary>
    /// Loads the configuration key names offered after <c>SET</c>/<c>RESET CLUSTER SETTING</c>.
    /// The catalog is fixed for a given server build, so unlike the table cache this is loaded once
    /// at startup and never refreshed — a <c>SET</c> changes a key's value, never the set of keys.
    /// Pass a database-less connection: the statements this completes need no current database, so
    /// the keys should be offered in a session that never runs <c>use</c>.
    /// </summary>
    public async Task RefreshSettingsAsync(CamusConnection connection)
    {
        List<string> names = new();

        // Restart-class keys are left out: this list is offered only in the SET/RESET CLUSTER
        // SETTING position, where the server rejects them by rule, so completing one would be
        // completing a statement that cannot succeed. They are still visible in SHOW VARIABLES,
        // which is where an operator goes to see them.
        //
        // Every row's first column is the key, but ask for it by name: SHOW VARIABLES carries the
        // value, default, source, mutability and scope beside it, and column order is the server's
        // to change.
        bool loaded = await LoadNamesAsync(
            connection,
            "show variables",
            names,
            "variable",
            row => Cell(row, "mutability") is not "restart");

        if (!loaded)
            return; // Server predates SHOW VARIABLES, or is unreachable — fall back to keywords.

        _settings = names.Distinct(StringComparer.OrdinalIgnoreCase)
                         .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                         .ToArray();
    }

    // A row's cell by column name, or null when the server does not send that column — an older
    // server without the classification columns keeps every key rather than losing them all.
    private static string? Cell(Dictionary<string, ColumnValue> row, string column)
    {
        return row.TryGetValue(column, out ColumnValue value) ? value.StrValue : null;
    }

    private static async Task<bool> LoadNamesAsync(
        CamusConnection connection,
        string sql,
        List<string> names,
        string? column = null,
        Func<Dictionary<string, ColumnValue>, bool>? include = null)
    {
        try
        {
            using CamusCommand cmd = connection.CreateSelectCommand(sql);
            cmd.CommandTimeout = 10;

            CamusDataReader reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);

                if (include is not null && !include(row))
                    continue;

                string? name = column is not null && row.TryGetValue(column, out ColumnValue named)
                    ? named.StrValue
                    : row.Values.Count > 0 ? row.Values.First().StrValue : null;

                if (!string.IsNullOrWhiteSpace(name))
                    names.Add(name);
            }

            return true;
        }
        catch
        {
            return false;
        }
    }
}
