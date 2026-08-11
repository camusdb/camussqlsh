
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
/// update, join, table, view, describe...), the cached list of table, view and
/// materialized-view names is offered instead of the static SQL vocabulary. Names are
/// loaded lazily via "show tables" / "show views" / "show materialized views" and
/// refreshed whenever the active database changes.
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

    private readonly string[] _staticWords;
    private volatile string[] _tables = [];

    public SqlCompletion(IEnumerable<string> staticWords)
    {
        _staticWords = staticWords.Distinct(StringComparer.OrdinalIgnoreCase)
                                  .OrderBy(w => w, StringComparer.OrdinalIgnoreCase)
                                  .ToArray();
    }

    public IEnumerable<string>? GetCompletions(string prefix, string word, string suffix)
    {
        if (IsTableContext(prefix) && _tables.Length > 0)
            return _tables;

        return _staticWords;
    }

    private static bool IsTableContext(string prefix)
    {
        string lastToken = LastToken(prefix);
        return lastToken.Length > 0 && TableContextKeywords.Contains(lastToken);
    }

    private static string LastToken(string prefix)
    {
        int end = prefix.Length;
        while (end > 0 && IsWordBoundary(prefix[end - 1]))
            end--;

        int start = end;
        while (start > 0 && !IsWordBoundary(prefix[start - 1]))
            start--;

        return prefix[start..end];
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

    private static async Task<bool> LoadNamesAsync(CamusConnection connection, string sql, List<string> names)
    {
        try
        {
            using CamusCommand cmd = connection.CreateSelectCommand(sql);
            cmd.CommandTimeout = 10;

            CamusDataReader reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
                string? name = row.Values.Count > 0 ? row.Values.First().StrValue : null;

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
