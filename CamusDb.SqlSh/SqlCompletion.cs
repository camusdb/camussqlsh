
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using CamusDB.Client;
using RadLine;

/// <summary>
/// Provides Tab-completion for the SQL shell. Completion is context-aware: when the
/// word being typed follows a keyword that expects a table or view name (from, into,
/// update, join, table, view, describe, truncate, or the FOR of SHOW STATISTICS FOR), the cached
/// list of table, view and materialized-view names is offered instead of the static SQL
/// vocabulary. Names are
/// loaded lazily via "show tables" / "show views" / "show materialized views" and
/// refreshed whenever the active database changes. Configuration keys are cached the
/// same way, from "show variables", for the cluster-settings statements, and index names
/// per table, from "show indexes", for the FROM INDEX target of SHOW RANGES.
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
        // TRUNCATE [TABLE] <table>. The optional TABLE noise word lands on the entry above, so this
        // one covers the short spelling, where the verb itself precedes the table name.
        "truncate",
    };

    // The word after SETTING is a configuration key: SET CLUSTER SETTING <key> = <value> and
    // RESET CLUSTER SETTING <key>. This is where a typo is most expensive — the server can only
    // answer "unknown key" after a round trip, and a near-miss on a real key is worse still.
    private static readonly HashSet<string> SettingContextKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "setting",
    };

    // SHOW RANGES FROM INDEX <table>@<index>, and its singular SHOW RANGE form. The index half is
    // listed per table rather than for the whole database, because that is the shape SHOW INDEXES
    // answers in: one statement per table. Each entry is loaded on first use and kept.
    private readonly ConcurrentDictionary<string, Task<string[]>> _indexes = new(StringComparer.OrdinalIgnoreCase);

    private readonly string[] _staticWords;
    private volatile string[] _tables = [];
    private volatile string[] _settings = [];

    // The connection the table cache was loaded over, kept so an index list can be fetched later,
    // when the user asks for one. Null until the first refresh, which is also the point before
    // which no database is selected and SHOW RANGES cannot run at all.
    private volatile CamusConnection? _connection;

    // How long a Tab press waits for an index list that is not cached yet. The load continues in
    // the background past this point, so a press that gives up still fills the cache for the next
    // one; the cap is what keeps a slow or unreachable server from freezing the editor.
    private static readonly TimeSpan IndexLoadTimeout = TimeSpan.FromMilliseconds(750);

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

        if (IsIndexContext(prefix))
            return IndexCompletions(word);

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

    // The INDEX of SHOW RANGES FROM INDEX <table>@<index>. INDEX alone is not enough — CREATE INDEX
    // and DROP INDEX name something that does not exist yet, or that is not qualified by a table —
    // so the FROM before it is what decides. No other statement spells FROM INDEX.
    private static bool IsIndexContext(string prefix)
    {
        return string.Equals(LastToken(prefix), "index", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(TokenFromEnd(prefix, 1), "from", StringComparison.OrdinalIgnoreCase);
    }

    // The FROM INDEX target is one word to the line editor, because '@' is not a word boundary
    // there. So the completion has to be the whole `table@index` pair: completing the table half
    // alone would leave the user to type the index half with no help at all.
    private IEnumerable<string>? IndexCompletions(string word)
    {
        int at = word.IndexOf('@');

        // No '@' typed yet: the table half is what is being written, and the index half cannot be
        // listed before the table that holds it is known.
        if (at < 0)
            return _tables.Length > 0 ? _tables : null;

        string table = word[..at];

        // The table half goes into a statement, so accept only a plain identifier. A half-typed or
        // quoted name is not sent to the server; the target itself rejects quoted identifiers too.
        if (!IsPlainIdentifier(table))
            return null;

        CamusConnection? connection = _connection;

        // No refresh has run yet, so no database is selected — and SHOW RANGES needs one.
        if (connection is null)
            return null;

        // Task.Run, because this is the editor's own key-handling thread and the first steps of an
        // async call run on the caller's thread.
        Task<string[]> load = _indexes.GetOrAdd(table, name => Task.Run(() => LoadIndexesAsync(connection, name)));

        // A cached list is already complete here, so this returns at once. A first Tab on a table
        // that has none waits a short time for the load. Wait rethrows a faulted one, and a key
        // handler is the last place an exception may surface, so the throw is taken here.
        try
        {
            load.Wait(IndexLoadTimeout);
        }
        catch (Exception)
        {
            // Ignored: the state of the load decides what happens next, not the throw.
        }

        // Still running: complete nothing this time rather than hold the editor open. The load
        // continues, and the next Tab answers from the cache it fills.
        if (!load.IsCompleted)
            return null;

        // A load that failed, or that named no index, is dropped rather than kept — a table that
        // did not exist when it ran may exist by the next Tab, and that press retries it.
        if (!load.IsCompletedSuccessfully || load.Result.Length == 0)
        {
            _indexes.TryRemove(table, out _);
            return null;
        }

        return load.Result;
    }

    private async Task<string[]> LoadIndexesAsync(CamusConnection connection, string table)
    {
        List<string> names = new();

        // SHOW INDEXES FROM <table> names the index in its Key_name column, beside the table, the
        // key columns, the covering payload and the uniqueness flag. Asking for the column by name
        // matters more here than elsewhere: the first column is the table name, so falling back to
        // it would offer `users@users` for every index the table has.
        bool loaded = await LoadNamesAsync(connection, $"show indexes from {table}", names, "Key_name", fallbackToFirstColumn: false);

        // An empty answer is what the caller reads as a failure, so a table that could not be read
        // — no such table, a dropped connection — is retried rather than remembered as indexless.
        if (!loaded)
            return [];

        // The name is qualified back onto the table the user typed, so what lands in the buffer is
        // the whole target. The primary index keeps its internal `~pk` spelling, which is what
        // SHOW INDEXES prints and what the parser accepts.
        return names.Select(name => $"{table}@{name}")
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(name => name, StringComparer.OrdinalIgnoreCase)
                    .ToArray();
    }

    private static bool IsPlainIdentifier(string name)
    {
        return name.Length > 0 && name.All(c => char.IsLetterOrDigit(c) || c == '_');
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
    /// Drops the cached index names. Called after a statement that changes the indexes of a table
    /// without changing the set of relations, which is the one case a table refresh does not cover.
    /// The lists are fetched again, one table at a time, the next time one is asked for.
    /// </summary>
    public void InvalidateIndexes()
    {
        _indexes.Clear();
    }

    /// <summary>
    /// Loads the table and view names for the active database into the completion cache.
    /// Each command fails independently (no database selected, connection issues, a server
    /// that predates views), so completion degrades gracefully to whatever could be loaded,
    /// and ultimately to the static vocabulary.
    /// </summary>
    public async Task RefreshTablesAsync(CamusConnection connection)
    {
        // The index lists are per database and per table, so they cannot outlive a `use` switch or
        // a statement that changes the set of relations. Both are exactly when this runs.
        _connection = connection;
        _indexes.Clear();

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
        return TryGetCell(row, column, out ColumnValue value) ? value.StrValue : null;
    }

    // A row's cell, matched without case. The reader builds the row with the server's own column
    // spelling, which differs between statements — `variable` against `Key_name` — so a call site
    // must not have to reproduce the case to read a column that is there.
    private static bool TryGetCell(Dictionary<string, ColumnValue> row, string column, out ColumnValue value)
    {
        foreach (KeyValuePair<string, ColumnValue> cell in row)
        {
            if (string.Equals(cell.Key, column, StringComparison.OrdinalIgnoreCase))
            {
                value = cell.Value;
                return true;
            }
        }

        value = default;
        return false;
    }

    private static async Task<bool> LoadNamesAsync(
        CamusConnection connection,
        string sql,
        List<string> names,
        string? column = null,
        Func<Dictionary<string, ColumnValue>, bool>? include = null,
        bool fallbackToFirstColumn = true)
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

                string? name = column is not null && TryGetCell(row, column, out ColumnValue named)
                    ? named.StrValue
                    : fallbackToFirstColumn && row.Values.Count > 0 ? row.Values.First().StrValue : null;

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
