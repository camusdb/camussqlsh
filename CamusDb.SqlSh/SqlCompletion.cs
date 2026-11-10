
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
/// update, join, table, view, describe, truncate, references, or the FOR of SHOW STATISTICS FOR), the cached
/// list of table, view and materialized-view names is offered instead of the static SQL
/// vocabulary. Names are
/// loaded lazily via "show tables" / "show views" / "show materialized views" and
/// refreshed whenever the active database changes. Configuration keys are cached the
/// same way, from "show variables", for the cluster-settings statements, and index names
/// per table, from "show indexes", for the FROM INDEX target of SHOW RANGES and the index of DROP
/// INDEX (the table.index form and ALTER TABLE … DROP INDEX). Sequence
/// names are loaded from "show sequences" with the relation names and offered after
/// SEQUENCE (ALTER, DROP, SHOW CREATE and COMMENT ON SEQUENCE). The members of the loaded C#
/// extensions come from "show extensions": procedure names after CALL, trigger types after
/// EXTERNAL NAME, and function names with the static vocabulary. The inline functions of the
/// current database come from "show functions", with the relation names: their names are offered
/// after DROP, ALTER, SHOW CREATE and CREATE OR REPLACE FUNCTION and after `\df`, and with the
/// static vocabulary everywhere else. LANGUAGE completes to csharp, the one language there is.
/// The triggers of the current database come from "show triggers", with the relation names: their
/// names are offered after DROP TRIGGER and SHOW CREATE TRIGGER, and the ON after such a name
/// offers the tables that have a trigger of that name.
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
        // The parent table of a foreign key: the inline REFERENCES <table> [(col)] of a column, and
        // FOREIGN KEY (cols) REFERENCES <table> of a table constraint or ALTER TABLE … ADD.
        "references",
    };

    // The word after SETTING is a configuration key: SET CLUSTER SETTING <key> = <value> and
    // RESET CLUSTER SETTING <key>. This is where a typo is most expensive — the server can only
    // answer "unknown key" after a round trip, and a near-miss on a real key is worse still.
    private static readonly HashSet<string> SettingContextKeywords = new(StringComparer.OrdinalIgnoreCase)
    {
        "setting",
    };

    // The index names of each table, as SHOW INDEXES gives them: for SHOW RANGES FROM INDEX
    // <table>@<index>, DROP INDEX <table>.<index> and ALTER TABLE <table> DROP INDEX <index>. They are
    // listed per table rather than for the whole database, because that is the shape SHOW INDEXES
    // answers in: one statement per table. Each entry is loaded on first use and kept.
    private readonly ConcurrentDictionary<string, Task<string[]>> _indexes = new(StringComparer.OrdinalIgnoreCase);

    private readonly string[] _baseWords;
    private volatile string[] _staticWords;
    private volatile string[] _tables = [];
    private volatile string[] _sequences = [];
    private volatile string[] _settings = [];
    private volatile string[] _functions = [];
    private volatile string[] _procedures = [];
    private volatile string[] _triggerTypes = [];
    private volatile string[] _inlineFunctions = [];

    // The triggers of the current database, of both forms, with the table of each one. A trigger
    // name is unique on its table only, so one name can have more than one table.
    private volatile (string Name, string Table)[] _triggers = [];

    // The languages of CREATE FUNCTION … LANGUAGE <language>.
    private static readonly string[] FunctionLanguages = ["csharp"];

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
        _baseWords = staticWords.ToArray();
        _staticWords = Vocabulary(_baseWords);
    }

    /// <summary>
    /// The SQL function names of the loaded C# extensions, aliases included. Empty until
    /// <see cref="RefreshExtensionsAsync"/> runs, and empty for a user who is not a superuser.
    /// </summary>
    public IReadOnlyList<string> ExtensionFunctions => _functions;

    /// <summary>
    /// The inline functions of the current database. Empty until <see cref="RefreshTablesAsync"/>
    /// or <see cref="RefreshInlineFunctionsAsync"/> runs with a database selected.
    /// </summary>
    public IReadOnlyList<string> InlineFunctions => _inlineFunctions;

    private static string[] Vocabulary(IEnumerable<string> words)
    {
        return words.Distinct(StringComparer.OrdinalIgnoreCase)
                    .OrderBy(w => w, StringComparer.OrdinalIgnoreCase)
                    .ToArray();
    }

    public IEnumerable<string>? GetCompletions(string prefix, string word, string suffix)
    {
        if (IsSettingContext(prefix) && _settings.Length > 0)
            return _settings;

        if (IsIndexContext(prefix))
            return IndexCompletions(word);

        if (IsAlterTableDropIndexContext(prefix, out string alteredTable))
            return DroppableIndexes(alteredTable);

        if (IsDropIndexContext(prefix))
            return DropIndexCompletions(word);

        if (IsSequenceContext(prefix) && _sequences.Length > 0)
            return _sequences;

        if (IsProcedureContext(prefix) && _procedures.Length > 0)
            return _procedures;

        if (IsTriggerTypeContext(prefix) && _triggerTypes.Length > 0)
            return _triggerTypes;

        if (IsFunctionNameContext(prefix) && _inlineFunctions.Length > 0)
            return _inlineFunctions;

        if (IsLanguageContext(prefix))
            return FunctionLanguages;

        if (IsTriggerNameContext(prefix) && _triggers.Length > 0)
            return TriggerNames();

        if (IsTriggerTableContext(prefix, out string trigger) && TriggerTables(trigger) is { Length: > 0 } triggerTables)
            return triggerTables;

        if (IsDescribeContext(prefix) && _tables.Length + _sequences.Length > 0)
            return [.. _tables, .. _sequences];

        if (IsTableContext(prefix) && _tables.Length > 0)
            return _tables;

        return _staticWords;
    }

    // The name after the shell's `\d` or `\d+` (or `/d`, `/d+`): any relation, a sequence too. Only
    // as the first word, since the command must start the line.
    private static bool IsDescribeContext(string prefix)
    {
        if (TokenFromEnd(prefix, 1).Length > 0)
            return false;

        return LastToken(prefix) is "\\d" or "\\d+" or "/d" or "/d+";
    }

    // The name of an inline function that exists: DROP FUNCTION [IF EXISTS] <name>, ALTER FUNCTION
    // <name>, SHOW CREATE FUNCTION <name> and CREATE OR REPLACE FUNCTION <name>, and the shell's
    // `\df` and `\df+` as the first word. CREATE FUNCTION names a function that does not exist yet,
    // so nothing is offered there.
    private static bool IsFunctionNameContext(string prefix)
    {
        if (TokenFromEnd(prefix, 1).Length == 0 && LastToken(prefix) is "\\df" or "\\df+" or "/df" or "/df+")
            return true;

        int at = SkipIfExists(prefix);

        if (!string.Equals(TokenFromEnd(prefix, at), "function", StringComparison.OrdinalIgnoreCase))
            return false;

        string before = TokenFromEnd(prefix, at + 1);

        if (before.Equals("drop", StringComparison.OrdinalIgnoreCase) ||
            before.Equals("alter", StringComparison.OrdinalIgnoreCase) ||
            before.Equals("replace", StringComparison.OrdinalIgnoreCase))
            return true;

        return before.Equals("create", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(TokenFromEnd(prefix, at + 2), "show", StringComparison.OrdinalIgnoreCase);
    }

    // The name of a trigger that exists: DROP TRIGGER <name> and SHOW CREATE TRIGGER <name>. CREATE
    // TRIGGER names one that does not exist yet, so nothing is offered there.
    private static bool IsTriggerNameContext(string prefix)
    {
        if (!string.Equals(LastToken(prefix), "trigger", StringComparison.OrdinalIgnoreCase))
            return false;

        string before = TokenFromEnd(prefix, 1);

        return before.Equals("drop", StringComparison.OrdinalIgnoreCase) ||
               (before.Equals("create", StringComparison.OrdinalIgnoreCase) &&
                string.Equals(TokenFromEnd(prefix, 2), "show", StringComparison.OrdinalIgnoreCase));
    }

    // The ON of DROP TRIGGER <name> ON <table> and SHOW CREATE TRIGGER <name> ON <table>: the tables
    // that have a trigger of that name. CREATE TRIGGER <name> ON is left to the table context, which
    // offers every table.
    private static bool IsTriggerTableContext(string prefix, out string trigger)
    {
        trigger = "";

        if (!string.Equals(LastToken(prefix), "on", StringComparison.OrdinalIgnoreCase) ||
            !string.Equals(TokenFromEnd(prefix, 2), "trigger", StringComparison.OrdinalIgnoreCase))
            return false;

        string verb = TokenFromEnd(prefix, 3);

        bool matches = verb.Equals("drop", StringComparison.OrdinalIgnoreCase) ||
                       (verb.Equals("create", StringComparison.OrdinalIgnoreCase) &&
                        string.Equals(TokenFromEnd(prefix, 4), "show", StringComparison.OrdinalIgnoreCase));

        trigger = matches ? TokenFromEnd(prefix, 1) : "";
        return matches;
    }

    private string[] TriggerNames()
    {
        return Vocabulary(_triggers.Select(t => t.Name));
    }

    private string[] TriggerTables(string trigger)
    {
        return Vocabulary(_triggers.Where(t => string.Equals(t.Name, trigger, StringComparison.OrdinalIgnoreCase)).Select(t => t.Table));
    }

    // CREATE FUNCTION … LANGUAGE <language>.
    private static bool IsLanguageContext(string prefix)
    {
        return string.Equals(LastToken(prefix), "language", StringComparison.OrdinalIgnoreCase);
    }

    private static bool IsTableContext(string prefix)
    {
        string lastToken = LastToken(prefix);

        if (lastToken.Length == 0)
            return false;

        if (TableContextKeywords.Contains(lastToken))
            return true;

        // CREATE [UNIQUE | INVERTED] INDEX <name> ON <table> and DROP INDEX <name> ON <table>, each
        // with its optional IF [NOT] EXISTS between INDEX and the name.
        if (string.Equals(lastToken, "on", StringComparison.OrdinalIgnoreCase) && IsIndexNameBefore(prefix, 1))
            return true;

        // CREATE TRIGGER <name> ON <table> and DROP TRIGGER <name> ON <table>. ON alone also joins
        // two tables by a condition, so the TRIGGER two words back is what decides.
        if (string.Equals(lastToken, "on", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(TokenFromEnd(prefix, 2), "trigger", StringComparison.OrdinalIgnoreCase))
            return true;

        // SHOW TRIGGERS ON <table>.
        if (string.Equals(lastToken, "on", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(TokenFromEnd(prefix, 1), "triggers", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(TokenFromEnd(prefix, 2), "show", StringComparison.OrdinalIgnoreCase))
            return true;

        // SHOW STATISTICS FOR <table>. FOR on its own says nothing about what comes next — SHOW
        // GRANTS FOR wants a user name — so the word before it is what decides. The TABLE noise word
        // of SHOW STATISTICS FOR TABLE <table> already lands in the branch above.
        return string.Equals(lastToken, "for", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(TokenFromEnd(prefix, 1), "statistics", StringComparison.OrdinalIgnoreCase);
    }

    // The name after SEQUENCE: ALTER, DROP, SHOW CREATE and COMMENT ON SEQUENCE all name one that
    // exists. CREATE SEQUENCE names one that does not exist yet, so nothing is offered there.
    private static bool IsSequenceContext(string prefix)
    {
        return string.Equals(LastToken(prefix), "sequence", StringComparison.OrdinalIgnoreCase) &&
               !string.Equals(TokenFromEnd(prefix, 1), "create", StringComparison.OrdinalIgnoreCase);
    }

    // CALL <procedure>(<args>). CALL is the first word of its statement, so a CALL anywhere else is
    // a name, such as a column called call, and completes from the static vocabulary. The text
    // before it must be empty or end a statement, because the TUI editor holds several of them.
    private static bool IsProcedureContext(string prefix)
    {
        if (!string.Equals(LastToken(prefix), "call", StringComparison.OrdinalIgnoreCase))
            return false;

        string before = prefix.TrimEnd()[..^"call".Length].TrimEnd();
        return before.Length == 0 || before[^1] == ';';
    }

    // The EXTERNAL NAME '<Type>' of CREATE TRIGGER. The type names are offered with their quotes,
    // because the line editor takes an opening quote as part of the word that is typed.
    private static bool IsTriggerTypeContext(string prefix)
    {
        return string.Equals(LastToken(prefix), "name", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(TokenFromEnd(prefix, 1), "external", StringComparison.OrdinalIgnoreCase);
    }

    // The INDEX of SHOW RANGES FROM INDEX <table>@<index>. INDEX alone is not enough — CREATE INDEX
    // and DROP INDEX name something that does not exist yet, or that is not qualified by a table —
    // so the FROM before it is what decides. No other statement spells FROM INDEX.
    private static bool IsIndexContext(string prefix)
    {
        return string.Equals(LastToken(prefix), "index", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(TokenFromEnd(prefix, 1), "from", StringComparison.OrdinalIgnoreCase);
    }

    // Whether the token `skip` positions back from the end names an index: the word after INDEX, or
    // after the IF EXISTS or IF NOT EXISTS that follows INDEX.
    private static bool IsIndexNameBefore(string prefix, int skip)
    {
        bool Is(int position, string word) =>
            string.Equals(TokenFromEnd(prefix, position), word, StringComparison.OrdinalIgnoreCase);

        int at = skip + 1;

        if (Is(at, "exists") && Is(at + 1, "if"))
            at += 2;
        else if (Is(at, "exists") && Is(at + 1, "not") && Is(at + 2, "if"))
            at += 3;

        return Is(at, "index");
    }

    // How many tokens a trailing IF EXISTS takes at the end of the prefix: 2, or 0 when there is none.
    private static int SkipIfExists(string prefix) =>
        string.Equals(LastToken(prefix), "exists", StringComparison.OrdinalIgnoreCase) &&
        string.Equals(TokenFromEnd(prefix, 1), "if", StringComparison.OrdinalIgnoreCase) ? 2 : 0;

    // The index of ALTER TABLE <table> DROP INDEX [IF EXISTS] <index>. The table is in the statement,
    // so the index names are the bare ones of that table.
    private static bool IsAlterTableDropIndexContext(string prefix, out string table)
    {
        int at = SkipIfExists(prefix);

        bool matches =
            string.Equals(TokenFromEnd(prefix, at), "index", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(TokenFromEnd(prefix, at + 1), "drop", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(TokenFromEnd(prefix, at + 3), "table", StringComparison.OrdinalIgnoreCase) &&
            string.Equals(TokenFromEnd(prefix, at + 4), "alter", StringComparison.OrdinalIgnoreCase);

        table = matches ? TokenFromEnd(prefix, at + 2) : "";
        return matches;
    }

    // The target of the standalone DROP INDEX [IF EXISTS]. ALTER TABLE … DROP INDEX is matched first,
    // so what is left here is the statement that starts with DROP INDEX.
    private static bool IsDropIndexContext(string prefix)
    {
        int at = SkipIfExists(prefix);

        return string.Equals(TokenFromEnd(prefix, at), "index", StringComparison.OrdinalIgnoreCase) &&
               string.Equals(TokenFromEnd(prefix, at + 1), "drop", StringComparison.OrdinalIgnoreCase);
    }

    // DROP INDEX needs its table, and the <table>.<index> form names it in the same word, because '.'
    // is not a word boundary to the line editor. Before the '.' the table names are offered. After it,
    // the indexes of that table are offered, qualified, so the whole target lands in the buffer. The
    // DROP INDEX <index> ON <table> form gets its table names from the ON.
    private IEnumerable<string>? DropIndexCompletions(string word)
    {
        int dot = word.IndexOf('.');

        if (dot < 0)
            return _tables.Length > 0 ? _tables : null;

        string table = word[..dot];

        return DroppableIndexes(table)?.Select(name => $"{table}.{name}").ToArray();
    }

    // The indexes of a table that DROP INDEX can drop. A name that starts with '~' (the primary key,
    // or the index a foreign key made for itself) cannot be written in SQL, and the server keeps it
    // out of reach of DROP INDEX, so it is not offered.
    private IEnumerable<string>? DroppableIndexes(string table)
    {
        string[]? names = TableIndexes(table)?.Where(name => !name.StartsWith('~')).ToArray();

        return names is { Length: > 0 } ? names : null;
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

        // The primary index keeps its internal `~pk` spelling, which is what SHOW INDEXES prints and
        // what the FROM INDEX target accepts.
        return TableIndexes(table)?.Select(name => $"{table}@{name}").ToArray();
    }

    // The index names of one table, from the cache or from a load that ends in time. Null when the
    // table name is not a plain identifier, when no database is selected yet, or when the load is
    // slow, failed or found no index.
    private string[]? TableIndexes(string table)
    {
        // The table name goes into a statement, so accept only a plain identifier. A half-typed or
        // quoted name is not sent to the server.
        if (!IsPlainIdentifier(table))
            return null;

        CamusConnection? connection = _connection;

        // No refresh has run yet, so no database is selected — and SHOW INDEXES needs one.
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

        // The bare names. Each caller qualifies them in the form its statement takes.
        return names.Distinct(StringComparer.OrdinalIgnoreCase)
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
    /// Loads the table, view and sequence names for the active database into the completion cache.
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

        if (loadedAny)
        {
            _tables = names.Distinct(StringComparer.OrdinalIgnoreCase)
                           .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                           .ToArray();
        }

        // Loaded on its own, so a server that predates sequences still completes relation names.
        // The name is asked for by column: SHOW SEQUENCES carries the counter's position and
        // options beside it.
        List<string> sequences = new();

        if (await LoadNamesAsync(connection, "show sequences", sequences, "sequence"))
        {
            _sequences = sequences.Distinct(StringComparer.OrdinalIgnoreCase)
                                  .OrderBy(n => n, StringComparer.OrdinalIgnoreCase)
                                  .ToArray();
        }

        await RefreshInlineFunctionsAsync(connection);
        await RefreshTriggersAsync(connection);
    }

    /// <summary>
    /// Loads the triggers of the current database, of both forms, from SHOW TRIGGERS. A failed load
    /// (a server that predates SHOW TRIGGERS) keeps the triggers that were loaded before.
    /// </summary>
    public async Task RefreshTriggersAsync(CamusConnection connection)
    {
        List<(string Name, string Table)> triggers = new();

        try
        {
            using CamusCommand cmd = connection.CreateSelectCommand("show triggers");
            cmd.CommandTimeout = 10;

            CamusDataReader reader = await cmd.ExecuteReaderAsync();

            while (await reader.ReadAsync())
            {
                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);

                string? name = Cell(row, "name");
                string? table = Cell(row, "table");

                if (!string.IsNullOrWhiteSpace(name) && !string.IsNullOrWhiteSpace(table))
                    triggers.Add((name, table));
            }
        }
        catch
        {
            return;
        }

        _triggers = triggers.ToArray();
    }

    /// <summary>
    /// Loads the inline function names of the current database from SHOW FUNCTIONS. A row in the
    /// <c>unloading</c> state is the code of a dropped or replaced function that a running statement
    /// still holds, so its name is left out. A failed load (a server that predates inline functions)
    /// keeps the names that were loaded before.
    /// </summary>
    public async Task RefreshInlineFunctionsAsync(CamusConnection connection)
    {
        List<string> functions = new();

        bool loaded = await LoadNamesAsync(
            connection,
            "show functions",
            functions,
            "name",
            row => Cell(row, "state") is not "unloading",
            fallbackToFirstColumn: false);

        if (!loaded)
            return;

        _inlineFunctions = Vocabulary(functions);
        RebuildStaticWords();
    }

    // The words offered where no context applies: the static vocabulary and the function names
    // that the server knows.
    private void RebuildStaticWords()
    {
        _staticWords = Vocabulary(_baseWords.Concat(_functions).Concat(_inlineFunctions));
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

    /// <summary>
    /// Loads the members of the C# extensions the node loaded at startup: function, procedure and
    /// trigger type names. Extensions change only when a node restarts, so this is loaded once at
    /// startup, like the configuration keys. SHOW EXTENSIONS needs a superuser and reads no
    /// database, so pass a database-less connection. A failed load keeps the static vocabulary.
    /// </summary>
    public async Task RefreshExtensionsAsync(CamusConnection connection)
    {
        List<string> functions = new();
        List<string> procedures = new();
        List<string> triggerTypes = new();

        try
        {
            using CamusCommand cmd = connection.CreateSelectCommand("show extensions");
            cmd.CommandTimeout = 10;

            CamusDataReader reader = await cmd.ExecuteReaderAsync();

            // One row per assembly. Each member column holds a comma-separated list, empty when the
            // assembly registered none of that kind.
            while (await reader.ReadAsync())
            {
                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);

                functions.AddRange(SplitList(Cell(row, "functions")));
                procedures.AddRange(SplitList(Cell(row, "procedures")));
                triggerTypes.AddRange(SplitList(Cell(row, "triggers")));
            }
        }
        catch
        {
            return; // Not a superuser, or a server that predates extensions.
        }

        _functions = Vocabulary(functions);
        _procedures = Vocabulary(procedures);

        // A CLR type name is case-sensitive, and EXTERNAL NAME takes it as a string literal.
        _triggerTypes = triggerTypes.Distinct(StringComparer.Ordinal)
                                    .OrderBy(t => t, StringComparer.Ordinal)
                                    .Select(t => $"'{t.Replace("'", "''", StringComparison.Ordinal)}'")
                                    .ToArray();

        RebuildStaticWords();
    }

    private static IEnumerable<string> SplitList(string? value)
    {
        return string.IsNullOrEmpty(value)
            ? []
            : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
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
