/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;
using System.Text.RegularExpressions;

/// <summary>
/// Decides what a statement is and where it has to run, plus the connection-string edits
/// those decisions need. Nothing here writes to the console or holds a connection, so the
/// line-editor shell and the full-screen mode classify a statement the same way.
///
/// Program.cs pulls these in with <c>using static SqlKind;</c>, so its call sites read
/// exactly as they did when these were local functions.
/// </summary>
internal static class SqlKind
{
    // Recognizes `use <database>`, with the name bare, `backticked`, or "quoted". A backtick-quoted
    // name is how a database whose name is a keyword or carries spaces is written, and doubled
    // backticks inside it are a literal one.
    internal static bool IsUseDatabase(string sql, out string database)
    {
        Match match = Regex.Match(
            sql.Trim(),
            """^use\s+(?:`(?<name>(?:``|[^`])+)`|"(?<name>[^"]+)"|(?<name>[^\s;`"]+))\s*;?$""",
            RegexOptions.IgnoreCase);
    
        database = match.Success ? match.Groups["name"].Value.Replace("``", "`") : string.Empty;
        return match.Success && database.Length > 0;
    }

    internal static IEnumerable<(string Sql, bool Vertical)> EscapeStringIntoLines(string input)
    {
        SqlScanner scanner = new();
    
        foreach (SqlStatement statement in scanner.Feed(input))
            yield return (statement.Sql, statement.Vertical);
    
        foreach (SqlStatement statement in scanner.Flush())
            yield return (statement.Sql, statement.Vertical);
    }

    internal static char FoldSmartQuote(char c) => c switch
    {
        // Single curly quotes and low-9 variants -> '
        '‘' or '’' or '‚' or '‛' => '\'',
        // Double curly quotes and low-9 variants -> "
        '“' or '”' or '„' or '‟' => '"',
        _ => c,
    };

    internal static string NormalizeSmartQuotes(string input)
    {
        StringBuilder? sb = null;
        for (int i = 0; i < input.Length; i++)
        {
            char folded = FoldSmartQuote(input[i]);
            if (folded == input[i])
            {
                sb?.Append(input[i]);
                continue;
            }
    
            sb ??= new StringBuilder(input, 0, i, input.Length);
            sb.Append(folded);
        }
    
        return sb?.ToString() ?? input;
    }

    // True for the shell's `backup …` family, with everything after the verb handed back as arguments.
    internal static bool IsBackupCommand(string sql, out string arguments)
    {
        arguments = "";
    
        string trimmedSql = sql.Trim().TrimEnd(';').Trim();
    
        if (!trimmedSql.StartsWith("backup", StringComparison.InvariantCultureIgnoreCase))
            return false;
    
        // Only the bare verb or the verb followed by a separator — never a table named `backups`.
        if (trimmedSql.Length > 6 && !char.IsWhiteSpace(trimmedSql[6]))
            return false;
    
        arguments = trimmedSql.Length > 6 ? trimmedSql[6..].Trim() : "";
        return true;
    }

    internal static bool IsBeginTx(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("begin", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("start", StringComparison.InvariantCultureIgnoreCase);
    }

    internal static bool IsCommitTx(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("commit", StringComparison.InvariantCultureIgnoreCase);
    }

    internal static bool IsRollbackTx(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("rollback", StringComparison.InvariantCultureIgnoreCase);
    }

    internal static bool IsQueryable(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("select ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("explain ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("analyze ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("show ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("desc ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("describe ", StringComparison.InvariantCultureIgnoreCase);
    }

    // Collapses a statement onto one line for display, clipping anything past `max` characters.
    internal static string Abbreviate(string sql, int max = 90)
    {
        string oneLine = Regex.Replace(sql.Trim(), @"\s+", " ");
    
        return oneLine.Length <= max ? oneLine : oneLine[..(max - 1)] + "…";
    }

    internal static bool HasDatabase(string connectionString)
    {
        return connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries))
            .Where(p => p.Length == 2 && string.Equals(p[0], "Database", StringComparison.OrdinalIgnoreCase))
            .Any(p => !string.IsNullOrWhiteSpace(p[1]));
    }

    internal static bool IsSystemLevelQuery(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("show databases", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("show orphan databases", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("show branches from ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("show ancestors from ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("show grants", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("show engine stats", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("show variables", StringComparison.InvariantCultureIgnoreCase) ||
               // The cluster-wide settings overlay: like SHOW VARIABLES it is read from no database, so
               // it runs on the endpoint connection and answers in a session that never ran `use`.
               StartsWithWords(trimmedSql, "show", "cluster", "settings");
    }

    // User and grant administration is server-level: like database DDL, these statements name their
    // target inside the SQL, touch only the shared auth catalog, and return no database descriptor —
    // so they run on a database-less connection and need no current database.
    internal static bool IsUserAdmin(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("create user ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("alter user ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop user ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("grant ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("revoke ", StringComparison.InvariantCultureIgnoreCase);
    }

    // Everything the server dispatches before opening a database — database lifecycle DDL, user and
    // grant administration, plus runtime cluster settings.
    internal static bool IsServerLevelDDL(string sql)
    {
        return IsDatabaseDDL(sql) || IsUserAdmin(sql) || IsClusterSettingsAdmin(sql);
    }

    // Runtime cluster settings are server-level for the same reasons user administration is: the key is
    // named inside the SQL, the change lands in the shared settings keyspace (replicated through the
    // settings log in cluster mode), and the statement returns no database descriptor — so it runs on a
    // database-less connection and needs no current database.
    //
    // CLUSTER and SETTING are plain identifiers to the parser rather than keywords, so the whole
    // three-word prefix is matched here instead of a single verb: SET alone is also how SET TRANSACTION
    // starts, and that one is database-scoped.
    internal static bool IsClusterSettingsAdmin(string sql)
    {
        return StartsWithWords(sql, "set", "cluster", "setting") ||
               StartsWithWords(sql, "reset", "cluster", "setting");
    }

    // True when `sql` opens with exactly these words in order, separated by any run of whitespace. The
    // prefixes it matches are three words long and typed by hand at a prompt, where the single-space
    // StartsWith spelling used for the older statements would quietly miss `SET  CLUSTER SETTING` and
    // send it down the needs-a-database path. Each word must end at a boundary, so `SET` does not match
    // the head of `SETTINGS`, and the last word may end the statement (`SHOW CLUSTER SETTINGS`) or be
    // followed by more of it (the key after `SETTING`).
    internal static bool StartsWithWords(string sql, params string[] words)
    {
        string trimmedSql = sql.TrimStart();
        int position = 0;
    
        foreach (string word in words)
        {
            if (string.Compare(trimmedSql, position, word, 0, word.Length, StringComparison.InvariantCultureIgnoreCase) != 0)
                return false;
    
            position += word.Length;
    
            if (position < trimmedSql.Length && !char.IsWhiteSpace(trimmedSql[position]) && trimmedSql[position] != ';')
                return false;
    
            while (position < trimmedSql.Length && char.IsWhiteSpace(trimmedSql[position]))
                position++;
        }
    
        return true;
    }

    // Statements the server dispatches before opening a database (StatementScope.IsDatabaseScopedMutation):
    // they name their target inside the SQL, so they must run on a database-less connection rather than
    // being rejected for having no current database. A rename has two accepted spellings —
    // RENAME DATABASE a TO b and ALTER DATABASE a RENAME TO b — and only COMMENT ON DATABASE is
    // database-scoped; COMMENT ON TABLE/COLUMN still needs a current database.
    internal static bool IsDatabaseDDL(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("create database ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop database ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("rename database ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("alter database ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("comment on database ", StringComparison.InvariantCultureIgnoreCase);
    }

    // Index DDL: what it changes is the set of index names on one table, not the set of relations.
    // The completion cache tells the two apart, because an index list is fetched per table and a
    // CREATE INDEX must not cost a reload of every relation name in the database.
    internal static bool ChangesIndexSet(string sql)
    {
        string trimmedSql = sql.TrimStart();

        return trimmedSql.StartsWith("create index ", StringComparison.InvariantCultureIgnoreCase) ||
               // CREATE UNIQUE INDEX is the same statement with a constraint on the key, and it is
               // spelled out because UNIQUE sits between the verb and the noun.
               trimmedSql.StartsWith("create unique index ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop index ", StringComparison.InvariantCultureIgnoreCase) ||
               // ALTER TABLE also adds, drops and renames keys, and a rename moves a name the
               // completion cache holds. The whole verb is taken rather than each of its forms.
               trimmedSql.StartsWith("alter table ", StringComparison.InvariantCultureIgnoreCase);
    }

    internal static bool ChangesTableSet(string sql)
    {
        string trimmedSql = sql.TrimStart();
    
        return trimmedSql.StartsWith("create table ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop table ", StringComparison.InvariantCultureIgnoreCase) ||
               IsViewDDL(trimmedSql);
    }

    // View and materialized-view DDL. CREATE OR REPLACE and ALTER … RENAME change what a name
    // resolves to, so all of these also trigger a completion-cache refresh via ChangesTableSet.
    internal static bool IsViewDDL(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return trimmedSql.StartsWith("create view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("create or replace view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("create materialized view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("create or replace materialized view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop materialized view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("alter view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("alter materialized view ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("refresh materialized view ", StringComparison.InvariantCultureIgnoreCase);
    }

    internal static bool IsDDL(string sql)
    {
        string trimmedSql = sql.Trim();
    
        return IsServerLevelDDL(trimmedSql) ||
               IsViewDDL(trimmedSql) ||
               trimmedSql.StartsWith("create table ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("create index ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop table ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("drop index ", StringComparison.InvariantCultureIgnoreCase) ||
               // The server dispatches TRUNCATE as DDL: it commits a replicated schema change that moves
               // the table's contents generation. One prefix covers both spellings, because the TABLE
               // keyword of TRUNCATE TABLE <table> is optional.
               trimmedSql.StartsWith("truncate ", StringComparison.InvariantCultureIgnoreCase) ||
               trimmedSql.StartsWith("alter table ", StringComparison.InvariantCultureIgnoreCase);
    }

    // Appends an empty Database= key when the connection string doesn't already carry one.
    internal static string EnsureDatabase(string connectionString)
    {
        return HasKey(connectionString, "Database")
            ? connectionString
            : connectionString.TrimEnd(';') + ";Database=";
    }

    // Returns the connection string with the given transport pinned via Protocol=, replacing any
    // existing Protocol= key.
    internal static string WithProtocol(string connectionString, string protocol)
    {
        return WithKey(connectionString, "Protocol", protocol);
    }

    // Returns the connection string with key=value set, replacing any existing occurrence of the key.
    internal static string WithKey(string connectionString, string key, string value)
    {
        List<string> parts = connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(p => !p.StartsWith(key + "=", StringComparison.OrdinalIgnoreCase))
            .ToList();
    
        parts.Add($"{key}={value}");
        return string.Join(';', parts);
    }

    internal static bool HasKey(string connectionString, string key)
    {
        return connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Any(p => p.StartsWith(key + "=", StringComparison.OrdinalIgnoreCase));
    }

    // Returns the value of a connection-string key, or null when the key is absent.
    internal static string? GetConnValue(string connectionString, string key)
    {
        return connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries))
            .Where(p => p.Length == 2 && string.Equals(p[0], key, StringComparison.OrdinalIgnoreCase))
            .Select(p => p[1])
            .FirstOrDefault();
    }

    // Produces a connection string for the same endpoint/transport but with no database selected,
    // used to reach system-level commands. Protocol= and Endpoint= are preserved so the transport
    // and port stay consistent with the live connection.
    internal static string GetEndpointConnectionString(string connectionString)
    {
        return SwapDatabase(connectionString, "");
    }

    internal static string SwapDatabase(string connectionString, string newDatabase)
    {
        List<string> parts = connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(p => !p.StartsWith("Database=", StringComparison.OrdinalIgnoreCase))
            .ToList();
    
        parts.Add($"Database={newDatabase}");
        return string.Join(';', parts);
    }
}
