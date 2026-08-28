
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDb.SqlSh.Tui;
using static SqlKind;
using CamusDB.Client;
using CommandLine;
using RadLine;
using Spectre.Console;
using System.Diagnostics;
using System.Globalization;
using System.Reflection;
using System.Text.RegularExpressions;
using System.Text;
using System.Text.Json;

string? informationalVersion = Assembly.GetExecutingAssembly()
    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
    ?.InformationalVersion;

string version = informationalVersion?.Split('+')[0]
    ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString(3)
    ?? "unknown";

Console.WriteLine($"CamusDB SQL Shell {version} (alpha)\n");

int workloadIdx = Array.FindIndex(args, a => string.Equals(a, "workload", StringComparison.OrdinalIgnoreCase));
if (workloadIdx >= 0)
{
    // Collect flags that appeared before "workload" and append them after
    string[] beforeWorkload = args[..workloadIdx];
    string[] afterWorkload = args[(workloadIdx + 1)..];
    await WorkloadCommand.RunAsync([.. afterWorkload, .. beforeWorkload]);
    return;
}

if (args.Length > 0 && (args[0] == "--help" || args[0] == "-h"))
{
    PrintHelp();
    return;
}

// Consumed here so CommandLineParser doesn't reject them as unknown options.
bool tuiMode = ConsumeFlag(ref args, "--tui");
bool debugKeys = ConsumeFlag(ref args, "--debug-keys");
bool diagnoseTerminal = ConsumeFlag(ref args, "--diagnose-terminal");
bool forceRich = ConsumeFlag(ref args, "--force-rich")
    || IsTruthy(Environment.GetEnvironmentVariable("CAMUS_FORCE_RICH"));

// Some capable terminals (e.g. Rio) advertise a TERM value that Spectre.Console's ANSI
// detector doesn't recognize, so it disables the rich editor even though the terminal
// handles ANSI fine. Forcing the capabilities lets the whole app render richly.
if (forceRich)
{
    AnsiConsole.Console = AnsiConsole.Create(new AnsiConsoleSettings
    {
        Ansi = AnsiSupport.Yes,
        Interactive = InteractionSupport.Yes,
        ColorSystem = ColorSystemSupport.Detect,
    });
}

ParserResult<Options> optsResult = Parser.Default.ParseArguments<Options>(NormalizeBuiltInOptions(args));

Options? opts = optsResult.Value;
if (opts is null)
    return;

// -e and -f both run SQL and exit; the shell never prompts in that case, so it also keeps stdout
// clean of the connection banner. "-f -" reads the script from standard input, so a heredoc or a
// pipe works the same as a file on disk.
bool readScriptFromStdin = opts.File == "-";
bool nonInteractive = !string.IsNullOrWhiteSpace(opts.Execute) || !string.IsNullOrWhiteSpace(opts.File);

// Checked before connecting: a typo in the path shouldn't cost a round trip or look like a
// server problem.
if (!string.IsNullOrWhiteSpace(opts.File) && !readScriptFromStdin && !File.Exists(opts.File))
{
    AnsiConsole.MarkupLine("[red]File not found: {0}[/]", Markup.Escape(opts.File));
    Environment.ExitCode = 1;
    return;
}

if (diagnoseTerminal)
{
    Capabilities caps = AnsiConsole.Console.Profile.Capabilities;
    bool supported = LineEditor.IsSupported(AnsiConsole.Console);
    AnsiConsole.MarkupLine(supported
        ? "[green]Rich editor supported.[/] Terminal capabilities:"
        : "[yellow]Rich editor disabled[/] (falling back to plain prompt). Terminal capabilities:");
    AnsiConsole.MarkupLine("  IsTerminal  : {0}", AnsiConsole.Console.Profile.Out.IsTerminal);
    AnsiConsole.MarkupLine("  Ansi        : {0}", caps.Ansi);
    AnsiConsole.MarkupLine("  Interactive : {0}", caps.Interactive);
    AnsiConsole.MarkupLine("  TERM        : {0}", Markup.Escape(Environment.GetEnvironmentVariable("TERM") ?? "(unset)"));
    AnsiConsole.MarkupLine("  NO_COLOR    : {0}", Markup.Escape(Environment.GetEnvironmentVariable("NO_COLOR") ?? "(unset)"));
    AnsiConsole.MarkupLine("  ForceRich   : {0}", forceRich);

    if (!supported)
    {
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[grey]If your terminal supports ANSI (e.g. Rio), force it with[/] [cyan]--force-rich[/] [grey]or[/] [cyan]CAMUS_FORCE_RICH=1[/][grey],[/]");
        AnsiConsole.MarkupLine("[grey]or set[/] [cyan]TERM=xterm-256color[/][grey].[/]");
    }

    return;
}

if (debugKeys)
{
    Console.Write("\u001b[?2004h"); // enable bracketed paste, like the real editor does
    Console.WriteLine("Key diagnostic. Paste your multi-line SQL now. Press Ctrl+D to finish.\n");
    int n = 0;
    while (true)
    {
        ConsoleKeyInfo k = Console.ReadKey(intercept: true);
        if (k.Key == ConsoleKey.D && (k.Modifiers & ConsoleModifiers.Control) != 0)
            break;
        Console.WriteLine($"#{n++,-3} Key={k.Key}({(int)k.Key})  Char=0x{(int)k.KeyChar:X2}  Mods={k.Modifiers}");
    }
    Console.Write("\u001b[?2004l");
    Console.WriteLine("\nDone. Copy everything above and share it.");
    return;
}

string historyPath = Path.GetTempPath() + Path.PathSeparator + "camusdb.history.json";

List<string>? history = await GetHistory(historyPath);

// Credentials come from the flags, then the environment. A user given without a password is
// prompted for one (silently skipped when stdin is piped, so scripts aren't left hanging on a
// prompt nobody can answer). The password is exchanged for a short-lived token by the driver on
// the first statement, so it never travels with the SQL.
string? authUser = opts.User ?? Environment.GetEnvironmentVariable("CAMUS_USER");
string? authPassword = opts.Password ?? Environment.GetEnvironmentVariable("CAMUS_PASSWORD");
string? authToken = opts.AccessToken ?? Environment.GetEnvironmentVariable("CAMUS_ACCESS_TOKEN");

if (!string.IsNullOrEmpty(authUser) && string.IsNullOrEmpty(authPassword) && string.IsNullOrEmpty(authToken))
    authPassword = PromptPassword();

List<string> connectionAttempts = BuildConnectionAttempts(opts, authUser, authPassword, authToken);

CamusConnection connection;
string activeConnectionString;

try
{
    (connection, activeConnectionString) = await ConnectionHelper.OpenFirstAsync(connectionAttempts);
}
catch (Exception ex)
{
    WriteConnectionError(ex);
    Environment.ExitCode = 1;
    return;
}

// Tell the user where and how they connected, unless they're scripting with -e/-f (keep stdout clean).
if (!nonInteractive)
{
    string server = GetConnValue(activeConnectionString, "Endpoint") ?? "unknown server";
    string database = GetConnValue(activeConnectionString, "Database") is { Length: > 0 } db
        ? $"[white]{Markup.Escape(db)}[/]"
        : "[grey58](none)[/]";

    // Never echo the credential itself — only who the shell is acting as.
    string identity = GetConnValue(activeConnectionString, "User") is { Length: > 0 } who
        ? $"[grey58], user:[/] [white]{Markup.Escape(who)}[/]"
        : HasKey(activeConnectionString, "AccessToken")
            ? "[grey58], authenticated with a token[/]"
            : "";

    AnsiConsole.MarkupLine(
        $"[grey58]Connected to[/] [white]{Markup.Escape(server)}[/] [grey58]over[/] " +
        $"[white]{DescribeTransport(activeConnectionString)}[/][grey58], database:[/] {database}{identity}\n");
}

LineEditor? editor = null;
CamusTransaction? transaction = null;
SqlCompletion? sqlCompletion = null;

// The TUI colors its editor pane with the same word list as the line editor, so the
// highlighter built below is kept where both can reach it.
WordHighlighter? sharedHighlighter = null;

// Non-interactive mode: run the supplied SQL, then exit. A failure here has no prompt to return
// to, so report it the way the interactive loop does — a diagnosable line, not a stack trace —
// and exit non-zero so a caller can tell the statement didn't run.
if (nonInteractive)
{
    // The statement that was running when things failed, so the error can point at it rather than
    // at the whole file, and where in the script it came from.
    string? failedSql = null;
    string? failedAt = null;

    try
    {
        // A script runs before -e, so `-f schema.sql -e "select ..."` reads back what the script wrote.
        if (!string.IsNullOrWhiteSpace(opts.File))
        {
            string origin = readScriptFromStdin ? "<stdin>" : opts.File;
            TextReader scriptReader = readScriptFromStdin ? Console.In : File.OpenText(opts.File);

            try
            {
                // Statements run one at a time, as the reader produces them (rather than slurping the
                // file and handing it to ExecuteSql) so a multi-gigabyte dump costs one statement of
                // memory, and so a failure can name the offending statement and stop the script there,
                // leaving the rest unrun instead of pressing on against a half-applied schema.
                await foreach (SqlStatement statement in ReadStatements(scriptReader))
                {
                    failedSql = statement.Sql;
                    failedAt = $"{origin}:{statement.Line}";
                    await ExecuteStatement(statement.Sql, statement.Vertical);
                }
            }
            finally
            {
                // Console.In outlives this block — only a file handle is ours to close.
                if (!readScriptFromStdin)
                    scriptReader.Dispose();
            }

            failedSql = null;
            failedAt = null;
        }

        if (!string.IsNullOrWhiteSpace(opts.Execute))
        {
            failedSql = opts.Execute;
            await ExecuteSql(opts.Execute);
        }
    }
    catch (Exception ex)
    {
        if (failedAt is not null)
            AnsiConsole.MarkupLine("[red]{0}[/]", Markup.Escape(failedAt));

        WriteStatementError(ex, failedSql);
        Environment.ExitCode = 1;
    }

    return;
}

bool richEditorSupported = LineEditor.IsSupported(AnsiConsole.Console);

if (richEditorSupported)
{
    string[] keywords = [
        "select",
        "update",
        "from",
        "where",
        "order",
        "by",
        "asc",
        "desc",
        "describe",
        "database",
        "table",
        "set",
        "create",
        "if",
        "exists",
        "default",
        "primary",
        "key",
        "index",
        "indexes",
        "constraint",
        "check",
        "limit",
        "insert",
        "into",
        "values",
        "delete",
        "alter",
        "rename",
        "column",
        "drop",
        // TRUNCATE [TABLE] <table>: empties a base table by moving its contents generation. The
        // TABLE noise word is optional, so the verb is what the editor colors.
        "truncate",
        "force",
        "relink",
        "orphan",
        "include",
        "null",
        "not",
        "string",
        "char",
        "varchar",
        "text",
        "int",
        "int64",
        "smallint",
        "float32",
        "float64",
        "real",
        "object_id",
        "oid",
        "bool",
        "boolean",
        "bytes",
        "blob",
        "date",
        "datetime",
        "timestamp",
        "uuid",
        "guid",
        "array",
        "is",
        "on",
        "in",
        "or",
        "and",
        "between",
        "like",
        "ilike",
        "add",
        "show",
        "use",
        "tables",
        "columns",
        "group",
        "join",
        "inner",
        "offset",
        "unique",
        "having",
        "explain",
        "analyze",
        "begin",
        "start",
        "transaction",
        "commit",
        "rollback",
        "evict",
        "cache",
        "comment",
        "as",
        // AS OF SYSTEM TIME is lexed as one token by the server, but the editor highlights
        // token by token, so each word is listed on its own.
        "of",
        "system",
        "time",
        "distinct",
        "cast",
        "case",
        "when",
        "then",
        "else",
        "end",
        "integer",
        "double",
        "float",
        "databases",
        "to",
        "branch",
        "branches",
        "ancestors",
        "isolation",
        "level",
        "read",
        "committed",
        "serializable",
        "only",
        "write",
        "locking",
        "optimistic",
        "pessimistic",
        "user",
        "identified",
        "with",
        "grant",
        "grants",
        "revoke",
        "privileges",
        "all",
        "for",
        "sha256_password",
        "engine",
        "stats",
        "variables",
        // SHOW STATISTICS FOR [TABLE] <table>: the optimizer's advisory table statistics. The server
        // matches STATISTICS as a plain identifier so it stays usable as a name, but the editor
        // colors it like the rest of the SHOW vocabulary.
        "statistics",
        // SHOW RANGES FROM TABLE|INDEX and SHOW RANGE … FOR ROW (…): where a relation's key space is
        // divided, and which span holds one row. Like STATISTICS, all three words are plain
        // identifiers to the parser and stay usable as table and column names; the editor colors
        // them because they read as part of the SHOW vocabulary.
        "ranges",
        "range",
        "row",
        // SET/RESET CLUSTER SETTING and SHOW CLUSTER SETTINGS. Both spellings of the last word are
        // listed because the statements differ: SETTING for the mutations, SETTINGS for the listing.
        "cluster",
        "setting",
        "settings",
        "reset",
        "view",
        "views",
        "materialized",
        "refresh",
        "cascade",
        "option",
        "local",
        "cascaded",
        "no",
        "data",
        "concurrently",
        "owner",
    ];

    string[] functions = [
        "count",
        "max",
        "min",
        "avg",
        "sum",
        "gen_id",
        "gen_uuid_v4",
        "gen_uuid_v7",
        "current_timestamp",
        "now",
        // Session functions: what session the statement runs in. They take no arguments and are
        // rejected as column defaults and CHECK conditions, which replay with no session behind them.
        "current_database",
        "current_user",
        "current_role",
        "is_superuser",
        "current_date",
        "date_add",
        "date_diff",
        "date_part",
        "date_trunc",
        "unix_timestamp",
        "from_unixtime",
        "abs",
        "ceil",
        "ceiling",
        "floor",
        "sqrt",
        "pow",
        "power",
        "mod",
        "sign",
        "random",
        "round",
        "length",
        "lower",
        "upper",
        "trim",
        "ltrim",
        "rtrim",
        "substring",
        "replace",
        "regexp_like",
        "regexp_match",
        "regexp_matches",
        "regexp_replace",
        "regexp_substr",
        "regexp_instr",
        "regexp_count",
        "regexp_split_to_array",
        "regexp_split_to_table",
        "contains",
        "starts_with",
        "ends_with",
        "concat",
        "json_valid",
        "json_type",
        "json_extract",
        "json_value",
        "json_contains",
        "json_array_length",
        "coalesce",
        "ifnull",
        "nvl",
        "to_string",
        "to_int64",
        "to_float64",
        "to_float32",
        "to_bool",
        "to_bytes",
        "to_date",
        "to_datetime",
        "to_id",
        "str_id",
        // Vector measurement and distance functions. A vector is a bytes value that holds packed
        // little-endian float32 elements, so these read a bytes column, not a dedicated type.
        // octet_length also accepts a string, where it counts UTF-8 bytes, not characters.
        "octet_length",
        "vector_dims",
        "l2_distance",
        "inner_product",
        "cosine_distance",
    ];

    string[] commands = [
        "clear",
        "source",
        "use",
        "exit",
        "quit",
        "workload",
        "init",
        "run",
        "backup",
    ];

    string[] constants = [
        "true",
        "false",
    ];

    string[] regexes = [
        @"(?<number>\b\d+(\.\d+)?\b)",
        @"(?<singlequote>'(?:\\'|[^'])*')",
        @"(?<escapedquote>`(?:\\`|[^`])*`)",
        "(?<doublequote>\"(?:\\\\\"|[^\"])*\")"
    ];

    string[] commentRegexes = [
        @"(?<linecomment>--.*$)",
        @"(?<blockcomment>/\*[\s\S]*?(\*/|$))"
    ];

    WordHighlighter worldHighlighter = new();
    sharedHighlighter = worldHighlighter;

    Style funcStyle = new(foreground: Color.Lime);
    Style keywordStyle = new(foreground: Color.Blue);
    Style commandStyle = new(foreground: Color.LightSkyBlue1);
    Style constantsStyle = new(foreground: Color.LightPink3);
    Style commentStyle = new(foreground: Color.Grey58);

    foreach (string keyword in keywords)
        worldHighlighter.AddWord(keyword, keywordStyle);

    foreach (string func in functions)
        worldHighlighter.AddWord(func, funcStyle);

    foreach (string command in commands)
        worldHighlighter.AddWord(command, commandStyle);

    foreach (string constant in constants)
        worldHighlighter.AddWord(constant, constantsStyle);

    foreach (string commentRegex in commentRegexes)
        worldHighlighter.AddRegex(commentRegex, commentStyle);

    foreach (string regex in regexes)
        worldHighlighter.AddRegex(regex, constantsStyle);

    sqlCompletion = new SqlCompletion([.. keywords, .. functions, .. commands, .. constants]);

    editor = new()
    {
        MultiLine = true,
        Text = "",
        Prompt = new MyLineNumberPrompt(new Style(foreground: Color.PaleTurquoise1)),
        Completion = sqlCompletion,
        Highlighter = worldHighlighter
    };

    // Enter submits the statement. To insert a new line without submitting, RadLine binds
    // Shift+Enter by default, but most terminals send a plain Enter for that combo and never
    // deliver the Shift modifier. Add alternatives that terminals can actually distinguish:
    //   - Alt/Option+Enter: natural, works in iTerm2 and any terminal with Option-as-Meta.
    //   - Ctrl+O: a real control character (0x0F), so it works in every terminal as a fallback.
    editor.KeyBindings.Add<NewLineCommand>(ConsoleKey.Enter, ConsoleModifiers.Alt);
    editor.KeyBindings.Add<NewLineCommand>(ConsoleKey.O, ConsoleModifiers.Control);

    // A pasted newline arrives as a LineFeed (0x0A), which .NET reports as Enter+Control
    // (real Enter is a CarriageReturn with no modifiers). Binding Enter+Control to a new line
    // keeps multi-line pastes intact instead of silently collapsing them onto one line.
    editor.KeyBindings.Add<NewLineCommand>(ConsoleKey.Enter, ConsoleModifiers.Control);

    if (history != null)
    {
        foreach (string item in history)
            editor.History.Add(item);
    }
}

Console.CancelKeyPress += delegate
{
    AnsiConsole.MarkupLine("[cyan]\nExiting...[/]");

    if (transaction is not null)
    {        
        AnsiConsole.MarkupLine("[yellow]Rolling back active transaction...[/]");

        ExecuteRollbackTx(connection).Wait();
    }
    
    SaveHistory(historyPath, history).Wait();
};

if (sqlCompletion is not null && HasDatabase(activeConnectionString))
    await sqlCompletion.RefreshTablesAsync(connection);

// Configuration keys come over the endpoint connection rather than the database one, because the
// statements they complete need no database either — the keys are offered even in a session that
// never runs `use`. Best-effort: completion falling back to the static vocabulary is not worth
// keeping the user from a prompt over.
if (sqlCompletion is not null)
{
    try
    {
        CamusConnection settingsConnection = await ConnectionHelper.OpenAsync(GetEndpointConnectionString(activeConnectionString));
        await sqlCompletion.RefreshSettingsAsync(settingsConnection);
    }
    catch (Exception)
    {
        // Ignored: the shell is already connected, and this cache is a convenience.
    }
}

// Full-screen mode takes over the terminal, so it runs instead of the REPL, never beside it.
if (tuiMode)
{
    if (!richEditorSupported)
    {
        AnsiConsole.MarkupLine("[red]--tui needs an ANSI terminal.[/] Try [cyan]--force-rich[/] or set [cyan]TERM=xterm-256color[/].");
        Environment.ExitCode = 1;
        return;
    }

    await CamusTui.RunAsync(
        connection,
        activeConnectionString,
        FormatValue,
        sharedHighlighter!,
        sqlCompletion,
        Path.Combine(Path.GetTempPath(), "camusdb.query.sql"));

    return;
}

StringBuilder pendingSql = new();

// The last statement handed to the server, for `show prepared` to report on.
string? lastExecutedSql = null;

// Deliver Ctrl+C to the line editor as input instead of killing the process, so the editor can
// clear a non-empty prompt on the first press and only exit when the prompt is already empty.
if (editor is not null)
    Console.TreatControlCAsInput = true;

while (true)
{
    string? lastSql = null;
    try
    {
        string? sql;

        if (editor is not null)
            sql = await editor.ReadLine(CancellationToken.None);
        else
            sql = AnsiConsole.Prompt(new TextPrompt<string>("camus> ").AllowEmpty());

        // The editor returns null when Ctrl+C is pressed on an empty prompt.
        if (sql is null)
        {
            // If a multi-statement is being accumulated, clear that first instead of exiting.
            if (pendingSql.Length > 0)
            {
                pendingSql.Clear();
                continue;
            }

            AnsiConsole.MarkupLine("[cyan]\nExiting...[/]");

            if (transaction is not null)
            {
                AnsiConsole.MarkupLine("[yellow]Rolling back active transaction...[/]");
                await ExecuteRollbackTx(connection);
            }

            await SaveHistory(historyPath, history);
            break;
        }

        if (string.IsNullOrWhiteSpace(sql))
            continue;

        // Pasted SQL often carries "smart" curly quotes (from editors/chat apps) that the parser
        // doesn't recognize as string delimiters. Fold them back to straight quotes so the
        // statement both parses server-side and is seen as complete here.
        sql = NormalizeSmartQuotes(sql);

        string sqlTrim = sql.Trim();

        if (pendingSql.Length == 0 &&
            (string.Equals(sqlTrim, "exit", StringComparison.InvariantCultureIgnoreCase)
            || string.Equals(sqlTrim, "quit", StringComparison.InvariantCultureIgnoreCase)))
        {
            if (transaction is not null)
            {
                AnsiConsole.MarkupLine("[red]There's an active transaction, please commit or rollback before exit[/]");
                continue;
            }

            await SaveHistory(historyPath, history);
            break;
        }

        if (pendingSql.Length == 0 && string.Equals(sqlTrim, "clear", StringComparison.InvariantCultureIgnoreCase))
        {
            AnsiConsole.Clear();
            continue;
        }

        // Handled here rather than sent on: preparation is a client-side decision, so only the driver
        // knows the answer. Takes `show prepared` before the server can see it as a SHOW statement.
        if (pendingSql.Length == 0 && IsShowPrepared(sqlTrim, out string? preparedArg))
        {
            ShowPrepared(activeConnectionString, preparedArg ?? lastExecutedSql, explicitSql: preparedArg is not null);
            continue;
        }

        if (pendingSql.Length == 0 && sqlTrim.StartsWith("source ", StringComparison.InvariantCultureIgnoreCase))
        {
            await LoadSource(sqlTrim[7..].Trim());
            continue;
        }

        // Taken here rather than left to run as a statement so a refused switch (mid-transaction)
        // is a message, not an error that discards the transaction the way a server-side failure does.
        if (pendingSql.Length == 0 && sqlTrim.StartsWith("use ", StringComparison.InvariantCultureIgnoreCase))
        {
            if (!IsUseDatabase(sqlTrim, out string newDb))
            {
                AnsiConsole.MarkupLine("[red]Usage: use <database>[/]");
                continue;
            }

            if (transaction is not null)
            {
                AnsiConsole.MarkupLine("[red]There's an active transaction, please commit or rollback before switching databases[/]");
                continue;
            }

            await SwitchDatabase(newDb);
            continue;
        }

        string executableSql = pendingSql.Length == 0
            ? sql
            : $"{pendingSql}{Environment.NewLine}{sql}";

        if (IsSqlIncomplete(executableSql))
        {
            pendingSql.Clear();
            pendingSql.Append(executableSql);
            continue;
        }

        pendingSql.Clear();
        lastSql = executableSql;
        lastExecutedSql = executableSql;

        // Add some history. A statement carrying a plaintext password (CREATE/ALTER USER … IDENTIFIED
        // BY '…') stays in the in-memory editor history — recalling it with Up is useful — but is kept
        // out of the on-disk history file, which outlives the session and is world-readable.
        if (editor is not null)
            editor.History.Add(executableSql);

        if (!CarriesPassword(executableSql))
            AddHistory(history, executableSql);

        await ExecuteSql(executableSql);

        if (sqlCompletion is not null && ChangesTableSet(executableSql) && HasDatabase(activeConnectionString))
            await sqlCompletion.RefreshTablesAsync(connection);
        else if (sqlCompletion is not null && ChangesIndexSet(executableSql))
            sqlCompletion.InvalidateIndexes();
    }
    catch (Exception ex)
    {
        WriteStatementError(ex, lastSql);

        // A failed statement aborts the transaction server-side, so the local handle is
        // now stale: any further command (including rollback) would fail with "Unknown
        // transaction". Discard it so the shell returns to a consistent, autocommit state.
        if (transaction is not null)
        {
            transaction = null;
            pendingSql.Clear();
            AnsiConsole.MarkupLine("[yellow]The active transaction was aborted and has been rolled back.[/]\n");
        }
    }
}

async Task LoadSource(string arguments)
{
    (string path, bool force) = ParseSourceArguments(arguments);

    if (string.IsNullOrWhiteSpace(path))
    {
        AnsiConsole.MarkupLine("[red]Usage: source <file> [[--force]][/]\n");
        return;
    }

    if (!File.Exists(path))
    {
        AnsiConsole.MarkupLine("[red]File not found: {0}[/]\n", Markup.Escape(path));
        return;
    }

    int executed = 0, failed = 0;

    using StreamReader reader = new(path);

    // Statements arrive as the file is read, so sourcing a dump larger than memory works and the
    // first statement runs immediately instead of after the whole file has been parsed.
    await foreach (SqlStatement statement in ReadStatements(reader))
    {
        try
        {
            await ExecuteStatement(statement.Sql, statement.Vertical);
            executed++;
        }
        catch (Exception ex)
        {
            failed++;

            // Point at the line the statement started on — in a file of thousands, "it failed" isn't
            // something you can act on.
            AnsiConsole.MarkupLine("[red]{0}:{1}[/]", Markup.Escape(path), statement.Line);

            // An open transaction is aborted server-side by the failure, so every remaining statement
            // would fail too. Stop regardless of --force and let the caller reset it.
            if (!force || transaction is not null)
                throw;

            WriteStatementError(ex, statement.Sql);
        }
    }

    if (force && failed > 0)
        AnsiConsole.MarkupLine("[yellow]{0} statement(s) failed, {1} succeeded.[/]\n", failed, executed);
}

// Streams statements out of a reader one line at a time. Feeding whole lines keeps the scanner's
// two-character lookahead (`--`, `/*`, `\G`, doubled quotes) inside a single call, since none of
// those tokens can straddle a line break.
async IAsyncEnumerable<SqlStatement> ReadStatements(TextReader reader)
{
    SqlScanner scanner = new();

    while (true)
    {
        string? line = await reader.ReadLineAsync();

        // Fold smart quotes per line so -f files and `source` benefit the way the prompt does.
        IEnumerable<SqlStatement> statements = line is null
            ? scanner.Flush()
            : scanner.Feed(NormalizeSmartQuotes(line) + "\n");

        foreach (SqlStatement statement in statements)
            yield return statement;

        if (line is null)
            yield break;
    }
}

// `source <file> [--force]` — --force keeps going after a failed statement instead of stopping
// there, the way mysql's own --force does.
static (string Path, bool Force) ParseSourceArguments(string arguments)
{
    bool force = false;
    List<string> parts = [];

    foreach (string token in arguments.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
    {
        if (string.Equals(token, "--force", StringComparison.InvariantCultureIgnoreCase))
        {
            force = true;
            continue;
        }

        parts.Add(token);
    }

    // Rejoin so an unquoted path with spaces still resolves; strip quotes if the user added them,
    // and the ';' that comes from typing the command the way every other statement is typed.
    return (string.Join(' ', parts).TrimEnd(';').Trim('"', '\''), force);
}

// Points the shell at another database by reopening the connection against it — there is no
// server-side session to tell, so the connection string is the only thing that changes.
async Task SwitchDatabase(string database)
{
    activeConnectionString = SwapDatabase(activeConnectionString, database);
    connection = await ConnectionHelper.OpenAsync(activeConnectionString);

    if (sqlCompletion is not null)
        await sqlCompletion.RefreshTablesAsync(connection);

    AnsiConsole.MarkupLine("Database changed to [cyan]{0}[/]\n", Markup.Escape(database));
}


async Task ExecuteSql(string input)
{
    // Also fold smart quotes here so the -e/--execute flag benefits, not just the interactive
    // prompt. Idempotent, so re-folding an already-normalized statement is a no-op.
    input = NormalizeSmartQuotes(input);

    foreach ((string sql, bool vertical) in EscapeStringIntoLines(input))
        await ExecuteStatement(sql, vertical);
}

async Task ExecuteStatement(string sql, bool vertical)
{
    if (string.IsNullOrWhiteSpace(sql))
        return;

    // USE is the shell's, not the server's — the database lives in the connection string. Handling it
    // here rather than only at the prompt means a script can switch databases mid-file, the way a
    // multi-database dump does, and it has to come before the no-database check below: a file that
    // opens with USE is exactly how a session with no database selected gets one.
    if (IsUseDatabase(sql, out string database))
    {
        // Refusing to switch would leave the rest of the file running against the wrong database,
        // so this stops the script instead of warning and carrying on.
        if (transaction is not null)
            throw new InvalidOperationException("There's an active transaction, please commit or rollback before switching databases");

        await SwitchDatabase(database);
        return;
    }

    // Backups are the shell's, not the server's: they have no SQL form at all — they are node-wide REST
    // admin calls that go out over HTTP even on a gRPC connection. Handled here rather than at the prompt
    // so a script file and -e get them too, and before the no-database check below because a backup
    // captures every database on the node and needs none of them selected.
    if (IsBackupCommand(sql, out string backupArguments))
    {
        await ExecuteBackup(backupArguments);
        return;
    }

    bool needsDb = !IsServerLevelDDL(sql) && !IsSystemLevelQuery(sql);
    if (needsDb && !HasDatabase(activeConnectionString))
    {
        AnsiConsole.MarkupLine("[red]No database selected.[/] Use [cyan]use <database>[/] to select one.\n");
        return;
    }

    if (IsSystemLevelQuery(sql))
    {
        string sysCs = GetEndpointConnectionString(activeConnectionString);
        CamusConnection sysConn = await ConnectionHelper.OpenAsync(sysCs);
        await ExecuteQuery(sysConn, sql, vertical);
    }
    else if (IsQueryable(sql))
        await ExecuteQuery(connection, sql, vertical);
    else if (IsServerLevelDDL(sql))
    {
        string sysCs = GetEndpointConnectionString(activeConnectionString);
        CamusConnection sysConn = await ConnectionHelper.OpenAsync(sysCs);
        await ExecuteDDL(sysConn, sql);
    }
    else if (IsDDL(sql))
        await ExecuteDDL(connection, sql);
    else if (IsBeginTx(sql))
        await ExecuteBeginTx(connection);
    else if (IsCommitTx(sql))
        await ExecuteCommitTx(connection);
    else if (IsRollbackTx(sql))
        await ExecuteRollbackTx(connection);
    else
        await ExecuteNonQuery(connection, sql);
}


// True while what's been typed so far can't stand on its own, so the prompt should keep reading.
// Shares the splitter's scanner: whether a ';' terminates a statement and whether a statement is
// finished are the same question, and answering them differently is how a ';' inside a comment or a
// `quoted identifier` ends up confusing the shell.
static bool IsSqlIncomplete(string input)
{
    if (string.IsNullOrWhiteSpace(input))
        return false;

    SqlScanner scanner = new();

    // Only what dangles after the last ';' matters here — statements it completed are already whole.
    foreach (SqlStatement _ in scanner.Feed(input))
    {
    }

    if (scanner.InsideQuotes || scanner.InsideBlockComment || scanner.ParenDepth > 0)
        return true;

    return scanner.HasPendingContent && scanner.LastContentChar == ',';
}

static async Task SaveHistory(string historyPath, List<string>? history)
{
    if (history is not null)
        await File.WriteAllTextAsync(historyPath, JsonSerializer.Serialize(history));
}

// True when the statement inlines a password, i.e. CREATE USER / ALTER USER … IDENTIFIED
// [WITH plugin] BY '…'.
static bool CarriesPassword(string sql)
{
    return Regex.IsMatch(sql, @"\bidentified\s+(with\s+\S+\s+)?by\b", RegexOptions.IgnoreCase);
}

static void AddHistory(List<string>? history, string sql)
{
    if (history is null)
        return;

    if (history.Count > 0 && string.Equals(history[^1], sql, StringComparison.Ordinal))
        return;

    history.Add(sql);
}

async Task ExecuteNonQuery(CamusConnection connection, string sql)
{
    await using CamusCommand cmd = connection.CreateCamusCommand(sql);

    cmd.CommandTimeout = 60;
    cmd.Transaction = transaction;

    Stopwatch stopwatch = Stopwatch.StartNew();

    int affected = await cmd.ExecuteNonQueryAsync();

    if (affected == 1)
        AnsiConsole.MarkupLine("Query OK, [blue]{0}[/] rows affected ({1})\n", affected, stopwatch.Elapsed);
    else if (affected > 1)
        AnsiConsole.MarkupLine("Query OK, [blue]{0}[/] rows affected ({1})\n", affected, stopwatch.Elapsed);
    else
        AnsiConsole.MarkupLine("Query OK, [yellow]{0}[/] rows affected ({1})\n", affected, stopwatch.Elapsed);
}

async Task ExecuteBeginTx(CamusConnection connection)
{
    if (transaction is not null)
    {
        AnsiConsole.MarkupLine("[red]There's an active transaction already[/]");
        return;
    }

    Stopwatch stopwatch = Stopwatch.StartNew();

    transaction = await connection.BeginTransactionAsync();

    AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", stopwatch.Elapsed);
}

async Task ExecuteCommitTx(CamusConnection connection)
{
    if (transaction is null)
    {
        AnsiConsole.MarkupLine("[red]There's no active transaction[/]");
        return;
    }

    Stopwatch stopwatch = Stopwatch.StartNew();

    try
    {
        await transaction.CommitAsync();
        AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", stopwatch.Elapsed);
    }
    finally
    {
        transaction = null;
    }
}

async Task ExecuteRollbackTx(CamusConnection connection)
{
    if (transaction is null)
    {
        AnsiConsole.MarkupLine("[red]There's no active transaction[/]");
        return;
    }

    Stopwatch stopwatch = Stopwatch.StartNew();

    try
    {
        await transaction.RollbackAsync();
        AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", stopwatch.Elapsed);
    }
    finally
    {
        transaction = null;
    }
}

async Task ExecuteQuery(CamusConnection connection, string sql, bool vertical = false)
{
    await using CamusCommand cmd = connection.CreateSelectCommand(sql);

    cmd.CommandTimeout = 60;
    cmd.Transaction = transaction;

    int rows = 0;
    Table? table = null;

    Stopwatch stopwatch = Stopwatch.StartNew();

    CamusDataReader reader = await cmd.ExecuteReaderAsync();

    TimeSpan duration = stopwatch.Elapsed;

    while (await reader.ReadAsync())
    {
        Dictionary<string, ColumnValue> current = ConnectionHelper.ReadCurrentRow(reader);

        if (vertical)
        {
            WriteVerticalRow(current, rows + 1);
            rows++;
            continue;
        }

        if (table is null)
        {
            table = new()
            {
                Border = TableBorder.Square
            };

            foreach (KeyValuePair<string, ColumnValue> item in current)
                table.AddColumn(item.Key);
        }

        string[] row = new string[current.Count];

        int i = 0;

        foreach (KeyValuePair<string, ColumnValue> item in current)
            row[i++] = FormatValue(item.Value);

        table.AddRow(row);
        rows++;
    }

    if (table is not null)
        AnsiConsole.Write(table);

    AnsiConsole.MarkupLine("[blue]{0}[/] rows in set ({1})\n", rows, duration);
}

// Runs one `backup …` subcommand against the node's backup admin API. A usage mistake is reported and
// swallowed the way `use` does it; a server-side failure is left to propagate, so it reaches
// WriteStatementError with its CADB code and aborts a script the way a failed statement does.
async Task ExecuteBackup(string arguments)
{
    string[] parts = arguments.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

    if (parts.Length == 0 || string.Equals(parts[0], "help", StringComparison.InvariantCultureIgnoreCase))
    {
        WriteBackupUsage();
        return;
    }

    string subcommand = parts[0].ToLowerInvariant();
    Stopwatch stopwatch = Stopwatch.StartNew();

    switch (subcommand)
    {
        case "full":
        case "coordinated":
        {
            if (parts.Length > 1)
            {
                AnsiConsole.MarkupLine("[red]Usage: backup {0}[/]\n", subcommand);
                return;
            }

            CamusBackupInfo info = subcommand == "full"
                ? await connection.Backups.TakeFullBackupAsync()
                : await connection.Backups.TakeCoordinatedBackupAsync();

            WriteBackupInfo(info, stopwatch.Elapsed);
            return;
        }

        case "incremental":
        {
            if (parts.Length != 2)
            {
                AnsiConsole.MarkupLine("[red]Usage: backup incremental <parent-backup-id>[/]\n");
                return;
            }

            CamusBackupInfo info = await connection.Backups.TakeIncrementalBackupAsync(TrimBackupId(parts[1]));

            WriteBackupInfo(info, stopwatch.Elapsed);
            return;
        }

        case "list":
        {
            if (parts.Length > 1)
            {
                AnsiConsole.MarkupLine("[red]Usage: backup list[/]\n");
                return;
            }

            IReadOnlyList<CamusBackupInfo> catalog = await connection.Backups.ListBackupsAsync();

            WriteBackupList(catalog, stopwatch.Elapsed);
            return;
        }

        case "chain":
        {
            if (parts.Length != 2)
            {
                AnsiConsole.MarkupLine("[red]Usage: backup chain <leaf-backup-id>[/]\n");
                return;
            }

            IReadOnlyList<CamusBackupInfo> chain = await connection.Backups.GetChainAsync(TrimBackupId(parts[1]));

            WriteBackupList(chain, stopwatch.Elapsed);
            WriteRecoverableWindow(chain);
            return;
        }

        case "gc":
        {
            bool preview = parts.Length == 2 &&
                (string.Equals(parts[1], "preview", StringComparison.InvariantCultureIgnoreCase) ||
                 string.Equals(parts[1], "--dry-run", StringComparison.InvariantCultureIgnoreCase));

            if (parts.Length > 1 && !preview)
            {
                AnsiConsole.MarkupLine("[red]Usage: backup gc [[preview]][/]\n");
                return;
            }

            CamusBackupGcResult result = preview
                ? await connection.Backups.PreviewGarbageCollectionAsync()
                : await connection.Backups.CollectGarbageAsync();

            WriteBackupGcResult(result, stopwatch.Elapsed);
            return;
        }

        default:
            AnsiConsole.MarkupLine("[red]Unknown backup command: {0}[/]\n", Markup.Escape(parts[0]));
            WriteBackupUsage();
            return;
    }
}


// Backup ids are GUIDs, but they're routinely copied out of a quoted context; accept those spellings
// rather than sending the quotes on to the server as part of the id.
static string TrimBackupId(string backupId) => backupId.Trim().Trim('\'', '"', '`');

static void WriteBackupUsage()
{
    AnsiConsole.MarkupLine("[white]Backup commands[/] [grey58](node-wide: every database on the server is captured)[/]");
    AnsiConsole.MarkupLine("  [cyan]backup full[/]                            take a full backup");
    AnsiConsole.MarkupLine("  [cyan]backup incremental <parent-backup-id>[/]  chain an incremental onto a backup");
    AnsiConsole.MarkupLine("  [cyan]backup coordinated[/]                     take a cluster-wide consistent backup");
    AnsiConsole.MarkupLine("  [cyan]backup list[/]                            list the node's backup catalog");
    AnsiConsole.MarkupLine("  [cyan]backup chain <leaf-backup-id>[/]          resolve and validate a restore chain");
    AnsiConsole.MarkupLine("  [cyan]backup gc preview[/]                      report what retention would reclaim");
    AnsiConsole.MarkupLine("  [cyan]backup gc[/]                              run retention now\n");
}

// One backup, reported field by field: a take is a single result, and the columns of a catalog listing
// would leave most of the width empty.
static void WriteBackupInfo(CamusBackupInfo info, TimeSpan elapsed)
{
    List<(string Name, string Value)> fields =
    [
        ("Backup Id", info.BackupId),
        ("Type", info.ActualKind is { Length: > 0 } kind ? kind : info.Type),
        ("Created (UTC)", FormatUtc(info.CreatedAtUtc)),
        ("Parent", info.ParentBackupId ?? "(none)"),
        ("Partitions", info.PartitionCount.ToString(CultureInfo.InvariantCulture)),
    ];

    if (info.ClusterId is { Length: > 0 } clusterId)
        fields.Add(("Cluster", clusterId));

    if (info.CoordinatorNode is { Length: > 0 } coordinator)
        fields.Add(("Coordinator", coordinator));

    // The coordinated cut's HLC — the point in time every partition was captured at. An embedded
    // single node reports an all-zero cut, which says nothing worth a line of its own.
    if (info.ClusterSnapshotPhysical is > 0)
        fields.Add(("Snapshot HLC", $"{info.ClusterSnapshotNode}:{info.ClusterSnapshotPhysical}:{info.ClusterSnapshotCounter}"));

    int nameWidth = fields.Max(f => f.Name.Length);

    foreach ((string name, string value) in fields)
        AnsiConsole.MarkupLine("[blue]{0}[/]: {1}", Markup.Escape(name.PadLeft(nameWidth)), Markup.Escape(value));

    // The call succeeded but cost a full image rather than an increment — worth saying out loud, since
    // the whole point of asking for an incremental was to avoid that.
    if (info.WasSubstituted)
    {
        AnsiConsole.MarkupLine(
            "[yellow]Requested {0}, the server took {1}: {2}[/]",
            Markup.Escape(info.RequestedKind ?? "?"),
            Markup.Escape(info.ActualKind ?? "?"),
            Markup.Escape(info.SubstitutionReason ?? "no reason reported"));
    }

    AnsiConsole.MarkupLine("Backup OK ({0})\n", elapsed);
}

static void WriteBackupList(IReadOnlyList<CamusBackupInfo> backups, TimeSpan elapsed)
{
    if (backups.Count > 0)
    {
        Table table = new()
        {
            Border = TableBorder.Square
        };

        table.AddColumn("Backup Id");
        table.AddColumn("Type");
        table.AddColumn("Created (UTC)");
        table.AddColumn("Parent");
        table.AddColumn("Partitions");
        table.AddColumn("Status");

        foreach (CamusBackupInfo info in backups)
        {
            table.AddRow(
                Markup.Escape(info.BackupId),
                Markup.Escape(info.ActualKind is { Length: > 0 } kind ? kind : info.Type),
                Markup.Escape(info.IsInvalid ? "" : FormatUtc(info.CreatedAtUtc)),
                Markup.Escape(info.ParentBackupId ?? ""),
                Markup.Escape(info.IsInvalid ? "" : info.PartitionCount.ToString(CultureInfo.InvariantCulture)),
                DescribeBackupStatus(info));
        }

        AnsiConsole.Write(table);
    }

    AnsiConsole.MarkupLine("[blue]{0}[/] backups in set ({1})\n", backups.Count, elapsed);
}

// A listing fails open on a single unreadable manifest, so an entry can be present but meaningless;
// say which, rather than printing a row of blanks with no explanation.
static string DescribeBackupStatus(CamusBackupInfo info)
{
    if (info.IsInvalid)
        return $"[red]invalid: {Markup.Escape(info.InvalidReason ?? "unreadable manifest")}[/]";

    if (info.WasSubstituted)
        return $"[yellow]substituted from {Markup.Escape(info.RequestedKind ?? "?")}[/]";

    return "[green]ok[/]";
}

// The window a point-in-time restore may target. The server reports it on the chain's root, not its
// leaf, so it is a property of the resolved chain and belongs under the table rather than in a column.
static void WriteRecoverableWindow(IReadOnlyList<CamusBackupInfo> chain)
{
    if (chain.Count == 0)
        return;

    CamusBackupInfo root = chain[0];

    // A chain the server reports no coverage for — a freshly taken backup on an idle node has none —
    // comes back as null or as epoch 0. Printing "1970-01-01" for that would read as a real window.
    if (root.MinRecoverablePhysicalMs is not long from || root.MaxRecoverablePhysicalMs is not long to || from <= 0 || to <= 0)
        return;

    AnsiConsole.MarkupLine(
        "Recoverable window (UTC): [white]{0}[/] .. [white]{1}[/]\n",
        Markup.Escape(FormatEpochMs(from)),
        Markup.Escape(FormatEpochMs(to)));
}

static void WriteBackupGcResult(CamusBackupGcResult result, TimeSpan elapsed)
{
    List<CamusBackupGcDeletion> deletions = result.RetentionDeletions ?? [];
    List<CamusBackupGcOrphan> orphans = result.OrphanReclamations ?? [];

    if (deletions.Count > 0)
    {
        Table table = new()
        {
            Border = TableBorder.Square
        };

        table.AddColumn("Backup Id");
        table.AddColumn("Type");
        table.AddColumn("Created (UTC)");
        table.AddColumn("Size");
        table.AddColumn("Reason");

        foreach (CamusBackupGcDeletion deletion in deletions)
        {
            table.AddRow(
                Markup.Escape(deletion.BackupId),
                Markup.Escape(deletion.Type),
                Markup.Escape(FormatUtc(deletion.CreatedAtUtc)),
                Markup.Escape(FormatBytes(deletion.Bytes)),
                Markup.Escape(deletion.Reason));
        }

        AnsiConsole.Write(table);
    }

    if (orphans.Count > 0)
    {
        Table table = new()
        {
            Border = TableBorder.Square
        };

        table.AddColumn("Orphan");
        table.AddColumn("Kind");
        table.AddColumn("Reason");

        foreach (CamusBackupGcOrphan orphan in orphans)
        {
            table.AddRow(
                Markup.Escape(orphan.Name),
                orphan.IsDirectory ? "directory" : "file",
                Markup.Escape(orphan.Reason));
        }

        AnsiConsole.Write(table);
    }

    // A preview deleted nothing; saying "reclaimed" there would be a lie an operator acts on.
    string verb = result.Applied ? "reclaimed" : "would reclaim";

    AnsiConsole.MarkupLine(
        "{0}: [blue]{1}[/] backups, [blue]{2}[/] orphans, [blue]{3}[/] {4} ({5})\n",
        result.Applied ? "Retention applied" : "Retention preview",
        deletions.Count,
        orphans.Count,
        Markup.Escape(FormatBytes(result.BytesReclaimed)),
        verb,
        elapsed);
}

static string FormatUtc(DateTime value)
    => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

static string FormatEpochMs(long epochMs)
    => FormatUtc(DateTimeOffset.FromUnixTimeMilliseconds(epochMs).UtcDateTime);

static string FormatBytes(long bytes)
{
    string[] units = ["B", "KB", "MB", "GB", "TB", "PB"];

    double size = bytes;
    int unit = 0;

    while (size >= 1024 && unit < units.Length - 1)
    {
        size /= 1024;
        unit++;
    }

    return unit == 0
        ? $"{bytes} B"
        : string.Format(CultureInfo.InvariantCulture, "{0:0.##} {1}", size, units[unit]);
}

// Reports a failed statement: the exception, the server's error code, a caret under the offending
// token when the message carries a location, and — for the authorization codes — the action that
// actually fixes it, since "insufficient privilege" alone doesn't say which grant is missing.
static void WriteStatementError(Exception ex, string? sql)
{
    string errorCode = ex is CamusException ce ? ce.Code : $"0x{ex.HResult:X8}";

    AnsiConsole.MarkupLine("[red]{0}[/] ([grey]{1}[/]): {2}", Markup.Escape(ex.GetType().Name), Markup.Escape(errorCode), Markup.Escape(ex.Message));
    WriteErrorCaret(sql, ex.Message);

    switch (errorCode)
    {
        case "CADB0516":
            AnsiConsole.MarkupLine("[grey58]Not authenticated. Start the shell with[/] [cyan]-u <user>[/] [grey58](and[/] [cyan]-p[/][grey58]), or re-run it if the token was revoked.[/]");
            break;

        case "CADB0517":
            AnsiConsole.MarkupLine("[grey58]Authenticated, but missing a privilege on some table the statement touches. Grant it with[/] [cyan]GRANT … ON db.* TO <user>[/][grey58], as a superuser.[/]");
            break;

        case "CADB0519":
            AnsiConsole.MarkupLine("[grey58]The server requires TLS for credential-bearing requests. Use an[/] [cyan]https://[/] [grey58]endpoint.[/]");
            break;
    }

    AnsiConsole.WriteLine();
}

// Renders a Rust-compiler-style pointer under the offending line when the error
// message carries a "(line N, col M)" location, e.g.:
//
//     create table x1 (id uuid primary key default(gen_uuid_v7()), name string(20) ...
//                                                  ^
static void WriteErrorCaret(string? sql, string message)
{
    if (string.IsNullOrEmpty(sql))
        return;

    Match m = Regex.Match(message, @"line\s+(\d+),\s*col\s+(\d+)", RegexOptions.IgnoreCase);
    if (!m.Success)
        return;

    if (!int.TryParse(m.Groups[1].Value, out int line) || !int.TryParse(m.Groups[2].Value, out int col))
        return;

    string[] lines = sql.Replace("\r\n", "\n").Split('\n');
    if (line < 1 || line > lines.Length)
        return;

    string source = lines[line - 1];

    // Columns are 1-based; clamp into the line so a caret always renders.
    int caret = Math.Clamp(col - 1, 0, source.Length);

    // Preserve tabs in the padding so the caret stays aligned with the source.
    StringBuilder pad = new();
    for (int i = 0; i < caret; i++)
        pad.Append(source[i] == '\t' ? '\t' : ' ');

    AnsiConsole.WriteLine();
    AnsiConsole.WriteLine("  " + source);
    AnsiConsole.MarkupLine("  [red]{0}^[/]", pad.ToString());
}

static string FormatValue(ColumnValue value)
{
    return value.Type switch
    {
        ColumnType.Id => !string.IsNullOrEmpty(value.StrValue) ? value.StrValue! : "",
        ColumnType.String => !string.IsNullOrEmpty(value.StrValue) ? Markup.Escape(value.StrValue!) : "",
        ColumnType.Integer64 => value.LongValue.ToString(),
        ColumnType.Float64 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
        ColumnType.Bool => value.BoolValue.ToString(),
        ColumnType.Uuid => !string.IsNullOrEmpty(value.UuidValue) ? value.UuidValue! : "",
        _ => "null"
    };
}

static void WriteVerticalRow(Dictionary<string, ColumnValue> row, int rowNumber)
{
    AnsiConsole.MarkupLine("[grey]*************************** {0}. row ***************************[/]", rowNumber);

    int nameWidth = row.Keys.Count == 0 ? 0 : row.Keys.Max(k => k.Length);

    foreach (KeyValuePair<string, ColumnValue> item in row)
    {
        string name = Markup.Escape(item.Key.PadLeft(nameWidth));
        AnsiConsole.MarkupLine("[blue]{0}[/]: {1}", name, FormatValue(item.Value));
    }
}

static async Task ExecuteDDL(CamusConnection connection, string sql)
{
    using CamusCommand cmd = connection.CreateCamusCommand(sql);

    cmd.CommandTimeout = 60;

    Stopwatch stopwatch = Stopwatch.StartNew();

    bool success = await cmd.ExecuteDDLAsync();

    if (success)
        AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", stopwatch.Elapsed);
}


// Recognizes the shell's own `show prepared` / `\prepared` command, optionally followed by the
// statement to ask about instead of the last one executed.
static bool IsShowPrepared(string sql, out string? statement)
{
    statement = null;
    string trimmed = sql.Trim().TrimEnd(';').TrimEnd();

    string[] prefixes = ["show prepared", "\\prepared"];
    foreach (string prefix in prefixes)
    {
        if (!trimmed.StartsWith(prefix, StringComparison.InvariantCultureIgnoreCase))
            continue;

        string rest = trimmed[prefix.Length..];

        // "show preparedfoo" is not this command; a bare prefix or one followed by SQL is.
        if (rest.Length > 0 && !char.IsWhiteSpace(rest[0]))
            continue;

        rest = rest.Trim();
        statement = rest.Length > 0 ? rest : null;
        return true;
    }

    return false;
}

// Reports what the driver currently keeps prepared. The count is shared per deployment and
// settings rather than per connection, so it survives `use` and outlives any one connection.
static void ShowPrepared(string connectionString, string? sql, bool explicitSql)
{
    CamusConnectionStringBuilder builder = new(connectionString);

    AnsiConsole.MarkupLine(
        "Prepared statements: [blue]{0}[/] [grey58](MaxAutoPrepare={1}, AutoPrepareMinUsages={2})[/]",
        builder.PreparedStatementCount, builder.MaxAutoPrepare, builder.AutoPrepareMinUsages);

    if (builder.MaxAutoPrepare == 0)
        AnsiConsole.MarkupLine("[yellow]Automatic preparation is off[/] [grey58](MaxAutoPrepare=0 in the connection string).[/]");

    if (sql is null)
    {
        AnsiConsole.MarkupLine("[grey58]Run a statement, or pass one to check: [/][cyan]show prepared SELECT ...[/]\n");
        return;
    }

    foreach ((string statement, bool _) in EscapeStringIntoLines(sql))
    {
        if (string.IsNullOrWhiteSpace(statement))
            continue;

        // Typed statements carry their values inline, so the text differs on every execution and
        // never repeats often enough to be registered — which is what this usually reports.
        bool prepared = builder.IsPrepared(statement);

        AnsiConsole.MarkupLine(
            prepared ? "  [green]prepared[/]     {0}" : "  [grey58]inline[/]       {0}",
            Markup.Escape(Abbreviate(statement)));
    }

    if (!explicitSql)
        AnsiConsole.MarkupLine("[grey58](the statement you ran last)[/]");

    AnsiConsole.WriteLine();
}


// Default listener ports for a local server: REST on 5095, gRPC on 5096 (both enabled by default).
const int DefaultRestPort = 5095;
const int DefaultGrpcPort = 5096;

// Builds the ordered list of connection strings to try. gRPC is preferred and REST is the
// fallback, so a server that only speaks one of them (or has gRPC disabled) still connects.
// When the caller pins Protocol= explicitly, that choice is honored with no fallback.
static List<string> BuildConnectionAttempts(Options opts, string? user, string? password, string? token)
{
    if (!string.IsNullOrEmpty(opts.ConnectionSource))
    {
        string cs = WithCredentials(EnsureDatabase(opts.ConnectionSource), user, password, token);

        // Respect an explicit Protocol= — the user has chosen the transport deliberately.
        if (HasKey(cs, "Protocol"))
            return [cs];

        // No protocol given: try gRPC against their endpoint, then REST against the same endpoint.
        // The gRPC attempt carries the same endpoint as its BackupEndpoint, because that is exactly
        // where REST is being tried next — the backup admin API speaks HTTP only.
        return [WithBackupEndpoint(WithProtocol(cs, "grpc"), GetConnValue(cs, "Endpoint")), WithProtocol(cs, "rest")];
    }

    string db = opts.Database ?? "";

    // No connection string at all: use the well-known local ports for each transport. The gRPC
    // attempt names the well-known HTTP port for backups, which is on a different port than gRPC.
    return
    [
        WithCredentials(
            WithBackupEndpoint($"Endpoint=http://localhost:{DefaultGrpcPort};Database={db};Protocol=grpc", $"http://localhost:{DefaultRestPort}"),
            user, password, token),
        WithCredentials($"Endpoint=http://localhost:{DefaultRestPort};Database={db};Protocol=rest", user, password, token),
    ];
}

// Adds the authentication keys the driver understands. Credentials passed on the command line (or
// in the environment) win over the same key inside -c, since they were given more deliberately;
// anything not supplied is left untouched, so a -c that already carries them still works.
static string WithCredentials(string connectionString, string? user, string? password, string? token)
{
    if (!string.IsNullOrEmpty(user))
        connectionString = WithKey(connectionString, "User", user);

    if (!string.IsNullOrEmpty(password))
        connectionString = WithKey(connectionString, "Password", password);

    if (!string.IsNullOrEmpty(token))
        connectionString = WithKey(connectionString, "AccessToken", token);

    return connectionString;
}

// Reads a password without echoing it. Returns null when there is no terminal to prompt on, so a
// piped/scripted invocation fails on the server's authentication error rather than blocking here.
static string? PromptPassword()
{
    if (Console.IsInputRedirected)
        return null;

    string entered = AnsiConsole.Prompt(new TextPrompt<string>("Password:").Secret().AllowEmpty());
    return string.IsNullOrEmpty(entered) ? null : entered;
}

// Startup connection failures are the one place where a raw stack trace helps nobody: the causes
// are few and each has a concrete fix, so name the likely one.
static void WriteConnectionError(Exception ex)
{
    string code = ex is CamusException ce ? ce.Code : "";

    AnsiConsole.MarkupLine("[red]Connection failed[/]{0}: {1}\n",
        code.Length > 0 ? $" ([grey]{Markup.Escape(code)}[/])" : "",
        Markup.Escape(ex.Message));

    switch (code)
    {
        case "CADB0516":
            AnsiConsole.MarkupLine("[grey58]Authentication failed. Check[/] [cyan]-u[/][grey58]/[/][cyan]-p[/][grey58], or that the user exists on this server.[/]");
            break;

        case "CADB0518":
            AnsiConsole.MarkupLine("[grey58]Too many login attempts for this account; the server rate-limits logins per minute. Wait and retry.[/]");
            break;

        case "CADB0519":
            AnsiConsole.MarkupLine("[grey58]The server refuses credentials over plaintext. Use an[/] [cyan]https://[/] [grey58]endpoint, or start the server with[/] [cyan]--require-tls-when-auth-enabled false[/] [grey58]when TLS terminates in front of it.[/]");
            break;
    }
}


// Points `backup …` at an HTTP endpoint on a gRPC connection: the backup admin API is REST-only, so
// without this every backup command on the shell's default (gRPC) connection would fail asking for the
// key. An endpoint the user set themselves is left alone, and so is an `Endpoint=` pool's tail — a
// backup goes to one node, and for a coordinated one that node has to be the coordinator, so pinning
// it deliberately is the caller's job.
static string WithBackupEndpoint(string connectionString, string? endpoint)
{
    if (HasKey(connectionString, "BackupEndpoint") || string.IsNullOrWhiteSpace(endpoint))
        return connectionString;

    string first = endpoint.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .FirstOrDefault() ?? endpoint;

    return WithKey(connectionString, "BackupEndpoint", first);
}


// Human-readable transport name for the resolved connection string (Protocol= defaults to REST).
static string DescribeTransport(string connectionString)
{
    string? protocol = connectionString
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries))
        .Where(p => p.Length == 2 && string.Equals(p[0], "Protocol", StringComparison.OrdinalIgnoreCase))
        .Select(p => p[1])
        .FirstOrDefault();

    return string.Equals(protocol, "grpc", StringComparison.OrdinalIgnoreCase) ? "gRPC" : "REST";
}


static async Task<List<string>> GetHistory(string historyPath)
{
    List<string>? history = [];

    if (File.Exists(historyPath))
    {
        try
        {
            string historyText = await File.ReadAllTextAsync(historyPath);
            history = JsonSerializer.Deserialize<List<string>>(historyText);
        }
        catch
        {
            Console.WriteLine("Found invalid history");
        }
    }

    history ??= [];
    history = RemoveAdjacentDuplicates(history);

    return history;
}

static List<string> RemoveAdjacentDuplicates(IEnumerable<string> history)
{
    List<string> result = [];

    foreach (string item in history)
    {
        if (result.Count == 0 ||
            !string.Equals(result[^1], item, StringComparison.Ordinal))
        {
            result.Add(item);
        }
    }

    return result;
}

static void PrintHelp()
{
    AnsiConsole.MarkupLine("Usage: camus-cli [[database]] [[options]]");
    AnsiConsole.MarkupLine("       camus-cli workload <init|run> <bank|northwind|factory|tpcc|tpcb> [[options]]");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Options:[/]");
    AnsiConsole.MarkupLine("  database                      Database name to connect to (default: test)");
    AnsiConsole.MarkupLine("  -c, --connection-source       Connection string (default: gRPC on http://localhost:5096,");
    AnsiConsole.MarkupLine("                                falling back to REST on http://localhost:5095)");
    AnsiConsole.MarkupLine("  -e, --execute                 Execute a SQL statement (or ;-separated statements) and exit");
    AnsiConsole.MarkupLine("  -f, --file                    Execute the statements in a .sql file and exit; stops at the");
    AnsiConsole.MarkupLine("                                first error. Use [cyan]-f -[/] to read the script from standard input");
    AnsiConsole.MarkupLine("  -u, --user                    User to authenticate as (only needed on a server with");
    AnsiConsole.MarkupLine("                                authentication enabled)");
    AnsiConsole.MarkupLine("  -p, --password                That user's password; prompted for when -u is given without it");
    AnsiConsole.MarkupLine("  --token                       Use a bearer token obtained elsewhere instead of logging in");
    AnsiConsole.MarkupLine("  --tui                         Open the full-screen browser: catalog, editor and results");
    AnsiConsole.MarkupLine("                                in three panes. TAB moves between panes, F5 runs the query");
    AnsiConsole.MarkupLine("  --force-rich                  Force the rich line editor (colors, multiline, Tab completion)");
    AnsiConsole.MarkupLine("                                on terminals whose TERM value Spectre.Console doesn't recognize");
    AnsiConsole.MarkupLine("  --diagnose-terminal           Print terminal capabilities and exit (why rich mode is on/off)");
    AnsiConsole.MarkupLine("  -h, --help                    Show this help message");
    AnsiConsole.MarkupLine("  -v, --version                 Show version information");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Environment variables:[/]");
    AnsiConsole.MarkupLine("  CAMUS_FORCE_RICH=1            Same as [cyan]--force-rich[/] (accepts 1/true/yes)");
    AnsiConsole.MarkupLine("  CAMUS_USER                    Default for [cyan]-u[/]");
    AnsiConsole.MarkupLine("  CAMUS_PASSWORD                Default for [cyan]-p[/] (keeps the password out of the shell history)");
    AnsiConsole.MarkupLine("  CAMUS_ACCESS_TOKEN            Default for [cyan]--token[/]");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Subcommands:[/]");
    AnsiConsole.MarkupLine("  [cyan]workload init[/] <bank|northwind|factory|tpcc|tpcb>  Create schema and seed data for a workload");
    AnsiConsole.MarkupLine("  [cyan]workload run[/]  <bank|northwind|factory|tpcc|tpcb>  Run a continuous workload against the database");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Workload options:[/]");
    AnsiConsole.MarkupLine("  -c, --connection-source       Connection string");
    AnsiConsole.MarkupLine("  --database                    Target database name (default: demo)");
    AnsiConsole.MarkupLine("  --rows N                      Rows to generate for init (default: 1000, bank only)");
    AnsiConsole.MarkupLine("  --concurrency N               Parallel workers for run (default: 3)");
    AnsiConsole.MarkupLine("  --duration N                  Run duration in seconds (default: 60)");
    AnsiConsole.MarkupLine("  --locking MODE                Locking mode: optimistic | pessimistic (default: optimistic)");
    AnsiConsole.MarkupLine("  --isolation LEVEL             Isolation level: serializable | read-committed (default: serializable)");
    AnsiConsole.MarkupLine("  --no-prepare                  Run statements inline instead of preparing them, to measure");
    AnsiConsole.MarkupLine("                                against the default prepared path");
    AnsiConsole.MarkupLine("  -u, --user / -p, --password   Credentials for an authenticated server");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Shell commands:[/]");
    AnsiConsole.MarkupLine("  [cyan]use[/] <database>              Switch the current database; the name may be");
    AnsiConsole.MarkupLine("                                [cyan]`backticked`[/]. Works in script files too");
    AnsiConsole.MarkupLine("  [cyan]source[/] <file> [[--force]]     Run the statements in a file, streaming it;");
    AnsiConsole.MarkupLine("                                [cyan]--force[/] carries on past failures");
    AnsiConsole.MarkupLine("  [cyan]show prepared[/] [[sql]]         What the driver keeps prepared; with SQL, whether that");
    AnsiConsole.MarkupLine("                                statement is prepared (alias: [cyan]\\prepared[/])");
    AnsiConsole.MarkupLine("  [cyan]clear[/] / [cyan]exit[/] / [cyan]quit[/]         Clear the screen / leave the shell");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Cluster settings:[/] [grey58](SQL, not shell commands; each runs without a current database)[/]");
    AnsiConsole.MarkupLine("  [cyan]show variables[/] [[like '<pattern>']]");
    AnsiConsole.MarkupLine("                                This node's effective configuration, with each");
    AnsiConsole.MarkupLine("                                setting's [white]mutability[/] (runtime|restart) and [white]scope[/]");
    AnsiConsole.MarkupLine("  [cyan]show cluster settings[/] [[like '<pattern>']]");
    AnsiConsole.MarkupLine("                                What the cluster overlay currently carries");
    AnsiConsole.MarkupLine("  [cyan]set cluster setting[/] <key> = <value>");
    AnsiConsole.MarkupLine("                                Change a runtime setting fleet-wide, from any node");
    AnsiConsole.MarkupLine("  [cyan]reset cluster setting[/] <key>   Drop the overlay entry; each node resolves the key");
    AnsiConsole.MarkupLine("                                through its own config again");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Introspection:[/] [grey58](SQL, not shell commands)[/]");
    AnsiConsole.MarkupLine("  [cyan]show statistics for[/] [[table]] <name>");
    AnsiConsole.MarkupLine("                                What the optimizer believes about a table: row counts,");
    AnsiConsole.MarkupLine("                                column min/max, histogram buckets, distinct-value counts,");
    AnsiConsole.MarkupLine("                                and how stale they are ([cyan]analyze <table>[/] refreshes them)");
    AnsiConsole.MarkupLine("  [cyan]show ranges from table[/] <name>");
    AnsiConsole.MarkupLine("  [cyan]show ranges from index[/] <table>@<index>");
    AnsiConsole.MarkupLine("                                How a relation's key space is divided into spans, and");
    AnsiConsole.MarkupLine("                                where each span's leader is, as [white]this node[/] sees it");
    AnsiConsole.MarkupLine("  [cyan]show range from table[/] <name> [cyan]for row[/] (<primary key>)");
    AnsiConsole.MarkupLine("  [cyan]show range from index[/] <table>@<index> [cyan]for row[/] (<values>)");
    AnsiConsole.MarkupLine("                                The one span that holds a row. Fifteen columns, so");
    AnsiConsole.MarkupLine("                                append [cyan]\\G[/] for vertical output");
    AnsiConsole.MarkupLine("  [cyan]show engine stats[/]             Runtime engine metrics for the node answering");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Examples:[/]");
    AnsiConsole.MarkupLine("  camus-cli mydb");
    AnsiConsole.MarkupLine("  camus-cli mydb -u app -p app-secret");
    AnsiConsole.MarkupLine("  camus-cli mydb -u app                     [grey58](prompts for the password)[/]");
    AnsiConsole.MarkupLine("  camus-cli -c \"Endpoint=http://localhost:5096;Database=mydb;Protocol=grpc\"");
    AnsiConsole.MarkupLine("  camus-cli -c \"Endpoint=http://localhost:5095;Database=mydb;Protocol=rest\"");
    AnsiConsole.MarkupLine("  camus-cli mydb -e \"SELECT * FROM users\"");
    AnsiConsole.MarkupLine("  camus-cli mydb -f schema.sql");
    AnsiConsole.MarkupLine("  cat schema.sql | camus-cli mydb -f -");
    AnsiConsole.MarkupLine("  camus-cli workload init bank --database demo --rows 5000");
    AnsiConsole.MarkupLine("  camus-cli workload run northwind --concurrency 5 --duration 120");
    AnsiConsole.MarkupLine("  camus-cli workload init factory --database factory");
    AnsiConsole.MarkupLine("  camus-cli workload run factory --concurrency 4 --duration 120");
    AnsiConsole.MarkupLine("  camus-cli workload init tpcc --database tpcc --rows 1");
    AnsiConsole.MarkupLine("  camus-cli workload run tpcc --concurrency 4 --duration 120");
    AnsiConsole.MarkupLine("  camus-cli workload init tpcb --database tpcb --rows 10000");
    AnsiConsole.MarkupLine("  camus-cli workload run tpcb --concurrency 8 --duration 120");
}

static bool ConsumeFlag(ref string[] args, string flag)
{
    bool present = Array.Exists(args, a => string.Equals(a, flag, StringComparison.OrdinalIgnoreCase));
    if (present)
        args = [.. args.Where(a => !string.Equals(a, flag, StringComparison.OrdinalIgnoreCase))];

    return present;
}

static bool IsTruthy(string? value)
{
    return !string.IsNullOrEmpty(value)
        && (value == "1"
            || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
            || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase));
}

static IEnumerable<string> NormalizeBuiltInOptions(IEnumerable<string> args)
{
    foreach (string arg in args)
    {
        yield return arg switch
        {
            "-h" => "--help",
            "-v" => "--version",
            _ => arg
        };
    }
}

// One statement pulled out of a script, with the line it started on so an error can point at it.
public readonly record struct SqlStatement(string Sql, bool Vertical, int Line);

// Splits SQL text into statements, tracking where a ';' actually terminates one: not inside a
// string, a `quoted identifier`, or a comment. Holding the state in an object (rather than
// re-scanning a whole string) lets callers feed a file line by line instead of loading it whole,
// and lets the prompt ask whether what's been typed so far is finished.
public sealed class SqlScanner
{
    private readonly StringBuilder current = new();

    private bool inSingleQuote, inDoubleQuote, inBacktick, inLineComment, inBlockComment;
    private int parenDepth;

    // The line the scanner is reading, and the one the pending statement's first real character
    // sat on — leading blank lines and comments shouldn't shift the reported location.
    private int line = 1;
    private int statementLine = 1;

    private bool hasContent;
    private char lastContentChar;

    /// <summary>Inside a string literal or a quoted identifier, so the statement can't be over.</summary>
    public bool InsideQuotes => inSingleQuote || inDoubleQuote || inBacktick;

    /// <summary>Inside an unterminated /* … */. A line comment self-terminates, so it doesn't count.</summary>
    public bool InsideBlockComment => inBlockComment;

    public int ParenDepth => parenDepth;

    /// <summary>The pending text holds something other than whitespace and comments.</summary>
    public bool HasPendingContent => hasContent;

    /// <summary>Last non-whitespace character outside a comment, for spotting a trailing comma.</summary>
    public char LastContentChar => lastContentChar;

    /// <summary>
    /// Consumes a chunk of SQL and yields whatever statements it completed. Two-character tokens
    /// (--, /*, */, \G, doubled quotes) are looked up within the chunk, so callers must not split a
    /// chunk mid-token; feeding whole lines is always safe, since none of them can span a newline.
    /// </summary>
    public IEnumerable<SqlStatement> Feed(string text)
    {
        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];
            char next = i + 1 < text.Length ? text[i + 1] : '\0';

            if (inLineComment)
            {
                Skip(c);

                if (c == '\n')
                    inLineComment = false;

                continue;
            }

            if (inBlockComment)
            {
                Skip(c);

                if (c == '*' && next == '/')
                {
                    Skip(next);
                    i++;
                    inBlockComment = false;
                }

                continue;
            }

            if (InsideQuotes)
            {
                // Backslash escapes a character inside a string literal, but a backtick-quoted
                // identifier has no escape other than doubling.
                if (c == '\\' && next != '\0' && !inBacktick)
                {
                    Append(c, content: true);
                    Append(next, content: true);
                    i++;
                    continue;
                }

                char delimiter = inSingleQuote ? '\'' : inDoubleQuote ? '"' : '`';

                if (c == delimiter)
                {
                    // A doubled delimiter ('' "" ``) is a literal one, not the end of the quote.
                    if (next == delimiter)
                    {
                        Append(c, content: true);
                        Append(next, content: true);
                        i++;
                        continue;
                    }

                    inSingleQuote = inDoubleQuote = inBacktick = false;
                }

                Append(c, content: true);
                continue;
            }

            // -- only opens a comment when whitespace follows it, so `a--b` stays arithmetic. The end
            // of a chunk counts as whitespace: callers feed lines, and a trailing -- ends the line.
            if (c == '-' && next == '-' && (i + 2 >= text.Length || char.IsWhiteSpace(text[i + 2])))
            {
                OpenComment();
                Skip(c);
                Skip(next);
                inLineComment = true;
                i++;
                continue;
            }

            if (c == '#')
            {
                OpenComment();
                Skip(c);
                inLineComment = true;
                continue;
            }

            if (c == '/' && next == '*')
            {
                OpenComment();
                Skip(c);
                Skip(next);
                inBlockComment = true;
                i++;
                continue;
            }

            if (c is '\'' or '"' or '`')
            {
                inSingleQuote = c == '\'';
                inDoubleQuote = c == '"';
                inBacktick = c == '`';
                Append(c, content: true);
                continue;
            }

            // MySQL-style \G terminator: ends the statement and requests vertical output.
            if (c == '\\' && next == 'G')
            {
                i++;

                if (TryTake(vertical: true, out SqlStatement verticalStatement))
                    yield return verticalStatement;

                continue;
            }

            if (c == ';')
            {
                if (TryTake(vertical: false, out SqlStatement statement))
                    yield return statement;

                continue;
            }

            if (c == '(')
                parenDepth++;
            else if (c == ')' && parenDepth > 0)
                parenDepth--;

            Append(c, content: true);
        }
    }

    /// <summary>Yields a final statement left unterminated at the end of the input, if any.</summary>
    public IEnumerable<SqlStatement> Flush()
    {
        if (TryTake(vertical: false, out SqlStatement statement))
            yield return statement;
    }

    private void Append(char c, bool content)
    {
        current.Append(c);

        if (content && !char.IsWhiteSpace(c))
        {
            if (!hasContent)
            {
                statementLine = line;
                hasContent = true;
            }

            lastContentChar = c;
        }

        if (c == '\n')
            line++;
    }

    // Comment text is dropped rather than forwarded: servers disagree on which comment syntaxes they
    // accept (CamusDB rejects '#'), and a dump full of them shouldn't fail on that. Newlines survive
    // so the statement keeps its shape and the server's "line N, col M" still lands on the right row.
    private void Skip(char c)
    {
        if (c == '\n')
        {
            current.Append(c);
            line++;
        }
    }

    // A comment can sit between two tokens (a/*x*/b), so leave a separator where it was.
    private void OpenComment()
    {
        current.Append(' ');
    }

    private bool TryTake(bool vertical, out SqlStatement statement)
    {
        string sql = current.ToString().Trim();
        int at = statementLine;
        bool any = hasContent;

        current.Clear();
        parenDepth = 0;
        hasContent = false;
        lastContentChar = '\0';
        statementLine = line;

        // Nothing but whitespace and comments: an empty ";;", or a trailing comment at end of file.
        if (!any)
        {
            statement = default;
            return false;
        }

        statement = new SqlStatement(sql, vertical, at);
        return true;
    }
}

public sealed class MyLineNumberPrompt : ILineEditorPrompt
{
    private readonly Style _style;

    public MyLineNumberPrompt(Style? style = null)
    {
        _style = style ?? new Style(foreground: Color.Yellow, background: Color.Blue);
    }

    public (Markup Markup, int Margin) GetPrompt(ILineEditorState state, int line)
    {
        return (new Markup(line == 0 ? "camus> " : "   -> ", _style), 1);
    }
}

public sealed class Options
{
    [Value(0, Required = false, MetaName = "database", HelpText = "Set the database name")]
    public string? Database { get; set; }

    [Option('c', "connection-source", Required = false, HelpText = "Set the connection string")]
    public string? ConnectionSource { get; set; }

    [Option('e', "execute", Required = false, HelpText = "Execute a SQL statement (or ;-separated statements) and exit")]
    public string? Execute { get; set; }

    [Option('f', "file", Required = false, HelpText = "Execute the statements in a .sql file and exit (- reads standard input)")]
    public string? File { get; set; }

    [Option('u', "user", Required = false, HelpText = "User to authenticate as")]
    public string? User { get; set; }

    [Option('p', "password", Required = false, HelpText = "That user's password (prompted for when omitted)")]
    public string? Password { get; set; }

    [Option("token", Required = false, HelpText = "Use a bearer token obtained elsewhere instead of logging in")]
    public string? AccessToken { get; set; }
}
