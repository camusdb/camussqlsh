
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

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

// Tell the user where and how they connected, unless they're scripting with -e (keep stdout clean).
if (string.IsNullOrWhiteSpace(opts.Execute))
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

// Non-interactive mode: run the supplied SQL, then exit. A failure here has no prompt to return
// to, so report it the way the interactive loop does — a diagnosable line, not a stack trace —
// and exit non-zero so a caller can tell the statement didn't run.
if (!string.IsNullOrWhiteSpace(opts.Execute))
{
    try
    {
        await ExecuteSql(connection, opts.Execute);
    }
    catch (Exception ex)
    {
        WriteStatementError(ex, opts.Execute);
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

StringBuilder pendingSql = new();

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

        if (pendingSql.Length == 0 && sqlTrim.StartsWith("source ", StringComparison.InvariantCultureIgnoreCase))
        {
            await LoadSource(connection, sqlTrim[7..].Trim());
            continue;
        }

        if (pendingSql.Length == 0 && sqlTrim.StartsWith("use ", StringComparison.InvariantCultureIgnoreCase))
        {
            string newDb = sqlTrim[4..].Trim().TrimEnd(';');
            if (string.IsNullOrWhiteSpace(newDb))
            {
                AnsiConsole.MarkupLine("[red]Usage: use <database>[/]");
                continue;
            }

            if (transaction is not null)
            {
                AnsiConsole.MarkupLine("[red]There's an active transaction, please commit or rollback before switching databases[/]");
                continue;
            }

            activeConnectionString = SwapDatabase(activeConnectionString, newDb);
            connection = await ConnectionHelper.OpenAsync(activeConnectionString);

            if (sqlCompletion is not null)
                await sqlCompletion.RefreshTablesAsync(connection);

            AnsiConsole.MarkupLine("Database changed to [cyan]{0}[/]\n", Markup.Escape(newDb));
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

        // Add some history. A statement carrying a plaintext password (CREATE/ALTER USER … IDENTIFIED
        // BY '…') stays in the in-memory editor history — recalling it with Up is useful — but is kept
        // out of the on-disk history file, which outlives the session and is world-readable.
        if (editor is not null)
            editor.History.Add(executableSql);

        if (!CarriesPassword(executableSql))
            AddHistory(history, executableSql);

        await ExecuteSql(connection, executableSql);

        if (sqlCompletion is not null && ChangesTableSet(executableSql) && HasDatabase(activeConnectionString))
            await sqlCompletion.RefreshTablesAsync(connection);
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

async Task LoadSource(CamusConnection connection, string paths)
{
    if (!File.Exists(paths))
    {
        AnsiConsole.MarkupLine("[red]File not found: {0}[/]\n", Markup.Escape(paths));
        return;
    }

    //int numberLine = 0;
    string fileContents = await File.ReadAllTextAsync(paths);

    foreach ((string sql, bool vertical) in EscapeStringIntoLines(fileContents))
    {
        if (string.IsNullOrEmpty(sql))
        {
            //numberLine++;
            continue;
        }

        await ExecuteSql(connection, vertical ? sql + "\\G" : sql);

        //numberLine++;
    }
}

async Task ExecuteSql(CamusConnection connection, string input)
{
    // Also fold smart quotes here so the -e/--execute flag and `source` files benefit, not just
    // the interactive prompt. Idempotent, so re-folding an already-normalized statement is a no-op.
    input = NormalizeSmartQuotes(input);

    foreach ((string sql, bool vertical) in EscapeStringIntoLines(input))
    {
        if (string.IsNullOrWhiteSpace(sql))
            continue;

        bool needsDb = !IsServerLevelDDL(sql) && !IsSystemLevelQuery(sql);
        if (needsDb && !HasDatabase(activeConnectionString))
        {
            AnsiConsole.MarkupLine("[red]No database selected.[/] Use [cyan]use <database>[/] to select one.\n");
            continue;
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
}

static IEnumerable<(string Sql, bool Vertical)> EscapeStringIntoLines(string input)
{
    StringBuilder currentLine = new();
    bool inSingleQuote = false, inDoubleQuote = false;

    for (int i = 0; i < input.Length; i++)
    {
        char c = input[i];

        // Check for escaped quotes
        if (c == '\\' && i + 1 < input.Length && (input[i + 1] == '\'' || input[i + 1] == '\"'))
        {
            currentLine.Append(c); // Append the escape character
            currentLine.Append(input[++i]); // Append the quote and skip next character
            continue;
        }

        // MySQL-style \G terminator: ends the statement and requests vertical output.
        if (c == '\\' && i + 1 < input.Length && input[i + 1] == 'G' && !inSingleQuote && !inDoubleQuote)
        {
            i++; // skip the 'G'
            yield return (currentLine.ToString().Trim(), true);
            currentLine.Clear();
            continue;
        }

        if (c == '\'' && !inDoubleQuote)
        {
            inSingleQuote = !inSingleQuote;
        }
        else if (c == '\"' && !inSingleQuote)
        {
            inDoubleQuote = !inDoubleQuote;
        }

        if (c == ';' && !inSingleQuote && !inDoubleQuote)
        {
            yield return (currentLine.ToString().Trim(), false);
            currentLine.Clear();
        }
        else
        {
            currentLine.Append(c);
        }
    }

    if (currentLine.Length > 0)
        yield return (currentLine.ToString().Trim(), false);
}

static char FoldSmartQuote(char c) => c switch
{
    // Single curly quotes and low-9 variants -> '
    '‘' or '’' or '‚' or '‛' => '\'',
    // Double curly quotes and low-9 variants -> "
    '“' or '”' or '„' or '‟' => '"',
    _ => c,
};

static string NormalizeSmartQuotes(string input)
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

static bool IsSqlIncomplete(string input)
{
    string trimmed = input.Trim();

    if (string.IsNullOrEmpty(trimmed))
        return false;

    int parenDepth = 0;
    bool inSingleQuote = false;
    bool inDoubleQuote = false;

    for (int i = 0; i < input.Length; i++)
    {
        char c = input[i];

        if (c == '\\' && i + 1 < input.Length && (input[i + 1] == '\'' || input[i + 1] == '"'))
        {
            i++;
            continue;
        }

        if (c == '\'' && !inDoubleQuote)
        {
            inSingleQuote = !inSingleQuote;
            continue;
        }

        if (c == '"' && !inSingleQuote)
        {
            inDoubleQuote = !inDoubleQuote;
            continue;
        }

        if (inSingleQuote || inDoubleQuote)
            continue;

        if (c == '(')
            parenDepth++;
        else if (c == ')' && parenDepth > 0)
            parenDepth--;
    }

    if (inSingleQuote || inDoubleQuote || parenDepth > 0)
        return true;

    return trimmed.EndsWith(',');
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

static bool IsBeginTx(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("begin", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("start", StringComparison.InvariantCultureIgnoreCase);
}

static bool IsCommitTx(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("commit", StringComparison.InvariantCultureIgnoreCase);
}

static bool IsRollbackTx(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("rollback", StringComparison.InvariantCultureIgnoreCase);
}


static bool IsQueryable(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("select ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("explain ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("analyze ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("show ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("desc ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("describe ", StringComparison.InvariantCultureIgnoreCase);
}

static bool HasDatabase(string connectionString)
{
    return connectionString
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries))
        .Where(p => p.Length == 2 && string.Equals(p[0], "Database", StringComparison.OrdinalIgnoreCase))
        .Any(p => !string.IsNullOrWhiteSpace(p[1]));
}

static bool IsSystemLevelQuery(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("show databases", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("show orphan databases", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("show branches from ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("show ancestors from ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("show grants", StringComparison.InvariantCultureIgnoreCase);
}

// User and grant administration is server-level: like database DDL, these statements name their
// target inside the SQL, touch only the shared auth catalog, and return no database descriptor —
// so they run on a database-less connection and need no current database.
static bool IsUserAdmin(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("create user ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("alter user ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop user ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("grant ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("revoke ", StringComparison.InvariantCultureIgnoreCase);
}

// Everything the server dispatches before opening a database — database lifecycle DDL plus user
// and grant administration.
static bool IsServerLevelDDL(string sql)
{
    return IsDatabaseDDL(sql) || IsUserAdmin(sql);
}

// Statements the server dispatches before opening a database (StatementScope.IsDatabaseScopedMutation):
// they name their target inside the SQL, so they must run on a database-less connection rather than
// being rejected for having no current database. A rename has two accepted spellings —
// RENAME DATABASE a TO b and ALTER DATABASE a RENAME TO b — and only COMMENT ON DATABASE is
// database-scoped; COMMENT ON TABLE/COLUMN still needs a current database.
static bool IsDatabaseDDL(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("create database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("rename database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("alter database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("comment on database ", StringComparison.InvariantCultureIgnoreCase);
}

static bool ChangesTableSet(string sql)
{
    string trimmedSql = sql.TrimStart();

    return trimmedSql.StartsWith("create table ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop table ", StringComparison.InvariantCultureIgnoreCase);
}

static bool IsDDL(string sql)
{
    string trimmedSql = sql.Trim();

    return IsServerLevelDDL(trimmedSql) ||
           trimmedSql.StartsWith("create table ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("create index ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop table ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop index ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("alter table ", StringComparison.InvariantCultureIgnoreCase);
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
        return [WithProtocol(cs, "grpc"), WithProtocol(cs, "rest")];
    }

    string db = opts.Database ?? "";

    // No connection string at all: use the well-known local ports for each transport.
    return
    [
        WithCredentials($"Endpoint=http://localhost:{DefaultGrpcPort};Database={db};Protocol=grpc", user, password, token),
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

// Appends an empty Database= key when the connection string doesn't already carry one.
static string EnsureDatabase(string connectionString)
{
    return HasKey(connectionString, "Database")
        ? connectionString
        : connectionString.TrimEnd(';') + ";Database=";
}

// Returns the connection string with the given transport pinned via Protocol=, replacing any
// existing Protocol= key.
static string WithProtocol(string connectionString, string protocol)
{
    return WithKey(connectionString, "Protocol", protocol);
}

// Returns the connection string with key=value set, replacing any existing occurrence of the key.
static string WithKey(string connectionString, string key, string value)
{
    List<string> parts = connectionString
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Where(p => !p.StartsWith(key + "=", StringComparison.OrdinalIgnoreCase))
        .ToList();

    parts.Add($"{key}={value}");
    return string.Join(';', parts);
}

static bool HasKey(string connectionString, string key)
{
    return connectionString
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Any(p => p.StartsWith(key + "=", StringComparison.OrdinalIgnoreCase));
}

// Returns the value of a connection-string key, or null when the key is absent.
static string? GetConnValue(string connectionString, string key)
{
    return connectionString
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries))
        .Where(p => p.Length == 2 && string.Equals(p[0], key, StringComparison.OrdinalIgnoreCase))
        .Select(p => p[1])
        .FirstOrDefault();
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

// Produces a connection string for the same endpoint/transport but with no database selected,
// used to reach system-level commands. Protocol= and Endpoint= are preserved so the transport
// and port stay consistent with the live connection.
static string GetEndpointConnectionString(string connectionString)
{
    return SwapDatabase(connectionString, "");
}

static string SwapDatabase(string connectionString, string newDatabase)
{
    List<string> parts = connectionString
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Where(p => !p.StartsWith("Database=", StringComparison.OrdinalIgnoreCase))
        .ToList();

    parts.Add($"Database={newDatabase}");
    return string.Join(';', parts);
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
    AnsiConsole.MarkupLine("       camus-cli workload <init|run> <bank|northwind|factory|tpcc> [[options]]");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Options:[/]");
    AnsiConsole.MarkupLine("  database                      Database name to connect to (default: test)");
    AnsiConsole.MarkupLine("  -c, --connection-source       Connection string (default: gRPC on http://localhost:5096,");
    AnsiConsole.MarkupLine("                                falling back to REST on http://localhost:5095)");
    AnsiConsole.MarkupLine("  -e, --execute                 Execute a SQL statement (or ;-separated statements) and exit");
    AnsiConsole.MarkupLine("  -u, --user                    User to authenticate as (only needed on a server with");
    AnsiConsole.MarkupLine("                                authentication enabled)");
    AnsiConsole.MarkupLine("  -p, --password                That user's password; prompted for when -u is given without it");
    AnsiConsole.MarkupLine("  --token                       Use a bearer token obtained elsewhere instead of logging in");
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
    AnsiConsole.MarkupLine("  [cyan]workload init[/] <bank|northwind|factory|tpcc>  Create schema and seed data for a workload");
    AnsiConsole.MarkupLine("  [cyan]workload run[/]  <bank|northwind|factory|tpcc>  Run a continuous workload against the database");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Workload options:[/]");
    AnsiConsole.MarkupLine("  -c, --connection-source       Connection string");
    AnsiConsole.MarkupLine("  --database                    Target database name (default: demo)");
    AnsiConsole.MarkupLine("  --rows N                      Rows to generate for init (default: 1000, bank only)");
    AnsiConsole.MarkupLine("  --concurrency N               Parallel workers for run (default: 3)");
    AnsiConsole.MarkupLine("  --duration N                  Run duration in seconds (default: 60)");
    AnsiConsole.MarkupLine("  --locking MODE                Locking mode: optimistic | pessimistic (default: optimistic)");
    AnsiConsole.MarkupLine("  --isolation LEVEL             Isolation level: serializable | read-committed (default: serializable)");
    AnsiConsole.MarkupLine("  -u, --user / -p, --password   Credentials for an authenticated server");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Examples:[/]");
    AnsiConsole.MarkupLine("  camus-cli mydb");
    AnsiConsole.MarkupLine("  camus-cli mydb -u app -p app-secret");
    AnsiConsole.MarkupLine("  camus-cli mydb -u app                     [grey58](prompts for the password)[/]");
    AnsiConsole.MarkupLine("  camus-cli -c \"Endpoint=http://localhost:5096;Database=mydb;Protocol=grpc\"");
    AnsiConsole.MarkupLine("  camus-cli -c \"Endpoint=http://localhost:5095;Database=mydb;Protocol=rest\"");
    AnsiConsole.MarkupLine("  camus-cli mydb -e \"SELECT * FROM users\"");
    AnsiConsole.MarkupLine("  camus-cli workload init bank --database demo --rows 5000");
    AnsiConsole.MarkupLine("  camus-cli workload run northwind --concurrency 5 --duration 120");
    AnsiConsole.MarkupLine("  camus-cli workload init factory --database factory");
    AnsiConsole.MarkupLine("  camus-cli workload run factory --concurrency 4 --duration 120");
    AnsiConsole.MarkupLine("  camus-cli workload init tpcc --database tpcc --rows 1");
    AnsiConsole.MarkupLine("  camus-cli workload run tpcc --concurrency 4 --duration 120");
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

    [Option('u', "user", Required = false, HelpText = "User to authenticate as")]
    public string? User { get; set; }

    [Option('p', "password", Required = false, HelpText = "That user's password (prompted for when omitted)")]
    public string? Password { get; set; }

    [Option("token", Required = false, HelpText = "Use a bearer token obtained elsewhere instead of logging in")]
    public string? AccessToken { get; set; }
}
