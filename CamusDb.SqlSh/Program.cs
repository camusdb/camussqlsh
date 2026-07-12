
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

string historyPath = Path.GetTempPath() + Path.PathSeparator + "camusdb.history.json";

List<string>? history = await GetHistory(historyPath);

string activeConnectionString = BuildConnectionString(opts);
CamusConnection connection = await ConnectionHelper.OpenAsync(activeConnectionString);

LineEditor? editor = null;
CamusTransaction? transaction = null;
SqlCompletion? sqlCompletion = null;

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
        "limit",
        "insert",
        "into",
        "values",
        "delete",
        "alter",
        "rename",
        "column",
        "drop",
        "null",
        "not",
        "string",
        "char",
        "varchar",
        "int",
        "int64",
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
        "view",
        "views",
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
        "as",
        "distinct",
        "cast",
        "integer",
        "double",
        "float",
        "databases",
        "to",
        "at",
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
        "to_string",
        "to_int64",
        "to_float64",
        "to_bool",
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

        if (string.IsNullOrWhiteSpace(sql))
            continue;

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

        // Add some history
        if (editor is not null)
            editor.History.Add(executableSql);

        AddHistory(history, executableSql);

        await ExecuteSql(connection, executableSql);

        if (sqlCompletion is not null && ChangesTableSet(executableSql) && HasDatabase(activeConnectionString))
            await sqlCompletion.RefreshTablesAsync(connection);
    }
    catch (Exception ex)
    {
        string errorCode = ex is CamusException ce ? ce.Code : $"0x{ex.HResult:X8}";
        AnsiConsole.MarkupLine("[red]{0}[/] ([grey]{1}[/]): {2}", Markup.Escape(ex.GetType().Name), Markup.Escape(errorCode), Markup.Escape(ex.Message));
        WriteErrorCaret(lastSql, ex.Message);
        AnsiConsole.WriteLine();

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
    foreach ((string sql, bool vertical) in EscapeStringIntoLines(input))
    {
        if (string.IsNullOrWhiteSpace(sql))
            continue;

        bool needsDb = !IsDatabaseDDL(sql) && !IsSystemLevelQuery(sql);
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
        else if (IsDatabaseDDL(sql))
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
           trimmedSql.StartsWith("show branches from ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("show ancestors from ", StringComparison.InvariantCultureIgnoreCase);
}

static bool IsDatabaseDDL(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("create database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("rename database ", StringComparison.InvariantCultureIgnoreCase);
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

    return trimmedSql.StartsWith("create database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("rename database ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("create table ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("create index ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop table ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop index ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("alter table ", StringComparison.InvariantCultureIgnoreCase);
}

static string BuildConnectionString(Options opts)
{
    if (!string.IsNullOrEmpty(opts.ConnectionSource))
    {
        string cs = opts.ConnectionSource;
        bool hasDatabase = cs.Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Any(p => p.StartsWith("Database=", StringComparison.OrdinalIgnoreCase));
        return hasDatabase ? cs : cs.TrimEnd(';') + ";Database=";
    }

    return string.IsNullOrWhiteSpace(opts.Database)
        ? "Endpoint=http://localhost:5095;Database="
        : $"Endpoint=http://localhost:5095;Database={opts.Database}";
}

static string GetEndpointConnectionString(string connectionString)
{
    string? endpoint = connectionString
        .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
        .Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries))
        .Where(p => p.Length == 2 && string.Equals(p[0], "Endpoint", StringComparison.OrdinalIgnoreCase))
        .Select(p => p[1])
        .FirstOrDefault();

    return $"Endpoint={endpoint ?? "http://localhost:5095"};Database=";
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
    AnsiConsole.MarkupLine("  -c, --connection-source       Connection string (default: Endpoint=http://localhost:5095;Database=test)");
    AnsiConsole.MarkupLine("  --force-rich                  Force the rich line editor (colors, multiline, Tab completion)");
    AnsiConsole.MarkupLine("                                on terminals whose TERM value Spectre.Console doesn't recognize");
    AnsiConsole.MarkupLine("  --diagnose-terminal           Print terminal capabilities and exit (why rich mode is on/off)");
    AnsiConsole.MarkupLine("  -h, --help                    Show this help message");
    AnsiConsole.MarkupLine("  -v, --version                 Show version information");
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Environment variables:[/]");
    AnsiConsole.MarkupLine("  CAMUS_FORCE_RICH=1            Same as [cyan]--force-rich[/] (accepts 1/true/yes)");
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
    AnsiConsole.WriteLine();
    AnsiConsole.MarkupLine("[bold]Examples:[/]");
    AnsiConsole.MarkupLine("  camus-cli mydb");
    AnsiConsole.MarkupLine("  camus-cli -c \"Endpoint=http://localhost:5095;Database=mydb\"");
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
}
