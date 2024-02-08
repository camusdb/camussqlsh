
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
using System.Text;
using System.Text.Json;

ParserResult<Options> optsResult = Parser.Default.ParseArguments<Options>(args);

Options? opts = optsResult.Value;
if (opts is null)
    return;

Console.WriteLine("CamusDB SQL Shell 0.0.10 (alpha)\n");

string historyPath = Path.GetTempPath() + Path.PathSeparator + "camusdb.history.json";

List<string>? history = await GetHistory(historyPath);

CamusConnection connection = await GetConnection(opts);

LineEditor? editor = null;
CamusTransaction? transaction = null;

if (LineEditor.IsSupported(AnsiConsole.Console))
{
    string[] keywords = new string[] {
        "select",
        "update",
        "from",
        "where",
        "order",
        "by",
        "asc",
        "desc",
        "table",
        "set",
        "create",
        "primary",
        "key",
        "index",
        "indexes",
        "limit",
        "insert",
        "into",
        "values",
        "delete",
        "alter",
        "column",
        "drop",
        "null",
        "not",
        "string",
        "int64",
        "float64",
        "oid",
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
        "offset",
        "unique",
        "having",
        "begin",
        "start",
        "transaction",
        "commit",
        "rollback",
    };

    string[] functions = new string[] {
        "count",
        "distinct",
        "max",
        "min",
        "avg",
        "sum"
    };

    string[] commands = new string[] {
        "clear",
        "source",
        "use",
        "exit",
    };

    WordHighlighter worldHighlighter = new();

    Style funcStyle = new(foreground: Color.Aqua);
    Style keywordStyle = new(foreground: Color.Blue);
    Style commandStyle = new(foreground: Color.LightSkyBlue1);

    foreach (string keyword in keywords)
        worldHighlighter.AddWord(keyword, keywordStyle);

    foreach (string func in functions)
        worldHighlighter.AddWord(func, funcStyle);

    foreach (string command in commands)
        worldHighlighter.AddWord(command, commandStyle);

    editor = new()
    {
        MultiLine = false,
        Text = "",
        Prompt = new MyLineNumberPrompt(new Style(foreground: Color.PaleTurquoise1)),
        //Completion = new TestCompletion(),        
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

while (true)
{
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

        if (string.Equals(sqlTrim, "exit", StringComparison.InvariantCultureIgnoreCase))
        {
            if (transaction is not null)
            {
                AnsiConsole.MarkupLine("[red]There's an active transaction, please commit or rollback before exit[/]");
                continue;
            }

            await SaveHistory(historyPath, history);
            break;
        }

        if (string.Equals(sqlTrim, "clear", StringComparison.InvariantCultureIgnoreCase))
        {
            AnsiConsole.Clear();
            continue;
        }

        if (sqlTrim.StartsWith("source ", StringComparison.InvariantCultureIgnoreCase))
        {
            await LoadSource(connection, sqlTrim[7..].Trim());
            continue;
        }

        // Add some history
        if (editor is not null)
            editor.History.Add(sql);

        if (history is not null)
            history.Add(sql);

        if (IsQueryable(sql))
            await ExecuteQuery(connection, sql);
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
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", Markup.Escape(ex.GetType().Name), Markup.Escape(ex.Message));
    }
}

async Task LoadSource(CamusConnection connection, string paths)
{
    if (!File.Exists(paths))
    {
        AnsiConsole.Markup("[red]File not found: {0}[/]", Markup.Escape(paths));
        return;
    }

    int numberLine = 0;
    string fileContents = await File.ReadAllTextAsync(paths);

    foreach (string sql in EscapeStringIntoLines(fileContents))
    {
        if (string.IsNullOrEmpty(sql))
        {
            numberLine++;
            continue;
        }

        if (IsQueryable(sql))
            await ExecuteQuery(connection, sql);
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

        numberLine++;
    }
}

static IEnumerable<string> EscapeStringIntoLines(string input)
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
            yield return currentLine.ToString().Trim();
            currentLine.Clear();
        }
        else
        {
            currentLine.Append(c);
        }
    }

    if (currentLine.Length > 0)
        yield return currentLine.ToString().Trim();
}

static async Task SaveHistory(string historyPath, List<string>? history)
{
    if (history is not null)
        await File.WriteAllTextAsync(historyPath, JsonSerializer.Serialize(history));
}

async Task ExecuteNonQuery(CamusConnection connection, string sql)
{
    using CamusCommand cmd = connection.CreateCamusCommand(sql);

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

    await transaction.CommitAsync();

    AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", stopwatch.Elapsed);

    transaction = null;
}

async Task ExecuteRollbackTx(CamusConnection connection)
{
    if (transaction is null)
    {
        AnsiConsole.MarkupLine("[red]There's no active transaction[/]");
        return;
    }

    Stopwatch stopwatch = Stopwatch.StartNew();

    await transaction.RollbackAsync();

    AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", stopwatch.Elapsed);

    transaction = null;
}

async Task ExecuteQuery(CamusConnection connection, string sql)
{
    using CamusCommand cmd = connection.CreateSelectCommand(sql);

    cmd.CommandTimeout = 60;
    cmd.Transaction = transaction;

    int rows = 0;
    Table? table = null;

    Stopwatch stopwatch = Stopwatch.StartNew();

    CamusDataReader reader = await cmd.ExecuteReaderAsync();

    TimeSpan duration = stopwatch.Elapsed;

    while (await reader.ReadAsync())
    {
        Dictionary<string, ColumnValue> current = reader.GetCurrent();

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
        {
            if (item.Value.Type == ColumnType.Id)
                row[i++] = !string.IsNullOrEmpty(item.Value.StrValue) ? item.Value.StrValue!.ToString() : "";
            else if (item.Value.Type == ColumnType.String)
                row[i++] = !string.IsNullOrEmpty(item.Value.StrValue) ? Markup.Escape(item.Value.StrValue!.ToString()) : "";
            else if (item.Value.Type == ColumnType.Integer64)
                row[i++] = item.Value.LongValue.ToString();
            else if (item.Value.Type == ColumnType.Float64)
                row[i++] = item.Value.FloatValue.ToString();
            else if (item.Value.Type == ColumnType.Bool)
                row[i++] = item.Value.BoolValue.ToString();
            else
                row[i++] = "null";
        }

        table.AddRow(row);
        rows++;
    }

    if (table is not null)
        AnsiConsole.Write(table);

    AnsiConsole.MarkupLine("[blue]{0}[/] rows in set ({1})\n", rows, duration);
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
           trimmedSql.StartsWith("show ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("desc ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("describe ", StringComparison.InvariantCultureIgnoreCase);
}

static bool IsDDL(string sql)
{
    string trimmedSql = sql.Trim();

    return trimmedSql.StartsWith("create table ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("create index ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop table ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("drop index ", StringComparison.InvariantCultureIgnoreCase) ||
           trimmedSql.StartsWith("alter table ", StringComparison.InvariantCultureIgnoreCase);
}

static async Task<CamusConnection> GetConnection(Options opts)
{
    CamusConnection cmConnection;

    SessionPoolOptions options = new()
    {
        MinimumPooledSessions = 1,
        MaximumActiveSessions = 20,
    };

    string? connectionString = opts.ConnectionSource;

    if (string.IsNullOrEmpty(connectionString))
        connectionString = $"Endpoint=https://localhost:7141;Database=test";

    SessionPoolManager manager = SessionPoolManager.Create(options);

    CamusConnectionStringBuilder builder = new(connectionString)
    {
        SessionPoolManager = manager
    };

    cmConnection = new(builder);

    await cmConnection.OpenAsync();

    CamusPingCommand pingCommand = cmConnection.CreatePingCommand();

    await pingCommand.ExecuteNonQueryAsync();

    return cmConnection;
}

static async Task<List<string>> GetHistory(string historyPath)
{
    List<string>? history = new();

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

    history ??= new();

    return history;
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
        return (new Markup("camus> ", _style), 1);
    }
}

public sealed class Options
{
    [Option('c', "connection-source", Required = false, HelpText = "Set the connection string")]
    public string? ConnectionSource { get; set; }
}
