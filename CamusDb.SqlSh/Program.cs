
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
using System.Text.Json;

Console.WriteLine("CamusDB SQL Shell 0.0.3\n");

ParserResult<Options> optsResult = Parser.Default.ParseArguments<Options>(args);

Options? opts = optsResult.Value;
if (opts is null)
    return;

string historyPath = Path.GetTempPath() + Path.PathSeparator + "camusdb.history.json";

List<string>? history = await GetHistory(historyPath);

CamusConnection connection = await GetConnection(opts);

LineEditor? editor = null;

if (LineEditor.IsSupported(AnsiConsole.Console))
{
    editor = new()
    {
        MultiLine = false,
        Text = "",
        Prompt = new MyLineNumberPrompt(new Style(foreground: Color.Yellow, background: Color.Black)),
        //Completion = new TestCompletion(),        
        Highlighter = new WordHighlighter()
                        .AddWord("select", new Style(foreground: Color.Blue))
                        .AddWord("update", new Style(foreground: Color.Blue))
                        .AddWord("from", new Style(foreground: Color.Blue))
                        .AddWord("where", new Style(foreground: Color.Blue))
                        .AddWord("order", new Style(foreground: Color.Blue))
                        .AddWord("by", new Style(foreground: Color.Blue))
                        .AddWord("table", new Style(foreground: Color.Blue))
                        .AddWord("set", new Style(foreground: Color.Blue))
                        .AddWord("create", new Style(foreground: Color.Blue))
                        .AddWord("primary", new Style(foreground: Color.Blue))
                        .AddWord("key", new Style(foreground: Color.Blue))
                        .AddWord("index", new Style(foreground: Color.Blue))
                        .AddWord("limit", new Style(foreground: Color.Blue))
                        .AddWord("insert", new Style(foreground: Color.Blue))
                        .AddWord("into", new Style(foreground: Color.Blue))
                        .AddWord("values", new Style(foreground: Color.Blue))
                        .AddWord("delete", new Style(foreground: Color.Blue))
                        .AddWord("alter", new Style(foreground: Color.Blue))
                        .AddWord("column", new Style(foreground: Color.Blue))
                        .AddWord("drop", new Style(foreground: Color.Blue))
                        .AddWord("null", new Style(foreground: Color.Blue))
                        .AddWord("not", new Style(foreground: Color.Blue))
                        .AddWord("string", new Style(foreground: Color.Blue))
                        .AddWord("int64", new Style(foreground: Color.Blue))
                        .AddWord("float", new Style(foreground: Color.Blue))
                        .AddWord("oid", new Style(foreground: Color.Blue))
                        .AddWord("is", new Style(foreground: Color.Blue))
                        .AddWord("add", new Style(foreground: Color.Blue))
                        .AddWord("show", new Style(foreground: Color.Blue))
                        .AddWord("use", new Style(foreground: Color.Blue))
                        .AddWord("tables", new Style(foreground: Color.Blue))
                        .AddWord("view", new Style(foreground: Color.Blue))
                        .AddWord("views", new Style(foreground: Color.Blue))
                        .AddWord("columns", new Style(foreground: Color.Blue))
    };

    if (history != null)
    {
        foreach (string item in history)
            editor.History.Add(item);
    }
}

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

        if (sql == "exit")
        {
            await SaveHistory(historyPath, history);
            break;
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
        else
            await ExecuteNonQuery(connection, sql);
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", ex.GetType().Name, ex.Message);
    }
}

static async Task SaveHistory(string historyPath, List<string>? history)
{
    if (history is not null)
        await File.WriteAllTextAsync(historyPath, JsonSerializer.Serialize(history));
}

static async Task ExecuteNonQuery(CamusConnection connection, string sql)
{
    using CamusCommand cmd = connection.CreateCamusCommand(sql);

    Stopwatch stopwatch = Stopwatch.StartNew();

    int affected = await cmd.ExecuteNonQueryAsync();

    if (affected == 1)
        AnsiConsole.MarkupLine("Query OK, [blue]{0}[/] rows affected ({1})\n", affected, stopwatch.Elapsed);
    else if (affected > 1)
        AnsiConsole.MarkupLine("Query OK, [blue]{0}[/] rows affected ({1})\n", affected, stopwatch.Elapsed);
    else
        AnsiConsole.MarkupLine("Query OK, [yellow]{0}[/] rows affected ({1})\n", affected, stopwatch.Elapsed);
}

static async Task ExecuteQuery(CamusConnection connection, string sql)
{
    using CamusCommand cmd = connection.CreateSelectCommand(sql);

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
            else if (item.Value.Type == ColumnType.Float)
                row[i++] = item.Value.LongValue.ToString();
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

    Stopwatch stopwatch = Stopwatch.StartNew();

    bool success = await cmd.ExecuteDDLAsync();

    if (success)        
        AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", stopwatch.Elapsed);    
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
