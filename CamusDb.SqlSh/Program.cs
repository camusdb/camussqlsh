
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using RadLine;
using Spectre.Console;
using System.Diagnostics;
using System.Text.Json;

Console.WriteLine("CamusDB 0.0.1\n");

List<string>? history = new();

string historyPath = Path.GetTempPath() + Path.PathSeparator + "camusdb.history.json";

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

(CamusConnection connection, CamusConnectionStringBuilder builder) = await GetConnection();

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
            await File.WriteAllTextAsync(historyPath, JsonSerializer.Serialize(history));
            break;
        }

        // Add some history
        if (editor is not null)
            editor.History.Add(sql);

        if (history is not null)
            history.Add(sql);

        if (sql.Trim().StartsWith("select ", StringComparison.InvariantCultureIgnoreCase))
            await ExecuteQuery(connection, sql);
        else
            await ExecuteNonQuery(builder, sql);
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", ex.GetType().Name, ex.Message);
    }
}

static async Task ExecuteNonQuery(CamusConnectionStringBuilder builder, string sql)
{
    using CamusCommand cmd = new CamusCommand(sql, builder);

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
            table = new();
            table.Border = TableBorder.Square;

            foreach (KeyValuePair<string, ColumnValue> item in current)
                table.AddColumn(item.Key);
        }

        string[] row = new string[current.Count];

        int i = 0;

        foreach (KeyValuePair<string, ColumnValue> item in current)
        {
            if (item.Value.Type == ColumnType.Id || item.Value.Type == ColumnType.String)
                row[i++] = !string.IsNullOrEmpty(item.Value.StrValue) ? item.Value.StrValue!.ToString() : "";
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

    Console.WriteLine("{0} rows in set ({1})\n", rows, duration);
}

static async Task<(CamusConnection, CamusConnectionStringBuilder)> GetConnection()
{
    CamusConnection cmConnection;

    SessionPoolOptions options = new()
    {
        MinimumPooledSessions = 100,
        MaximumActiveSessions = 200,
    };

    string connectionString = $"Endpoint=https://localhost:7141;Database=test";

    SessionPoolManager manager = SessionPoolManager.Create(options);

    CamusConnectionStringBuilder builder = new(connectionString)
    {
        SessionPoolManager = manager
    };

    cmConnection = new(builder);

    await cmConnection.OpenAsync();

    return (cmConnection, builder);
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

