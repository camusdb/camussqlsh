
/**
 * This file is part of CamusDB  
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;

Console.WriteLine("CamusDB 0.0.1\n");

(CamusConnection connection, CamusConnectionStringBuilder builder) = await GetConnection();

while (true)
{
    try
    {
        string sql = AnsiConsole.Prompt(
            new TextPrompt<string>("camus> ").            
            AllowEmpty()
        );
        
        if (string.IsNullOrWhiteSpace(sql))
            continue;

        if (sql == "exit")
            break;

        if (sql.Trim().StartsWith("select ", StringComparison.InvariantCultureIgnoreCase))
            await ExecuteQuery(connection, sql);
        else
            await ExecuteNonQuery(connection, builder, sql);
    }
    catch (Exception ex)
    {
        AnsiConsole.MarkupLine("[red]{0}[/]: {1}\n", ex.GetType().Name, ex.Message);        
    }
}

static async Task ExecuteNonQuery(CamusConnection connection, CamusConnectionStringBuilder builder, string sql)
{
    using CamusCommand cmd = new CamusCommand(sql, builder);

    Stopwatch stopwatch = new();

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