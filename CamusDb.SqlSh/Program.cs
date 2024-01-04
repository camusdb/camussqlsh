
using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;

Console.WriteLine("CamusDB 0.0.1");

(CamusConnection connection, CamusConnectionStringBuilder builder) = await GetConnection();


while (true)
{
    try
    {
        var sql = AnsiConsole.Ask<string>("camus> ");
        //Console.WriteLine(sql);

        if (sql == "exit")
            break;

        if (sql.Trim().StartsWith("select ", StringComparison.InvariantCultureIgnoreCase))
            await ExecuteQuery(connection, sql);
        else
            await ExecuteNonQuery(connection, builder, sql);

    }
    catch (Exception ex)
    {
        Console.WriteLine("{0} {1}", ex.Message, ex.StackTrace);
    }
}

static async Task ExecuteNonQuery(CamusConnection connection, CamusConnectionStringBuilder builder, string sql)
{
    using CamusCommand cmd = new CamusCommand(sql, builder);

    Stopwatch stopwatch = new();

    int affected = await cmd.ExecuteNonQueryAsync();

    Console.WriteLine("Query OK, {0} row affected ({1})", affected, stopwatch.Elapsed);
}

static async Task ExecuteQuery(CamusConnection connection, string sql)
{
    using CamusCommand cmd = connection.CreateSelectCommand(sql);

    /*cmd.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.Generate());
    cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
    cmd.Parameters.Add("@type", ColumnType.String, "mechanical");
    cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));*/

    //Assert.Equal(1, await cmd.ExecuteNonQueryAsync());        

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

    Console.WriteLine("{0} rows in set ({1})", rows, duration);
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