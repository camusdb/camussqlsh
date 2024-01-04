
using CamusDB.Client;
using Spectre.Console;

Console.WriteLine("CamusDB 0.0.1");

CamusConnection connection = await GetConnection();


while (true)
{
    try
    {
        var sql = AnsiConsole.Ask<string>("camus> ");
        //Console.WriteLine(sql);

        if (sql == "exit")
            break;

        using CamusCommand cmd = connection.CreateSelectCommand(sql);

        /*cmd.Parameters.Add("@id", ColumnType.Id, CamusObjectIdGenerator.Generate());
        cmd.Parameters.Add("@name", ColumnType.String, Guid.NewGuid().ToString()[..20]);
        cmd.Parameters.Add("@type", ColumnType.String, "mechanical");
        cmd.Parameters.Add("@year", ColumnType.Integer64, Random.Shared.Next(1900, 2050));*/

        //Assert.Equal(1, await cmd.ExecuteNonQueryAsync());        

        Table? table = null;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            /*Console.WriteLine(reader.GetString(0));
            Console.WriteLine(reader.GetString(1));
            Console.WriteLine(reader.GetString(2));
            Console.WriteLine(reader.GetInt64(3));*/

            if (table is null)
            {
                table = new();
                table.Border = TableBorder.Square;

                // Add some columns
                table.AddColumn("Id");
                table.AddColumn("Name");
                table.AddColumn("Type");
                table.AddColumn("Year");
            }

            // Add some rows
            table.AddRow(new string[] { reader.GetString(0), reader.GetString(1), reader.GetString(2), reader.GetInt64(3).ToString() });            
        }

        if (table is not null)           
            AnsiConsole.Write(table);
    }
    catch (Exception ex)
    {
        Console.WriteLine("{0} {1}", ex.Message, ex.StackTrace);
    }
}

static async Task<CamusConnection> GetConnection()
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

    return cmConnection;
}