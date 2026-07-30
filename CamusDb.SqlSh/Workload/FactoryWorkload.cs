
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;
using static WorkloadHelpers;

internal static class FactoryWorkload
{
    private const int BatchSize = 5;

    // -------------------------------------------------------------------------
    // SQL templates — every value travels as a bound parameter. GEN_ID() stays in the
    // text because it's a server-side function call, not a value.
    // -------------------------------------------------------------------------

    private const string RobotInsertSql =
        "INSERT INTO robots (id, name, kind, year) VALUES (GEN_ID(), @name, @kind, @year)";

    private const string CustomerInsertSql =
        "INSERT INTO customers (id, name, email, country) VALUES (GEN_ID(), @name, @email, @country)";

    private const string InventoryInsertSql =
        "INSERT INTO inventory (id, robot_id, quantity, unit_price) VALUES (GEN_ID(), @robot_id, @quantity, @unit_price)";

    private const string OrderInsertSql =
        "INSERT INTO orders (id, customer_id, robot_id, status, order_date, total) " +
        "VALUES (GEN_ID(), @customer_id, @robot_id, @status, @order_date, @total)";

    private const string OwnershipInsertSql =
        "INSERT INTO robot_ownership (id, robot_id, customer_id, acquired_date) " +
        "VALUES (GEN_ID(), @robot_id, @customer_id, @acquired_date)";

    private const string InventoryByKindSql =
        "SELECT r.name, i.quantity, i.unit_price FROM robots r JOIN inventory i ON r.id = i.robot_id WHERE r.kind = @kind";

    private const string OrdersByCustomerSql =
        "SELECT id, robot_id, status, order_date, total FROM orders WHERE customer_id = @customer_id";

    private const string DecrementInventorySql =
        "UPDATE inventory SET quantity = quantity - 1 WHERE robot_id = @robot_id AND quantity > 0";

    private const string RestockInventorySql =
        "UPDATE inventory SET quantity = quantity + @quantity WHERE robot_id = @robot_id";

    /// <summary>
    /// Every statement the <c>run</c> phase issues, for the prepared-statement warm-up. Keep in sync
    /// with <see cref="RunAsync"/>: a statement missing here isn't broken, it just pays the driver's
    /// usual two-execution warm-up instead of being registered before the clock starts.
    /// </summary>
    internal static IReadOnlyList<string> RunStatements =>
    [
        InventoryByKindSql, OrdersByCustomerSql, OrderInsertSql,
        DecrementInventorySql, OwnershipInsertSql, RestockInventorySql,
    ];

    private static readonly string[] Statuses = ["pending", "shipped", "delivered", "cancelled"];

    private static readonly string[] Kinds =
        ["android", "protocol", "astromech", "ai", "cleanup", "probe", "combat", "service", "medical"];

    private static readonly double[] Prices =
        [4999.99, 8999.00, 3500.00, 12000.00, 2200.00, 7800.00, 15000.00, 9500.00, 1800.00, 5500.00];

    // -------------------------------------------------------------------------
    // Init
    // -------------------------------------------------------------------------

    internal static async Task InitAsync(CamusConnection conn, int concurrency, CamusTransactionOptions txOptions)
    {
        AnsiConsole.MarkupLine("[cyan]Creating factory schema...[/]");

        string[] ddls =
        [
            """
            CREATE TABLE IF NOT EXISTS robots (
              id OID PRIMARY KEY NOT NULL,
              name STRING NOT NULL,
              kind STRING NOT NULL,
              year INT64 DEFAULT (2024)
            )
            """,
            "CREATE TABLE IF NOT EXISTS customers (id OID PRIMARY KEY NOT NULL, name STRING NOT NULL, email STRING NOT NULL, country STRING NOT NULL)",
            "CREATE TABLE IF NOT EXISTS robot_ownership (id OID PRIMARY KEY NOT NULL, robot_id OID NOT NULL, customer_id OID NOT NULL, acquired_date STRING NOT NULL)",
            "CREATE TABLE IF NOT EXISTS orders (id OID PRIMARY KEY NOT NULL, customer_id OID NOT NULL, robot_id OID NOT NULL, status STRING NOT NULL, order_date STRING NOT NULL, total FLOAT64 NOT NULL)",
            "CREATE TABLE IF NOT EXISTS inventory (id OID PRIMARY KEY NOT NULL, robot_id OID NOT NULL, quantity INT64 NOT NULL, unit_price FLOAT64 NOT NULL)",
        ];

        foreach (string ddl in ddls)
            await DDL(conn, ddl);

        // Index the run-phase predicate columns. A non-indexed predicate takes a [-inf,+inf] range lock
        // over the whole table, which serializes the workers however many of them there are.
        string[] indexes =
        [
            "ALTER TABLE robots ADD INDEX idx_robots_kind (kind)",
            "ALTER TABLE inventory ADD INDEX idx_inventory_robot (robot_id)",
            "ALTER TABLE orders ADD INDEX idx_orders_customer (customer_id)",
            "ALTER TABLE robot_ownership ADD INDEX idx_ownership_robot (robot_id)",
        ];

        foreach (string index in indexes)
        {
            // A re-run of init hits an already-created index; that's not a failure.
            try { await DDL(conn, index); } catch (CamusException) { }
        }

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Clearing existing data...");
        foreach (string t in new[] { "robot_ownership", "orders", "inventory", "customers", "robots" })
            await Exec(conn, $"DELETE FROM {t} WHERE id IS NOT NULL");

        AnsiConsole.MarkupLine("Inserting seed data ([blue]{0}[/] parallel writers)...\n", concurrency);

        (string Name, string Kind, int Year)[] robots =
        [
            ("C-3PO",    "protocol",  1977),
            ("T-800",    "android",   1984),
            ("R2-D2",    "astromech", 1977),
            ("HAL 9000", "ai",        1968),
            ("WALL-E",   "cleanup",   2008),
            ("EVE",      "probe",     2008),
            ("Optimus",  "combat",    1984),
            ("Data",     "android",   1987),
            ("Bender",   "service",   1999),
            ("Baymax",   "medical",   2014),
        ];

        (string Name, string Email, string Country)[] customers =
        [
            ("Alice Nakamura", "alice@example.com",  "Japan"),
            ("Bob Steiner",    "bob@example.com",    "Germany"),
            ("Clara Fontaine", "clara@example.com",  "France"),
            ("Diego Herrera",  "diego@example.com",  "Mexico"),
            ("Elena Volkov",   "elena@example.com",  "Russia"),
            ("Frank Osei",     "frank@example.com",  "Ghana"),
            ("Grace Kim",      "grace@example.com",  "South Korea"),
            ("Hector Perez",   "hector@example.com", "Spain"),
            ("Ingrid Larsson", "ingrid@example.com", "Sweden"),
            ("James O'Brien",  "james@example.com",  "Ireland"),
            ("Keiko Tanaka",   "keiko@example.com",  "Japan"),
            ("Luca Bianchi",   "luca@example.com",   "Italy"),
        ];

        Random rng = new(42);

        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .HideCompleted(false)
            .StartAsync(async ctx =>
            {
                ProgressTask tRob = ctx.AddTask("[green]robots    [/]", maxValue: robots.Length);
                ProgressTask tCus = ctx.AddTask("[green]customers [/]", maxValue: customers.Length);
                ProgressTask tInv = ctx.AddTask("[green]inventory [/]", maxValue: robots.Length);
                ProgressTask tOrd = ctx.AddTask("[green]orders    [/]", maxValue: 20);
                ProgressTask tOwn = ctx.AddTask("[green]ownership [/]", maxValue: 10);

                await Seed(conn, txOptions, concurrency, robots.Select(r => (RobotInsertSql, new Param[]
                {
                    P("@name", ColumnType.String, r.Name),
                    P("@kind", ColumnType.String, r.Kind),
                    P("@year", ColumnType.Integer64, (long)r.Year),
                })).ToList(), tRob);

                await Seed(conn, txOptions, concurrency, customers.Select(c => (CustomerInsertSql, new Param[]
                {
                    P("@name", ColumnType.String, c.Name),
                    P("@email", ColumnType.String, c.Email),
                    P("@country", ColumnType.String, c.Country),
                })).ToList(), tCus);

                // The server generated the OIDs, so read them back before seeding the rows that reference them.
                List<string> robotIds = await LoadIds(conn, "robots");
                List<string> customerIds = await LoadIds(conn, "customers");

                // inventory — one entry per robot
                await Seed(conn, txOptions, concurrency, robotIds.Select((robotId, i) => (InventoryInsertSql, new Param[]
                {
                    P("@robot_id", ColumnType.Id, robotId),
                    P("@quantity", ColumnType.Integer64, (long)rng.Next(5, 51)),
                    P("@unit_price", ColumnType.Float64, Prices[i % Prices.Length]),
                })).ToList(), tInv);

                // seed orders
                List<(string Sql, Param[] Parameters)> orders = [];
                for (int i = 0; i < 20; i++)
                {
                    orders.Add((OrderInsertSql,
                    [
                        P("@customer_id", ColumnType.Id, customerIds[rng.Next(customerIds.Count)]),
                        P("@robot_id", ColumnType.Id, robotIds[rng.Next(robotIds.Count)]),
                        P("@status", ColumnType.String, Statuses[rng.Next(Statuses.Length)]),
                        P("@order_date", ColumnType.String, new DateTime(2024, rng.Next(1, 13), rng.Next(1, 28)).ToString("yyyy-MM-dd")),
                        P("@total", ColumnType.Float64, Prices[rng.Next(Prices.Length)]),
                    ]));
                }
                await Seed(conn, txOptions, concurrency, orders, tOrd);

                // seed robot_ownership (10 delivered robots assigned to customers)
                List<(string Sql, Param[] Parameters)> ownership = [];
                for (int i = 0; i < 10; i++)
                {
                    ownership.Add((OwnershipInsertSql,
                    [
                        P("@robot_id", ColumnType.Id, robotIds[i % robotIds.Count]),
                        P("@customer_id", ColumnType.Id, customerIds[rng.Next(customerIds.Count)]),
                        P("@acquired_date", ColumnType.String, new DateTime(2024, rng.Next(1, 13), rng.Next(1, 28)).ToString("yyyy-MM-dd")),
                    ]));
                }
                await Seed(conn, txOptions, concurrency, ownership, tOwn);
            });

        AnsiConsole.MarkupLine("\n[green]Factory workload initialized.[/]");
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run factory[/] to start generating activity.");
    }

    private static Task Seed(
        CamusConnection conn,
        CamusTransactionOptions txOptions,
        int concurrency,
        List<(string Sql, Param[] Parameters)> statements,
        ProgressTask progress)
    {
        return ForEachAsync(Chunk(statements, BatchSize), concurrency, async batch =>
        {
            await RunSeedBatch(conn, txOptions, batch);
            progress.Increment(batch.Count);
        });
    }

    // -------------------------------------------------------------------------
    // Run
    // -------------------------------------------------------------------------

    internal static async Task RunAsync(CamusConnection conn, int concurrency, int durationSeconds, CamusTransactionOptions txOptions)
    {
        List<string> robotIds = await LoadIds(conn, "robots");
        List<string> customerIds = await LoadIds(conn, "customers");

        if (robotIds.Count == 0 || customerIds.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]No robots or customers found.[/] Run [blue]camus-cli workload init factory[/] first.");
            return;
        }

        AnsiConsole.MarkupLine("Starting factory workload: [blue]{0}[/] workers, [blue]{1}s[/] duration, [blue]{2}[/] robots, [blue]{3}[/] customers",
            concurrency, durationSeconds, robotIds.Count, customerIds.Count);
        AnsiConsole.MarkupLine("Press [grey]Ctrl+C[/] to stop early.\n");

        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(durationSeconds));
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        long totalOps = 0;
        long totalErrors = 0;
        Stopwatch sw = Stopwatch.StartNew();

        Task statsPrinter = Task.Run(async () =>
        {
            long lastOps = 0;
            while (!cts.Token.IsCancellationRequested)
            {
                try { await Task.Delay(1000, cts.Token); } catch { break; }
                long ops = Interlocked.Read(ref totalOps);
                long errs = Interlocked.Read(ref totalErrors);
                AnsiConsole.MarkupLine("  {0,6:F1}s  ops: [green]{1,8}[/]  ops/sec: [green]{2,6}[/]  errors: [red]{3}[/]",
                    sw.Elapsed.TotalSeconds, ops, ops - lastOps, errs);
                lastOps = ops;
            }
        });

        Task[] workers = Enumerable.Range(0, concurrency).Select(workerIdx => Task.Run(async () =>
        {
            Random rng = new(workerIdx);
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    int op = rng.Next(6);
                    string robotId = robotIds[rng.Next(robotIds.Count)];
                    string custId = customerIds[rng.Next(customerIds.Count)];

                    if (op <= 1)
                    {
                        // read: list inventory for a robot kind
                        await QueryWithParams(conn, InventoryByKindSql,
                        [
                            P("@kind", ColumnType.String, Kinds[rng.Next(Kinds.Length)]),
                        ], ct: cts.Token);
                    }
                    else if (op == 2)
                    {
                        // read: list orders for a customer
                        await QueryWithParams(conn, OrdersByCustomerSql,
                        [
                            P("@customer_id", ColumnType.Id, custId),
                        ], ct: cts.Token);
                    }
                    else if (op == 3)
                    {
                        // write: place a new order and decrement inventory
                        CamusTransaction tx = await conn.BeginTransactionAsync(txOptions, cts.Token);
                        try
                        {
                            await ExecWithParams(conn, OrderInsertSql,
                            [
                                P("@customer_id", ColumnType.Id, custId),
                                P("@robot_id", ColumnType.Id, robotId),
                                P("@status", ColumnType.String, "pending"),
                                P("@order_date", ColumnType.String, DateTime.UtcNow.ToString("yyyy-MM-dd")),
                                P("@total", ColumnType.Float64, Prices[rng.Next(Prices.Length)]),
                            ], tx, cts.Token);

                            await ExecWithParams(conn, DecrementInventorySql,
                            [
                                P("@robot_id", ColumnType.Id, robotId),
                            ], tx, cts.Token);

                            await tx.CommitAsync(cts.Token);
                        }
                        catch
                        {
                            try { await tx.RollbackAsync(); } catch { }
                            throw;
                        }
                    }
                    else if (op == 4)
                    {
                        // write: mark a delivered order as ownership transfer
                        await ExecWithParams(conn, OwnershipInsertSql,
                        [
                            P("@robot_id", ColumnType.Id, robotId),
                            P("@customer_id", ColumnType.Id, custId),
                            P("@acquired_date", ColumnType.String, DateTime.UtcNow.ToString("yyyy-MM-dd")),
                        ], ct: cts.Token);
                    }
                    else
                    {
                        // write: restock inventory
                        await ExecWithParams(conn, RestockInventorySql,
                        [
                            P("@quantity", ColumnType.Integer64, (long)rng.Next(1, 11)),
                            P("@robot_id", ColumnType.Id, robotId),
                        ], ct: cts.Token);
                    }

                    Interlocked.Increment(ref totalOps);
                }
                catch
                {
                    Interlocked.Increment(ref totalErrors);
                }
            }
        })).ToArray();

        await Task.WhenAll(workers);
        await statsPrinter;

        double elapsed = sw.Elapsed.TotalSeconds;
        AnsiConsole.MarkupLine("\n[green]Done:[/] {0} ops in {1:F1}s ({2:F1} ops/sec), {3} errors",
            totalOps, elapsed, elapsed > 0 ? totalOps / elapsed : 0, totalErrors);
    }

    private static async Task<List<string>> LoadIds(CamusConnection conn, string table)
    {
        List<string> ids = [];
        using CamusCommand cmd = conn.CreateSelectCommand($"SELECT id FROM {table}");
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            if (row.TryGetValue("id", out ColumnValue val) && !string.IsNullOrEmpty(val.StrValue))
                ids.Add(val.StrValue);
        }
        return ids;
    }
}
