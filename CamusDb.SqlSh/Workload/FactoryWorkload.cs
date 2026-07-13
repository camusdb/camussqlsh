
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;

internal static class FactoryWorkload
{
    internal static async Task InitAsync(CamusConnection conn)
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
            await WorkloadHelpers.DDL(conn, ddl);

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Clearing existing data...");
        foreach (string t in new[] { "robot_ownership", "orders", "inventory", "customers", "robots" })
            await WorkloadHelpers.Exec(conn, $"DELETE FROM {t} WHERE id IS NOT NULL");

        AnsiConsole.MarkupLine("Inserting seed data...\n");

        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .HideCompleted(false)
            .StartAsync(async ctx =>
            {
                ProgressTask tRob = ctx.AddTask("[green]robots    [/]", maxValue: 10);
                ProgressTask tCus = ctx.AddTask("[green]customers [/]", maxValue: 12);
                ProgressTask tInv = ctx.AddTask("[green]inventory [/]", maxValue: 10);
                ProgressTask tOrd = ctx.AddTask("[green]orders    [/]", maxValue: 20);
                ProgressTask tOwn = ctx.AddTask("[green]ownership [/]", maxValue: 10);

                // robots — exact values from spec plus additional ones
                (string name, string kind, int year)[] robots =
                [
                    ("C-3PO",     "protocol",  1977),
                    ("T-800",     "android",   1984),
                    ("R2-D2",     "astromech", 1977),
                    ("HAL 9000",  "ai",        1968),
                    ("WALL-E",    "cleanup",   2008),
                    ("EVE",       "probe",     2008),
                    ("Optimus",   "combat",    1984),
                    ("Data",      "android",   1987),
                    ("Bender",    "service",   1999),
                    ("Baymax",    "medical",   2014),
                ];
                foreach (var r in robots)
                {
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO robots (id, name, kind, year) VALUES (GEN_ID(), \"{WorkloadHelpers.Esc(r.name)}\", \"{r.kind}\", {r.year})");
                    tRob.Increment(1);
                }

                // customers
                (string name, string email, string country)[] customers =
                [
                    ("Alice Nakamura",    "alice@example.com",   "Japan"),
                    ("Bob Steiner",       "bob@example.com",     "Germany"),
                    ("Clara Fontaine",    "clara@example.com",   "France"),
                    ("Diego Herrera",     "diego@example.com",   "Mexico"),
                    ("Elena Volkov",      "elena@example.com",   "Russia"),
                    ("Frank Osei",        "frank@example.com",   "Ghana"),
                    ("Grace Kim",         "grace@example.com",   "South Korea"),
                    ("Hector Perez",      "hector@example.com",  "Spain"),
                    ("Ingrid Larsson",    "ingrid@example.com",  "Sweden"),
                    ("James O'Brien",     "james@example.com",   "Ireland"),
                    ("Keiko Tanaka",      "keiko@example.com",   "Japan"),
                    ("Luca Bianchi",      "luca@example.com",    "Italy"),
                ];
                foreach (var c in customers)
                {
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO customers (id, name, email, country) VALUES (GEN_ID(), \"{WorkloadHelpers.Esc(c.name)}\", \"{c.email}\", \"{c.country}\")");
                    tCus.Increment(1);
                }

                // fetch robot IDs for inventory and orders seed
                List<string> robotIds = await LoadIds(conn, "robots");
                List<string> customerIds = await LoadIds(conn, "customers");

                Random rng = new(42);
                double[] prices = [4999.99, 8999.00, 3500.00, 12000.00, 2200.00, 7800.00, 15000.00, 9500.00, 1800.00, 5500.00];

                // inventory — one entry per robot
                for (int i = 0; i < robotIds.Count; i++)
                {
                    int qty = rng.Next(5, 51);
                    double price = prices[i % prices.Length];
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO inventory (id, robot_id, quantity, unit_price) VALUES (GEN_ID(), STR_ID(\"{robotIds[i]}\"), {qty}, {price.ToString("F2", System.Globalization.CultureInfo.InvariantCulture)})");
                    tInv.Increment(1);
                }

                // seed orders
                string[] statuses = ["pending", "shipped", "delivered", "cancelled"];
                for (int i = 0; i < 20; i++)
                {
                    string custId = customerIds[rng.Next(customerIds.Count)];
                    string robotId = robotIds[rng.Next(robotIds.Count)];
                    string status = statuses[rng.Next(statuses.Length)];
                    string orderDate = new DateTime(2024, rng.Next(1, 13), rng.Next(1, 28)).ToString("yyyy-MM-dd");
                    double total = prices[rng.Next(prices.Length)];
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO orders (id, customer_id, robot_id, status, order_date, total) VALUES (GEN_ID(), STR_ID(\"{custId}\"), STR_ID(\"{robotId}\"), \"{status}\", \"{orderDate}\", {total.ToString("F2", System.Globalization.CultureInfo.InvariantCulture)})");
                    tOrd.Increment(1);
                }

                // seed robot_ownership (10 delivered robots assigned to customers)
                List<string> deliveredRobots = robotIds.Take(10).ToList();
                for (int i = 0; i < 10; i++)
                {
                    string custId = customerIds[rng.Next(customerIds.Count)];
                    string robotId = deliveredRobots[i % deliveredRobots.Count];
                    string date = new DateTime(2024, rng.Next(1, 13), rng.Next(1, 28)).ToString("yyyy-MM-dd");
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO robot_ownership (id, robot_id, customer_id, acquired_date) VALUES (GEN_ID(), STR_ID(\"{robotId}\"), STR_ID(\"{custId}\"), \"{date}\")");
                    tOwn.Increment(1);
                }
            });

        AnsiConsole.MarkupLine("\n[green]Factory workload initialized.[/]");
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run factory[/] to start generating activity.");
    }

    internal static async Task RunAsync(CamusConnection conn, int concurrency, int durationSeconds)
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

        string[] statuses = ["pending", "shipped", "delivered", "cancelled"];
        double[] prices = [4999.99, 8999.00, 3500.00, 12000.00, 2200.00, 7800.00, 15000.00, 9500.00, 1800.00, 5500.00];

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
                        string[] kinds = ["android", "protocol", "astromech", "ai", "cleanup", "probe", "combat", "service", "medical"];
                        string kind = kinds[rng.Next(kinds.Length)];
                        using CamusCommand cmd = conn.CreateSelectCommand($"SELECT r.name, i.quantity, i.unit_price FROM robots r JOIN inventory i ON r.id = i.robot_id WHERE r.kind = \"{kind}\"");
                        cmd.CommandTimeout = 30;
                        CamusDataReader reader = await cmd.ExecuteReaderAsync();
                        while (await reader.ReadAsync()) { /* consume */ }
                    }
                    else if (op == 2)
                    {
                        // read: list orders for a customer
                        using CamusCommand cmd = conn.CreateSelectCommand($"SELECT id, robot_id, status, order_date, total FROM orders WHERE customer_id = STR_ID(\"{custId}\")");
                        cmd.CommandTimeout = 30;
                        CamusDataReader reader = await cmd.ExecuteReaderAsync();
                        while (await reader.ReadAsync()) { /* consume */ }
                    }
                    else if (op == 3)
                    {
                        // write: place a new order and decrement inventory
                        string status = "pending";
                        string orderDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
                        double total = prices[rng.Next(prices.Length)];

                        CamusTransaction tx = await conn.BeginTransactionAsync();
                        try
                        {
                            await WorkloadHelpers.Exec(conn, $"INSERT INTO orders (id, customer_id, robot_id, status, order_date, total) VALUES (GEN_ID(), STR_ID(\"{custId}\"), STR_ID(\"{robotId}\"), \"{status}\", \"{orderDate}\", {total.ToString("F2", System.Globalization.CultureInfo.InvariantCulture)})", tx);
                            await WorkloadHelpers.Exec(conn, $"UPDATE inventory SET quantity = quantity - 1 WHERE robot_id = STR_ID(\"{robotId}\") AND quantity > 0", tx);
                            await tx.CommitAsync();
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
                        string date = DateTime.UtcNow.ToString("yyyy-MM-dd");
                        await WorkloadHelpers.Exec(conn, $"INSERT INTO robot_ownership (id, robot_id, customer_id, acquired_date) VALUES (GEN_ID(), STR_ID(\"{robotId}\"), STR_ID(\"{custId}\"), \"{date}\")");
                    }
                    else
                    {
                        // write: restock inventory
                        int qty = rng.Next(1, 11);
                        await WorkloadHelpers.Exec(conn, $"UPDATE inventory SET quantity = quantity + {qty} WHERE robot_id = STR_ID(\"{robotId}\")");
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
        List<string> ids = new();
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
