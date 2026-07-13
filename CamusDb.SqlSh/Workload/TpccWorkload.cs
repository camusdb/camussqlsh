
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;
using System.Globalization;

internal static class TpccWorkload
{
    private const int DistrictsPerWarehouse = 10;
    private const int CustomersPerDistrict = 30;
    private const int ItemCount = 1000;
    private const int BatchSize = 10;

    // -------------------------------------------------------------------------
    // SQL templates
    // -------------------------------------------------------------------------

    private const string WarehouseInsertSql =
        "INSERT INTO tpcc_warehouse (id, w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) " +
        "VALUES (GEN_ID(), @w_id, @w_name, @w_street_1, @w_street_2, @w_city, @w_state, @w_zip, @w_tax, @w_ytd)";

    private const string DistrictInsertSql =
        "INSERT INTO tpcc_district (id, d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) " +
        "VALUES (GEN_ID(), @d_id, @d_w_id, @d_name, @d_street_1, @d_street_2, @d_city, @d_state, @d_zip, @d_tax, @d_ytd, @d_next_o_id)";

    private const string CustomerInsertSql =
        "INSERT INTO tpcc_customer (id, c_id, c_d_id, c_w_id, c_first, c_middle, c_last, " +
        "c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, " +
        "c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
        "VALUES (GEN_ID(), @c_id, @c_d_id, @c_w_id, @c_first, @c_middle, @c_last, " +
        "@c_street_1, @c_street_2, @c_city, @c_state, @c_zip, @c_phone, @c_since, @c_credit, " +
        "@c_credit_lim, @c_discount, @c_balance, @c_ytd_payment, @c_payment_cnt, @c_delivery_cnt, @c_data)";

    private const string ItemInsertSql =
        "INSERT INTO tpcc_item (id, i_id, i_im_id, i_name, i_price, i_data) " +
        "VALUES (GEN_ID(), @i_id, @i_im_id, @i_name, @i_price, @i_data)";

    private const string StockInsertSql =
        "INSERT INTO tpcc_stock (id, s_i_id, s_w_id, s_quantity, " +
        "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
        "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, " +
        "s_ytd, s_order_cnt, s_remote_cnt, s_data) " +
        "VALUES (GEN_ID(), @s_i_id, @s_w_id, @s_quantity, " +
        "@s_dist_01, @s_dist_02, @s_dist_03, @s_dist_04, @s_dist_05, " +
        "@s_dist_06, @s_dist_07, @s_dist_08, @s_dist_09, @s_dist_10, " +
        "0.00, 0, 0, @s_data)";

    private const string OrderInsertSql =
        "INSERT INTO tpcc_orders (id, o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_carrier_id, o_ol_cnt, o_all_local) " +
        "VALUES (GEN_ID(), @o_id, @o_d_id, @o_w_id, @o_c_id, @o_entry_d, @o_carrier_id, @o_ol_cnt, @o_all_local)";

    private const string NewOrderInsertSql =
        "INSERT INTO tpcc_new_order (id, no_o_id, no_d_id, no_w_id) " +
        "VALUES (GEN_ID(), @no_o_id, @no_d_id, @no_w_id)";

    private const string OrderLineInsertSql =
        "INSERT INTO tpcc_order_line (id, ol_o_id, ol_d_id, ol_w_id, ol_number, ol_i_id, ol_supply_w_id, ol_delivery_d, ol_quantity, ol_amount, ol_dist_info) " +
        "VALUES (GEN_ID(), @ol_o_id, @ol_d_id, @ol_w_id, @ol_number, @ol_i_id, @ol_supply_w_id, '', @ol_quantity, @ol_amount, '')";

    private const string HistoryInsertSql =
        "INSERT INTO tpcc_history (id, h_c_id, h_c_d_id, h_c_w_id, h_d_id, h_w_id, h_date, h_amount, h_data) " +
        "VALUES (GEN_ID(), @h_c_id, @h_c_d_id, @h_c_w_id, @h_d_id, @h_w_id, @h_date, @h_amount, @h_data)";

    // -------------------------------------------------------------------------
    // Init
    // -------------------------------------------------------------------------

    internal static async Task InitAsync(CamusConnection conn, int warehouses)
    {
        if (warehouses < 1) warehouses = 1;

        AnsiConsole.MarkupLine("[cyan]Creating TPC-C schema...[/]");

        string[] ddls =
        [
            """
            CREATE TABLE IF NOT EXISTS tpcc_warehouse (
              id OID PRIMARY KEY NOT NULL,
              w_id INT64 NOT NULL,
              w_name STRING NOT NULL,
              w_street_1 STRING NOT NULL,
              w_street_2 STRING NOT NULL,
              w_city STRING NOT NULL,
              w_state STRING NOT NULL,
              w_zip STRING NOT NULL,
              w_tax FLOAT64 NOT NULL,
              w_ytd FLOAT64 NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_district (
              id OID PRIMARY KEY NOT NULL,
              d_id INT64 NOT NULL,
              d_w_id INT64 NOT NULL,
              d_name STRING NOT NULL,
              d_street_1 STRING NOT NULL,
              d_street_2 STRING NOT NULL,
              d_city STRING NOT NULL,
              d_state STRING NOT NULL,
              d_zip STRING NOT NULL,
              d_tax FLOAT64 NOT NULL,
              d_ytd FLOAT64 NOT NULL,
              d_next_o_id INT64 NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_customer (
              id OID PRIMARY KEY NOT NULL,
              c_id INT64 NOT NULL,
              c_d_id INT64 NOT NULL,
              c_w_id INT64 NOT NULL,
              c_first STRING NOT NULL,
              c_middle STRING NOT NULL,
              c_last STRING NOT NULL,
              c_street_1 STRING NOT NULL,
              c_street_2 STRING NOT NULL,
              c_city STRING NOT NULL,
              c_state STRING NOT NULL,
              c_zip STRING NOT NULL,
              c_phone STRING NOT NULL,
              c_since STRING NOT NULL,
              c_credit STRING NOT NULL,
              c_credit_lim FLOAT64 NOT NULL,
              c_discount FLOAT64 NOT NULL,
              c_balance FLOAT64 NOT NULL,
              c_ytd_payment FLOAT64 NOT NULL,
              c_payment_cnt INT64 NOT NULL,
              c_delivery_cnt INT64 NOT NULL,
              c_data STRING NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_item (
              id OID PRIMARY KEY NOT NULL,
              i_id INT64 NOT NULL,
              i_im_id INT64 NOT NULL,
              i_name STRING NOT NULL,
              i_price FLOAT64 NOT NULL,
              i_data STRING NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_stock (
              id OID PRIMARY KEY NOT NULL,
              s_i_id INT64 NOT NULL,
              s_w_id INT64 NOT NULL,
              s_quantity INT64 NOT NULL,
              s_dist_01 STRING NOT NULL,
              s_dist_02 STRING NOT NULL,
              s_dist_03 STRING NOT NULL,
              s_dist_04 STRING NOT NULL,
              s_dist_05 STRING NOT NULL,
              s_dist_06 STRING NOT NULL,
              s_dist_07 STRING NOT NULL,
              s_dist_08 STRING NOT NULL,
              s_dist_09 STRING NOT NULL,
              s_dist_10 STRING NOT NULL,
              s_ytd FLOAT64 NOT NULL,
              s_order_cnt INT64 NOT NULL,
              s_remote_cnt INT64 NOT NULL,
              s_data STRING NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_orders (
              id OID PRIMARY KEY NOT NULL,
              o_id INT64 NOT NULL,
              o_d_id INT64 NOT NULL,
              o_w_id INT64 NOT NULL,
              o_c_id INT64 NOT NULL,
              o_entry_d STRING NOT NULL,
              o_carrier_id INT64 NOT NULL,
              o_ol_cnt INT64 NOT NULL,
              o_all_local INT64 NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_new_order (
              id OID PRIMARY KEY NOT NULL,
              no_o_id INT64 NOT NULL,
              no_d_id INT64 NOT NULL,
              no_w_id INT64 NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_order_line (
              id OID PRIMARY KEY NOT NULL,
              ol_o_id INT64 NOT NULL,
              ol_d_id INT64 NOT NULL,
              ol_w_id INT64 NOT NULL,
              ol_number INT64 NOT NULL,
              ol_i_id INT64 NOT NULL,
              ol_supply_w_id INT64 NOT NULL,
              ol_delivery_d STRING NOT NULL,
              ol_quantity INT64 NOT NULL,
              ol_amount FLOAT64 NOT NULL,
              ol_dist_info STRING NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcc_history (
              id OID PRIMARY KEY NOT NULL,
              h_c_id INT64 NOT NULL,
              h_c_d_id INT64 NOT NULL,
              h_c_w_id INT64 NOT NULL,
              h_d_id INT64 NOT NULL,
              h_w_id INT64 NOT NULL,
              h_date STRING NOT NULL,
              h_amount FLOAT64 NOT NULL,
              h_data STRING NOT NULL
            )
            """,
        ];

        foreach (string ddl in ddls)
            await WorkloadHelpers.DDL(conn, ddl);

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Clearing existing data...");

        string[] tables = ["tpcc_history", "tpcc_order_line", "tpcc_new_order", "tpcc_orders",
                           "tpcc_stock", "tpcc_customer", "tpcc_item", "tpcc_district", "tpcc_warehouse"];
        foreach (string t in tables)
            await WorkloadHelpers.Exec(conn, $"DELETE FROM {t} WHERE id IS NOT NULL");

        AnsiConsole.MarkupLine("Seeding data for [blue]{0}[/] warehouse(s)...\n", warehouses);

        Random rng = new(42);
        string now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        int totalItems = ItemCount;
        int totalStock = totalItems * warehouses;
        int totalDistricts = DistrictsPerWarehouse * warehouses;
        int totalCustomers = CustomersPerDistrict * totalDistricts;

        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .HideCompleted(false)
            .StartAsync(async ctx =>
            {
                ProgressTask tWh  = ctx.AddTask("[green]warehouses[/]",  maxValue: warehouses);
                ProgressTask tDis = ctx.AddTask("[green]districts [/]",  maxValue: totalDistricts);
                ProgressTask tCus = ctx.AddTask("[green]customers [/]",  maxValue: totalCustomers);
                ProgressTask tItm = ctx.AddTask("[green]items     [/]",  maxValue: totalItems);
                ProgressTask tStk = ctx.AddTask("[green]stock     [/]",  maxValue: totalStock);

                // warehouses
                for (int w = 1; w <= warehouses; w++)
                {
                    double tax = Math.Round(rng.NextDouble() * 0.2, 4);
                    await WorkloadHelpers.ExecWithParams(conn, WarehouseInsertSql,
                    [
                        ("@w_id",       ColumnType.Integer64, (object)(long)w),
                        ("@w_name",     ColumnType.String,    $"Warehouse-{w}"),
                        ("@w_street_1", ColumnType.String,    "100 Main St"),
                        ("@w_street_2", ColumnType.String,    $"Suite {w}"),
                        ("@w_city",     ColumnType.String,    "Springfield"),
                        ("@w_state",    ColumnType.String,    "ST"),
                        ("@w_zip",      ColumnType.String,    ZipCode(rng)),
                        ("@w_tax",      ColumnType.Float64,   tax),
                        ("@w_ytd",      ColumnType.Float64,   300000.00),
                    ]);
                    tWh.Increment(1);
                }

                // districts
                for (int w = 1; w <= warehouses; w++)
                {
                    for (int d = 1; d <= DistrictsPerWarehouse; d++)
                    {
                        double dtax = Math.Round(rng.NextDouble() * 0.2, 4);
                        await WorkloadHelpers.ExecWithParams(conn, DistrictInsertSql,
                        [
                            ("@d_id",        ColumnType.Integer64, (object)(long)d),
                            ("@d_w_id",      ColumnType.Integer64, (long)w),
                            ("@d_name",      ColumnType.String,    $"District-{d}"),
                            ("@d_street_1",  ColumnType.String,    $"{d} Commerce Ave"),
                            ("@d_street_2",  ColumnType.String,    ""),
                            ("@d_city",      ColumnType.String,    "Shelbyville"),
                            ("@d_state",     ColumnType.String,    "ST"),
                            ("@d_zip",       ColumnType.String,    ZipCode(rng)),
                            ("@d_tax",       ColumnType.Float64,   dtax),
                            ("@d_ytd",       ColumnType.Float64,   30000.00),
                            ("@d_next_o_id", ColumnType.Integer64, 3001L),
                        ]);
                        tDis.Increment(1);
                    }
                }

                // customers — batch 10 at a time
                var cusBatch = new List<(string sql, (string name, ColumnType type, object value)[] parms)>(BatchSize);
                for (int w = 1; w <= warehouses; w++)
                {
                    for (int d = 1; d <= DistrictsPerWarehouse; d++)
                    {
                        for (int c = 1; c <= CustomersPerDistrict; c++)
                        {
                            string credit = rng.NextDouble() < 0.1 ? "BC" : "GC";
                            double discount = Math.Round(rng.NextDouble() * 0.5, 4);
                            cusBatch.Add((CustomerInsertSql,
                            [
                                ("@c_id",           ColumnType.Integer64, (object)(long)c),
                                ("@c_d_id",         ColumnType.Integer64, (long)d),
                                ("@c_w_id",         ColumnType.Integer64, (long)w),
                                ("@c_first",        ColumnType.String,    FirstName(rng)),
                                ("@c_middle",       ColumnType.String,    "OE"),
                                ("@c_last",         ColumnType.String,    LastName(rng)),
                                ("@c_street_1",     ColumnType.String,    $"{c} Elm St"),
                                ("@c_street_2",     ColumnType.String,    ""),
                                ("@c_city",         ColumnType.String,    "Capital City"),
                                ("@c_state",        ColumnType.String,    "ST"),
                                ("@c_zip",          ColumnType.String,    ZipCode(rng)),
                                ("@c_phone",        ColumnType.String,    PhoneNumber(rng)),
                                ("@c_since",        ColumnType.String,    now),
                                ("@c_credit",       ColumnType.String,    credit),
                                ("@c_credit_lim",   ColumnType.Float64,   50000.00),
                                ("@c_discount",     ColumnType.Float64,   discount),
                                ("@c_balance",      ColumnType.Float64,   -10.00),
                                ("@c_ytd_payment",  ColumnType.Float64,   10.00),
                                ("@c_payment_cnt",  ColumnType.Integer64, 1L),
                                ("@c_delivery_cnt", ColumnType.Integer64, 0L),
                                ("@c_data",         ColumnType.String,    ""),
                            ]));
                            tCus.Increment(1);

                            if (cusBatch.Count >= BatchSize)
                            {
                                await RunBatch(conn, cusBatch);
                                cusBatch.Clear();
                            }
                        }
                    }
                }
                if (cusBatch.Count > 0)
                {
                    await RunBatch(conn, cusBatch);
                    cusBatch.Clear();
                }

                // items — batch 10 at a time
                var itmBatch = new List<(string sql, (string name, ColumnType type, object value)[] parms)>(BatchSize);
                for (int i = 1; i <= totalItems; i++)
                {
                    double price = Math.Round(1.0 + rng.NextDouble() * 99.0, 2);
                    string data = rng.NextDouble() < 0.1 ? "ORIGINAL" : RandString(rng, 26, 50);
                    itmBatch.Add((ItemInsertSql,
                    [
                        ("@i_id",   ColumnType.Integer64, (object)(long)i),
                        ("@i_im_id", ColumnType.Integer64, (long)rng.Next(1, 10001)),
                        ("@i_name", ColumnType.String,    $"Item-{i}"),
                        ("@i_price", ColumnType.Float64,  price),
                        ("@i_data", ColumnType.String,    data),
                    ]));
                    tItm.Increment(1);

                    if (itmBatch.Count >= BatchSize)
                    {
                        await RunBatch(conn, itmBatch);
                        itmBatch.Clear();
                    }
                }
                if (itmBatch.Count > 0)
                {
                    await RunBatch(conn, itmBatch);
                    itmBatch.Clear();
                }

                // stock — batch 10 at a time, one row per (item, warehouse)
                var stkBatch = new List<(string sql, (string name, ColumnType type, object value)[] parms)>(BatchSize);
                for (int w = 1; w <= warehouses; w++)
                {
                    for (int i = 1; i <= totalItems; i++)
                    {
                        int qty = rng.Next(10, 101);
                        string sdata = rng.NextDouble() < 0.1 ? "ORIGINAL" : RandString(rng, 26, 50);
                        stkBatch.Add((StockInsertSql,
                        [
                            ("@s_i_id",    ColumnType.Integer64, (object)(long)i),
                            ("@s_w_id",    ColumnType.Integer64, (long)w),
                            ("@s_quantity", ColumnType.Integer64, (long)qty),
                            ("@s_dist_01", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_02", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_03", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_04", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_05", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_06", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_07", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_08", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_09", ColumnType.String,    RandStr24(rng)),
                            ("@s_dist_10", ColumnType.String,    RandStr24(rng)),
                            ("@s_data",    ColumnType.String,    sdata),
                        ]));
                        tStk.Increment(1);

                        if (stkBatch.Count >= BatchSize)
                        {
                            await RunBatch(conn, stkBatch);
                            stkBatch.Clear();
                        }
                    }
                }
                if (stkBatch.Count > 0)
                {
                    await RunBatch(conn, stkBatch);
                    stkBatch.Clear();
                }
            });

        AnsiConsole.MarkupLine("\n[green]TPC-C workload initialized.[/]");
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run tpcc[/] to start generating activity.");
    }

    // -------------------------------------------------------------------------
    // Run
    // -------------------------------------------------------------------------

    internal static async Task RunAsync(CamusConnection conn, int concurrency, int durationSeconds)
    {
        List<long> warehouseIds = await LoadLongColumn(conn, "tpcc_warehouse", "w_id");
        if (warehouseIds.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]No warehouses found.[/] Run [blue]camus-cli workload init tpcc[/] first.");
            return;
        }

        int maxItemId = ItemCount;

        AnsiConsole.MarkupLine("Starting TPC-C workload: [blue]{0}[/] workers, [blue]{1}s[/] duration, [blue]{2}[/] warehouse(s)",
            concurrency, durationSeconds, warehouseIds.Count);
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
                    int txType = WeightedTx(rng);
                    long wId = warehouseIds[rng.Next(warehouseIds.Count)];

                    switch (txType)
                    {
                        case 0: await TxNewOrder(conn, rng, wId, maxItemId); break;
                        case 1: await TxPayment(conn, rng, wId); break;
                        case 2: await TxOrderStatus(conn, rng, wId); break;
                        case 3: await TxDelivery(conn, rng, wId); break;
                        case 4: await TxStockLevel(conn, rng, wId); break;
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

    // -------------------------------------------------------------------------
    // TPC-C transactions
    // -------------------------------------------------------------------------

    // New-Order (~45%): insert an order with 5-15 line items
    private static async Task TxNewOrder(CamusConnection conn, Random rng, long wId, int maxItemId)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        long cId = rng.Next(1, CustomersPerDistrict + 1);
        int lineCount = rng.Next(5, 16);
        string entryDate = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        long oId = await FetchNextOrderId(conn, dId, wId);

        CamusTransaction tx = await conn.BeginTransactionAsync();
        try
        {
            await WorkloadHelpers.ExecWithParams(conn, OrderInsertSql,
            [
                ("@o_id",        ColumnType.Integer64, (object)oId),
                ("@o_d_id",      ColumnType.Integer64, dId),
                ("@o_w_id",      ColumnType.Integer64, wId),
                ("@o_c_id",      ColumnType.Integer64, cId),
                ("@o_entry_d",   ColumnType.String,    entryDate),
                ("@o_carrier_id", ColumnType.Integer64, 0L),
                ("@o_ol_cnt",    ColumnType.Integer64, (long)lineCount),
                ("@o_all_local", ColumnType.Integer64, 1L),
            ], tx);

            await WorkloadHelpers.ExecWithParams(conn, NewOrderInsertSql,
            [
                ("@no_o_id", ColumnType.Integer64, (object)oId),
                ("@no_d_id", ColumnType.Integer64, dId),
                ("@no_w_id", ColumnType.Integer64, wId),
            ], tx);

            await WorkloadHelpers.ExecWithParams(conn,
                "UPDATE tpcc_district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = @d_id AND d_w_id = @d_w_id",
            [
                ("@d_id",   ColumnType.Integer64, (object)dId),
                ("@d_w_id", ColumnType.Integer64, wId),
            ], tx);

            for (int ol = 1; ol <= lineCount; ol++)
            {
                long iId = rng.Next(1, maxItemId + 1);
                int qty = rng.Next(1, 11);
                double amount = Math.Round(qty * (1.0 + rng.NextDouble() * 99.0), 2);

                await WorkloadHelpers.ExecWithParams(conn, OrderLineInsertSql,
                [
                    ("@ol_o_id",       ColumnType.Integer64, (object)oId),
                    ("@ol_d_id",       ColumnType.Integer64, dId),
                    ("@ol_w_id",       ColumnType.Integer64, wId),
                    ("@ol_number",     ColumnType.Integer64, (long)ol),
                    ("@ol_i_id",       ColumnType.Integer64, iId),
                    ("@ol_supply_w_id", ColumnType.Integer64, wId),
                    ("@ol_quantity",   ColumnType.Integer64, (long)qty),
                    ("@ol_amount",     ColumnType.Float64,   amount),
                ], tx);

                await WorkloadHelpers.ExecWithParams(conn,
                    "UPDATE tpcc_stock SET s_quantity = s_quantity - @qty, s_order_cnt = s_order_cnt + 1 " +
                    "WHERE s_i_id = @i_id AND s_w_id = @w_id AND s_quantity >= @qty",
                [
                    ("@qty",  ColumnType.Integer64, (object)(long)qty),
                    ("@i_id", ColumnType.Integer64, iId),
                    ("@w_id", ColumnType.Integer64, wId),
                ], tx);
            }

            await tx.CommitAsync();
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
    }

    // Payment (~43%): update customer balance and district/warehouse YTD
    private static async Task TxPayment(CamusConnection conn, Random rng, long wId)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        long cId = rng.Next(1, CustomersPerDistrict + 1);
        double amount = Math.Round(1.0 + rng.NextDouble() * 4999.0, 2);
        string date = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        CamusTransaction tx = await conn.BeginTransactionAsync();
        try
        {
            await WorkloadHelpers.ExecWithParams(conn,
                "UPDATE tpcc_warehouse SET w_ytd = w_ytd + @amount WHERE w_id = @w_id",
            [
                ("@amount", ColumnType.Float64,   (object)amount),
                ("@w_id",   ColumnType.Integer64, wId),
            ], tx);

            await WorkloadHelpers.ExecWithParams(conn,
                "UPDATE tpcc_district SET d_ytd = d_ytd + @amount WHERE d_id = @d_id AND d_w_id = @d_w_id",
            [
                ("@amount", ColumnType.Float64,   (object)amount),
                ("@d_id",   ColumnType.Integer64, dId),
                ("@d_w_id", ColumnType.Integer64, wId),
            ], tx);

            await WorkloadHelpers.ExecWithParams(conn,
                "UPDATE tpcc_customer SET c_balance = c_balance - @amount, c_ytd_payment = c_ytd_payment + @amount, " +
                "c_payment_cnt = c_payment_cnt + 1 WHERE c_id = @c_id AND c_d_id = @c_d_id AND c_w_id = @c_w_id",
            [
                ("@amount", ColumnType.Float64,   (object)amount),
                ("@c_id",   ColumnType.Integer64, cId),
                ("@c_d_id", ColumnType.Integer64, dId),
                ("@c_w_id", ColumnType.Integer64, wId),
            ], tx);

            await WorkloadHelpers.ExecWithParams(conn, HistoryInsertSql,
            [
                ("@h_c_id",   ColumnType.Integer64, (object)cId),
                ("@h_c_d_id", ColumnType.Integer64, dId),
                ("@h_c_w_id", ColumnType.Integer64, wId),
                ("@h_d_id",   ColumnType.Integer64, dId),
                ("@h_w_id",   ColumnType.Integer64, wId),
                ("@h_date",   ColumnType.String,    date),
                ("@h_amount", ColumnType.Float64,   amount),
                ("@h_data",   ColumnType.String,    "payment"),
            ], tx);

            await tx.CommitAsync();
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
    }

    // Order-Status (~4%): look up a customer's most recent order
    private static async Task TxOrderStatus(CamusConnection conn, Random rng, long wId)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        long cId = rng.Next(1, CustomersPerDistrict + 1);

        using CamusCommand cmd = conn.CreateSelectCommand(
            "SELECT o_id, o_entry_d, o_carrier_id FROM tpcc_orders " +
            "WHERE o_w_id = @w_id AND o_d_id = @d_id AND o_c_id = @c_id");
        cmd.Parameters.Add("@w_id", ColumnType.Integer64, wId);
        cmd.Parameters.Add("@d_id", ColumnType.Integer64, dId);
        cmd.Parameters.Add("@c_id", ColumnType.Integer64, cId);
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync()) { /* consume */ }
    }

    // Delivery (~4%): deliver the oldest new-order in each district for the warehouse
    private static async Task TxDelivery(CamusConnection conn, Random rng, long wId)
    {
        int carrierId = rng.Next(1, 11);
        string deliveryDate = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        for (int d = 1; d <= DistrictsPerWarehouse; d++)
        {
            using CamusCommand findCmd = conn.CreateSelectCommand(
                "SELECT no_o_id FROM tpcc_new_order WHERE no_w_id = @w_id AND no_d_id = @d_id");
            findCmd.Parameters.Add("@w_id", ColumnType.Integer64, wId);
            findCmd.Parameters.Add("@d_id", ColumnType.Integer64, (long)d);
            findCmd.CommandTimeout = 30;
            CamusDataReader findReader = await findCmd.ExecuteReaderAsync();

            long? minOId = null;
            while (await findReader.ReadAsync())
            {
                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(findReader);
                if (row.TryGetValue("no_o_id", out ColumnValue v))
                {
                    long oid = v.LongValue;
                    if (minOId is null || oid < minOId)
                        minOId = oid;
                }
            }

            if (minOId is null) continue;

            CamusTransaction tx = await conn.BeginTransactionAsync();
            try
            {
                await WorkloadHelpers.ExecWithParams(conn,
                    "DELETE FROM tpcc_new_order WHERE no_o_id = @no_o_id AND no_d_id = @d_id AND no_w_id = @w_id",
                [
                    ("@no_o_id", ColumnType.Integer64, (object)minOId.Value),
                    ("@d_id",    ColumnType.Integer64, (long)d),
                    ("@w_id",    ColumnType.Integer64, wId),
                ], tx);

                await WorkloadHelpers.ExecWithParams(conn,
                    "UPDATE tpcc_orders SET o_carrier_id = @carrier_id WHERE o_id = @o_id AND o_d_id = @d_id AND o_w_id = @w_id",
                [
                    ("@carrier_id", ColumnType.Integer64, (object)(long)carrierId),
                    ("@o_id",       ColumnType.Integer64, minOId.Value),
                    ("@d_id",       ColumnType.Integer64, (long)d),
                    ("@w_id",       ColumnType.Integer64, wId),
                ], tx);

                await WorkloadHelpers.ExecWithParams(conn,
                    "UPDATE tpcc_order_line SET ol_delivery_d = @delivery_d WHERE ol_o_id = @o_id AND ol_d_id = @d_id AND ol_w_id = @w_id",
                [
                    ("@delivery_d", ColumnType.String,    (object)deliveryDate),
                    ("@o_id",       ColumnType.Integer64, minOId.Value),
                    ("@d_id",       ColumnType.Integer64, (long)d),
                    ("@w_id",       ColumnType.Integer64, wId),
                ], tx);

                await tx.CommitAsync();
            }
            catch
            {
                try { await tx.RollbackAsync(); } catch { }
            }
        }
    }

    // Stock-Level (~4%): count items below threshold in recent orders of a district
    private static async Task TxStockLevel(CamusConnection conn, Random rng, long wId)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        int threshold = rng.Next(10, 21);

        using CamusCommand cmd = conn.CreateSelectCommand(
            "SELECT s_i_id, s_quantity FROM tpcc_stock WHERE s_w_id = @w_id AND s_quantity < @threshold");
        cmd.Parameters.Add("@w_id",      ColumnType.Integer64, wId);
        cmd.Parameters.Add("@threshold", ColumnType.Integer64, (long)threshold);
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        int count = 0;
        while (await reader.ReadAsync()) count++;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    private static async Task RunBatch(
        CamusConnection conn,
        List<(string sql, (string name, ColumnType type, object value)[] parms)> batch)
    {
        CamusTransaction tx = await conn.BeginTransactionAsync();
        try
        {
            foreach (var (sql, parms) in batch)
                await WorkloadHelpers.ExecWithParams(conn, sql, parms, tx);
            await tx.CommitAsync();
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
    }

    private static async Task<long> FetchNextOrderId(CamusConnection conn, long dId, long wId)
    {
        using CamusCommand cmd = conn.CreateSelectCommand(
            "SELECT d_next_o_id FROM tpcc_district WHERE d_id = @d_id AND d_w_id = @d_w_id");
        cmd.Parameters.Add("@d_id",   ColumnType.Integer64, dId);
        cmd.Parameters.Add("@d_w_id", ColumnType.Integer64, wId);
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            if (row.TryGetValue("d_next_o_id", out ColumnValue v))
                return v.LongValue;
        }
        return 1;
    }

    private static async Task<List<long>> LoadLongColumn(CamusConnection conn, string table, string column)
    {
        List<long> result = new();
        using CamusCommand cmd = conn.CreateSelectCommand($"SELECT {column} FROM {table}");
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            if (row.TryGetValue(column, out ColumnValue v))
                result.Add(v.LongValue);
        }
        return result;
    }

    // TPC-C transaction mix weights: 45% new-order, 43% payment, 4% each for the rest
    private static int WeightedTx(Random rng)
    {
        int roll = rng.Next(100);
        if (roll < 45) return 0; // new-order
        if (roll < 88) return 1; // payment
        if (roll < 92) return 2; // order-status
        if (roll < 96) return 3; // delivery
        return 4;                // stock-level
    }

    private static string F(double v) => v.ToString("F2", CultureInfo.InvariantCulture);

    private static string ZipCode(Random rng) => $"{rng.Next(10000, 99999)}-1111";

    private static string PhoneNumber(Random rng) =>
        $"{rng.Next(100, 999)}-{rng.Next(100, 999)}-{rng.Next(1000, 9999)}";

    private static readonly string[] FirstNames =
        ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
         "William", "Barbara", "David", "Susan", "Richard", "Jessica", "Thomas", "Sarah"];

    private static readonly string[] LastSyllables =
        ["BAR", "OUGHT", "ABLE", "PRI", "PRES", "ESE", "ANTI", "CALLY", "ATION", "EING"];

    private static string FirstName(Random rng) => FirstNames[rng.Next(FirstNames.Length)];

    private static string LastName(Random rng) =>
        LastSyllables[rng.Next(LastSyllables.Length)] +
        LastSyllables[rng.Next(LastSyllables.Length)] +
        LastSyllables[rng.Next(LastSyllables.Length)];

    private static readonly char[] AlphaNum = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789".ToCharArray();

    private static string RandStr24(Random rng)
    {
        char[] buf = new char[24];
        for (int i = 0; i < 24; i++)
            buf[i] = AlphaNum[rng.Next(AlphaNum.Length)];
        return new string(buf);
    }

    private static string RandString(Random rng, int minLen, int maxLen)
    {
        int len = rng.Next(minLen, maxLen + 1);
        char[] buf = new char[len];
        for (int i = 0; i < len; i++)
            buf[i] = AlphaNum[rng.Next(AlphaNum.Length)];
        return new string(buf);
    }
}
