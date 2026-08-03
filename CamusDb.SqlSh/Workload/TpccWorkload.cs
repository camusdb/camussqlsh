
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
using static WorkloadHelpers;

internal static class TpccWorkload
{
    private const int DistrictsPerWarehouse = 10;
    private const int CustomersPerDistrict = 30;
    private const int ItemCount = 1000;
    private const int BatchSize = 10;

    // -------------------------------------------------------------------------
    // SQL templates
    // -------------------------------------------------------------------------

    // Seed inserts carry an explicit @id instead of GEN_ID(): the id is derived deterministically
    // from the row's natural key (see SeedId), so replaying a seed batch whose commit reply was
    // lost re-inserts the same primary keys and the duplicate-key error confirms the batch is
    // durable — with GEN_ID() every replay would mint fresh ids and silently double-insert.
    // Run-phase inserts keep GEN_ID(); they are never replayed.
    private const string WarehouseInsertSql =
        "INSERT INTO tpcc_warehouse (id, w_id, w_name, w_street_1, w_street_2, w_city, w_state, w_zip, w_tax, w_ytd) " +
        "VALUES (@id, @w_id, @w_name, @w_street_1, @w_street_2, @w_city, @w_state, @w_zip, @w_tax, @w_ytd)";

    private const string DistrictInsertSql =
        "INSERT INTO tpcc_district (id, d_id, d_w_id, d_name, d_street_1, d_street_2, d_city, d_state, d_zip, d_tax, d_ytd, d_next_o_id) " +
        "VALUES (@id, @d_id, @d_w_id, @d_name, @d_street_1, @d_street_2, @d_city, @d_state, @d_zip, @d_tax, @d_ytd, @d_next_o_id)";

    private const string CustomerInsertSql =
        "INSERT INTO tpcc_customer (id, c_id, c_d_id, c_w_id, c_first, c_middle, c_last, " +
        "c_street_1, c_street_2, c_city, c_state, c_zip, c_phone, c_since, c_credit, " +
        "c_credit_lim, c_discount, c_balance, c_ytd_payment, c_payment_cnt, c_delivery_cnt, c_data) " +
        "VALUES (@id, @c_id, @c_d_id, @c_w_id, @c_first, @c_middle, @c_last, " +
        "@c_street_1, @c_street_2, @c_city, @c_state, @c_zip, @c_phone, @c_since, @c_credit, " +
        "@c_credit_lim, @c_discount, @c_balance, @c_ytd_payment, @c_payment_cnt, @c_delivery_cnt, @c_data)";

    private const string ItemInsertSql =
        "INSERT INTO tpcc_item (id, i_id, i_im_id, i_name, i_price, i_data) " +
        "VALUES (@id, @i_id, @i_im_id, @i_name, @i_price, @i_data)";

    private const string StockInsertSql =
        "INSERT INTO tpcc_stock (id, s_i_id, s_w_id, s_quantity, " +
        "s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05, " +
        "s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10, " +
        "s_ytd, s_order_cnt, s_remote_cnt, s_data) " +
        "VALUES (@id, @s_i_id, @s_w_id, @s_quantity, " +
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

    private const string NextOrderIdSql =
        "SELECT d_next_o_id FROM tpcc_district WHERE d_id = @d_id AND d_w_id = @d_w_id";

    private const string BumpNextOrderIdSql =
        "UPDATE tpcc_district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = @d_id AND d_w_id = @d_w_id";

    private const string StockDecrementSql =
        "UPDATE tpcc_stock SET s_quantity = s_quantity - @qty, s_order_cnt = s_order_cnt + 1 " +
        "WHERE s_i_id = @i_id AND s_w_id = @w_id AND s_quantity >= @qty";

    private const string WarehouseYtdSql =
        "UPDATE tpcc_warehouse SET w_ytd = w_ytd + @amount WHERE w_id = @w_id";

    private const string DistrictYtdSql =
        "UPDATE tpcc_district SET d_ytd = d_ytd + @amount WHERE d_id = @d_id AND d_w_id = @d_w_id";

    private const string CustomerPaymentSql =
        "UPDATE tpcc_customer SET c_balance = c_balance - @amount, c_ytd_payment = c_ytd_payment + @amount, " +
        "c_payment_cnt = c_payment_cnt + 1 WHERE c_id = @c_id AND c_d_id = @c_d_id AND c_w_id = @c_w_id";

    private const string OrderStatusSql =
        "SELECT o_id, o_entry_d, o_carrier_id FROM tpcc_orders " +
        "WHERE o_w_id = @w_id AND o_d_id = @d_id AND o_c_id = @c_id";

    private const string OldestNewOrderSql =
        "SELECT no_o_id FROM tpcc_new_order WHERE no_w_id = @w_id AND no_d_id = @d_id";

    private const string NewOrderDeleteSql =
        "DELETE FROM tpcc_new_order WHERE no_o_id = @no_o_id AND no_d_id = @d_id AND no_w_id = @w_id";

    private const string OrderCarrierSql =
        "UPDATE tpcc_orders SET o_carrier_id = @carrier_id WHERE o_id = @o_id AND o_d_id = @d_id AND o_w_id = @w_id";

    private const string OrderLineDeliverySql =
        "UPDATE tpcc_order_line SET ol_delivery_d = @delivery_d WHERE ol_o_id = @o_id AND ol_d_id = @d_id AND ol_w_id = @w_id";

    private const string StockLevelSql =
        "SELECT s_i_id, s_quantity FROM tpcc_stock WHERE s_w_id = @w_id AND s_quantity < @threshold";

    /// <summary>
    /// Every statement the <c>run</c> phase issues, for the prepared-statement warm-up. Keep in sync
    /// with the transaction bodies below: a statement missing here isn't broken, it just pays the
    /// driver's usual two-execution warm-up instead of being registered before the clock starts.
    /// </summary>
    internal static IReadOnlyList<string> RunStatements =>
    [
        NextOrderIdSql, OrderInsertSql, NewOrderInsertSql, BumpNextOrderIdSql, OrderLineInsertSql, StockDecrementSql,
        WarehouseYtdSql, DistrictYtdSql, CustomerPaymentSql, HistoryInsertSql,
        OrderStatusSql,
        OldestNewOrderSql, NewOrderDeleteSql, OrderCarrierSql, OrderLineDeliverySql,
        StockLevelSql,
    ];

    // -------------------------------------------------------------------------
    // Init
    // -------------------------------------------------------------------------

    internal static async Task InitAsync(CamusConnection conn, int warehouses, int concurrency, CamusTransactionOptions txOptions)
    {
        if (warehouses < 1) warehouses = 1;

        AnsiConsole.MarkupLine("[cyan]Creating TPC-C schema...[/]");

        // Re-init clears by dropping the tables rather than DELETEing rows: a bulk DELETE of a seeded
        // 1000-warehouse dataset is ~1.2M mutations in one transaction, which exceeds the server's
        // per-transaction mutation cap and fails the whole init. Dropping is constant-cost and leaves
        // the CREATEs below to rebuild the schema. A missing table on first init is not a failure.
        string[] dropOrder = ["tpcc_history", "tpcc_order_line", "tpcc_new_order", "tpcc_orders",
                              "tpcc_stock", "tpcc_customer", "tpcc_item", "tpcc_district", "tpcc_warehouse"];
        foreach (string t in dropOrder)
        {
            try { await DDL(conn, $"DROP TABLE {t}"); } catch (CamusException) { }
        }

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
            await DDL(conn, ddl);

        // Index every column the run-phase predicates filter on. Without these, an UPDATE/SELECT with a
        // non-indexed predicate takes a [-inf,+inf] range lock on the whole table, which serializes the
        // workers no matter how many of them there are.
        string[] indexes =
        [
            "ALTER TABLE tpcc_warehouse ADD INDEX idx_w (w_id)",
            "ALTER TABLE tpcc_district ADD INDEX idx_d (d_w_id, d_id)",
            "ALTER TABLE tpcc_customer ADD INDEX idx_c (c_w_id, c_d_id, c_id)",
            "ALTER TABLE tpcc_item ADD INDEX idx_i (i_id)",
            "ALTER TABLE tpcc_stock ADD INDEX idx_s (s_w_id, s_i_id)",
            "ALTER TABLE tpcc_orders ADD INDEX idx_o (o_w_id, o_d_id, o_c_id)",
            "ALTER TABLE tpcc_new_order ADD INDEX idx_no (no_w_id, no_d_id)",
            "ALTER TABLE tpcc_order_line ADD INDEX idx_ol (ol_w_id, ol_d_id, ol_o_id)",
        ];

        foreach (string index in indexes)
        {
            // A re-run of init hits an already-created index; that's not a failure.
            try { await DDL(conn, index); } catch (CamusException) { }
        }

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Seeding data for [blue]{0}[/] warehouse(s) ([blue]{1}[/] parallel writers)...\n", warehouses, concurrency);

        Random rng = new(42);
        string now = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        int totalItems = ItemCount;
        int totalStock = totalItems * warehouses;
        int totalDistricts = DistrictsPerWarehouse * warehouses;
        int totalCustomers = CustomersPerDistrict * totalDistricts;

        // Every row is generated single-threaded from a seeded RNG (Random isn't thread-safe), then the
        // resulting statements are chunked into transactions and fanned out across `concurrency` writers.
        List<(string Sql, Param[] Parameters)> warehouseRows = [];
        for (int w = 1; w <= warehouses; w++)
        {
            warehouseRows.Add((WarehouseInsertSql,
            [
                ("@id",         ColumnType.Id,        (object)SeedId(WarehouseTag, w)),
                ("@w_id",       ColumnType.Integer64, (long)w),
                ("@w_name",     ColumnType.String,    $"Warehouse-{w}"),
                ("@w_street_1", ColumnType.String,    "100 Main St"),
                ("@w_street_2", ColumnType.String,    $"Suite {w}"),
                ("@w_city",     ColumnType.String,    "Springfield"),
                ("@w_state",    ColumnType.String,    "ST"),
                ("@w_zip",      ColumnType.String,    ZipCode(rng)),
                ("@w_tax",      ColumnType.Float64,   Math.Round(rng.NextDouble() * 0.2, 4)),
                ("@w_ytd",      ColumnType.Float64,   300000.00),
            ]));
        }

        List<(string Sql, Param[] Parameters)> districtRows = [];
        for (int w = 1; w <= warehouses; w++)
        {
            for (int d = 1; d <= DistrictsPerWarehouse; d++)
            {
                districtRows.Add((DistrictInsertSql,
                [
                    ("@id",          ColumnType.Id,        (object)SeedId(DistrictTag, w, d)),
                    ("@d_id",        ColumnType.Integer64, (long)d),
                    ("@d_w_id",      ColumnType.Integer64, (long)w),
                    ("@d_name",      ColumnType.String,    $"District-{d}"),
                    ("@d_street_1",  ColumnType.String,    $"{d} Commerce Ave"),
                    ("@d_street_2",  ColumnType.String,    ""),
                    ("@d_city",      ColumnType.String,    "Shelbyville"),
                    ("@d_state",     ColumnType.String,    "ST"),
                    ("@d_zip",       ColumnType.String,    ZipCode(rng)),
                    ("@d_tax",       ColumnType.Float64,   Math.Round(rng.NextDouble() * 0.2, 4)),
                    ("@d_ytd",       ColumnType.Float64,   30000.00),
                    ("@d_next_o_id", ColumnType.Integer64, 3001L),
                ]));
            }
        }

        List<(string Sql, Param[] Parameters)> customerRows = [];
        for (int w = 1; w <= warehouses; w++)
        {
            for (int d = 1; d <= DistrictsPerWarehouse; d++)
            {
                for (int c = 1; c <= CustomersPerDistrict; c++)
                {
                    customerRows.Add((CustomerInsertSql,
                    [
                        ("@id",             ColumnType.Id,        (object)SeedId(CustomerTag, w, d, c)),
                        ("@c_id",           ColumnType.Integer64, (long)c),
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
                        ("@c_credit",       ColumnType.String,    rng.NextDouble() < 0.1 ? "BC" : "GC"),
                        ("@c_credit_lim",   ColumnType.Float64,   50000.00),
                        ("@c_discount",     ColumnType.Float64,   Math.Round(rng.NextDouble() * 0.5, 4)),
                        ("@c_balance",      ColumnType.Float64,   -10.00),
                        ("@c_ytd_payment",  ColumnType.Float64,   10.00),
                        ("@c_payment_cnt",  ColumnType.Integer64, 1L),
                        ("@c_delivery_cnt", ColumnType.Integer64, 0L),
                        ("@c_data",         ColumnType.String,    ""),
                    ]));
                }
            }
        }

        List<(string Sql, Param[] Parameters)> itemRows = [];
        for (int i = 1; i <= totalItems; i++)
        {
            itemRows.Add((ItemInsertSql,
            [
                ("@id",      ColumnType.Id,        (object)SeedId(ItemTag, i)),
                ("@i_id",    ColumnType.Integer64, (long)i),
                ("@i_im_id", ColumnType.Integer64, (long)rng.Next(1, 10001)),
                ("@i_name",  ColumnType.String,    $"Item-{i}"),
                ("@i_price", ColumnType.Float64,   Math.Round(1.0 + rng.NextDouble() * 99.0, 2)),
                ("@i_data",  ColumnType.String,    rng.NextDouble() < 0.1 ? "ORIGINAL" : RandString(rng, 26, 50)),
            ]));
        }

        List<(string Sql, Param[] Parameters)> stockRows = [];
        for (int w = 1; w <= warehouses; w++)
        {
            for (int i = 1; i <= totalItems; i++)
            {
                stockRows.Add((StockInsertSql,
                [
                    ("@id",         ColumnType.Id,        (object)SeedId(StockTag, w, 0, i)),
                    ("@s_i_id",     ColumnType.Integer64, (long)i),
                    ("@s_w_id",     ColumnType.Integer64, (long)w),
                    ("@s_quantity", ColumnType.Integer64, (long)rng.Next(10, 101)),
                    ("@s_dist_01",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_02",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_03",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_04",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_05",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_06",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_07",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_08",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_09",  ColumnType.String,    RandStr24(rng)),
                    ("@s_dist_10",  ColumnType.String,    RandStr24(rng)),
                    ("@s_data",     ColumnType.String,    rng.NextDouble() < 0.1 ? "ORIGINAL" : RandString(rng, 26, 50)),
                ]));
            }
        }

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

                await Seed(conn, txOptions, concurrency, warehouseRows, tWh);
                await Seed(conn, txOptions, concurrency, districtRows, tDis);
                await Seed(conn, txOptions, concurrency, customerRows, tCus);
                await Seed(conn, txOptions, concurrency, itemRows, tItm);
                await Seed(conn, txOptions, concurrency, stockRows, tStk);
            });

        AnsiConsole.MarkupLine("\n[green]TPC-C workload initialized.[/]");
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run tpcc[/] to start generating activity.");
    }

    // -------------------------------------------------------------------------
    // Run
    // -------------------------------------------------------------------------

    internal static async Task RunAsync(CamusConnection conn, int concurrency, int durationSeconds, CamusTransactionOptions txOptions)
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
                        case 0: await TxNewOrder(conn, txOptions, rng, wId, maxItemId, cts.Token); break;
                        case 1: await TxPayment(conn, txOptions, rng, wId, cts.Token); break;
                        case 2: await TxOrderStatus(conn, rng, wId, cts.Token); break;
                        case 3: await TxDelivery(conn, txOptions, rng, wId, cts.Token); break;
                        case 4: await TxStockLevel(conn, rng, wId, cts.Token); break;
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
    private static async Task TxNewOrder(CamusConnection conn, CamusTransactionOptions txOptions, Random rng, long wId, int maxItemId, CancellationToken ct)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        long cId = rng.Next(1, CustomersPerDistrict + 1);
        int lineCount = rng.Next(5, 16);
        string entryDate = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        CamusTransaction tx = await conn.BeginTransactionAsync(txOptions, ct);
        try
        {
            long oId = await FetchNextOrderId(conn, tx, dId, wId, ct);

            // Statements are awaited one at a time, deliberately: the write set has no data dependencies, but
            // firing it as a concurrent wave (Task.WhenAll over the BatchExecute stream) measured 7x WORSE
            // (24.5 -> 3.4 tx/s at 8 clients) — concurrent ops on one transaction handle work correctly but
            // hit a seconds-scale server-side stall tail (execute p99 2s vs p50 11ms) that freezes the whole
            // wave. Until that per-transaction path handles concurrency, sequential await is the fast shape.
            await ExecWithParams(conn, OrderInsertSql,
            [
                ("@o_id",        ColumnType.Integer64, (object)oId),
                ("@o_d_id",      ColumnType.Integer64, dId),
                ("@o_w_id",      ColumnType.Integer64, wId),
                ("@o_c_id",      ColumnType.Integer64, cId),
                ("@o_entry_d",   ColumnType.String,    entryDate),
                ("@o_carrier_id", ColumnType.Integer64, 0L),
                ("@o_ol_cnt",    ColumnType.Integer64, (long)lineCount),
                ("@o_all_local", ColumnType.Integer64, 1L),
            ], tx, ct);

            await ExecWithParams(conn, NewOrderInsertSql,
            [
                ("@no_o_id", ColumnType.Integer64, (object)oId),
                ("@no_d_id", ColumnType.Integer64, dId),
                ("@no_w_id", ColumnType.Integer64, wId),
            ], tx, ct);

            await ExecWithParams(conn, BumpNextOrderIdSql,
            [
                ("@d_id",   ColumnType.Integer64, (object)dId),
                ("@d_w_id", ColumnType.Integer64, wId),
            ], tx, ct);

            for (int ol = 1; ol <= lineCount; ol++)
            {
                long iId = rng.Next(1, maxItemId + 1);
                int qty = rng.Next(1, 11);
                double amount = Math.Round(qty * (1.0 + rng.NextDouble() * 99.0), 2);

                await ExecWithParams(conn, OrderLineInsertSql,
                [
                    ("@ol_o_id",       ColumnType.Integer64, (object)oId),
                    ("@ol_d_id",       ColumnType.Integer64, dId),
                    ("@ol_w_id",       ColumnType.Integer64, wId),
                    ("@ol_number",     ColumnType.Integer64, (long)ol),
                    ("@ol_i_id",       ColumnType.Integer64, iId),
                    ("@ol_supply_w_id", ColumnType.Integer64, wId),
                    ("@ol_quantity",   ColumnType.Integer64, (long)qty),
                    ("@ol_amount",     ColumnType.Float64,   amount),
                ], tx, ct);

                await ExecWithParams(conn, StockDecrementSql,
                [
                    ("@qty",  ColumnType.Integer64, (object)(long)qty),
                    ("@i_id", ColumnType.Integer64, iId),
                    ("@w_id", ColumnType.Integer64, wId),
                ], tx, ct);
            }

            await tx.CommitAsync(ct);
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
    }

    // Payment (~43%): update customer balance and district/warehouse YTD
    private static async Task TxPayment(CamusConnection conn, CamusTransactionOptions txOptions, Random rng, long wId, CancellationToken ct)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        long cId = rng.Next(1, CustomersPerDistrict + 1);
        double amount = Math.Round(1.0 + rng.NextDouble() * 4999.0, 2);
        string date = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        CamusTransaction tx = await conn.BeginTransactionAsync(txOptions, ct);
        try
        {
            await ExecWithParams(conn, WarehouseYtdSql,
            [
                ("@amount", ColumnType.Float64,   (object)amount),
                ("@w_id",   ColumnType.Integer64, wId),
            ], tx, ct);

            await ExecWithParams(conn, DistrictYtdSql,
            [
                ("@amount", ColumnType.Float64,   (object)amount),
                ("@d_id",   ColumnType.Integer64, dId),
                ("@d_w_id", ColumnType.Integer64, wId),
            ], tx, ct);

            await ExecWithParams(conn, CustomerPaymentSql,
            [
                ("@amount", ColumnType.Float64,   (object)amount),
                ("@c_id",   ColumnType.Integer64, cId),
                ("@c_d_id", ColumnType.Integer64, dId),
                ("@c_w_id", ColumnType.Integer64, wId),
            ], tx, ct);

            await ExecWithParams(conn, HistoryInsertSql,
            [
                ("@h_c_id",   ColumnType.Integer64, (object)cId),
                ("@h_c_d_id", ColumnType.Integer64, dId),
                ("@h_c_w_id", ColumnType.Integer64, wId),
                ("@h_d_id",   ColumnType.Integer64, dId),
                ("@h_w_id",   ColumnType.Integer64, wId),
                ("@h_date",   ColumnType.String,    date),
                ("@h_amount", ColumnType.Float64,   amount),
                ("@h_data",   ColumnType.String,    "payment"),
            ], tx, ct);

            await tx.CommitAsync(ct);
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
    }

    // Order-Status (~4%): look up a customer's most recent order
    private static async Task TxOrderStatus(CamusConnection conn, Random rng, long wId, CancellationToken ct)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        long cId = rng.Next(1, CustomersPerDistrict + 1);

        using CamusCommand cmd = conn.CreateSelectCommand(OrderStatusSql);
        cmd.Parameters.Add("@w_id", ColumnType.Integer64, wId);
        cmd.Parameters.Add("@d_id", ColumnType.Integer64, dId);
        cmd.Parameters.Add("@c_id", ColumnType.Integer64, cId);
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct)) { /* consume */ }
    }

    // Delivery (~4%): deliver the oldest new-order in each district for the warehouse
    private static async Task TxDelivery(CamusConnection conn, CamusTransactionOptions txOptions, Random rng, long wId, CancellationToken ct)
    {
        int carrierId = rng.Next(1, 11);
        string deliveryDate = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

        for (int d = 1; d <= DistrictsPerWarehouse && !ct.IsCancellationRequested; d++)
        {
            using CamusCommand findCmd = conn.CreateSelectCommand(OldestNewOrderSql);
            findCmd.Parameters.Add("@w_id", ColumnType.Integer64, wId);
            findCmd.Parameters.Add("@d_id", ColumnType.Integer64, (long)d);
            findCmd.CommandTimeout = 30;
            CamusDataReader findReader = await findCmd.ExecuteReaderAsync(ct);

            long? minOId = null;
            while (await findReader.ReadAsync(ct))
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

            CamusTransaction tx = await conn.BeginTransactionAsync(txOptions, ct);
            try
            {
                await ExecWithParams(conn, NewOrderDeleteSql,
                [
                    ("@no_o_id", ColumnType.Integer64, (object)minOId.Value),
                    ("@d_id",    ColumnType.Integer64, (long)d),
                    ("@w_id",    ColumnType.Integer64, wId),
                ], tx, ct);

                await ExecWithParams(conn, OrderCarrierSql,
                [
                    ("@carrier_id", ColumnType.Integer64, (object)(long)carrierId),
                    ("@o_id",       ColumnType.Integer64, minOId.Value),
                    ("@d_id",       ColumnType.Integer64, (long)d),
                    ("@w_id",       ColumnType.Integer64, wId),
                ], tx, ct);

                await ExecWithParams(conn, OrderLineDeliverySql,
                [
                    ("@delivery_d", ColumnType.String,    (object)deliveryDate),
                    ("@o_id",       ColumnType.Integer64, minOId.Value),
                    ("@d_id",       ColumnType.Integer64, (long)d),
                    ("@w_id",       ColumnType.Integer64, wId),
                ], tx, ct);

                await tx.CommitAsync(ct);
            }
            catch
            {
                try { await tx.RollbackAsync(); } catch { }
            }
        }
    }

    // Stock-Level (~4%): count items below threshold in recent orders of a district
    private static async Task TxStockLevel(CamusConnection conn, Random rng, long wId, CancellationToken ct)
    {
        long dId = rng.Next(1, DistrictsPerWarehouse + 1);
        int threshold = rng.Next(10, 21);

        using CamusCommand cmd = conn.CreateSelectCommand(StockLevelSql);
        cmd.Parameters.Add("@w_id",      ColumnType.Integer64, wId);
        cmd.Parameters.Add("@threshold", ColumnType.Integer64, (long)threshold);
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync(ct);
        int count = 0;
        while (await reader.ReadAsync(ct)) count++;
    }

    // -------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------

    /// <summary>Chunks a table's rows into transactions and runs them with `concurrency` writers in flight.</summary>
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

    // Read inside the caller's transaction so the value the order uses is the one the transaction's
    // own d_next_o_id increment is validated against.
    private static async Task<long> FetchNextOrderId(CamusConnection conn, CamusTransaction tx, long dId, long wId, CancellationToken ct)
    {
        using CamusCommand cmd = conn.CreateSelectCommand(NextOrderIdSql);
        cmd.Transaction = tx;
        cmd.Parameters.Add("@d_id",   ColumnType.Integer64, dId);
        cmd.Parameters.Add("@d_w_id", ColumnType.Integer64, wId);
        cmd.CommandTimeout = 30;
        CamusDataReader reader = await cmd.ExecuteReaderAsync(ct);
        while (await reader.ReadAsync(ct))
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

    // Seed-table tags for SeedId. One per seeded table so ids can never collide across tables.
    private const int WarehouseTag = 1;
    private const int DistrictTag = 2;
    private const int CustomerTag = 3;
    private const int ItemTag = 4;
    private const int StockTag = 5;

    /// <summary>
    /// Deterministic 24-hex OID for a seed row, packed as table tag (2) + a (8) + b (6) + c (8) hex
    /// digits. The same natural key always yields the same primary key, which is what makes seed-batch
    /// replays idempotent (see <see cref="WorkloadHelpers.RunSeedBatch"/>).
    /// </summary>
    private static string SeedId(int tableTag, long a, long b = 0, long c = 0) =>
        $"{tableTag:x2}{a:x8}{b:x6}{c:x8}";

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
