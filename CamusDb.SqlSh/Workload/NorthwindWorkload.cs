
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

internal static class NorthwindWorkload
{
    private const int BatchSize = 10;

    // -------------------------------------------------------------------------
    // SQL templates — every value travels as a bound parameter
    // -------------------------------------------------------------------------

    private const string CategoryInsertSql =
        "INSERT INTO categories (category_id, category_name, description) VALUES (@category_id, @category_name, @description)";

    private const string SupplierInsertSql =
        "INSERT INTO suppliers (supplier_id, company_name, contact_name, country, phone) " +
        "VALUES (@supplier_id, @company_name, @contact_name, @country, @phone)";

    private const string ProductInsertSql =
        "INSERT INTO products (product_id, product_name, supplier_id, category_id, unit_price, units_in_stock) " +
        "VALUES (@product_id, @product_name, @supplier_id, @category_id, @unit_price, @units_in_stock)";

    private const string CustomerInsertSql =
        "INSERT INTO customers (customer_id, company_name, contact_name, country, city, phone) " +
        "VALUES (@customer_id, @company_name, @contact_name, @country, @city, @phone)";

    private const string EmployeeInsertSql =
        "INSERT INTO employees (employee_id, last_name, first_name, title, hire_date) " +
        "VALUES (@employee_id, @last_name, @first_name, @title, @hire_date)";

    private const string OrderInsertSql =
        "INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_country) " +
        "VALUES (@order_id, @customer_id, @employee_id, @order_date, @ship_country)";

    private const string OrderDetailInsertSql =
        "INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) " +
        "VALUES (@order_id, @product_id, @unit_price, @quantity, @discount)";

    private const string ProductsByCategorySql =
        "SELECT product_id, product_name, units_in_stock FROM products WHERE category_id = @category_id";

    /// <summary>
    /// Every statement the <c>run</c> phase issues, for the prepared-statement warm-up. Keep in sync
    /// with <see cref="RunAsync"/>: a statement missing here isn't broken, it just pays the driver's
    /// usual two-execution warm-up instead of being registered before the clock starts.
    /// </summary>
    internal static IReadOnlyList<string> RunStatements =>
        [ProductsByCategorySql, OrderInsertSql, OrderDetailInsertSql];

    // -------------------------------------------------------------------------
    // Seed data
    // -------------------------------------------------------------------------

    private static readonly (int Id, string Name, string Desc)[] Categories =
    [
        (1, "Beverages",      "Soft drinks, coffees, teas, beers and ales"),
        (2, "Condiments",     "Sweet and savory sauces, relishes, spreads and seasonings"),
        (3, "Confections",    "Desserts, candies and sweet breads"),
        (4, "Dairy Products", "Cheeses"),
        (5, "Grains/Cereals", "Breads, crackers, pasta and cereal"),
        (6, "Meat/Poultry",   "Prepared meats"),
        (7, "Produce",        "Dried fruit and bean curd"),
        (8, "Seafood",        "Seaweed and fish"),
    ];

    private static readonly (int Id, string Company, string Contact, string Country, string Phone)[] Suppliers =
    [
        (1, "Exotic Liquid",                    "Charlotte Cooper",           "UK",        "(171) 555-2222"),
        (2, "New Orleans Cajun Delights",       "Shelley Burke",              "USA",       "(100) 555-4822"),
        (3, "Grandma Kelly's Homestead",        "Regina Murphy",              "USA",       "(313) 555-5735"),
        (4, "Tokyo Traders",                    "Yoshi Nagase",               "Japan",     "(03) 3555-5011"),
        (5, "Cooperativa de Quesos Las Cabras", "Antonio del Valle Saavedra", "Spain",     "(98) 598 76 54"),
        (6, "Mayumi's",                         "Mayumi Ohno",                "Japan",     "(06) 431-7877"),
        (7, "Pavlova Ltd.",                     "Ian Devling",                "Australia", "(03) 444-2343"),
        (8, "Specialty Biscuits Ltd.",          "Peter Wilson",               "UK",        "(161) 555-4448"),
    ];

    private static readonly (int Id, string Name, int Sup, int Cat, double Price, int Stock)[] Products =
    [
        ( 1, "Chai",                           1, 1, 18.00,  39),
        ( 2, "Chang",                          1, 1, 19.00,  17),
        ( 3, "Aniseed Syrup",                  1, 2, 10.00,  13),
        ( 4, "Chef Anton's Cajun Seasoning",   2, 2, 22.00,  53),
        ( 5, "Chef Anton's Gumbo Mix",         2, 2, 21.35,   0),
        ( 6, "Grandma's Boysenberry Spread",   3, 2, 25.00, 120),
        ( 7, "Uncle Bob's Organic Dried Pears", 3, 7, 30.00,  15),
        ( 8, "Northwoods Cranberry Sauce",     3, 2, 40.00,   6),
        ( 9, "Mishi Kobe Niku",                4, 6, 97.00,  29),
        (10, "Ikura",                          4, 8, 31.00,  31),
        (11, "Queso Cabrales",                 5, 4, 21.00,  22),
        (12, "Queso Manchego La Pastora",      5, 4, 38.00,  86),
        (13, "Konbu",                          6, 8,  6.00,  24),
        (14, "Tofu",                           6, 7, 23.25,  35),
        (15, "Genen Shouyu",                   6, 2, 15.50,  39),
        (16, "Pavlova",                        7, 3, 17.45,  29),
        (17, "Alice Mutton",                   7, 6, 39.00,   0),
        (18, "Carnarvon Tigers",               7, 8, 62.50,  42),
        (19, "Teatime Chocolate Biscuits",     8, 3,  9.20,  25),
        (20, "Sir Rodney's Marmalade",         8, 3, 81.00,  40),
    ];

    private static readonly (string Id, string Company, string Contact, string Country, string City, string Phone)[] Customers =
    [
        ("ALFKI", "Alfreds Futterkiste",                "Maria Anders",       "Germany",     "Berlin",       "030-0074321"),
        ("ANATR", "Ana Trujillo Emparedados y helados", "Ana Trujillo",       "Mexico",      "Mexico D.F.",  "(5) 555-4729"),
        ("ANTON", "Antonio Moreno Taqueria",            "Antonio Moreno",     "Mexico",      "Mexico D.F.",  "(5) 555-3932"),
        ("AROUT", "Around the Horn",                    "Thomas Hardy",       "UK",          "London",       "(171) 555-7788"),
        ("BERGS", "Berglunds snabbkop",                 "Christina Berglund", "Sweden",      "Lulea",        "0921-12 34 65"),
        ("BLAUS", "Blauer See Delikatessen",            "Hanna Moos",         "Germany",     "Mannheim",     "0621-08460"),
        ("BLONP", "Blondesddsl pere et fils",           "Frederique Citeaux", "France",      "Strasbourg",   "88.60.15.31"),
        ("BOLID", "Bolido Comidas preparadas",          "Martin Sommer",      "Spain",       "Madrid",       "(91) 555 22 82"),
        ("BONAP", "Bon app",                            "Laurence Lebihan",   "France",      "Marseille",    "91.24.45.40"),
        ("BOTTM", "Bottom-Dollar Markets",              "Elizabeth Lincoln",  "Canada",      "Tsawassen",    "(604) 555-4729"),
        ("BSBEV", "B's Beverages",                      "Victoria Ashworth",  "UK",          "London",       "(171) 555-1212"),
        ("CACTU", "Cactus Comidas para llevar",         "Patricio Simpson",   "Argentina",   "Buenos Aires", "(1) 135-5555"),
        ("CENTC", "Centro comercial Moctezuma",         "Francisco Chang",    "Mexico",      "Mexico D.F.",  "(5) 555-3392"),
        ("CHOPS", "Chop-suey Chinese",                  "Yang Wang",          "Switzerland", "Bern",         "0452-076545"),
        ("COMMI", "Comercio Mineiro",                   "Pedro Afonso",       "Brazil",      "Sao Paulo",    "(11) 555-7647"),
    ];

    private static readonly (int Id, string Last, string First, string Title, string Hire)[] Employees =
    [
        (1, "Davolio",   "Nancy",    "Sales Representative", "1992-05-01"),
        (2, "Fuller",    "Andrew",   "Vice President Sales", "1992-08-14"),
        (3, "Leverling", "Janet",    "Sales Representative", "1992-04-01"),
        (4, "Peacock",   "Margaret", "Sales Representative", "1993-05-03"),
        (5, "Buchanan",  "Steven",   "Sales Manager",        "1993-10-17"),
    ];

    // -------------------------------------------------------------------------
    // Init
    // -------------------------------------------------------------------------

    internal static async Task InitAsync(CamusConnection conn, int concurrency, CamusTransactionOptions txOptions)
    {
        AnsiConsole.MarkupLine("[cyan]Creating northwind schema...[/]");

        string[] ddls =
        [
            "CREATE TABLE IF NOT EXISTS categories (category_id INT64 PRIMARY KEY, category_name STRING NOT NULL, description STRING)",
            "CREATE TABLE IF NOT EXISTS suppliers (supplier_id INT64 PRIMARY KEY, company_name STRING NOT NULL, contact_name STRING, country STRING, phone STRING)",
            "CREATE TABLE IF NOT EXISTS products (product_id INT64 PRIMARY KEY, product_name STRING NOT NULL, supplier_id INT64, category_id INT64, unit_price FLOAT64 NOT NULL, units_in_stock INT64 NOT NULL)",
            "CREATE TABLE IF NOT EXISTS customers (customer_id STRING PRIMARY KEY, company_name STRING NOT NULL, contact_name STRING, country STRING, city STRING, phone STRING)",
            "CREATE TABLE IF NOT EXISTS employees (employee_id INT64 PRIMARY KEY, last_name STRING NOT NULL, first_name STRING NOT NULL, title STRING, hire_date STRING)",
            "CREATE TABLE IF NOT EXISTS orders (order_id INT64 PRIMARY KEY, customer_id STRING NOT NULL, employee_id INT64 NOT NULL, order_date STRING NOT NULL, ship_country STRING)",
            // Keyed on (order_id, product_id) as in real Northwind. A primary key is mandatory here —
            // the original DDL declared none at all, which is why table creation used to fail.
            "CREATE TABLE IF NOT EXISTS order_details (order_id INT64 NOT NULL, product_id INT64 NOT NULL, unit_price FLOAT64 NOT NULL, quantity INT64 NOT NULL, discount FLOAT64 NOT NULL, PRIMARY KEY (order_id, product_id))",
        ];

        foreach (string ddl in ddls)
            await DDL(conn, ddl);

        // Index the run-phase predicate columns. A non-indexed predicate takes a [-inf,+inf] range lock
        // over the whole table, which serializes the workers however many of them there are.
        string[] indexes =
        [
            "ALTER TABLE products ADD INDEX idx_products_category (category_id)",
            "ALTER TABLE orders ADD INDEX idx_orders_customer (customer_id)",
            // order_details needs no index here: its (order_id, product_id) primary key already covers
            // lookups by order_id as a leading-column prefix.
        ];

        foreach (string index in indexes)
        {
            // A re-run of init hits an already-created index; that's not a failure.
            try { await DDL(conn, index); } catch (CamusException) { }
        }

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Inserting seed data ([blue]{0}[/] parallel writers)...\n", concurrency);

        // Reference rows first (categories/suppliers/products/customers/employees), then the orders that
        // point at them. Each group fans out; the groups themselves stay ordered.
        List<(string Sql, Param[] Parameters)> categories = Categories.Select(c => (CategoryInsertSql, new Param[]
        {
            P("@category_id", ColumnType.Integer64, (long)c.Id),
            P("@category_name", ColumnType.String, c.Name),
            P("@description", ColumnType.String, c.Desc),
        })).ToList();

        List<(string Sql, Param[] Parameters)> suppliers = Suppliers.Select(s => (SupplierInsertSql, new Param[]
        {
            P("@supplier_id", ColumnType.Integer64, (long)s.Id),
            P("@company_name", ColumnType.String, s.Company),
            P("@contact_name", ColumnType.String, s.Contact),
            P("@country", ColumnType.String, s.Country),
            P("@phone", ColumnType.String, s.Phone),
        })).ToList();

        List<(string Sql, Param[] Parameters)> products = Products.Select(p => (ProductInsertSql, new Param[]
        {
            P("@product_id", ColumnType.Integer64, (long)p.Id),
            P("@product_name", ColumnType.String, p.Name),
            P("@supplier_id", ColumnType.Integer64, (long)p.Sup),
            P("@category_id", ColumnType.Integer64, (long)p.Cat),
            P("@unit_price", ColumnType.Float64, p.Price),
            P("@units_in_stock", ColumnType.Integer64, (long)p.Stock),
        })).ToList();

        List<(string Sql, Param[] Parameters)> customers = Customers.Select(c => (CustomerInsertSql, new Param[]
        {
            P("@customer_id", ColumnType.String, c.Id),
            P("@company_name", ColumnType.String, c.Company),
            P("@contact_name", ColumnType.String, c.Contact),
            P("@country", ColumnType.String, c.Country),
            P("@city", ColumnType.String, c.City),
            P("@phone", ColumnType.String, c.Phone),
        })).ToList();

        List<(string Sql, Param[] Parameters)> employees = Employees.Select(e => (EmployeeInsertSql, new Param[]
        {
            P("@employee_id", ColumnType.Integer64, (long)e.Id),
            P("@last_name", ColumnType.String, e.Last),
            P("@first_name", ColumnType.String, e.First),
            P("@title", ColumnType.String, e.Title),
            P("@hire_date", ColumnType.String, e.Hire),
        })).ToList();

        // orders + order_details (30 orders, 1–3 details each), generated single-threaded from a seeded RNG
        Random rng = new(42);
        List<(string Sql, Param[] Parameters)> orders = [];
        List<(string Sql, Param[] Parameters)> details = [];

        for (int orderId = 1001; orderId <= 1030; orderId++)
        {
            var customer = Customers[rng.Next(Customers.Length)];
            long empId = rng.Next(1, 6);
            string orderDate = new DateTime(2024, rng.Next(1, 13), rng.Next(1, 28)).ToString("yyyy-MM-dd");

            orders.Add((OrderInsertSql,
            [
                P("@order_id", ColumnType.Integer64, (long)orderId),
                P("@customer_id", ColumnType.String, customer.Id),
                P("@employee_id", ColumnType.Integer64, empId),
                P("@order_date", ColumnType.String, orderDate),
                P("@ship_country", ColumnType.String, customer.Country),
            ]));

            int detailCount = rng.Next(1, 4);
            HashSet<int> usedProducts = [];
            for (int d = 0; d < detailCount; d++)
            {
                int prodId;
                do { prodId = rng.Next(1, 21); } while (!usedProducts.Add(prodId));

                details.Add((OrderDetailInsertSql,
                [
                    P("@order_id", ColumnType.Integer64, (long)orderId),
                    P("@product_id", ColumnType.Integer64, (long)prodId),
                    P("@unit_price", ColumnType.Float64, Products[prodId - 1].Price),
                    P("@quantity", ColumnType.Integer64, (long)rng.Next(1, 30)),
                    P("@discount", ColumnType.Float64, rng.Next(0, 5) == 0 ? 0.05 : 0.0),
                ]));
            }
        }

        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .HideCompleted(false)
            .StartAsync(async ctx =>
            {
                ProgressTask tCat = ctx.AddTask("[green]categories  [/]", maxValue: categories.Count);
                ProgressTask tSup = ctx.AddTask("[green]suppliers   [/]", maxValue: suppliers.Count);
                ProgressTask tPro = ctx.AddTask("[green]products    [/]", maxValue: products.Count);
                ProgressTask tCus = ctx.AddTask("[green]customers   [/]", maxValue: customers.Count);
                ProgressTask tEmp = ctx.AddTask("[green]employees   [/]", maxValue: employees.Count);
                ProgressTask tOrd = ctx.AddTask("[green]orders      [/]", maxValue: orders.Count);
                ProgressTask tDet = ctx.AddTask("[green]order_details[/]", maxValue: details.Count);

                await Seed(conn, txOptions, concurrency, categories, tCat);
                await Seed(conn, txOptions, concurrency, suppliers, tSup);
                await Seed(conn, txOptions, concurrency, products, tPro);
                await Seed(conn, txOptions, concurrency, customers, tCus);
                await Seed(conn, txOptions, concurrency, employees, tEmp);
                await Seed(conn, txOptions, concurrency, orders, tOrd);
                await Seed(conn, txOptions, concurrency, details, tDet);
            });

        AnsiConsole.MarkupLine("\n[green]Northwind workload initialized.[/]");
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run northwind[/] to start generating activity.");
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
        AnsiConsole.MarkupLine("Starting northwind workload: [blue]{0}[/] workers, [blue]{1}s[/] duration", concurrency, durationSeconds);
        AnsiConsole.MarkupLine("Press [grey]Ctrl+C[/] to stop early.\n");

        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(durationSeconds));
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        long totalOps = 0;
        long totalErrors = 0;
        long nextOrderId = 2000;
        Stopwatch sw = Stopwatch.StartNew();

        string[] custIds = Customers.Select(c => c.Id).ToArray();
        string[] countries = ["Germany", "Mexico", "UK", "Sweden", "France", "Spain", "Canada", "Argentina", "Brazil", "Switzerland"];

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
                    int op = rng.Next(5); // 0-1 = reads, 2-4 = writes
                    if (op <= 1)
                    {
                        await QueryWithParams(conn, ProductsByCategorySql,
                        [
                            P("@category_id", ColumnType.Integer64, (long)rng.Next(1, 9)),
                        ], cts.Token);
                    }
                    else
                    {
                        long orderId = Interlocked.Increment(ref nextOrderId);
                        string custId = custIds[rng.Next(custIds.Length)];
                        long empId = rng.Next(1, 6);
                        string orderDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
                        string shipCountry = countries[rng.Next(countries.Length)];

                        int detailCount = rng.Next(1, 3);
                        HashSet<int> used = [];
                        List<(int ProdId, long Qty)> lines = [];
                        for (int d = 0; d < detailCount; d++)
                        {
                            int prodId;
                            do { prodId = rng.Next(1, 21); } while (!used.Add(prodId));
                            lines.Add((prodId, rng.Next(1, 20)));
                        }

                        // The order header and its lines are one atomic unit, so a partially-written
                        // order can never be observed by the read side.
                        CamusTransaction tx = await conn.BeginTransactionAsync(txOptions, cts.Token);
                        try
                        {
                            await ExecWithParams(conn, OrderInsertSql,
                            [
                                P("@order_id", ColumnType.Integer64, orderId),
                                P("@customer_id", ColumnType.String, custId),
                                P("@employee_id", ColumnType.Integer64, empId),
                                P("@order_date", ColumnType.String, orderDate),
                                P("@ship_country", ColumnType.String, shipCountry),
                            ], tx, cts.Token);

                            foreach ((int prodId, long qty) in lines)
                            {
                                await ExecWithParams(conn, OrderDetailInsertSql,
                                [
                                    P("@order_id", ColumnType.Integer64, orderId),
                                    P("@product_id", ColumnType.Integer64, (long)prodId),
                                    P("@unit_price", ColumnType.Float64, Products[prodId - 1].Price),
                                    P("@quantity", ColumnType.Integer64, qty),
                                    P("@discount", ColumnType.Float64, 0.0),
                                ], tx, cts.Token);
                            }

                            await tx.CommitAsync(cts.Token);
                        }
                        catch
                        {
                            try { await tx.RollbackAsync(); } catch { }
                            throw;
                        }
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
}
