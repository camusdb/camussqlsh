
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

internal static class NorthwindWorkload
{
    internal static async Task InitAsync(CamusConnection conn)
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
            "CREATE TABLE IF NOT EXISTS order_details (order_id INT64 NOT NULL, product_id INT64 NOT NULL, unit_price FLOAT64 NOT NULL, quantity INT64 NOT NULL, discount FLOAT64 NOT NULL)",
        ];

        foreach (string ddl in ddls)
            await WorkloadHelpers.DDL(conn, ddl);

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Inserting seed data...\n");

        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .HideCompleted(false)
            .StartAsync(async ctx =>
            {
                ProgressTask tCat = ctx.AddTask("[green]categories  [/]", maxValue: 8);
                ProgressTask tSup = ctx.AddTask("[green]suppliers   [/]", maxValue: 8);
                ProgressTask tPro = ctx.AddTask("[green]products    [/]", maxValue: 20);
                ProgressTask tCus = ctx.AddTask("[green]customers   [/]", maxValue: 15);
                ProgressTask tEmp = ctx.AddTask("[green]employees   [/]", maxValue: 5);
                ProgressTask tOrd = ctx.AddTask("[green]orders      [/]", maxValue: 30);
                ProgressTask tDet = ctx.AddTask("[green]order_details[/]", maxValue: 60);

                // categories
                (int id, string name, string desc)[] categories =
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
                foreach (var c in categories)
                {
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO categories (category_id, category_name, description) VALUES ({c.id}, '{WorkloadHelpers.Esc(c.name)}', '{WorkloadHelpers.Esc(c.desc)}')");
                    tCat.Increment(1);
                }

                // suppliers
                (int id, string company, string contact, string country, string phone)[] suppliers =
                [
                    (1, "Exotic Liquid",                    "Charlotte Cooper",           "UK",        "(171) 555-2222"),
                    (2, "New Orleans Cajun Delights",       "Shelley Burke",              "USA",       "(100) 555-4822"),
                    (3, "Grandma Kelly''s Homestead",       "Regina Murphy",              "USA",       "(313) 555-5735"),
                    (4, "Tokyo Traders",                    "Yoshi Nagase",               "Japan",     "(03) 3555-5011"),
                    (5, "Cooperativa de Quesos Las Cabras", "Antonio del Valle Saavedra", "Spain",     "(98) 598 76 54"),
                    (6, "Mayumi''s",                        "Mayumi Ohno",                "Japan",     "(06) 431-7877"),
                    (7, "Pavlova Ltd.",                     "Ian Devling",                "Australia", "(03) 444-2343"),
                    (8, "Specialty Biscuits Ltd.",          "Peter Wilson",               "UK",        "(161) 555-4448"),
                ];
                foreach (var s in suppliers)
                {
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO suppliers (supplier_id, company_name, contact_name, country, phone) VALUES ({s.id}, '{s.company}', '{s.contact}', '{s.country}', '{s.phone}')");
                    tSup.Increment(1);
                }

                // products
                (int id, string name, int sup, int cat, double price, int stock)[] products =
                [
                    ( 1, "Chai",                             1, 1, 18.00,  39),
                    ( 2, "Chang",                            1, 1, 19.00,  17),
                    ( 3, "Aniseed Syrup",                    1, 2, 10.00,  13),
                    ( 4, "Chef Anton''s Cajun Seasoning",    2, 2, 22.00,  53),
                    ( 5, "Chef Anton''s Gumbo Mix",          2, 2, 21.35,   0),
                    ( 6, "Grandma''s Boysenberry Spread",    3, 2, 25.00, 120),
                    ( 7, "Uncle Bob''s Organic Dried Pears", 3, 7, 30.00,  15),
                    ( 8, "Northwoods Cranberry Sauce",       3, 2, 40.00,   6),
                    ( 9, "Mishi Kobe Niku",                  4, 6, 97.00,  29),
                    (10, "Ikura",                            4, 8, 31.00,  31),
                    (11, "Queso Cabrales",                   5, 4, 21.00,  22),
                    (12, "Queso Manchego La Pastora",        5, 4, 38.00,  86),
                    (13, "Konbu",                            6, 8,  6.00,  24),
                    (14, "Tofu",                             6, 7, 23.25,  35),
                    (15, "Genen Shouyu",                     6, 2, 15.50,  39),
                    (16, "Pavlova",                          7, 3, 17.45,  29),
                    (17, "Alice Mutton",                     7, 6, 39.00,   0),
                    (18, "Carnarvon Tigers",                 7, 8, 62.50,  42),
                    (19, "Teatime Chocolate Biscuits",       8, 3,  9.20,  25),
                    (20, "Sir Rodney''s Marmalade",          8, 3, 81.00,  40),
                ];
                foreach (var p in products)
                {
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO products (product_id, product_name, supplier_id, category_id, unit_price, units_in_stock) VALUES ({p.id}, '{p.name}', {p.sup}, {p.cat}, {p.price.ToString(CultureInfo.InvariantCulture)}, {p.stock})");
                    tPro.Increment(1);
                }

                // customers
                (string id, string company, string contact, string country, string city, string phone)[] customers =
                [
                    ("ALFKI", "Alfreds Futterkiste",                 "Maria Anders",       "Germany",     "Berlin",      "030-0074321"),
                    ("ANATR", "Ana Trujillo Emparedados y helados",  "Ana Trujillo",       "Mexico",      "Mexico D.F.", "(5) 555-4729"),
                    ("ANTON", "Antonio Moreno Taqueria",             "Antonio Moreno",     "Mexico",      "Mexico D.F.", "(5) 555-3932"),
                    ("AROUT", "Around the Horn",                     "Thomas Hardy",       "UK",          "London",      "(171) 555-7788"),
                    ("BERGS", "Berglunds snabbkop",                  "Christina Berglund", "Sweden",      "Lulea",       "0921-12 34 65"),
                    ("BLAUS", "Blauer See Delikatessen",             "Hanna Moos",         "Germany",     "Mannheim",    "0621-08460"),
                    ("BLONP", "Blondesddsl pere et fils",            "Frederique Citeaux", "France",      "Strasbourg",  "88.60.15.31"),
                    ("BOLID", "Bolido Comidas preparadas",           "Martin Sommer",      "Spain",       "Madrid",      "(91) 555 22 82"),
                    ("BONAP", "Bon app",                             "Laurence Lebihan",   "France",      "Marseille",   "91.24.45.40"),
                    ("BOTTM", "Bottom-Dollar Markets",               "Elizabeth Lincoln",  "Canada",      "Tsawassen",   "(604) 555-4729"),
                    ("BSBEV", "B''s Beverages",                      "Victoria Ashworth",  "UK",          "London",      "(171) 555-1212"),
                    ("CACTU", "Cactus Comidas para llevar",          "Patricio Simpson",   "Argentina",   "Buenos Aires","(1) 135-5555"),
                    ("CENTC", "Centro comercial Moctezuma",          "Francisco Chang",    "Mexico",      "Mexico D.F.", "(5) 555-3392"),
                    ("CHOPS", "Chop-suey Chinese",                   "Yang Wang",          "Switzerland", "Bern",        "0452-076545"),
                    ("COMMI", "Comercio Mineiro",                    "Pedro Afonso",       "Brazil",      "Sao Paulo",   "(11) 555-7647"),
                ];
                foreach (var c in customers)
                {
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO customers (customer_id, company_name, contact_name, country, city, phone) VALUES ('{c.id}', '{c.company}', '{c.contact}', '{c.country}', '{c.city}', '{c.phone}')");
                    tCus.Increment(1);
                }

                // employees
                (int id, string last, string first, string title, string hire)[] employees =
                [
                    (1, "Davolio",   "Nancy",    "Sales Representative", "1992-05-01"),
                    (2, "Fuller",    "Andrew",   "Vice President Sales", "1992-08-14"),
                    (3, "Leverling", "Janet",    "Sales Representative", "1992-04-01"),
                    (4, "Peacock",   "Margaret", "Sales Representative", "1993-05-03"),
                    (5, "Buchanan",  "Steven",   "Sales Manager",        "1993-10-17"),
                ];
                foreach (var e in employees)
                {
                    await WorkloadHelpers.Exec(conn, $"INSERT INTO employees (employee_id, last_name, first_name, title, hire_date) VALUES ({e.id}, '{e.last}', '{e.first}', '{e.title}', '{e.hire}')");
                    tEmp.Increment(1);
                }

                // orders + order_details (30 orders, 1–3 details each)
                Random rng = new(42);
                string[] custIds = customers.Select(c => c.id).ToArray();
                double[] prices = products.Select(p => p.price).ToArray();

                for (int orderId = 1001; orderId <= 1030; orderId++)
                {
                    string custId = custIds[rng.Next(custIds.Length)];
                    int empId = rng.Next(1, 6);
                    string orderDate = new DateTime(2024, rng.Next(1, 13), rng.Next(1, 28)).ToString("yyyy-MM-dd");
                    string shipCountry = customers.First(c => c.id == custId).country;

                    await WorkloadHelpers.Exec(conn, $"INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_country) VALUES ({orderId}, '{custId}', {empId}, '{orderDate}', '{shipCountry}')");
                    tOrd.Increment(1);

                    int detailCount = rng.Next(1, 4);
                    HashSet<int> usedProducts = new();
                    for (int d = 0; d < detailCount; d++)
                    {
                        int prodId;
                        do { prodId = rng.Next(1, 21); } while (!usedProducts.Add(prodId));
                        double unitPrice = prices[prodId - 1];
                        int qty = rng.Next(1, 30);
                        double discount = rng.Next(0, 5) == 0 ? 0.05 : 0.0;

                        await WorkloadHelpers.Exec(conn, $"INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES ({orderId}, {prodId}, {unitPrice.ToString(CultureInfo.InvariantCulture)}, {qty}, {discount.ToString(CultureInfo.InvariantCulture)})");
                        tDet.Increment(1);
                    }
                }
            });

        AnsiConsole.MarkupLine("\n[green]Northwind workload initialized.[/]");
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run northwind[/] to start generating activity.");
    }

    internal static async Task RunAsync(CamusConnection conn, int concurrency, int durationSeconds)
    {
        AnsiConsole.MarkupLine("Starting northwind workload: [blue]{0}[/] workers, [blue]{1}s[/] duration", concurrency, durationSeconds);
        AnsiConsole.MarkupLine("Press [grey]Ctrl+C[/] to stop early.\n");

        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(durationSeconds));
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        long totalOps = 0;
        long totalErrors = 0;
        long nextOrderId = 2000;
        Stopwatch sw = Stopwatch.StartNew();

        string[] custIds = ["ALFKI", "ANATR", "ANTON", "AROUT", "BERGS", "BLAUS", "BLONP", "BOLID", "BONAP", "BOTTM", "BSBEV", "CACTU", "CENTC", "CHOPS", "COMMI"];
        string[] countries = ["Germany", "Mexico", "UK", "Sweden", "France", "Spain", "Canada", "Argentina", "Brazil", "Switzerland"];
        double[] unitPrices = [18.00, 19.00, 10.00, 22.00, 21.35, 25.00, 30.00, 40.00, 97.00, 31.00, 21.00, 38.00, 6.00, 23.25, 15.50, 17.45, 39.00, 62.50, 9.20, 81.00];

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
                        int catId = rng.Next(1, 9);
                        using CamusCommand cmd = conn.CreateSelectCommand($"SELECT product_id, product_name, units_in_stock FROM products WHERE category_id = {catId}");
                        cmd.CommandTimeout = 30;
                        CamusDataReader reader = await cmd.ExecuteReaderAsync();
                        while (await reader.ReadAsync()) { /* consume */ }
                    }
                    else
                    {
                        long orderId = Interlocked.Increment(ref nextOrderId);
                        string custId = custIds[rng.Next(custIds.Length)];
                        int empId = rng.Next(1, 6);
                        string orderDate = DateTime.UtcNow.ToString("yyyy-MM-dd");
                        string shipCountry = countries[rng.Next(countries.Length)];

                        await WorkloadHelpers.Exec(conn, $"INSERT INTO orders (order_id, customer_id, employee_id, order_date, ship_country) VALUES ({orderId}, '{custId}', {empId}, '{orderDate}', '{shipCountry}')");

                        int detailCount = rng.Next(1, 3);
                        HashSet<int> used = new();
                        for (int d = 0; d < detailCount; d++)
                        {
                            int prodId;
                            do { prodId = rng.Next(1, 21); } while (!used.Add(prodId));
                            double price = unitPrices[prodId - 1];
                            int qty = rng.Next(1, 20);
                            await WorkloadHelpers.Exec(conn, $"INSERT INTO order_details (order_id, product_id, unit_price, quantity, discount) VALUES ({orderId}, {prodId}, {price.ToString(CultureInfo.InvariantCulture)}, {qty}, 0)");
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
