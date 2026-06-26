
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;

internal static class WorkloadCommand
{
    internal static async Task RunAsync(string[] args)
    {
        WorkloadArgs wa = ParseArgs(args);

        if (wa.Command != "init" && wa.Command != "run")
        {
            AnsiConsole.MarkupLine("[red]Unknown workload command:[/] {0}", Markup.Escape(wa.Command.Length > 0 ? wa.Command : "(none)"));
            AnsiConsole.MarkupLine("Usage: camus-cli workload <init|run> <bank|northwind|factory|tpcc> [[options]]");
            AnsiConsole.MarkupLine("Options:");
            AnsiConsole.MarkupLine("  -c, --connection-source  Connection string (default: Endpoint=http://localhost:5095;Database=demo)");
            AnsiConsole.MarkupLine("  --database               Target database name (default: demo)");
            AnsiConsole.MarkupLine("  --rows N                 Number of rows to generate for init (default: 1000, bank only)");
            AnsiConsole.MarkupLine("  --concurrency N          Number of parallel workers for run (default: 3)");
            AnsiConsole.MarkupLine("  --duration N             Run duration in seconds (default: 60)");
            return;
        }

        if (wa.WorkloadName != "bank" && wa.WorkloadName != "northwind" && wa.WorkloadName != "factory" && wa.WorkloadName != "tpcc")
        {
            AnsiConsole.MarkupLine("[red]Unknown workload:[/] {0}", Markup.Escape(wa.WorkloadName.Length > 0 ? wa.WorkloadName : "(none)"));
            AnsiConsole.MarkupLine("Available workloads: bank, northwind, factory, tpcc");
            return;
        }

        string connStr = BuildConnectionString(wa.ConnectionSource, wa.Database);
        AnsiConsole.MarkupLine("Connecting to [blue]{0}[/]...", Markup.Escape(connStr));

        CamusConnection conn;
        try
        {
            conn = await ConnectionHelper.OpenAsync(connStr);
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine("[red]Connection failed:[/] {0}", Markup.Escape(ex.Message));
            return;
        }

        AnsiConsole.MarkupLine("[green]Connected.[/]\n");

        switch (wa.WorkloadName)
        {
            case "bank":
                if (wa.Command == "init")
                    await BankWorkload.InitAsync(conn, wa.Rows);
                else
                    await BankWorkload.RunAsync(conn, wa.Concurrency, wa.Duration);
                break;
            case "northwind":
                if (wa.Command == "init")
                    await NorthwindWorkload.InitAsync(conn);
                else
                    await NorthwindWorkload.RunAsync(conn, wa.Concurrency, wa.Duration);
                break;
            case "factory":
                if (wa.Command == "init")
                    await FactoryWorkload.InitAsync(conn);
                else
                    await FactoryWorkload.RunAsync(conn, wa.Concurrency, wa.Duration);
                break;
            case "tpcc":
                if (wa.Command == "init")
                    await TpccWorkload.InitAsync(conn, wa.Rows);
                else
                    await TpccWorkload.RunAsync(conn, wa.Concurrency, wa.Duration);
                break;
        }
    }

    internal static WorkloadArgs ParseArgs(string[] args)
    {
        string command = args.Length > 0 ? args[0].ToLowerInvariant() : "";
        string workloadName = args.Length > 1 ? args[1].ToLowerInvariant() : "";
        string connectionSource = "";
        string database = "demo";
        int rows = 1000;
        int concurrency = 3;
        int duration = 60;

        for (int i = 2; i < args.Length; i++)
        {
            switch (args[i])
            {
                case "-c":
                case "--connection-source":
                    if (i + 1 < args.Length) connectionSource = args[++i];
                    break;
                case "--database":
                    if (i + 1 < args.Length) database = args[++i];
                    break;
                case "--rows":
                    if (i + 1 < args.Length) int.TryParse(args[++i], out rows);
                    break;
                case "--concurrency":
                    if (i + 1 < args.Length) int.TryParse(args[++i], out concurrency);
                    break;
                case "--duration":
                    if (i + 1 < args.Length) int.TryParse(args[++i], out duration);
                    break;
            }
        }

        return new WorkloadArgs(command, workloadName, connectionSource, database, rows, concurrency, duration);
    }

    internal static string BuildConnectionString(string connectionSource, string database)
    {
        if (string.IsNullOrEmpty(connectionSource))
            return $"Endpoint=http://localhost:5095;Database={database}";

        bool hasDatabase = connectionSource
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Any(p => p.StartsWith("Database=", StringComparison.OrdinalIgnoreCase));

        return hasDatabase ? connectionSource : $"{connectionSource};Database={database}";
    }
}

internal record WorkloadArgs(
    string Command,
    string WorkloadName,
    string ConnectionSource,
    string Database,
    int Rows,
    int Concurrency,
    int Duration
);
