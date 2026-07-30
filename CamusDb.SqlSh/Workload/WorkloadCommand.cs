
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
            AnsiConsole.MarkupLine("  -c, --connection-source  Connection string (default: gRPC on http://localhost:5096,");
            AnsiConsole.MarkupLine("                           falling back to REST on http://localhost:5095)");
            AnsiConsole.MarkupLine("  --database               Target database name (default: demo)");
            AnsiConsole.MarkupLine("  --rows N                 Number of rows to generate for init (default: 1000, bank only)");
            AnsiConsole.MarkupLine("  --concurrency N          Parallel workers for run, parallel writers for init (default: 64)");
            AnsiConsole.MarkupLine("  --duration N             Run duration in seconds (default: 60)");
            AnsiConsole.MarkupLine("  --locking MODE           Locking mode: optimistic | pessimistic (default: optimistic)");
            AnsiConsole.MarkupLine("  --isolation LEVEL        Isolation level: serializable | read-committed (default: serializable)");
            AnsiConsole.MarkupLine("  --no-prepare             Run every statement inline instead of preparing it, to compare");
            AnsiConsole.MarkupLine("                           against the default prepared path");
            AnsiConsole.MarkupLine("  -u, --user NAME          User to authenticate as (authenticated servers only)");
            AnsiConsole.MarkupLine("  -p, --password SECRET    That user's password");
            return;
        }

        if (wa.WorkloadName != "bank" && wa.WorkloadName != "northwind" && wa.WorkloadName != "factory" && wa.WorkloadName != "tpcc")
        {
            AnsiConsole.MarkupLine("[red]Unknown workload:[/] {0}", Markup.Escape(wa.WorkloadName.Length > 0 ? wa.WorkloadName : "(none)"));
            AnsiConsole.MarkupLine("Available workloads: bank, northwind, factory, tpcc");
            return;
        }

        // Validate the concurrency flags up front so a typo fails loudly instead of silently
        // falling back to the server default.
        string? locking = null;
        if (wa.Locking is not null && (locking = NormalizeLocking(wa.Locking)) is null)
        {
            AnsiConsole.MarkupLine("[red]Invalid --locking value:[/] {0} [grey58](expected: optimistic | pessimistic)[/]", Markup.Escape(wa.Locking));
            return;
        }

        string? isolation = null;
        if (wa.Isolation is not null && (isolation = NormalizeIsolation(wa.Isolation)) is null)
        {
            AnsiConsole.MarkupLine("[red]Invalid --isolation value:[/] {0} [grey58](expected: serializable | read-committed)[/]", Markup.Escape(wa.Isolation));
            return;
        }

        List<string> attempts = BuildConnectionAttempts(wa.ConnectionSource, wa.Database, locking, isolation, wa.User, wa.Password, wa.NoPrepare);
        AnsiConsole.MarkupLine("Connecting to [blue]{0}[/]...", Markup.Escape(GetConnValue(attempts[0], "Endpoint") ?? "server"));

        CamusConnection conn;
        string connStr;
        try
        {
            (conn, connStr) = await ConnectionHelper.OpenFirstAsync(attempts);
        }
        catch (Exception ex)
        {
            AnsiConsole.MarkupLine("[red]Connection failed:[/] {0}", Markup.Escape(ex.Message));
            return;
        }

        // Every transaction a workload opens — seeding included — carries these knobs explicitly, so
        // the mode is the workload's own choice rather than whatever the server happens to default to.
        CamusTransactionOptions txOptions = new()
        {
            Locking = locking == "Pessimistic" ? CamusLocking.Pessimistic : CamusLocking.Optimistic,
            IsolationLevel = isolation == "ReadCommitted" ? CamusIsolationLevel.ReadCommitted : CamusIsolationLevel.Serializable,
            Mode = CamusTransactionMode.ReadWrite,
        };
        conn.DefaultTransactionOptions = txOptions;

        AnsiConsole.MarkupLine(
            "[green]Connected[/] [grey58]over[/] [white]{0}[/] [grey58]({1})[/]\n",
            DescribeTransport(connStr),
            Markup.Escape($"{txOptions.Locking} / {txOptions.IsolationLevel}, {(wa.NoPrepare ? "inline statements" : "prepared statements")}"));

        try
        {
            // Only the measured run phase is warmed up. Seeding repeats one INSERT per table thousands
            // of times, so the driver's own auto-preparation covers it after two executions and the
            // warm-up would save a round trip that nobody is timing.
            if (wa.Command == "run" && !wa.NoPrepare)
                await WarmUpAsync(wa.WorkloadName, conn, connStr);

            await DispatchAsync(wa, conn, txOptions);
        }
        catch (Exception ex)
        {
            // Without this the first unhandled batch failure escapes through Parallel.ForEachAsync and
            // aborts the process (SIGABRT) with a raw stack trace instead of a diagnosable message.
            AnsiConsole.MarkupLine("\n[red]Workload failed:[/] {0}", Markup.Escape(ex.Message));
            Environment.ExitCode = 1;
        }
    }

    /// <summary>
    /// Prepares the statements the chosen workload's run phase issues, so the first executions
    /// measure the same path as the millionth one.
    /// </summary>
    private static async Task WarmUpAsync(string workloadName, CamusConnection conn, string connectionString)
    {
        IReadOnlyList<string> statements = workloadName switch
        {
            "bank" => BankWorkload.RunStatements,
            "northwind" => NorthwindWorkload.RunStatements,
            "factory" => FactoryWorkload.RunStatements,
            "tpcc" => TpccWorkload.RunStatements,
            _ => [],
        };

        if (statements.Count == 0)
            return;

        await WorkloadHelpers.PrepareAllAsync(conn, statements);

        // Ask the driver what actually ended up registered rather than trusting that the calls
        // returned: a server without prepared-statement support declines without raising anything.
        CamusConnectionStringBuilder builder = new(connectionString);
        int prepared = statements.Count(builder.IsPrepared);

        if (prepared == statements.Count)
            AnsiConsole.MarkupLine("[grey58]Prepared[/] [white]{0}[/] [grey58]statements.[/]\n", prepared);
        else
            AnsiConsole.MarkupLine(
                "[grey58]Prepared[/] [white]{0}[/][grey58]/{1} statements; the rest run inline.[/]\n",
                prepared, statements.Count);
    }

    private static async Task DispatchAsync(WorkloadArgs wa, CamusConnection conn, CamusTransactionOptions txOptions)
    {
        switch (wa.WorkloadName)
        {
            case "bank":
                if (wa.Command == "init")
                    await BankWorkload.InitAsync(conn, wa.Rows, wa.Concurrency, txOptions);
                else
                    await BankWorkload.RunAsync(conn, wa.Concurrency, wa.Duration, txOptions);
                break;
            case "northwind":
                if (wa.Command == "init")
                    await NorthwindWorkload.InitAsync(conn, wa.Concurrency, txOptions);
                else
                    await NorthwindWorkload.RunAsync(conn, wa.Concurrency, wa.Duration, txOptions);
                break;
            case "factory":
                if (wa.Command == "init")
                    await FactoryWorkload.InitAsync(conn, wa.Concurrency, txOptions);
                else
                    await FactoryWorkload.RunAsync(conn, wa.Concurrency, wa.Duration, txOptions);
                break;
            case "tpcc":
                if (wa.Command == "init")
                    await TpccWorkload.InitAsync(conn, wa.Rows, wa.Concurrency, txOptions);
                else
                    await TpccWorkload.RunAsync(conn, wa.Concurrency, wa.Duration, txOptions);
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
        int concurrency = 64;
        int duration = 60;

        // Concurrency knobs. Null means "not specified on the command line"; workloads default to
        // optimistic locking and serializable isolation unless the connection string overrides them.
        string? locking = null;
        string? isolation = null;

        // Prepared statements are on by default (the driver's own policy). --no-prepare turns them
        // off so the same workload can be measured against the inline path.
        bool noPrepare = false;

        // Credentials, for a server with authentication enabled. They also come from the
        // environment, so a scripted run needn't put the password on the command line.
        string? user = Environment.GetEnvironmentVariable("CAMUS_USER");
        string? password = Environment.GetEnvironmentVariable("CAMUS_PASSWORD");

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
                case "--locking":
                    if (i + 1 < args.Length) locking = args[++i];
                    break;
                case "--isolation":
                    if (i + 1 < args.Length) isolation = args[++i];
                    break;
                case "--no-prepare":
                    noPrepare = true;
                    break;
                case "-u":
                case "--user":
                    if (i + 1 < args.Length) user = args[++i];
                    break;
                case "-p":
                case "--password":
                    if (i + 1 < args.Length) password = args[++i];
                    break;
            }
        }

        return new WorkloadArgs(command, workloadName, connectionSource, database, rows, concurrency, duration, locking, isolation, user, password, noPrepare);
    }

    // Canonical connection-string value for --locking, or null if the value is unrecognized.
    internal static string? NormalizeLocking(string value) => value.Trim().ToLowerInvariant() switch
    {
        "optimistic" => "Optimistic",
        "pessimistic" => "Pessimistic",
        _ => null,
    };

    // Canonical connection-string value for --isolation, or null if the value is unrecognized.
    internal static string? NormalizeIsolation(string value) => value.Trim().ToLowerInvariant() switch
    {
        "serializable" => "Serializable",
        "readcommitted" or "read-committed" or "read committed" => "ReadCommitted",
        _ => null,
    };

    // Default listener ports for a local server: REST on 5095, gRPC on 5096 (both enabled by default).
    private const int DefaultRestPort = 5095;
    private const int DefaultGrpcPort = 5096;

    // The client's connection-string default is 10 seconds, which is the budget commit gets
    // (CamusTransaction.FinalizeAsync passes builder.CommandTimeout). Under a high --concurrency fan-out
    // the batcher can easily queue past that and fail the commit with a bare TaskCanceledException whose
    // outcome is unknowable. Workloads deliberately ask for a longer, matching budget instead.
    private const int DefaultTimeoutSeconds = 60;

    // Builds the ordered list of connection strings a workload should try. Like the interactive
    // shell, gRPC is preferred and REST is the fallback; an explicit Protocol= in -c is honored
    // with no fallback. Locking/IsolationLevel/Timeout keys are applied to every attempt.
    internal static List<string> BuildConnectionAttempts(
        string connectionSource,
        string database,
        string? locking,
        string? isolation,
        string? user = null,
        string? password = null,
        bool noPrepare = false)
    {
        if (string.IsNullOrEmpty(connectionSource))
        {
            // No connection string: use the well-known local ports for each transport.
            string grpc = ApplyWorkloadDefaults($"Endpoint=http://localhost:{DefaultGrpcPort};Database={database};Protocol=grpc", locking, isolation, user, password, noPrepare);
            string rest = ApplyWorkloadDefaults($"Endpoint=http://localhost:{DefaultRestPort};Database={database};Protocol=rest", locking, isolation, user, password, noPrepare);
            return [grpc, rest];
        }

        string cs = HasKey(connectionSource, "Database") ? connectionSource : $"{connectionSource};Database={database}";
        cs = ApplyWorkloadDefaults(cs, locking, isolation, user, password, noPrepare);

        // Respect an explicit Protocol= — the user has chosen the transport deliberately.
        if (HasKey(cs, "Protocol"))
            return [cs];

        // No protocol given: try gRPC against their endpoint, then REST against the same endpoint.
        return [WithKey(cs, "Protocol", "grpc"), WithKey(cs, "Protocol", "rest")];
    }

    // Applies the Locking/IsolationLevel/Timeout keys, defaulting to optimistic + serializable and a
    // command timeout wide enough for a batched commit. A flag value always wins; the default only fills
    // in a key the connection string doesn't already carry.
    private static string ApplyWorkloadDefaults(string connectionString, string? locking, string? isolation, string? user, string? password, bool noPrepare)
    {
        connectionString = ApplyKey(connectionString, "Locking", locking, defaultValue: "Optimistic");
        connectionString = ApplyKey(connectionString, "IsolationLevel", isolation, defaultValue: "Serializable");
        connectionString = ApplyKey(connectionString, "Timeout", value: null, defaultValue: DefaultTimeoutSeconds.ToString());

        // --no-prepare wins over a MaxAutoPrepare= in -c: the flag is the later, more specific word.
        // Without it the driver's own default applies, which is why nothing is set in that case.
        if (noPrepare)
            connectionString = WithKey(connectionString, "MaxAutoPrepare", "0");

        // Credentials have no default: unset means "unauthenticated server", which is how CamusDB
        // ships. When given they override whatever -c carried, since the flag is the later word.
        if (!string.IsNullOrEmpty(user))
            connectionString = WithKey(connectionString, "User", user);

        if (!string.IsNullOrEmpty(password))
            connectionString = WithKey(connectionString, "Password", password);

        return connectionString;
    }

    // Human-readable transport name for a resolved connection string (Protocol= defaults to REST).
    private static string DescribeTransport(string connectionString)
        => string.Equals(GetConnValue(connectionString, "Protocol"), "grpc", StringComparison.OrdinalIgnoreCase) ? "gRPC" : "REST";

    // Returns the value of a connection-string key, or null when the key is absent.
    private static string? GetConnValue(string connectionString, string key)
    {
        return connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(p => p.Split('=', 2, StringSplitOptions.TrimEntries))
            .Where(p => p.Length == 2 && string.Equals(p[0], key, StringComparison.OrdinalIgnoreCase))
            .Select(p => p[1])
            .FirstOrDefault();
    }

    // Sets key=value when the user passed the flag (value != null), otherwise falls back to
    // defaultValue only if the connection string doesn't already carry the key.
    private static string ApplyKey(string connectionString, string key, string? value, string defaultValue)
    {
        if (value is not null)
            return WithKey(connectionString, key, value);

        return HasKey(connectionString, key)
            ? connectionString
            : WithKey(connectionString, key, defaultValue);
    }

    private static bool HasKey(string connectionString, string key)
    {
        return connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Any(p => p.StartsWith(key + "=", StringComparison.OrdinalIgnoreCase));
    }

    private static string WithKey(string connectionString, string key, string value)
    {
        List<string> parts = connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Where(p => !p.StartsWith(key + "=", StringComparison.OrdinalIgnoreCase))
            .ToList();

        parts.Add($"{key}={value}");
        return string.Join(';', parts);
    }
}

internal record WorkloadArgs(
    string Command,
    string WorkloadName,
    string ConnectionSource,
    string Database,
    int Rows,
    int Concurrency,
    int Duration,
    string? Locking,
    string? Isolation,
    string? User = null,
    string? Password = null,
    bool NoPrepare = false
);
