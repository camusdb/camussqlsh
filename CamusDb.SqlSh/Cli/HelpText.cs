
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;

internal static class HelpText
{
    internal static void PrintUsage()
    {
        AnsiConsole.MarkupLine("Usage: camus-cli [[database]] [[options]]");
        AnsiConsole.MarkupLine("       camus-cli workload <init|run> <bank|northwind|factory|tpcc|tpcb> [[options]]");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Options:[/]");
        AnsiConsole.MarkupLine("  database                      Database to use at start (default: none; select one with");
        AnsiConsole.MarkupLine("                                [cyan]use <database>[/])");
        AnsiConsole.MarkupLine("  -c, --connection-source       Connection string (default: gRPC on http://localhost:5096,");
        AnsiConsole.MarkupLine("                                falling back to REST on http://localhost:5095)");
        AnsiConsole.MarkupLine("  -e, --execute                 Execute a SQL statement (or ;-separated statements) and exit");
        AnsiConsole.MarkupLine("  -f, --file                    Execute the statements in a .sql file and exit; stops at the");
        AnsiConsole.MarkupLine("                                first error. Use [cyan]-f -[/] to read the script from standard input");
        AnsiConsole.MarkupLine("  --format                      How [cyan]-e[/] and [cyan]-f[/] print rows: table (default), json or yaml.");
        AnsiConsole.MarkupLine("                                With json or yaml, stdout carries only the rows; status");
        AnsiConsole.MarkupLine("                                lines and errors go to stderr");
        AnsiConsole.MarkupLine("  -u, --user                    User to authenticate as (only needed on a server with");
        AnsiConsole.MarkupLine("                                authentication enabled)");
        AnsiConsole.MarkupLine("  -p, --password                That user's password. Give [cyan]-p[/] with no value to be prompted");
        AnsiConsole.MarkupLine("                                for it, or set CAMUS_PASSWORD: on the command line the password");
        AnsiConsole.MarkupLine("                                is visible to every other process on the machine");
        AnsiConsole.MarkupLine("  --token                       Use a bearer token obtained elsewhere instead of logging in");
        AnsiConsole.MarkupLine("  --allow-insecure-credentials  Send credentials to a plaintext [cyan]http://[/] endpoint that is not on this");
        AnsiConsole.MarkupLine("                                machine. Refused without it, because the password and the token");
        AnsiConsole.MarkupLine("                                would cross the network in the clear");
        AnsiConsole.MarkupLine("  --no-history                  Do not load or save the statement history");
        AnsiConsole.MarkupLine("  --tui                         Open the full-screen browser: catalog, editor and results");
        AnsiConsole.MarkupLine("                                in three panes. TAB moves between panes, F5 runs the query");
        AnsiConsole.MarkupLine("  --force-rich                  Force the rich line editor (colors, multiline, Tab completion)");
        AnsiConsole.MarkupLine("                                on terminals whose TERM value Spectre.Console doesn't recognize");
        AnsiConsole.MarkupLine("  --diagnose-terminal           Print terminal capabilities and exit (why rich mode is on/off)");
        AnsiConsole.MarkupLine("  -h, --help                    Show this help message");
        AnsiConsole.MarkupLine("  -v, --version                 Show version information");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Environment variables:[/]");
        AnsiConsole.MarkupLine("  CAMUS_FORCE_RICH=1            Same as [cyan]--force-rich[/] (accepts 1/true/yes)");
        AnsiConsole.MarkupLine("  CAMUS_USER                    Default for [cyan]-u[/]");
        AnsiConsole.MarkupLine("  CAMUS_PASSWORD                Default for [cyan]-p[/] (keeps the password out of the shell history)");
        AnsiConsole.MarkupLine("  CAMUS_ACCESS_TOKEN            Default for [cyan]--token[/]");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Subcommands:[/]");
        AnsiConsole.MarkupLine("  [cyan]workload init[/] <bank|northwind|factory|tpcc|tpcb>  Create schema and seed data for a workload");
        AnsiConsole.MarkupLine("  [cyan]workload run[/]  <bank|northwind|factory|tpcc|tpcb>  Run a continuous workload against the database");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Workload options:[/]");
        AnsiConsole.MarkupLine("  -c, --connection-source       Connection string");
        AnsiConsole.MarkupLine("  --database                    Target database name (default: demo)");
        AnsiConsole.MarkupLine("  --rows N                      Rows to generate for init (default: 1000, bank only)");
        AnsiConsole.MarkupLine("  --concurrency N               Parallel workers for run (default: 3)");
        AnsiConsole.MarkupLine("  --duration N                  Run duration in seconds (default: 60)");
        AnsiConsole.MarkupLine("  --locking MODE                Locking mode: optimistic | pessimistic (default: optimistic)");
        AnsiConsole.MarkupLine("  --isolation LEVEL             Isolation level: serializable | read-committed (default: serializable)");
        AnsiConsole.MarkupLine("  --no-prepare                  Run statements inline instead of preparing them, to measure");
        AnsiConsole.MarkupLine("                                against the default prepared path");
        AnsiConsole.MarkupLine("  -u, --user / -p, --password   Credentials for an authenticated server");
        AnsiConsole.MarkupLine("                                (prefer CAMUS_PASSWORD over -p; see the shell's -p above)");
        AnsiConsole.WriteLine();
        PrintShellHelp();
        AnsiConsole.MarkupLine("[bold]Examples:[/]");
        AnsiConsole.MarkupLine("  camus-cli mydb");
        AnsiConsole.MarkupLine("  camus-cli mydb -u app                     [grey58](prompts for the password; or set CAMUS_PASSWORD)[/]");
        AnsiConsole.MarkupLine("  camus-cli -c \"Endpoint=http://localhost:5096;Database=mydb;Protocol=grpc\"");
        AnsiConsole.MarkupLine("  camus-cli -c \"Endpoint=http://localhost:5095;Database=mydb;Protocol=rest\"");
        AnsiConsole.MarkupLine("  camus-cli mydb -e \"SELECT * FROM users\"");
        AnsiConsole.MarkupLine("  camus-cli mydb -e \"SELECT * FROM users\" --format json | jq '.[[]].name'");
        AnsiConsole.MarkupLine("  camus-cli mydb -f schema.sql");
        AnsiConsole.MarkupLine("  cat schema.sql | camus-cli mydb -f -");
        AnsiConsole.MarkupLine("  camus-cli workload init bank --database demo --rows 5000");
        AnsiConsole.MarkupLine("  camus-cli workload run northwind --concurrency 5 --duration 120");
        AnsiConsole.MarkupLine("  camus-cli workload init factory --database factory");
        AnsiConsole.MarkupLine("  camus-cli workload run factory --concurrency 4 --duration 120");
        AnsiConsole.MarkupLine("  camus-cli workload init tpcc --database tpcc --rows 1");
        AnsiConsole.MarkupLine("  camus-cli workload run tpcc --concurrency 4 --duration 120");
        AnsiConsole.MarkupLine("  camus-cli workload init tpcb --database tpcb --rows 10000");
        AnsiConsole.MarkupLine("  camus-cli workload run tpcb --concurrency 8 --duration 120");
    }

    // The commands that work inside the interactive shell: the shell's own commands and the SQL that is
    // worth a reminder. `help` prints this section alone; `--help` prints it as part of the full usage.
    internal static void PrintShellHelp()
    {
        AnsiConsole.MarkupLine("[bold]Shell commands:[/]");
        AnsiConsole.MarkupLine("  [cyan]use[/] <database>              Switch the current database; the name may be");
        AnsiConsole.MarkupLine("                                [cyan]`backticked`[/]. Works in script files too");
        AnsiConsole.MarkupLine("  [cyan]source[/] <file> [[--force]]     Run the statements in a file, streaming it;");
        AnsiConsole.MarkupLine("                                [cyan]--force[/] carries on past failures");
        AnsiConsole.MarkupLine("  [cyan]show prepared[/] [[sql]]         What the driver keeps prepared; with SQL, whether that");
        AnsiConsole.MarkupLine("                                statement is prepared (alias: [cyan]\\prepared[/])");
        AnsiConsole.MarkupLine("  [cyan]\\plan[/] [[flow|table]]          How EXPLAIN shows a plan: a flow from the source to");
        AnsiConsole.MarkupLine("                                the result (default), or the rows as a table. [cyan]\\G[/] and");
        AnsiConsole.MarkupLine("                                [cyan]--format[/] always give the rows. [cyan]/plan[/] also works");
        AnsiConsole.MarkupLine("  [cyan]\\export[/] csv|json|yaml [[to <file>]] <query>");
        AnsiConsole.MarkupLine("                                Write the rows of a query to a file. With no file, the");
        AnsiConsole.MarkupLine("                                shell suggests <table>-<date>-<time>.<format>; press");
        AnsiConsole.MarkupLine("                                Enter to use it, or edit it. [cyan]/export[/] also works");
        AnsiConsole.MarkupLine("  [cyan]backup[/] full|incremental <id>|coordinated|list|chain <id>|gc [[preview]]");
        AnsiConsole.MarkupLine("                                Take and manage backups through the node's backup API");
        AnsiConsole.MarkupLine("  [cyan]clear[/]                       Clear the screen");
        AnsiConsole.MarkupLine("  [cyan]help[/]                        Show these commands; [cyan]\\h[/], [cyan]\\?[/], [cyan]?[/] and [cyan]/help[/] also work");
        AnsiConsole.MarkupLine("  [cyan]exit[/] / [cyan]quit[/]                 Leave the shell; [cyan]\\q[/], [cyan]/quit[/] and [cyan]/exit[/] also work");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Cluster settings:[/] [grey58](SQL, not shell commands; each runs without a current database)[/]");
        AnsiConsole.MarkupLine("  [cyan]show variables[/] [[like '<pattern>']]");
        AnsiConsole.MarkupLine("                                This node's effective configuration, with each");
        AnsiConsole.MarkupLine("                                setting's [white]mutability[/] (runtime|restart) and [white]scope[/]");
        AnsiConsole.MarkupLine("  [cyan]show cluster settings[/] [[like '<pattern>']]");
        AnsiConsole.MarkupLine("                                What the cluster overlay currently carries");
        AnsiConsole.MarkupLine("  [cyan]set cluster setting[/] <key> = <value>");
        AnsiConsole.MarkupLine("                                Change a runtime setting fleet-wide, from any node");
        AnsiConsole.MarkupLine("  [cyan]reset cluster setting[/] <key>   Drop the overlay entry; each node resolves the key");
        AnsiConsole.MarkupLine("                                through its own config again");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Introspection:[/] [grey58](SQL, not shell commands)[/]");
        AnsiConsole.MarkupLine("  [cyan]show statistics for[/] [[table]] <name>");
        AnsiConsole.MarkupLine("                                What the optimizer believes about a table: row counts,");
        AnsiConsole.MarkupLine("                                column min/max, histogram buckets, distinct-value counts,");
        AnsiConsole.MarkupLine("                                and how stale they are ([cyan]analyze <table>[/] refreshes them)");
        AnsiConsole.MarkupLine("  [cyan]show ranges from table[/] <name>");
        AnsiConsole.MarkupLine("  [cyan]show ranges from index[/] <table>@<index>");
        AnsiConsole.MarkupLine("                                How a relation's key space is divided into spans, and");
        AnsiConsole.MarkupLine("                                where each span's leader is, as [white]this node[/] sees it");
        AnsiConsole.MarkupLine("  [cyan]show range from table[/] <name> [cyan]for row[/] (<primary key>)");
        AnsiConsole.MarkupLine("  [cyan]show range from index[/] <table>@<index> [cyan]for row[/] (<values>)");
        AnsiConsole.MarkupLine("                                The one span that holds a row. Fifteen columns, so");
        AnsiConsole.MarkupLine("                                append [cyan]\\G[/] for vertical output");
        AnsiConsole.MarkupLine("  [cyan]show engine stats[/]             Runtime engine metrics for the node answering");
        AnsiConsole.MarkupLine("  [cyan]show slow queries[/] [[like '<pattern>']]");
        AnsiConsole.MarkupLine("                                What [white]this node[/] recorded over its slow-query threshold,");
        AnsiConsole.MarkupLine("                                newest first. Off by default, and lost on restart;");
        AnsiConsole.MarkupLine("                                fourteen columns, so append [cyan]\\G[/] for vertical output");
        AnsiConsole.WriteLine();
        AnsiConsole.MarkupLine("[bold]Running statements:[/] [grey58](SQL, not shell commands; each runs without a current database)[/]");
        AnsiConsole.MarkupLine("  [cyan]show[/] [[[cyan]cluster[/]]] [cyan]queries[/] [[like '<pattern>']]");
        AnsiConsole.MarkupLine("                                The statements that run now on [white]this node[/], or on");
        AnsiConsole.MarkupLine("                                every node with [cyan]cluster[/]. Append [cyan]\\G[/] for vertical output");
        AnsiConsole.MarkupLine("  [cyan]show[/] [[[cyan]cluster[/]]] [cyan]connections[/]");
        AnsiConsole.MarkupLine("                                The client connections that [white]this node[/], or every");
        AnsiConsole.MarkupLine("                                node with [cyan]cluster[/], holds open now");
        AnsiConsole.MarkupLine("  [cyan]cancel query[/] '<query_id>'     Stop a running read on any node. A write or a");
        AnsiConsole.MarkupLine("                                schema change cannot be stopped");
        AnsiConsole.WriteLine();
    }
}
