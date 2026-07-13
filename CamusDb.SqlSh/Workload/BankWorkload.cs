
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;

internal static class BankWorkload
{
    private const int BatchSize = 10;

    internal static async Task InitAsync(CamusConnection conn, int rows)
    {
        AnsiConsole.MarkupLine("[cyan]Creating bank schema...[/]");

        await WorkloadHelpers.DDL(conn, "CREATE TABLE IF NOT EXISTS accounts (id INT64 PRIMARY KEY, balance INT64 NOT NULL)");
        await WorkloadHelpers.DDL(conn, "CREATE TABLE IF NOT EXISTS transactions (id INT64 PRIMARY KEY, debit_account INT64 NOT NULL, credit_account INT64 NOT NULL, amount INT64 NOT NULL, created_at STRING NOT NULL)");

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Inserting [blue]{0}[/] accounts...\n", rows);

        Random rng = new();

        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .HideCompleted(false)
            .StartAsync(async ctx =>
            {
                ProgressTask task = ctx.AddTask("[green]accounts[/]", maxValue: rows);

                // accounts — batch BatchSize at a time
                var batch = new List<string>(BatchSize);
                for (int i = 1; i <= rows; i++)
                {
                    long balance = rng.NextInt64(10_000L, 1_000_000L); // $100–$10,000 in cents
                    batch.Add($"INSERT INTO accounts (id, balance) VALUES ({i}, {balance})");
                    task.Increment(1);

                    if (batch.Count >= BatchSize)
                    {
                        await RunBatch(conn, batch);
                        batch.Clear();
                    }
                }
                if (batch.Count > 0)
                {
                    await RunBatch(conn, batch);
                    batch.Clear();
                }
            });

        AnsiConsole.MarkupLine("\n[green]Bank workload initialized:[/] {0} accounts in database.", rows);
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run bank[/] to start generating transfers.");
    }

    internal static async Task RunAsync(CamusConnection conn, int concurrency, int durationSeconds)
    {
        long accountCount = 0;
        using (CamusCommand countCmd = conn.CreateSelectCommand("SELECT COUNT(*) as cnt FROM accounts"))
        {
            countCmd.CommandTimeout = 30;
            CamusDataReader reader = await countCmd.ExecuteReaderAsync();
            if (await reader.ReadAsync())
            {
                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
                if (row.TryGetValue("cnt", out ColumnValue val))
                    accountCount = val.LongValue;
            }
        }

        if (accountCount == 0)
        {
            AnsiConsole.MarkupLine("[red]No accounts found.[/] Run [blue]camus-cli workload init bank[/] first.");
            return;
        }

        AnsiConsole.MarkupLine("Starting bank workload: [blue]{0}[/] workers, [blue]{1}s[/] duration, [blue]{2}[/] accounts",
            concurrency, durationSeconds, accountCount);
        AnsiConsole.MarkupLine("Press [grey]Ctrl+C[/] to stop early.\n");

        using CancellationTokenSource cts = new(TimeSpan.FromSeconds(durationSeconds));
        Console.CancelKeyPress += (_, e) => { e.Cancel = true; cts.Cancel(); };

        long totalOps = 0;
        long totalErrors = 0;
        long txCounter = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
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

        Task[] workers = Enumerable.Range(0, concurrency).Select(_ => Task.Run(async () =>
        {
            Random rng = new();
            while (!cts.Token.IsCancellationRequested)
            {
                try
                {
                    int from = (int)(rng.NextInt64(1, accountCount + 1));
                    int to;
                    do { to = (int)(rng.NextInt64(1, accountCount + 1)); } while (to == from);
                    long amount = rng.NextInt64(100L, 10_000L); // $1–$100 in cents

                    long txId = Interlocked.Increment(ref txCounter);
                    string ts = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss");

                    CamusTransaction tx = await conn.BeginTransactionAsync();
                    try
                    {
                        await WorkloadHelpers.Exec(conn, $"UPDATE accounts SET balance = balance - {amount} WHERE id = {from}", tx);
                        await WorkloadHelpers.Exec(conn, $"UPDATE accounts SET balance = balance + {amount} WHERE id = {to}", tx);
                        await WorkloadHelpers.Exec(conn, $"INSERT INTO transactions (id, debit_account, credit_account, amount, created_at) VALUES ({txId}, {from}, {to}, {amount}, '{ts}')", tx);
                        await tx.CommitAsync();
                        Interlocked.Increment(ref totalOps);
                    }
                    catch
                    {
                        try { await tx.RollbackAsync(); } catch { }
                        Interlocked.Increment(ref totalErrors);
                    }
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

    private static async Task RunBatch(CamusConnection conn, List<string> batch)
    {
        CamusTransaction tx = await conn.BeginTransactionAsync();
        try
        {
            foreach (string sql in batch)
                await WorkloadHelpers.Exec(conn, sql, tx);
            await tx.CommitAsync();
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
    }
}
