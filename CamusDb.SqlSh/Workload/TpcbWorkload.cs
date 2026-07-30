
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

/// <summary>
/// TPC-B-like workload: one short read-modify-write transaction, repeated as fast as the server
/// will take it. Every transaction updates an account, reads the new balance back, updates the
/// teller and the branch, and appends a history row.
/// </summary>
internal static class TpcbWorkload
{
    // TPC-B's row ratio is 1 branch : 10 tellers : 100,000 accounts. The account count is scaled
    // down 100x here so a default `init` finishes in seconds; the shape of the contention — every
    // transaction touching one of very few branch rows — is what the benchmark is actually about,
    // and that is preserved.
    private const int AccountsPerBranch = 1000;
    private const int TellersPerBranch = 10;

    private const int BatchSize = 10;

    // Fraction of transactions whose account belongs to the teller's own branch. TPC-B specifies
    // 85%; the remaining 15% pick an account from anywhere, which is what makes a multi-branch run
    // conflict across branches rather than partitioning cleanly.
    private const double LocalAccountRatio = 0.85;

    // -------------------------------------------------------------------------
    // SQL templates
    // -------------------------------------------------------------------------

    private const string BranchInsertSql =
        "INSERT INTO tpcb_branches (branch_id, balance) VALUES (@branch_id, @balance)";

    private const string TellerInsertSql =
        "INSERT INTO tpcb_tellers (teller_id, branch_id, balance) VALUES (@teller_id, @branch_id, @balance)";

    private const string AccountInsertSql =
        "INSERT INTO tpcb_accounts (account_id, branch_id, balance) VALUES (@account_id, @branch_id, @balance)";

    private const string UpdateAccountSql =
        "UPDATE tpcb_accounts SET balance = balance + @delta WHERE account_id = @account_id";

    private const string SelectBalanceSql =
        "SELECT balance FROM tpcb_accounts WHERE account_id = @account_id";

    private const string UpdateTellerSql =
        "UPDATE tpcb_tellers SET balance = balance + @delta WHERE teller_id = @teller_id";

    private const string UpdateBranchSql =
        "UPDATE tpcb_branches SET balance = balance + @delta WHERE branch_id = @branch_id";

    // The benchmark's history column is called `timestamp`, which CamusDB's parser reads as the
    // TIMESTAMP type keyword rather than an identifier, so the column is named `mtime` (pgbench's
    // name for the same thing). CURRENT_TIMESTAMP likewise has to be written as a call.
    private const string HistoryInsertSql =
        "INSERT INTO tpcb_history (id, account_id, teller_id, branch_id, delta, mtime) " +
        "VALUES (GEN_ID(), @account_id, @teller_id, @branch_id, @delta, CURRENT_TIMESTAMP())";

    /// <summary>
    /// Every statement the <c>run</c> phase issues, for the prepared-statement warm-up. Keep in sync
    /// with <see cref="TxAccountUpdate"/>: a statement missing here isn't broken, it just pays the
    /// driver's usual two-execution warm-up instead of being registered before the clock starts.
    /// </summary>
    internal static IReadOnlyList<string> RunStatements =>
        [UpdateAccountSql, SelectBalanceSql, UpdateTellerSql, UpdateBranchSql, HistoryInsertSql];

    // -------------------------------------------------------------------------
    // Init
    // -------------------------------------------------------------------------

    internal static async Task InitAsync(CamusConnection conn, int accounts, int concurrency, CamusTransactionOptions txOptions)
    {
        if (accounts < 1) accounts = 1;

        // `--rows` is the account count, and the branch/teller counts follow from it, so the one knob
        // scales the whole database the way TPC-B's scale factor does.
        int branches = Math.Max(1, accounts / AccountsPerBranch);
        int tellers = branches * TellersPerBranch;

        AnsiConsole.MarkupLine("[cyan]Creating TPC-B schema...[/]");

        string[] ddls =
        [
            """
            CREATE TABLE IF NOT EXISTS tpcb_branches (
              branch_id INT64 PRIMARY KEY NOT NULL,
              balance INT64 NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcb_tellers (
              teller_id INT64 PRIMARY KEY NOT NULL,
              branch_id INT64 NOT NULL,
              balance INT64 NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcb_accounts (
              account_id INT64 PRIMARY KEY NOT NULL,
              branch_id INT64 NOT NULL,
              balance INT64 NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS tpcb_history (
              id OID PRIMARY KEY NOT NULL,
              account_id INT64 NOT NULL,
              teller_id INT64 NOT NULL,
              branch_id INT64 NOT NULL,
              delta INT64 NOT NULL,
              mtime TIMESTAMP NOT NULL
            )
            """,
        ];

        foreach (string ddl in ddls)
            await DDL(conn, ddl);

        AnsiConsole.MarkupLine("[green]Schema ready.[/] Clearing existing data...");

        (string Table, string Key)[] tables =
        [
            ("tpcb_history",  "id"),
            ("tpcb_accounts", "account_id"),
            ("tpcb_tellers",  "teller_id"),
            ("tpcb_branches", "branch_id"),
        ];

        foreach ((string table, string key) in tables)
            await Exec(conn, $"DELETE FROM {table} WHERE {key} IS NOT NULL");

        AnsiConsole.MarkupLine(
            "Seeding [blue]{0}[/] branches, [blue]{1}[/] tellers, [blue]{2}[/] accounts ([blue]{3}[/] parallel writers)...\n",
            branches, tellers, accounts, concurrency);

        // Every row is generated single-threaded from a seeded RNG (Random isn't thread-safe), then the
        // resulting statements are chunked into transactions and fanned out across `concurrency` writers.
        Random rng = new(42);

        List<(string Sql, Param[] Parameters)> branchRows = new(branches);
        for (int b = 1; b <= branches; b++)
        {
            branchRows.Add((BranchInsertSql,
            [
                ("@branch_id", ColumnType.Integer64, (object)(long)b),
                ("@balance",   ColumnType.Integer64, 0L),
            ]));
        }

        List<(string Sql, Param[] Parameters)> tellerRows = new(tellers);
        for (int t = 1; t <= tellers; t++)
        {
            tellerRows.Add((TellerInsertSql,
            [
                ("@teller_id", ColumnType.Integer64, (object)(long)t),
                ("@branch_id", ColumnType.Integer64, (long)BranchOfTeller(t)),
                ("@balance",   ColumnType.Integer64, 0L),
            ]));
        }

        List<(string Sql, Param[] Parameters)> accountRows = new(accounts);
        for (int a = 1; a <= accounts; a++)
        {
            accountRows.Add((AccountInsertSql,
            [
                ("@account_id", ColumnType.Integer64, (object)(long)a),
                ("@branch_id",  ColumnType.Integer64, (long)BranchOfAccount(a, branches)),
                ("@balance",    ColumnType.Integer64, rng.NextInt64(10_000L, 1_000_000L)),
            ]));
        }

        await AnsiConsole.Progress()
            .AutoRefresh(true)
            .HideCompleted(false)
            .StartAsync(async ctx =>
            {
                ProgressTask tBra = ctx.AddTask("[green]branches[/]", maxValue: branches);
                ProgressTask tTel = ctx.AddTask("[green]tellers [/]", maxValue: tellers);
                ProgressTask tAcc = ctx.AddTask("[green]accounts[/]", maxValue: accounts);

                await Seed(conn, txOptions, concurrency, branchRows, tBra);
                await Seed(conn, txOptions, concurrency, tellerRows, tTel);
                await Seed(conn, txOptions, concurrency, accountRows, tAcc);
            });

        AnsiConsole.MarkupLine("\n[green]TPC-B workload initialized.[/]");
        AnsiConsole.MarkupLine("Run [blue]camus-cli workload run tpcb[/] to start generating transactions.");
    }

    // -------------------------------------------------------------------------
    // Run
    // -------------------------------------------------------------------------

    internal static async Task RunAsync(CamusConnection conn, int concurrency, int durationSeconds, CamusTransactionOptions txOptions)
    {
        long accountCount = await CountRows(conn, "tpcb_accounts");
        long branchCount = await CountRows(conn, "tpcb_branches");
        long tellerCount = await CountRows(conn, "tpcb_tellers");

        if (accountCount == 0 || branchCount == 0 || tellerCount == 0)
        {
            AnsiConsole.MarkupLine("[red]No TPC-B data found.[/] Run [blue]camus-cli workload init tpcb[/] first.");
            return;
        }

        AnsiConsole.MarkupLine(
            "Starting TPC-B workload: [blue]{0}[/] workers, [blue]{1}s[/] duration, [blue]{2}[/] accounts across [blue]{3}[/] branch(es)",
            concurrency, durationSeconds, accountCount, branchCount);

        if (branchCount == 1)
        {
            // Worth saying out loud: at one branch every transaction updates the same branch row, so a
            // serializable run reports mostly conflicts rather than throughput.
            AnsiConsole.MarkupLine(
                "[yellow]One branch:[/] every transaction contends on the same branch row. Seed more accounts " +
                "([blue]--rows {0}[/] gives 10 branches) to spread the writes.", AccountsPerBranch * 10);
        }

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
                    await TxAccountUpdate(conn, txOptions, rng, accountCount, tellerCount, branchCount, cts.Token);
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
        AnsiConsole.MarkupLine("\n[green]Done:[/] {0} ops in {1:F1}s ({2:F1} tps), {3} errors",
            totalOps, elapsed, elapsed > 0 ? totalOps / elapsed : 0, totalErrors);
    }

    // -------------------------------------------------------------------------
    // The TPC-B transaction
    // -------------------------------------------------------------------------

    /// <summary>
    /// The single TPC-B transaction: post <c>delta</c> to an account, read the resulting balance back,
    /// post the same delta to the teller and its branch, then append the history row.
    /// </summary>
    private static async Task TxAccountUpdate(
        CamusConnection conn,
        CamusTransactionOptions txOptions,
        Random rng,
        long accountCount,
        long tellerCount,
        long branchCount,
        CancellationToken ct)
    {
        long tellerId = rng.NextInt64(1, tellerCount + 1);
        long branchId = Math.Min(BranchOfTeller(tellerId), branchCount);
        long accountId = PickAccount(rng, accountCount, branchCount, branchId);
        long delta = rng.NextInt64(-999_999L, 1_000_000L);

        CamusTransaction tx = await conn.BeginTransactionAsync(txOptions, ct);
        try
        {
            await ExecWithParams(conn, UpdateAccountSql,
            [
                ("@delta",      ColumnType.Integer64, (object)delta),
                ("@account_id", ColumnType.Integer64, accountId),
            ], tx, ct);

            // Read inside the caller's transaction: the point of this statement in TPC-B is that the
            // client sees the balance its own UPDATE just produced.
            await FetchBalance(conn, tx, accountId, ct);

            await ExecWithParams(conn, UpdateTellerSql,
            [
                ("@delta",     ColumnType.Integer64, (object)delta),
                ("@teller_id", ColumnType.Integer64, tellerId),
            ], tx, ct);

            await ExecWithParams(conn, UpdateBranchSql,
            [
                ("@delta",     ColumnType.Integer64, (object)delta),
                ("@branch_id", ColumnType.Integer64, branchId),
            ], tx, ct);

            await ExecWithParams(conn, HistoryInsertSql,
            [
                ("@account_id", ColumnType.Integer64, (object)accountId),
                ("@teller_id",  ColumnType.Integer64, tellerId),
                ("@branch_id",  ColumnType.Integer64, branchId),
                ("@delta",      ColumnType.Integer64, delta),
            ], tx, ct);

            await tx.CommitAsync(ct);
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
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

    // Ids are assigned in blocks, so both of these are pure arithmetic and the run phase never has to
    // query for the branch an account or teller belongs to.
    private static long BranchOfTeller(long tellerId) => (tellerId - 1) / TellersPerBranch + 1;

    private static long BranchOfAccount(long accountId, long branches)
        => Math.Min((accountId - 1) / AccountsPerBranch + 1, branches);

    /// <summary>
    /// Picks the account to debit: 85% of the time one belonging to <paramref name="branchId"/>, the
    /// rest drawn from the whole table. That's TPC-B's local/remote split, and it's what keeps a
    /// multi-branch run from degenerating into independent per-branch workloads.
    /// </summary>
    private static long PickAccount(Random rng, long accountCount, long branchCount, long branchId)
    {
        if (branchCount <= 1 || rng.NextDouble() >= LocalAccountRatio)
            return rng.NextInt64(1, accountCount + 1);

        long first = (branchId - 1) * AccountsPerBranch + 1;
        long last = Math.Min(branchId * AccountsPerBranch, accountCount);

        // The last branch absorbs any remainder, so `last` can only fall short of `first` if the table
        // was seeded by something other than InitAsync; fall back to the whole range rather than throw.
        return last >= first ? rng.NextInt64(first, last + 1) : rng.NextInt64(1, accountCount + 1);
    }

    private static async Task<long> FetchBalance(CamusConnection conn, CamusTransaction tx, long accountId, CancellationToken ct)
    {
        using CamusCommand cmd = conn.CreateSelectCommand(SelectBalanceSql);
        cmd.Transaction = tx;
        cmd.CommandTimeout = 30;
        cmd.Parameters.Add("@account_id", ColumnType.Integer64, accountId);

        CamusDataReader reader = await cmd.ExecuteReaderAsync(ct);
        long balance = 0;
        while (await reader.ReadAsync(ct))
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            if (row.TryGetValue("balance", out ColumnValue v))
                balance = v.LongValue;
        }
        return balance;
    }

    private static async Task<long> CountRows(CamusConnection conn, string table)
    {
        using CamusCommand cmd = conn.CreateSelectCommand($"SELECT COUNT(*) as cnt FROM {table}");
        cmd.CommandTimeout = 30;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        if (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            if (row.TryGetValue("cnt", out ColumnValue v))
                return v.LongValue;
        }
        return 0;
    }
}
