/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using System.Diagnostics;
using static SqlKind;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// Runs the statements of a tab. One run is allowed at a time, in any tab. A run captures its tab
/// when it starts and writes its rows, status and messages to that tab only, so the user can change
/// tabs while it runs.
///
/// <para>Statements are classified by <see cref="SqlKind"/>, the same code the line-editor shell uses,
/// so a statement runs in the same place and against the same connection in both modes.</para>
/// </summary>
internal sealed class StatementRunner
{
    private readonly TuiSession _session;
    private readonly TuiContext _context;
    private readonly CatalogPane _catalog;
    private readonly SqlCompletion? _completion;

    private CancellationTokenSource? _cancel;

    public StatementRunner(TuiSession session, TuiContext context, CatalogPane catalog, SqlCompletion? completion)
    {
        _session = session;
        _context = context;
        _catalog = catalog;
        _completion = completion;
    }

    /// <summary>The tab of the run in progress. Null when nothing runs.</summary>
    public QueryTab? RunningTab { get; private set; }

    public void Cancel() => _cancel?.Cancel();

    // Runs only the statement at the cursor. The editor text comes back from the last session, so
    // running the whole editor every time ran its old INSERT and DDL statements again too. The cursor
    // belongs to a statement from its first character up to the character after its terminator, so a
    // cursor at the end of "select 1;" runs that statement. A cursor after the last statement runs the
    // last statement.
    public void RunCurrent(QueryTab tab)
    {
        string text = tab.Editor.Text;
        int cursor = tab.Editor.CursorOffset;

        // Folding smart quotes keeps every character at its offset, and a folded quote opens a string
        // here just as it does when the statement runs.
        SqlScanner scanner = new();
        List<SqlStatement> statements = scanner.Feed(NormalizeSmartQuotes(text)).ToList();
        statements.AddRange(scanner.Flush());

        if (statements.Count == 0)
            return;

        SqlStatement pick = statements.FirstOrDefault(s => cursor <= s.End + 1, statements[^1]);

        Start(tab, text[pick.Start..(pick.End + 1)]);
    }

    public void Start(QueryTab tab, string text)
    {
        if (_context.Running)
            return;

        text = text.Trim();

        if (text.Length == 0)
            return;

        _context.Running = true;
        RunningTab = tab;
        _cancel = new CancellationTokenSource();
        tab.Results.SetStatus("Running…");

        _ = RunScriptAsync(tab, text, _cancel.Token);
    }

    // The editor holds a script, not a single statement, so it is split the same way -e is. Each
    // statement reports its own outcome. The last one that returns rows fills the grid.
    private async Task RunScriptAsync(QueryTab tab, string text, CancellationToken token)
    {
        ResultView results = tab.Results;
        RunLog log = new();
        string current = "";
        int grid = results.GridVersion;

        try
        {
            await results.ClosePageAsync();

            foreach ((string sql, bool _) in EscapeStringIntoLines(NormalizeSmartQuotes(text)))
            {
                if (token.IsCancellationRequested)
                {
                    log.Add("cancelled");
                    results.AddMessage("", "cancelled", false);
                    break;
                }

                current = sql;
                int before = log.Entries.Count;

                await RunStatementAsync(results, sql, log, token);

                foreach ((string entry, bool error) in log.Entries.Skip(before))
                    results.AddMessage(sql, entry, error);
            }
        }
        catch (Exception ex)
        {
            log.Fail(ex.Message);
            results.AddMessage(current, ex.Message, true);
        }
        finally
        {
            // A failed run opens the messages, because the status line can be too short for the
            // error. A run that fills the grid shows the grid.
            if (log.Failed)
            {
                results.ShowMessages = true;
                results.MessageFollow = true;
            }
            else if (results.GridVersion != grid)
            {
                results.ShowMessages = false;
            }

            RunningTab = null;
            _context.Running = false;
            results.SetStatus(
                log.Entries.Count == 0 ? "Nothing to run." : string.Join("   ·   ", log.Entries.Select(e => e.Text)),
                log.Failed);
        }
    }

    internal async Task RunStatementAsync(ResultView results, string sql, RunLog log, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return;

        // USE belongs to the shell, not the server: the database lives in the connection string.
        if (IsUseDatabase(sql, out string database))
        {
            if (_session.Transaction is not null)
                throw new InvalidOperationException("There's an active transaction, please commit or rollback before switching databases");

            await _session.SwitchDatabaseAsync(database);
            await _catalog.OnDatabaseChangedAsync();
            log.Add($"database changed to {database}");
            return;
        }

        // Backups are node-wide REST admin calls with rich table output of their own. They stay at
        // the prompt rather than getting a second, worse rendering here.
        if (IsBackupCommand(sql, out _))
        {
            log.Add("backup commands are only available at the prompt, not in --tui");
            return;
        }

        // The refusal stops the run, as a failed statement does. The server did not see the statement,
        // so an open transaction stays open.
        if (_context.ReadOnly)
            ReadOnlyGuard.Check(sql);

        bool needsDb = !IsServerLevelDDL(sql) && !IsSystemLevelQuery(sql);

        if (needsDb && !_session.HasDatabase)
        {
            log.Fail("no database selected — run `use <database>`");
            return;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();
        CamusConnection connection = _session.Connection;

        if (IsSystemLevelQuery(sql))
        {
            await using CamusConnection sys = await _session.OpenEndpointAsync();
            log.Add(await QueryAsync(results, sys, sql, log, token));
        }
        else if (IsQueryable(sql))
        {
            log.Add(await QueryAsync(results, connection, sql, log, token));
        }
        // The server runs a statement for the whole server outside every transaction, and ignores
        // transaction IDs that come with it (mirrors ShellSession).
        else if (IsServerLevelDDL(sql))
        {
            await using CamusConnection sys = await _session.OpenEndpointAsync();
            await DdlAsync(sys, sql, transaction: null);
            log.Add($"OK ({stopwatch.Elapsed})");

            // CREATE, DROP and RENAME DATABASE change the database list in the catalog.
            if (IsDatabaseDDL(sql))
                await _catalog.LoadAsync();
        }
        // DDL in the database goes in the open transaction, so the server decides whether it can run
        // there. TRUNCATE cannot, and fails with CADB0538 (mirrors ShellSession).
        else if (IsDDL(sql))
        {
            await DdlAsync(connection, sql, _session.Transaction);
            log.Add($"OK ({stopwatch.Elapsed})");

            // A CREATE or DROP changes what the catalog should show.
            if (ChangesTableSet(sql))
                await _catalog.RefreshAsync();
            else if (ChangesIndexSet(sql))
                _completion?.InvalidateIndexes();
            else if (ChangesFunctionSet(sql) && _completion is not null)
                await _completion.RefreshInlineFunctionsAsync(connection);
            else if (IsTriggerDDL(sql) && _completion is not null)
                await _completion.RefreshTriggersAsync(connection);
        }
        else if (IsBeginTx(sql))
        {
            if (_session.Transaction is not null)
                throw new InvalidOperationException("There's an active transaction already");

            _session.Transaction = await connection.BeginTransactionAsync();
            log.Add($"transaction started ({stopwatch.Elapsed})");
        }
        else if (IsCommitTx(sql))
        {
            CamusTransaction transaction = _session.Transaction
                ?? throw new InvalidOperationException("There's no active transaction");

            try
            {
                await transaction.CommitAsync();
                log.Add($"committed ({stopwatch.Elapsed})");
            }
            finally
            {
                _session.Transaction = null;
            }
        }
        else if (IsRollbackTx(sql))
        {
            CamusTransaction transaction = _session.Transaction
                ?? throw new InvalidOperationException("There's no active transaction");

            try
            {
                await transaction.RollbackAsync();
                log.Add($"rolled back ({stopwatch.Elapsed})");
            }
            finally
            {
                _session.Transaction = null;
            }
        }
        else if (IsReturningWrite(sql))
        {
            log.Add(await WriteAsync(results, sql, log, token));
        }
        else
        {
            await using CamusCommand cmd = connection.CreateCamusCommand(sql);
            cmd.CommandTimeout = 60;
            cmd.Transaction = _session.Transaction;

            int affected = await cmd.ExecuteNonQueryAsync();
            log.Add($"{affected} rows affected ({stopwatch.Elapsed})");
        }
    }

    private static async Task DdlAsync(CamusConnection connection, string sql, CamusTransaction? transaction)
    {
        using CamusCommand cmd = connection.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = transaction;

        await cmd.ExecuteDDLAsync();
    }

    // Reads the first page and leaves the reader open, so scrolling can pull the rest.
    private async Task<string> QueryAsync(ResultView results, CamusConnection connection, string sql, RunLog log, CancellationToken token)
    {
        await results.ClosePageAsync();

        Stopwatch stopwatch = Stopwatch.StartNew();

        CamusCommand cmd = connection.CreateSelectCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = _session.Transaction;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        TimeSpan first = stopwatch.Elapsed;

        results.StartGrid(cmd, reader);

        int wanted = _context.LimitOn ? Math.Min(TuiContext.LimitRows, TuiContext.PageRows) : TuiContext.PageRows;
        int got = await results.FetchAsync(wanted, log, token);

        return results.PageDone
            ? $"{results.Rows.Count} rows in {stopwatch.Elapsed}"
            : $"{got} rows so far (first in {first}) — scroll for more";
    }

    // An INSERT, UPSERT, UPDATE or DELETE runs through a reader, so a RETURNING list can show its rows
    // in the grid. The server sends a reader with columns only for a RETURNING list. Any other write
    // leaves the grid as it is and logs the count, as before. The RETURNING rows are already in memory
    // on the client, so they are read now and the reader closes at once.
    private async Task<string> WriteAsync(ResultView results, string sql, RunLog log, CancellationToken token)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        await using CamusCommand cmd = _session.Connection.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = _session.Transaction;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        int affected = reader.RecordsAffected;

        if (reader.FieldCount == 0)
            return $"{affected} rows affected ({stopwatch.Elapsed})";

        await results.ClosePageAsync();

        results.StartGrid(null, reader);

        // The display cap still applies, so the grid can show fewer rows than the count.
        await results.FetchAsync(int.MaxValue, log, token);

        return $"{affected} rows affected ({stopwatch.Elapsed})";
    }
}
