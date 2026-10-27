
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDb.SqlSh.Tui;
using CamusDB.Client;
using RadLine;
using Spectre.Console;
using System.Diagnostics;
using static SqlKind;

// One connected session of the shell: the connection, the database it points at, the open
// transaction, and the statements that run against them. The prompt, a script file and -e all run
// their statements through ExecuteStatement, so each statement means the same thing in all three.
internal sealed class ShellSession
{
    private readonly ShellEditor? editor;

    private readonly StatementHistory? history;

    private readonly ResultWriter resultWriter;

    // How EXPLAIN shows a plan in table output: as a flow from the source to the result, or as the
    // plain rows. `\plan flow` and `\plan table` change it for the rest of the session.
    private bool planAsFlow = true;

    public ShellSession(
        CamusConnection connection,
        string connectionString,
        ShellEditor? editor,
        StatementHistory? history,
        ResultWriter resultWriter)
    {
        Connection = connection;
        ConnectionString = connectionString;
        this.editor = editor;
        this.history = history;
        this.resultWriter = resultWriter;
    }

    public CamusConnection Connection { get; private set; }

    public string ConnectionString { get; private set; }

    public CamusTransaction? Transaction { get; private set; }

    // Null without the rich editor, and always null for -e and -f.
    public LineEditor? LineEditor => editor?.Editor;

    public SqlCompletion? Completion => editor?.Completion;

    // `fromPrompt` is true for a line typed at the prompt, where the shell can ask the user a question.
    public async Task ExecuteSql(string input, bool fromPrompt = false)
    {
        // Also fold smart quotes here so the -e/--execute flag benefits, not just the interactive
        // prompt. Idempotent, so re-folding an already-normalized statement is a no-op.
        input = NormalizeSmartQuotes(input);

        foreach ((string sql, bool vertical) in EscapeStringIntoLines(input))
            await ExecuteStatement(sql, vertical, fromPrompt);
    }

    public async Task ExecuteStatement(string sql, bool vertical, bool fromPrompt = false)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return;

        // USE is the shell's, not the server's — the database lives in the connection string. Handling it
        // here rather than only at the prompt means a script can switch databases mid-file, the way a
        // multi-database dump does, and it has to come before the no-database check below: a file that
        // opens with USE is exactly how a session with no database selected gets one.
        if (IsUseDatabase(sql, out string database))
        {
            // Refusing to switch would leave the rest of the file running against the wrong database,
            // so this stops the script instead of warning and carrying on.
            if (Transaction is not null)
                throw new InvalidOperationException("There's an active transaction, please commit or rollback before switching databases");

            await SwitchDatabase(database);
            return;
        }

        // Backups are the shell's, not the server's: they have no SQL form at all — they are node-wide REST
        // admin calls that go out over HTTP even on a gRPC connection. Handled here rather than at the prompt
        // so a script file and -e get them too, and before the no-database check below because a backup
        // captures every database on the node and needs none of them selected.
        if (IsBackupCommand(sql, out string backupArguments))
        {
            await BackupCommand.ExecuteAsync(Connection, backupArguments);
            return;
        }

        // The plan view is the shell's, not the server's. It is handled here, so a script file and -e can
        // switch it too, and before the no-database check, because it needs no database.
        if (IsPlanViewCommand(sql, out string planViewArguments))
        {
            SetPlanView(planViewArguments);
            return;
        }

        // The export is the shell's too: the server sends the rows as for any query, and the shell writes
        // them to a file. Handled here, so a script file and -e can export too.
        if (IsExportCommand(sql, out string exportArguments))
        {
            await ExportCommand.ExecuteAsync(this, exportArguments, fromPrompt);
            return;
        }

        bool needsDb = !IsServerLevelDDL(sql) && !IsSystemLevelQuery(sql);
        if (needsDb && !HasDatabase(ConnectionString))
        {
            AnsiConsole.MarkupLine("[red]No database selected.[/] Use [cyan]use <database>[/] to select one.\n");
            return;
        }

        if (IsSystemLevelQuery(sql))
            await ExecuteQuery(await OpenEndpointConnectionAsync(), sql, vertical);
        else if (IsQueryable(sql))
            await ExecuteQuery(Connection, sql, vertical);
        else if (IsServerLevelDDL(sql))
            await ExecuteDDL(await OpenEndpointConnectionAsync(), sql);
        else if (IsDDL(sql))
            await ExecuteDDL(Connection, sql);
        else if (IsBeginTx(sql))
            await ExecuteBeginTx();
        else if (IsCommitTx(sql))
            await ExecuteCommitTx();
        else if (IsRollbackTx(sql))
            await ExecuteRollbackTx();
        else if (IsInsert(sql))
            await ExecuteInsert(sql, vertical);
        else
            await ExecuteNonQuery(sql);
    }

    // Points the shell at another database by reopening the connection against it — there is no
    // server-side session to tell, so the connection string is the only thing that changes.
    public async Task SwitchDatabase(string database)
    {
        ConnectionString = SwapDatabase(ConnectionString, database);
        Connection = await ConnectionHelper.OpenAsync(ConnectionString);

        if (Completion is not null)
            await Completion.RefreshTablesAsync(Connection);

        AnsiConsole.MarkupLine("Database changed to [cyan]{0}[/]\n", Markup.Escape(database));
    }

    // A connection to the node with no database, for the statements that need none.
    public Task<CamusConnection> OpenEndpointConnectionAsync()
    {
        return ConnectionHelper.OpenAsync(GetEndpointConnectionString(ConnectionString));
    }

    // A failed statement aborts the transaction server-side, so the local handle is now stale: any
    // further command (including rollback) would fail with "Unknown transaction". Drops the handle so
    // the shell returns to autocommit, and returns true when there was one to drop.
    public bool DiscardTransaction()
    {
        if (Transaction is null)
            return false;

        Transaction = null;
        return true;
    }

    // Records a statement typed at the prompt: in the editor's history for Up, and in the history file.
    public void AddHistory(string sql)
    {
        LineEditor?.History.Add(sql);
        history?.Add(sql);
    }

    public Task SaveHistoryAsync()
    {
        return history?.SaveAsync() ?? Task.CompletedTask;
    }

    // What Ctrl+C and a clean exit both want: undo an open transaction and persist the history.
    public async Task ShutdownAsync()
    {
        // The TUI keeps its own transaction and puts the terminal in modes the prompt never uses.
        // Inside the TUI, Ctrl+C is a key, so this runs there only for a SIGINT from outside.
        if (CamusTui.Active is { } tui)
            await tui.ShutdownAsync();

        if (Transaction is not null)
        {
            AnsiConsole.MarkupLine("[yellow]Rolling back active transaction...[/]");
            await ExecuteRollbackTx();
        }

        await SaveHistoryAsync();
    }

    public void RegisterCancelKeyHandler()
    {
        Console.CancelKeyPress += (_, e) =>
        {
            // Take the shutdown over from the default kill: cancelling keeps the process alive just long
            // enough for a bounded, best-effort rollback and history save, then exits here. Blocking this
            // handler outright (.Wait()) risks a frozen process the user already tried to kill, while
            // returning without cancelling would kill the rollback mid-flight.
            e.Cancel = true;

            AnsiConsole.MarkupLine("[cyan]\nExiting...[/]");

            _ = Task.Run(async () =>
            {
                try
                {
                    await ShutdownAsync().WaitAsync(TimeSpan.FromSeconds(5));
                }
                catch
                {
                    // Shutdown is best-effort: a hung rollback or an unwritable history file must not
                    // keep the shell alive after the user asked it to die.
                }
                finally
                {
                    Environment.Exit(0);
                }
            });
        };
    }

    private async Task ExecuteNonQuery(string sql)
    {
        await using CamusCommand cmd = Connection.CreateCamusCommand(sql);

        cmd.CommandTimeout = 60;
        cmd.Transaction = Transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        int affected = await cmd.ExecuteNonQueryAsync();

        ResultWriter.WriteAffectedRows(affected, stopwatch.Elapsed);
    }

    private async Task ExecuteBeginTx()
    {
        if (Transaction is not null)
        {
            AnsiConsole.MarkupLine("[red]There's an active transaction already[/]");
            return;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();

        Transaction = await Connection.BeginTransactionAsync();

        ResultWriter.WriteQueryOk(stopwatch.Elapsed);
    }

    private async Task ExecuteCommitTx()
    {
        if (Transaction is null)
        {
            AnsiConsole.MarkupLine("[red]There's no active transaction[/]");
            return;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();

        try
        {
            await Transaction.CommitAsync();
            ResultWriter.WriteQueryOk(stopwatch.Elapsed);
        }
        finally
        {
            Transaction = null;
        }
    }

    private async Task ExecuteRollbackTx()
    {
        if (Transaction is null)
        {
            AnsiConsole.MarkupLine("[red]There's no active transaction[/]");
            return;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();

        try
        {
            await Transaction.RollbackAsync();
            ResultWriter.WriteQueryOk(stopwatch.Elapsed);
        }
        finally
        {
            Transaction = null;
        }
    }

    private async Task ExecuteQuery(CamusConnection connection, string sql, bool vertical = false)
    {
        await using CamusCommand cmd = connection.CreateSelectCommand(sql);

        cmd.CommandTimeout = 60;
        cmd.Transaction = Transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        TimeSpan duration = stopwatch.Elapsed;

        // \G and --format json|yaml ask for the rows themselves, so only the table output draws the flow.
        int rows = planAsFlow && !vertical && !resultWriter.WritesStructured && IsExplain(sql)
            ? await ResultWriter.WritePlan(reader)
            : await resultWriter.WriteRows(reader, vertical);

        ResultWriter.WriteRowsInSet(rows, duration);
    }

    // Runs an INSERT through a reader rather than ExecuteNonQueryAsync, which asks the server to drop
    // the rows of a RETURNING list and keep only the count. The server decides the shape: a RETURNING
    // list gives a reader with columns, and any other INSERT gives a reader with no columns. Either way
    // RecordsAffected is the inserted-row count, so the footer reads the same as for a plain INSERT.
    private async Task ExecuteInsert(string sql, bool vertical)
    {
        await using CamusCommand cmd = Connection.CreateCamusCommand(sql);

        cmd.CommandTimeout = 60;
        cmd.Transaction = Transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        TimeSpan duration = stopwatch.Elapsed;

        if (reader.FieldCount > 0)
            await resultWriter.WriteRows(reader, vertical);

        ResultWriter.WriteAffectedRows(reader.RecordsAffected, duration);
    }

    private static async Task ExecuteDDL(CamusConnection connection, string sql)
    {
        using CamusCommand cmd = connection.CreateCamusCommand(sql);

        cmd.CommandTimeout = 60;

        Stopwatch stopwatch = Stopwatch.StartNew();

        bool success = await cmd.ExecuteDDLAsync();

        if (success)
            ResultWriter.WriteQueryOk(stopwatch.Elapsed);
    }

    // Handles `\plan`, `\plan flow` and `\plan table`, and the same with `/plan`. With no argument, it
    // tells which view is on.
    private void SetPlanView(string argument)
    {
        if (argument.Length == 0)
        {
            AnsiConsole.MarkupLine("EXPLAIN shows the plan as [cyan]{0}[/]. Use [cyan]\\plan flow[/] or [cyan]\\plan table[/] to change it.\n",
                planAsFlow ? "a flow" : "a table");
            return;
        }

        if (argument.Equals("flow", StringComparison.InvariantCultureIgnoreCase))
            planAsFlow = true;
        else if (argument.Equals("table", StringComparison.InvariantCultureIgnoreCase))
            planAsFlow = false;
        else
        {
            AnsiConsole.MarkupLine("[red]Usage: \\plan [[flow|table]][/]\n");
            return;
        }

        AnsiConsole.MarkupLine("EXPLAIN now shows the plan as [cyan]{0}[/].\n", planAsFlow ? "a flow" : "a table");
    }
}
