
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
        ResultWriter resultWriter,
        bool readOnly = false)
    {
        Connection = connection;
        ConnectionString = connectionString;
        this.editor = editor;
        this.history = history;
        this.resultWriter = resultWriter;
        ReadOnly = readOnly;
    }

    // True with --read-only: ReadOnlyGuard refuses each statement that can change the database or the
    // server, before the shell sends it.
    public bool ReadOnly { get; }

    public CamusConnection Connection { get; private set; }

    public string ConnectionString { get; private set; }

    public CamusTransaction? Transaction { get; private set; }

    // True when a schema statement ran in the open transaction. With the server setting
    // transactional_ddl_enabled on, other sessions do not see the change until COMMIT. The editor
    // vocabulary loads outside the transaction, so it loads again when the transaction ends.
    private bool transactionChangedSchema;

    // Null without the rich editor, and always null for -e and -f.
    public LineEditor? LineEditor => editor?.Editor;

    public SqlCompletion? Completion => editor?.Completion;

    // The keys of the rich editor. Null without it.
    public PromptKeys? Keys => editor?.Keys;

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
            if (ReadOnly && ReadOnlyGuard.BackupRefusal(backupArguments) is { } backupRefusal)
                throw new ReadOnlyRefusedException(backupRefusal);

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

        // `\l` is SHOW DATABASES under its psql name, so it needs no database either.
        if (IsListDatabasesCommand(sql))
        {
            await ExecuteStatement("show databases", vertical);
            return;
        }

        bool needsDb = !IsServerLevelDDL(sql) && !IsSystemLevelQuery(sql);
        if (needsDb && !HasDatabase(ConnectionString))
        {
            AnsiConsole.MarkupLine("[red]No database selected.[/] Use [cyan]use <database>[/] to select one.\n");
            return;
        }

        // `\d` and its family describe the objects of the current database, so they come after the
        // no-database check. They run outside the open transaction, as the catalog needs none.
        if (IsDescribeCommand(sql, out DescribeRequest describeRequest))
        {
            await DescribeCommand.ExecuteAsync(Connection, describeRequest);
            return;
        }

        // `\df` lists the inline functions of the current database, so it needs one too.
        if (IsDescribeFunctionsCommand(sql, out bool functionsVerbose, out string? functionsPattern))
        {
            await DescribeCommand.ListFunctionsAsync(Connection, functionsVerbose, functionsPattern);
            return;
        }

        // The shell commands above only read, or check their own work. Each SQL statement is checked
        // here, before it goes to the server.
        if (ReadOnly)
            ReadOnlyGuard.Check(sql);

        bool write = !IsQueryable(sql) && !IsSystemLevelQuery(sql);

        await RunCancellableAsync(token => ExecuteServerStatement(sql, vertical, token), fromPrompt, write);
    }

    // Runs `run` with a token that ESC and Ctrl+C cancel, for a statement typed at the prompt. A
    // cancel becomes a StatementCancelledException. Only the rich editor reads the keys this way: in a
    // script, -e, or a prompt without the rich editor, Ctrl+C is a signal that ends the shell.
    // `write` is true for a statement that can change data.
    public async Task RunCancellableAsync(Func<CancellationToken, Task> run, bool fromPrompt, bool write)
    {
        if (!fromPrompt || editor is null)
        {
            await run(CancellationToken.None);
            return;
        }

        // In a transaction, the prompt rolls the whole transaction back after a cancel. Outside one,
        // the server commits a write by itself, and a cancel that arrives late does not undo it.
        bool autocommitWrite = write && Transaction is null;

        Stopwatch stopwatch = Stopwatch.StartNew();

        await using CancelWatch watch = editor.Keys.Watch();

        try
        {
            await run(watch.Token);
        }
        catch (Exception ex) when (watch.Cancelled)
        {
            throw new StatementCancelledException(watch.CancelKey ?? "ESC", stopwatch.Elapsed, autocommitWrite, ex);
        }
    }

    // The statements that go to the server. The client stops a call when `token` is cancelled. Over
    // REST, the server then stops the work too. Over gRPC, CamusDB.Client 0.19.4 only stops its own
    // wait, because its batched stream sends the server no cancel, and the server finishes the work.
    private async Task ExecuteServerStatement(string sql, bool vertical, CancellationToken token)
    {
        if (IsSystemLevelQuery(sql))
            await ExecuteQuery(await OpenEndpointConnectionAsync(), sql, vertical, token);
        else if (IsQueryable(sql))
            await ExecuteQuery(Connection, sql, vertical, token);
        // The server runs a statement for the whole server (a database, a user, a grant, a cluster
        // setting, a cancel) outside every transaction, and ignores transaction IDs that come with it.
        // So none go with it.
        else if (IsServerLevelDDL(sql))
            await ExecuteDDL(await OpenEndpointConnectionAsync(), sql, transaction: null, token);
        // DDL in the database goes in the open transaction, so the server decides whether it can run
        // there. TRUNCATE cannot: the client refuses it with CADB0538, and the server refuses it too. Sent
        // outside the transaction, it emptied the table and a later ROLLBACK did not undo that. With the
        // server setting transactional_ddl_enabled on, COMMIT and ROLLBACK also apply to CREATE, ALTER
        // and DROP, and the server refuses with CADB0538 each schema statement that it cannot yet run in
        // a transaction. With the setting off, a schema statement commits at once.
        else if (IsDDL(sql))
        {
            await ExecuteDDL(Connection, sql, Transaction, token);

            if (Transaction is not null)
                transactionChangedSchema = true;
        }
        // BEGIN, COMMIT and ROLLBACK do not take the token. A COMMIT that stops halfway leaves the
        // client with no way to know whether the transaction committed.
        else if (IsBeginTx(sql))
            await ExecuteBeginTx(sql);
        else if (IsCommitTx(sql))
            await ExecuteCommitTx();
        else if (IsRollbackTx(sql))
            await ExecuteRollbackTx();
        else if (IsReturningWrite(sql))
            await ExecuteWrite(sql, vertical, token);
        else
            await ExecuteNonQuery(sql, token);
    }

    // Points the shell at another database by reopening the connection against it — there is no
    // server-side session to tell, so the connection string is the only thing that changes.
    public async Task SwitchDatabase(string database)
    {
        ConnectionString = SwapDatabase(ConnectionString, database);
        Connection = await ConnectionHelper.OpenAsync(ConnectionString);

        if (editor is not null)
            await editor.RefreshDatabaseVocabularyAsync(Connection);

        AnsiConsole.MarkupLine("Database changed to [cyan]{0}[/]\n", Markup.Escape(database));
    }

    // A connection to the node with no database, for the statements that need none.
    public Task<CamusConnection> OpenEndpointConnectionAsync()
    {
        return ConnectionHelper.OpenAsync(GetEndpointConnectionString(ConnectionString));
    }

    // A failed statement ends the open transaction: the shell drops the handle and returns to
    // autocommit. A failure on the server already rolled the transaction back there. A statement that
    // the client refused, such as TRUNCATE in a transaction (CADB0538), did not reach the server, so the
    // transaction is still open there and keeps its locks. The rollback here ends it. When the server
    // ended it already, the rollback fails ("Unknown transaction"), and that is not an error here.
    // Returns true when there was a handle to drop.
    public async Task<bool> DiscardTransactionAsync()
    {
        if (Transaction is not { } transaction)
            return false;

        Transaction = null;

        try
        {
            await transaction.RollbackAsync().WaitAsync(TimeSpan.FromSeconds(5));
        }
        catch (Exception)
        {
            // The server ended the transaction already, or it did not answer in time. If it is still
            // open, the server ends it when it times out.
        }

        await OnTransactionEndedAsync();

        return true;
    }

    // Loads the editor vocabulary again when the transaction that ended ran a schema statement. The
    // relations, the indexes, the inline functions and the triggers can all change. A failed load
    // keeps the old names, because the statement itself completed.
    private async Task OnTransactionEndedAsync()
    {
        if (!transactionChangedSchema)
            return;

        transactionChangedSchema = false;

        if (editor is null || !HasDatabase(ConnectionString))
            return;

        try
        {
            await editor.RefreshDatabaseVocabularyAsync(Connection);
            await editor.RefreshInlineFunctionsAsync(Connection);
            await editor.Completion.RefreshTriggersAsync(Connection);
        }
        catch (Exception)
        {
            // The completion keeps the names it had.
        }
    }

    // Loads again the names of the current database that the editor completes and colors: the
    // relations, the sequences and the inline functions. Does nothing without the rich editor.
    public Task RefreshDatabaseVocabularyAsync()
    {
        return editor?.RefreshDatabaseVocabularyAsync(Connection) ?? Task.CompletedTask;
    }

    // Loads again the inline function names only, after a CREATE or DROP FUNCTION.
    public Task RefreshFunctionVocabularyAsync()
    {
        return editor?.RefreshInlineFunctionsAsync(Connection) ?? Task.CompletedTask;
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

    private async Task ExecuteNonQuery(string sql, CancellationToken token)
    {
        await using CamusCommand cmd = Connection.CreateCamusCommand(sql);

        cmd.CommandTimeout = 60;
        cmd.Transaction = Transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        int affected = await cmd.ExecuteNonQueryAsync(token);

        ResultWriter.WriteAffectedRows(affected, stopwatch.Elapsed);
    }

    // BEGIN and START TRANSACTION, with their modes: BEGIN READ ONLY declares a read-only transaction,
    // and the server then refuses its writes with CADB0849. The shell reads the modes, because it
    // opens the transaction through the client and does not send the BEGIN text.
    private async Task ExecuteBeginTx(string sql)
    {
        if (Transaction is not null)
        {
            AnsiConsole.MarkupLine("[red]There's an active transaction already[/]");
            return;
        }

        CamusTransactionOptions options = BeginStatement.Parse(sql);

        Stopwatch stopwatch = Stopwatch.StartNew();

        Transaction = await Connection.BeginTransactionAsync(options);
        transactionChangedSchema = false;

        string? modes = BeginStatement.DescribeModes(options);

        ResultWriter.WriteQueryOk(stopwatch.Elapsed, modes is null ? null : $"Transaction: {modes}");
    }

    private async Task ExecuteCommitTx()
    {
        if (Transaction is null)
        {
            AnsiConsole.MarkupLine("[red]There's no active transaction[/]");
            return;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();

        CamusTransaction transaction = Transaction;

        try
        {
            await transaction.CommitAsync();
            ResultWriter.WriteQueryOk(stopwatch.Elapsed, CommitDetail(transaction));
        }
        finally
        {
            Transaction = null;
            await OnTransactionEndedAsync();
        }
    }

    // The commit timestamp puts the commit in its order among the other commits of the cluster. It shows
    // as HLC(N:L:C), the format of the server logs. It is null for a transaction that wrote nothing, and
    // from a server older than CamusDB 0.18.4. The shell then shows no detail.
    internal static string? CommitDetail(CamusTransaction transaction)
    {
        return transaction.CommitTimestamp is { } timestamp ? $"Commit timestamp: {timestamp}" : null;
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
            await OnTransactionEndedAsync();
        }
    }

    private async Task ExecuteQuery(CamusConnection connection, string sql, bool vertical, CancellationToken token)
    {
        await using CamusCommand cmd = connection.CreateSelectCommand(sql);

        cmd.CommandTimeout = 60;
        cmd.Transaction = Transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        // The client reads every row before ExecuteReaderAsync returns, so the token covers all the
        // work on the server. The loops that write the rows read from memory.
        await using CamusDataReader reader = await cmd.ExecuteReaderAsync(token);

        TimeSpan duration = stopwatch.Elapsed;

        // \G and --format json|yaml ask for the rows themselves, so only the table output draws the flow.
        int rows = planAsFlow && !vertical && !resultWriter.WritesStructured && IsExplain(sql)
            ? await ResultWriter.WritePlan(reader)
            : await resultWriter.WriteRows(reader, vertical);

        ResultWriter.WriteRowsInSet(rows, duration);
    }

    // Runs an INSERT, UPSERT, UPDATE or DELETE through a reader rather than ExecuteNonQueryAsync, which
    // asks the server to drop the rows of a RETURNING list and keep only the count. The server decides
    // the shape: a RETURNING list gives a reader with columns, and any other write gives a reader with
    // no columns. Either way RecordsAffected is the written-row count, so the footer reads the same as
    // for a write without RETURNING.
    private async Task ExecuteWrite(string sql, bool vertical, CancellationToken token)
    {
        await using CamusCommand cmd = Connection.CreateCamusCommand(sql);

        cmd.CommandTimeout = 60;
        cmd.Transaction = Transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        await using CamusDataReader reader = await cmd.ExecuteReaderAsync(token);

        TimeSpan duration = stopwatch.Elapsed;

        if (reader.FieldCount > 0)
            await resultWriter.WriteRows(reader, vertical);

        ResultWriter.WriteAffectedRows(reader.RecordsAffected, duration);
    }

    private static async Task ExecuteDDL(CamusConnection connection, string sql, CamusTransaction? transaction, CancellationToken token)
    {
        using CamusCommand cmd = connection.CreateCamusCommand(sql);

        cmd.CommandTimeout = 60;
        cmd.Transaction = transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        bool success = await cmd.ExecuteDDLAsync(token);

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
