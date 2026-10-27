
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;
using System.Text;
using static SqlKind;

// The prompt loop: reads a line, joins lines until a statement is whole, takes the commands that only
// the prompt knows, and hands the rest to the session.
internal sealed class InteractiveShell
{
    private readonly ShellSession session;

    private readonly StringBuilder pendingSql = new();

    // Folds smart quotes for everything typed at the prompt. One instance serves the whole session
    // so a string literal spanning lines (accumulated through pendingSql) folds as one statement —
    // it is reset whenever a statement completes or an accumulation is abandoned, so a dangling
    // quote can't leak into later statements.
    private readonly SmartQuoteFolder promptFolder = new();

    // The last statement handed to the server, for `show prepared` to report on.
    private string? lastExecutedSql;

    public InteractiveShell(ShellSession session)
    {
        this.session = session;
    }

    public async Task RunAsync()
    {
        // Deliver Ctrl+C to the line editor as input instead of killing the process, so the editor can
        // clear a non-empty prompt on the first press and only exit when the prompt is already empty.
        if (session.LineEditor is not null)
            Console.TreatControlCAsInput = true;

        while (true)
        {
            string? lastSql = null;
            try
            {
                string? sql;

                if (session.LineEditor is not null)
                    sql = await session.LineEditor.ReadLine(CancellationToken.None);
                else
                    sql = AnsiConsole.Prompt(new TextPrompt<string>("camus> ").AllowEmpty());

                // The editor returns null when Ctrl+C is pressed on an empty prompt.
                if (sql is null)
                {
                    // If a multi-statement is being accumulated, clear that first instead of exiting.
                    if (pendingSql.Length > 0)
                    {
                        pendingSql.Clear();
                        promptFolder.Reset();
                        continue;
                    }

                    AnsiConsole.MarkupLine("[cyan]\nExiting...[/]");

                    await session.ShutdownAsync();
                    break;
                }

                if (string.IsNullOrWhiteSpace(sql))
                    continue;

                // Pasted SQL often carries "smart" curly quotes (from editors/chat apps) that the parser
                // doesn't recognize as string delimiters. Fold them back to straight quotes so the
                // statement both parses server-side and is seen as complete here — but only outside
                // string literals, so a ’ that is genuinely part of a value survives.
                sql = promptFolder.Fold(sql);

                string sqlTrim = sql.Trim();

                if (pendingSql.Length == 0)
                {
                    PromptCommand command = await TryRunPromptCommand(sqlTrim);

                    if (command == PromptCommand.Exit)
                        break;

                    if (command == PromptCommand.Handled)
                        continue;
                }

                string executableSql = pendingSql.Length == 0
                    ? sql
                    : $"{pendingSql}{Environment.NewLine}{sql}";

                if (SqlScript.IsIncomplete(executableSql))
                {
                    pendingSql.Clear();
                    pendingSql.Append(executableSql);
                    continue;
                }

                pendingSql.Clear();
                promptFolder.Reset();
                lastSql = executableSql;
                lastExecutedSql = executableSql;

                // A statement carrying a plaintext password stays in the in-memory editor history —
                // recalling it with Up is useful — but StatementHistory keeps it out of the file.
                session.AddHistory(executableSql);

                await session.ExecuteSql(executableSql, fromPrompt: true);

                if (session.Completion is not null && ChangesTableSet(executableSql) && HasDatabase(session.ConnectionString))
                    await session.Completion.RefreshTablesAsync(session.Connection);
                else if (session.Completion is not null && ChangesIndexSet(executableSql))
                    session.Completion.InvalidateIndexes();
            }
            catch (Exception ex)
            {
                ErrorReporter.WriteStatementError(ex, lastSql);

                // A failed statement aborts the transaction server-side, so the shell drops its handle
                // and returns to a consistent, autocommit state.
                if (session.DiscardTransaction())
                {
                    pendingSql.Clear();
                    AnsiConsole.MarkupLine("[yellow]The active transaction was aborted and has been rolled back.[/]\n");
                }
            }
        }
    }

    private enum PromptCommand
    {
        None,
        Handled,
        Exit,
    }

    // Runs the commands that only the prompt takes. They are checked only at the start of a
    // statement, so a line inside a multi-line statement is never one of them.
    private async Task<PromptCommand> TryRunPromptCommand(string sqlTrim)
    {
        if (ShellCommandParser.IsExitCommand(sqlTrim))
        {
            if (session.Transaction is not null)
            {
                AnsiConsole.MarkupLine("[red]There's an active transaction, please commit or rollback before exit[/]");
                return PromptCommand.Handled;
            }

            await session.SaveHistoryAsync();
            return PromptCommand.Exit;
        }

        if (string.Equals(sqlTrim, "clear", StringComparison.InvariantCultureIgnoreCase))
        {
            AnsiConsole.Clear();
            return PromptCommand.Handled;
        }

        if (ShellCommandParser.IsHelpCommand(sqlTrim))
        {
            HelpText.PrintShellHelp();
            return PromptCommand.Handled;
        }

        // Handled here rather than sent on: preparation is a client-side decision, so only the driver
        // knows the answer. Takes `show prepared` before the server can see it as a SHOW statement.
        if (ShellCommandParser.IsShowPrepared(sqlTrim, out string? preparedArg))
        {
            PreparedStatementReport.Write(session.ConnectionString, preparedArg ?? lastExecutedSql, explicitSql: preparedArg is not null);
            return PromptCommand.Handled;
        }

        if (sqlTrim.StartsWith("source ", StringComparison.InvariantCultureIgnoreCase))
        {
            await ScriptRunner.SourceAsync(session, sqlTrim[7..].Trim());
            return PromptCommand.Handled;
        }

        // Taken here rather than left to run as a statement so a refused switch (mid-transaction)
        // is a message, not an error that discards the transaction the way a server-side failure does.
        if (sqlTrim.StartsWith("use ", StringComparison.InvariantCultureIgnoreCase))
        {
            if (!IsUseDatabase(sqlTrim, out string newDb))
            {
                AnsiConsole.MarkupLine("[red]Usage: use <database>[/]");
                return PromptCommand.Handled;
            }

            if (session.Transaction is not null)
            {
                AnsiConsole.MarkupLine("[red]There's an active transaction, please commit or rollback before switching databases[/]");
                return PromptCommand.Handled;
            }

            await session.SwitchDatabase(newDb);
            return PromptCommand.Handled;
        }

        return PromptCommand.None;
    }
}
