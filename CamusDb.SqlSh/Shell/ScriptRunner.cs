
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;

// Runs SQL that does not come from the prompt: -f and -e, and the `source` command.
internal static class ScriptRunner
{
    // Non-interactive mode: run the supplied SQL, then exit. A failure here has no prompt to return
    // to, so report it the way the interactive loop does — a diagnosable line, not a stack trace —
    // and set a non-zero exit code so a caller can tell the statement didn't run.
    internal static async Task RunAsync(ShellSession session, Options opts)
    {
        bool readScriptFromStdin = opts.File == "-";

        // The statement that was running when things failed, so the error can point at it rather than
        // at the whole file, and where in the script it came from.
        string? failedSql = null;
        string? failedAt = null;

        try
        {
            // A script runs before -e, so `-f schema.sql -e "select ..."` reads back what the script wrote.
            if (!string.IsNullOrWhiteSpace(opts.File))
            {
                string origin = readScriptFromStdin ? "<stdin>" : opts.File;
                TextReader scriptReader = readScriptFromStdin ? Console.In : File.OpenText(opts.File);

                try
                {
                    // Statements run one at a time, as the reader produces them (rather than slurping the
                    // file and handing it to ExecuteSql) so a multi-gigabyte dump costs one statement of
                    // memory, and so a failure can name the offending statement and stop the script there,
                    // leaving the rest unrun instead of pressing on against a half-applied schema.
                    await foreach (SqlStatement statement in SqlScript.ReadStatements(scriptReader))
                    {
                        failedSql = statement.Sql;
                        failedAt = $"{origin}:{statement.Line}";
                        await session.ExecuteStatement(statement.Sql, statement.Vertical);
                    }
                }
                finally
                {
                    // Console.In outlives this block — only a file handle is ours to close.
                    if (!readScriptFromStdin)
                        scriptReader.Dispose();
                }

                failedSql = null;
                failedAt = null;
            }

            if (!string.IsNullOrWhiteSpace(opts.Execute))
            {
                failedSql = opts.Execute;
                await session.ExecuteSql(opts.Execute);
            }
        }
        catch (Exception ex)
        {
            if (failedAt is not null)
                AnsiConsole.MarkupLine("[red]{0}[/]", Markup.Escape(failedAt));

            ErrorReporter.WriteStatementError(ex, failedSql);
            Environment.ExitCode = 1;
        }
    }

    // Runs `source <file> [--force]` from the prompt.
    internal static async Task SourceAsync(ShellSession session, string arguments)
    {
        (string path, bool force) = ParseSourceArguments(arguments);

        if (string.IsNullOrWhiteSpace(path))
        {
            AnsiConsole.MarkupLine("[red]Usage: source <file> [[--force]][/]\n");
            return;
        }

        if (!File.Exists(path))
        {
            AnsiConsole.MarkupLine("[red]File not found: {0}[/]\n", Markup.Escape(path));
            return;
        }

        int executed = 0, failed = 0;

        using StreamReader reader = new(path);

        // Statements arrive as the file is read, so sourcing a dump larger than memory works and the
        // first statement runs immediately instead of after the whole file has been parsed.
        await foreach (SqlStatement statement in SqlScript.ReadStatements(reader))
        {
            try
            {
                await session.ExecuteStatement(statement.Sql, statement.Vertical);
                executed++;
            }
            catch (Exception ex)
            {
                failed++;

                // Point at the line the statement started on — in a file of thousands, "it failed" isn't
                // something you can act on.
                AnsiConsole.MarkupLine("[red]{0}:{1}[/]", Markup.Escape(path), statement.Line);

                // An open transaction is aborted server-side by the failure, so every remaining statement
                // would fail too. Stop regardless of --force and let the caller reset it.
                if (!force || session.Transaction is not null)
                    throw;

                ErrorReporter.WriteStatementError(ex, statement.Sql);
            }
        }

        if (force && failed > 0)
            AnsiConsole.MarkupLine("[yellow]{0} statement(s) failed, {1} succeeded.[/]\n", failed, executed);
    }

    // `source <file> [--force]` — --force keeps going after a failed statement instead of stopping
    // there, the way mysql's own --force does.
    private static (string Path, bool Force) ParseSourceArguments(string arguments)
    {
        bool force = false;
        List<string> parts = [];

        foreach (string token in arguments.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries))
        {
            if (string.Equals(token, "--force", StringComparison.InvariantCultureIgnoreCase))
            {
                force = true;
                continue;
            }

            parts.Add(token);
        }

        // Rejoin so an unquoted path with spaces still resolves; strip quotes if the user added them,
        // and the ';' that comes from typing the command the way every other statement is typed.
        return (string.Join(' ', parts).TrimEnd(';').Trim('"', '\''), force);
    }
}
