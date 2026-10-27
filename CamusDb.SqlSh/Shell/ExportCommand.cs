
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using RadLine;
using Spectre.Console;
using System.Diagnostics;
using static SqlKind;

internal static class ExportCommand
{
    // Runs `\export <csv|json|yaml> [to <file>] <query>`: the rows of the query go to a file, not to the
    // screen. With no file, the shell suggests <table>-<date>-<time>.<format> in the current directory: at
    // the prompt the user can edit that name first, and anywhere else the shell takes it. The rows go to a
    // temporary file first, so a query that fails halfway does not leave half a file at the target.
    internal static async Task ExecuteAsync(ShellSession session, string arguments, bool fromPrompt)
    {
        if (!TryParseExportArguments(arguments, out string formatName, out string? path, out string query)
            || !TryParseExportFormat(formatName, out OutputFormat format))
        {
            AnsiConsole.MarkupLine("[red]Usage: \\export csv|json|yaml [[to <file>]] <query>[/]\n");
            return;
        }

        bool systemLevel = IsSystemLevelQuery(query);

        if (!systemLevel && !IsQueryable(query))
        {
            AnsiConsole.MarkupLine("[red]\\export needs a statement that returns rows, such as SELECT or SHOW.[/]\n");
            return;
        }

        if (!systemLevel && !HasDatabase(session.ConnectionString))
        {
            AnsiConsole.MarkupLine("[red]No database selected.[/] Use [cyan]use <database>[/] to select one.\n");
            return;
        }

        if (path is null)
        {
            string extension = format == OutputFormat.Yaml ? "yaml" : formatName.ToLowerInvariant();
            string suggestion = SuggestExportFileName(query, extension, DateTime.Now);

            path = fromPrompt ? await AskExportPath(session.LineEditor is not null, suggestion) : suggestion;

            if (path is null)
            {
                AnsiConsole.MarkupLine("[yellow]Export cancelled.[/]\n");
                return;
            }
        }

        // The shell does not expand `~` the way a Unix shell does, so it is done here.
        if (path.StartsWith("~/", StringComparison.Ordinal))
            path = Environment.GetFolderPath(Environment.SpecialFolder.UserProfile) + path[1..];

        string fullPath = Path.GetFullPath(path);

        CamusConnection queryConnection = systemLevel
            ? await session.OpenEndpointConnectionAsync()
            : session.Connection;

        await using CamusCommand cmd = queryConnection.CreateSelectCommand(query);

        cmd.CommandTimeout = 60;
        cmd.Transaction = session.Transaction;

        Stopwatch stopwatch = Stopwatch.StartNew();

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        string tempPath = fullPath + ".tmp";
        int rows;

        try
        {
            await using (FileStream stream = new(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
                rows = await new StructuredResultWriter(format, stream).WriteAsync(reader);

            File.Move(tempPath, fullPath, overwrite: true);
        }
        catch
        {
            File.Delete(tempPath);
            throw;
        }

        AnsiConsole.MarkupLine("[blue]{0}[/] rows exported to [cyan]{1}[/] ({2})\n", rows, Markup.Escape(fullPath), stopwatch.Elapsed);
    }

    // Asks where an export goes, with the suggested name already typed in: Enter takes it, and any edit
    // changes it. Returns null when the user empties the line or cancels with Ctrl+C. Without the rich
    // editor, the name cannot be typed in, so the plain prompt shows it as the default instead.
    private static async Task<string?> AskExportPath(bool richEditor, string suggestion)
    {
        string? answer;

        if (!richEditor)
        {
            answer = AnsiConsole.Prompt(new TextPrompt<string>("Export to:").DefaultValue(suggestion));
        }
        else
        {
            LineEditor pathEditor = new()
            {
                Text = suggestion,
                Prompt = new LineEditorPrompt("[grey58]Export to:[/]")
            };

            answer = await pathEditor.ReadLine(CancellationToken.None);
        }

        answer = answer?.Trim().Trim('"', '\'');

        return string.IsNullOrEmpty(answer) ? null : answer;
    }

    private static bool TryParseExportFormat(string name, out OutputFormat format)
    {
        switch (name.ToLowerInvariant())
        {
            case "csv":
                format = OutputFormat.Csv;
                return true;

            case "json":
                format = OutputFormat.Json;
                return true;

            case "yaml" or "yml":
                format = OutputFormat.Yaml;
                return true;

            default:
                format = OutputFormat.Table;
                return false;
        }
    }
}
