
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using static SqlKind;

internal static class PreparedStatementReport
{
    // Reports what the driver currently keeps prepared. The count is shared per deployment and
    // settings rather than per connection, so it survives `use` and outlives any one connection.
    internal static void Write(string connectionString, string? sql, bool explicitSql)
    {
        CamusConnectionStringBuilder builder = new(connectionString);

        AnsiConsole.MarkupLine(
            "Prepared statements: [blue]{0}[/] [grey58](MaxAutoPrepare={1}, AutoPrepareMinUsages={2})[/]",
            builder.PreparedStatementCount, builder.MaxAutoPrepare, builder.AutoPrepareMinUsages);

        if (builder.MaxAutoPrepare == 0)
            AnsiConsole.MarkupLine("[yellow]Automatic preparation is off[/] [grey58](MaxAutoPrepare=0 in the connection string).[/]");

        if (sql is null)
        {
            AnsiConsole.MarkupLine("[grey58]Run a statement, or pass one to check: [/][cyan]show prepared SELECT ...[/]\n");
            return;
        }

        foreach ((string statement, bool _) in EscapeStringIntoLines(sql))
        {
            if (string.IsNullOrWhiteSpace(statement))
                continue;

            // Typed statements carry their values inline, so the text differs on every execution and
            // never repeats often enough to be registered — which is what this usually reports.
            bool prepared = builder.IsPrepared(statement);

            AnsiConsole.MarkupLine(
                prepared ? "  [green]prepared[/]     {0}" : "  [grey58]inline[/]       {0}",
                Markup.Escape(Abbreviate(statement)));
        }

        if (!explicitSql)
            AnsiConsole.MarkupLine("[grey58](the statement you ran last)[/]");

        AnsiConsole.WriteLine();
    }
}
