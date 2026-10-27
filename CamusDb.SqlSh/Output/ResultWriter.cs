
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;

// Writes what a statement returns: its rows as a table, as vertical blocks for \G, as one JSON or YAML
// document for --format, or the plan of an EXPLAIN as a flow.
internal sealed class ResultWriter
{
    // Set only for --format json|yaml, which is refused outside -e and -f.
    private readonly StructuredResultWriter? structuredWriter;

    public ResultWriter(StructuredResultWriter? structuredWriter)
    {
        this.structuredWriter = structuredWriter;
    }

    // --format json|yaml asks for the rows themselves, so the flow view is only for table output.
    public bool WritesStructured => structuredWriter is not null;

    // Writes every row of `reader` as one table, or as one vertical block per row for \G, and returns
    // the row count. Writes nothing when the reader has no rows. With --format json|yaml, the rows go to
    // stdout as one document instead, and \G has no effect.
    public async Task<int> WriteRows(CamusDataReader reader, bool vertical)
    {
        if (structuredWriter is not null)
            return await structuredWriter.WriteAsync(reader);

        int rows = 0;
        Table? table = null;

        while (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> current = ConnectionHelper.ReadCurrentRow(reader);

            if (vertical)
            {
                WriteVerticalRow(current, rows + 1);
                rows++;
                continue;
            }

            table ??= NewResultTable(current);
            AddResultRow(table, current);
            rows++;
        }

        if (table is not null)
            AnsiConsole.Write(table);

        return rows;
    }

    // Writes the rows of an EXPLAIN as a flow from the source to the result, the way the web console's
    // Flow view draws it. Rows that do not have the EXPLAIN shape go to the plain table instead.
    public static async Task<int> WritePlan(CamusDataReader reader)
    {
        List<Dictionary<string, ColumnValue>> rows = [];

        while (await reader.ReadAsync())
            rows.Add(ConnectionHelper.ReadCurrentRow(reader));

        if (QueryPlanParser.TryParse(rows) is { } plan)
        {
            QueryPlanFlow.Write(AnsiConsole.Console, plan);
        }
        else if (rows.Count > 0)
        {
            Table table = NewResultTable(rows[0]);

            foreach (Dictionary<string, ColumnValue> row in rows)
                AddResultRow(table, row);

            AnsiConsole.Write(table);
        }

        return rows.Count;
    }

    public static void WriteAffectedRows(int affected, TimeSpan elapsed)
    {
        if (affected > 0)
            AnsiConsole.MarkupLine("Query OK, [blue]{0}[/] rows affected ({1})\n", affected, elapsed);
        else
            AnsiConsole.MarkupLine("Query OK, [yellow]{0}[/] rows affected ({1})\n", affected, elapsed);
    }

    // The footer of a statement that changes no rows: DDL, BEGIN, COMMIT and ROLLBACK.
    public static void WriteQueryOk(TimeSpan elapsed)
    {
        AnsiConsole.MarkupLine("Query OK, [blue]0[/] rows affected ({0})\n", elapsed);
    }

    public static void WriteRowsInSet(int rows, TimeSpan elapsed)
    {
        AnsiConsole.MarkupLine("[blue]{0}[/] rows in set ({1})\n", rows, elapsed);
    }

    // The table for a result set, with one column for each column of `first`.
    private static Table NewResultTable(Dictionary<string, ColumnValue> first)
    {
        Table table = new()
        {
            Border = TableBorder.Square
        };

        // Column names come from the server, so they're data like everything else it sends:
        // escaped the same way, since a name with '[' or Spectre markup in it (possible from
        // a backtick-quoted identifier) would otherwise render as markup or throw mid-run.
        foreach (KeyValuePair<string, ColumnValue> item in first)
            table.AddColumn(Markup.Escape(item.Key));

        return table;
    }

    private static void AddResultRow(Table table, Dictionary<string, ColumnValue> current)
    {
        string[] row = new string[current.Count];

        int i = 0;

        foreach (KeyValuePair<string, ColumnValue> item in current)
            row[i++] = ValueFormatter.Format(item.Value);

        table.AddRow(row);
    }

    private static void WriteVerticalRow(Dictionary<string, ColumnValue> row, int rowNumber)
    {
        AnsiConsole.MarkupLine("[grey]*************************** {0}. row ***************************[/]", rowNumber);

        int nameWidth = row.Keys.Count == 0 ? 0 : row.Keys.Max(k => k.Length);

        foreach (KeyValuePair<string, ColumnValue> item in row)
        {
            string name = Markup.Escape(item.Key.PadLeft(nameWidth));
            AnsiConsole.MarkupLine("[blue]{0}[/]: {1}", name, ValueFormatter.Format(item.Value));
        }
    }
}
