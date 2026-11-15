/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;
using Spectre.Console.Rendering;
using static SqlKind;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// Draws the results of a tab: the grid, the value view or the messages, and the status line. The
/// renderer moves the windows of the view so that the selection stays in view.
/// </summary>
internal static class ResultsRenderer
{
    public static IRenderable Render(ResultView view, int height, int width, bool focused, bool running)
    {
        if (view.ValueView && view.SelectedRow is not null)
            return RenderValueView(view, height, width, focused);

        view.ValueView = false;

        if (view.ShowMessages)
            return RenderMessages(view, height, width, focused);

        // Two border rows, one header row, one status row.
        int visible = Math.Max(1, height - 4);
        view.VisibleRows = visible;

        view.SelRow = Math.Clamp(view.SelRow, 0, Math.Max(0, view.Rows.Count - 1));
        view.SelCol = Math.Clamp(view.SelCol, 0, Math.Max(0, view.Headers.Length - 1));

        if (view.SelRow < view.RowTop) view.RowTop = view.SelRow;
        if (view.SelRow >= view.RowTop + visible) view.RowTop = view.SelRow - visible + 1;

        List<string[]> window = view.Rows.Skip(view.RowTop).Take(visible).ToList();

        // The window shows only the columns that fit, so Spectre never squeezes a column to nothing.
        // It moves right until the selected column is one of them.
        int inner = Math.Max(10, width - 4);

        if (view.SelCol < view.ColLeft) view.ColLeft = view.SelCol;
        while (view.SelCol >= view.ColLeft + ColumnsThatFit(view.Headers, window, view.ColLeft, inner)) view.ColLeft++;

        int count = ColumnsThatFit(view.Headers, window, view.ColLeft, inner);

        Table table = new()
        {
            Border = TableBorder.None,
            Expand = true,
        };

        if (view.Headers.Length == 0)
        {
            table.AddColumn(new TableColumn("[grey](no result)[/]"));
        }
        else
        {
            for (int c = view.ColLeft; c < view.ColLeft + count; c++)
                table.AddColumn(new TableColumn($"[bold yellow]{Markup.Escape(TuiText.Clip(view.Headers[c], 24))}[/]").NoWrap());

            for (int r = 0; r < window.Count; r++)
            {
                string[] cells = new string[count];

                for (int c = 0; c < count; c++)
                {
                    string[] row = window[r];
                    string text = Markup.Escape(TuiText.Clip(TuiText.OneLine(row.ElementAtOrDefault(view.ColLeft + c) ?? ""), 40));
                    bool selected = focused && view.RowTop + r == view.SelRow && view.ColLeft + c == view.SelCol;
                    cells[c] = selected ? $"[black on aqua]{text}[/]" : text;
                }

                table.AddRow(cells);
            }
        }

        string more = view.PageDone ? "" : "+";

        string title = view.Rows.Count > 0
            ? $"Query Results ({view.Rows.Count}{more} rows)  row {view.SelRow + 1}  col {view.SelCol + 1}/{view.Headers.Length}"
            : "Query Results";

        return new Panel(new Rows(table, new Markup(Caption(view, width, running))))
        {
            Header = new PanelHeader(title),
            Border = BoxBorder.Rounded,
            BorderStyle = TuiText.Border(focused),
            Expand = true,
            Height = height,
        };
    }

    // The status line under the grid. When the status is too long for the line, the line says that M
    // shows all of it.
    private static string Caption(ResultView view, int width, bool running)
    {
        if (running)
            return "[yellow]running…[/]  ESC cancels";

        string colour = view.StatusIsError ? "red" : "grey";
        int room = Math.Max(10, width - 4);

        if (view.Status.Length <= room)
            return $"[{colour}]{Markup.Escape(view.Status)}[/]";

        return $"[{colour}]{Markup.Escape(TuiText.Clip(view.Status, room - 13))}[/] [aqua]M[/] [grey]messages[/]";
    }

    // How many columns, from `first`, fit in `room` cells. A column is as wide as its clipped header
    // or its widest clipped cell in the window, plus Spectre's one cell of padding on each side. At
    // least one column always shows.
    private static int ColumnsThatFit(string[] headers, List<string[]> window, int first, int room)
    {
        int used = 0;
        int count = 0;

        for (int c = first; c < headers.Length; c++)
        {
            int widest = Math.Min(24, headers[c].Length);

            foreach (string[] row in window)
                widest = Math.Max(widest, Math.Min(40, TuiText.OneLine(row.ElementAtOrDefault(c) ?? "").Length));

            used += widest + 2;

            if (used > room && count > 0)
                break;

            count++;
        }

        return Math.Max(1, count);
    }

    private static IRenderable RenderValueView(ResultView view, int height, int width, bool focused)
    {
        string[] row = view.SelectedRow!;
        string[] headers = view.Headers;
        int inner = Math.Max(10, width - 4);

        // Each line is markup. The text in it is escaped after the wrap, for the same reason as in
        // the grid.
        List<string> lines = new();

        if (view.RowView)
        {
            for (int c = 0; c < headers.Length; c++)
            {
                string name = Markup.Escape(TuiText.Clip(headers[c], inner));
                lines.Add(c == view.SelCol ? $"[black on aqua]{name}[/]" : $"[bold yellow]{name}[/]");

                foreach (string line in TuiText.Wrap(row.ElementAtOrDefault(c) ?? "", inner - 2))
                    lines.Add("  " + Markup.Escape(line));
            }
        }
        else
        {
            foreach (string line in TuiText.Wrap(row.ElementAtOrDefault(view.SelCol) ?? "", inner))
                lines.Add(Markup.Escape(line));
        }

        // Two border rows and one row for the key line.
        int visible = Math.Max(1, height - 3);
        view.ViewVisible = visible;
        view.ViewTop = Math.Clamp(view.ViewTop, 0, Math.Max(0, lines.Count - visible));

        List<IRenderable> shown = lines.Skip(view.ViewTop).Take(visible).Select(l => (IRenderable)new Markup(l)).ToList();

        while (shown.Count < visible)
            shown.Add(new Markup(""));

        string keys = view.RowView
            ? "[grey]←/→ row · ↑/↓ scroll · V cell view · C copy · ESC close[/]"
            : "[grey]←/→ column · ↑/↓ scroll · V row view · C copy · ESC close[/]";

        shown.Add(new Markup(keys));

        string position = lines.Count > visible
            ? $"  lines {view.ViewTop + 1}-{Math.Min(lines.Count, view.ViewTop + visible)} of {lines.Count}"
            : "";

        string title = view.RowView
            ? $"Row {view.SelRow + 1} of {view.Rows.Count}{position}"
            : $"{headers.ElementAtOrDefault(view.SelCol)}  row {view.SelRow + 1}  col {view.SelCol + 1}/{headers.Length}{position}";

        return new Panel(new Rows(shown))
        {
            Header = new PanelHeader(Markup.Escape(title)),
            Border = BoxBorder.Rounded,
            BorderStyle = TuiText.Border(focused),
            Expand = true,
            Height = height,
        };
    }

    private static IRenderable RenderMessages(ResultView view, int height, int width, bool focused)
    {
        TuiMessage[] messages = view.Messages();

        int inner = Math.Max(10, width - 4);

        // One line for the time and the statement, then the outcome below it, wrapped.
        List<string> lines = new();

        foreach (TuiMessage m in messages)
        {
            string time = $"[grey]{m.At:HH:mm:ss}[/]";
            string sql = m.Sql.Length > 0 ? Markup.Escape(Abbreviate(m.Sql, Math.Max(10, inner - 9))) : "";
            lines.Add($"{time} {sql}");

            string colour = m.Error ? "red" : "white";

            foreach (string line in TuiText.Wrap(m.Text, inner - 2))
                lines.Add($"  [{colour}]{Markup.Escape(line)}[/]");
        }

        // Two border rows and one row for the key line.
        int visible = Math.Max(1, height - 3);
        view.MessageVisible = visible;

        int bottom = Math.Max(0, lines.Count - visible);

        if (view.MessageFollow || view.MessageTop >= bottom)
        {
            view.MessageTop = bottom;
            view.MessageFollow = true;
        }

        List<IRenderable> shown = lines.Skip(view.MessageTop).Take(visible).Select(l => (IRenderable)new Markup(l)).ToList();

        if (lines.Count == 0)
            shown.Add(new Markup("[grey](no messages)[/]"));

        while (shown.Count < visible)
            shown.Add(new Markup(""));

        shown.Add(new Markup("[grey]↑/↓ scroll · C copy all · M or ESC back to the results[/]"));

        return new Panel(new Rows(shown))
        {
            Header = new PanelHeader($"Messages ({messages.Length})"),
            Border = BoxBorder.Rounded,
            BorderStyle = TuiText.Border(focused),
            Expand = true,
            Height = height,
        };
    }
}
