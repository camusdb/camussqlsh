/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;
using Spectre.Console.Rendering;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// The F1 help: every key of the full-screen mode, in groups, in a panel at the center of the screen.
/// Spectre's layout cannot draw one panel on top of another, so the panel takes the place of the panes
/// while it is open. On a wide screen the groups go in two columns. When the groups do not fit, the
/// arrows scroll them.
/// </summary>
internal static class HelpView
{
    private sealed record Group(string Title, (string Keys, string Action)[] Rows);

    private static readonly Group[] Left =
    [
        new("All panes",
        [
            ("Tab / Shift+Tab", "Next or previous pane"),
            ("Ctrl+R, F5, Shift+Enter", "Run the statement at the cursor"),
            ("Ctrl+G, F6", "Run all statements in the editor"),
            ("Esc, Ctrl+C", "Cancel the running statement"),
            ("F2", "Turn the row cap on or off"),
            ("F4", "Load the Data Catalog again"),
            ("Ctrl+S", "Save the text of all tabs"),
            ("Ctrl+L, Ctrl+U", "Empty the editor"),
            ("F1", "Show or hide this help"),
            ("Ctrl+Q", "Quit. Ctrl+C quits when nothing runs"),
        ]),
        new("Tabs",
        [
            ("Ctrl+T", "Open a new tab"),
            ("Ctrl+D", "Close the active tab"),
            ("Ctrl+O, F8, Ctrl+PgDn", "Next tab"),
            ("F7, Ctrl+PgUp", "Previous tab"),
            ("Alt+1 … Alt+9", "Go to tab 1 to 9"),
        ]),
        new("Query Editor",
        [
            ("Ctrl+N / Ctrl+P", "Next or previous completion"),
            ("Ctrl+A / Ctrl+E", "Start or end of the line"),
            ("Option+← / →, Ctrl+← / →", "One word left or right"),
            ("Option+Backspace, Ctrl+W", "Delete the word before the cursor"),
            ("Ctrl+Home / Ctrl+End", "Start or end of the editor"),
            ("Option+↑ / ↓, PgUp / PgDn", "One page up or down"),
        ]),
    ];

    private static readonly Group[] Right =
    [
        new("Data Catalog",
        [
            ("↑ / ↓, PgUp / PgDn, Home / End", "Move the selection"),
            ("→ / ←", "Expand or collapse"),
            ("Enter", "Use the database, or expand"),
            ("Space", "Put the name into the editor"),
        ]),
        new("Query Results",
        [
            ("Arrows, PgUp / PgDn", "Move the selected cell"),
            ("Home / End", "First cell, or last row"),
            ("Enter", "Show the full value"),
            ("c / Shift+C", "Copy the cell, or the row"),
            ("m", "Show the messages"),
        ]),
        new("Value view",
        [
            ("← / →", "Previous or next value"),
            ("↑ / ↓, PgUp / PgDn", "Scroll"),
            ("v", "Change between the cell and the row"),
            ("c", "Copy what the view shows"),
            ("Esc, Enter", "Close"),
        ]),
        new("Messages",
        [
            ("↑ / ↓, PgUp / PgDn", "Scroll"),
            ("c", "Copy all messages"),
            ("m, Esc", "Back to the results"),
        ]),
    ];

    private const string Notes =
        "On a Mac, Alt is Option, and only when the terminal sends Option as Esc. " +
        "Fn is necessary for the F keys, PgUp, PgDn, Home and End, unless the keyboard settings change that.";

    // The widest key text, so the actions line up in each column.
    private static readonly int KeyWidth = Left.Concat(Right).SelectMany(g => g.Rows).Max(r => r.Keys.Length);

    private static readonly int ActionWidth = Left.Concat(Right).SelectMany(g => g.Rows).Max(r => r.Action.Length);

    // One column: the key text, two spaces, the action.
    private static int ColumnWidth => KeyWidth + 2 + ActionWidth;

    /// <summary>
    /// The help panel at the center of an area of <paramref name="height"/> by <paramref name="width"/>
    /// cells. <paramref name="top"/> is the first line in view. The method keeps it in range.
    /// <paramref name="visible"/> is the number of lines in view, for PageUp and PageDown.
    /// </summary>
    public static IRenderable Render(int height, int width, ref int top, out int visible)
    {
        // Two columns with a gap of four cells, plus the borders and the padding of the panel.
        bool twoColumns = width >= ColumnWidth * 2 + 4 + 6;

        int inner = twoColumns ? ColumnWidth * 2 + 4 : Math.Min(ColumnWidth, Math.Max(20, width - 6));

        List<string> lines = twoColumns
            ? Merge(Lines(Left, ColumnWidth), Lines(Right, ColumnWidth))
            : Lines(Left.Concat(Right), inner);

        List<string> notes = WordWrap(Notes, inner);

        // The borders take two rows, and the notes take a blank row and their own rows.
        int room = Math.Max(3, height - 2 - notes.Count - 1);
        visible = Math.Min(lines.Count, room);
        top = Math.Clamp(top, 0, Math.Max(0, lines.Count - visible));

        List<IRenderable> shown = lines.Skip(top).Take(visible).Select(l => (IRenderable)new Markup(l)).ToList();
        shown.Add(new Markup(" "));
        shown.AddRange(notes.Select(n => (IRenderable)new Markup($"[grey]{Markup.Escape(n)}[/]")));

        string position = lines.Count > visible
            ? $" lines {top + 1}-{top + visible} of {lines.Count} · ↑/↓ scroll ·"
            : "";

        Panel panel = new(new Rows(shown))
        {
            Header = new PanelHeader($"[bold] Keyboard shortcuts [/][grey]{position} F1 or Esc closes [/]"),
            Border = BoxBorder.Rounded,
            BorderStyle = new Style(Color.Aqua),
            Padding = new Padding(2, 0, 2, 0),
            Width = Math.Min(width, inner + 6),
        };

        return new Align(panel, HorizontalAlignment.Center, VerticalAlignment.Middle) { Height = height };
    }

    // The markup lines of some groups: a title line, a line for each key, and a blank line between
    // the groups. Each line is exactly `width` cells, so two columns can go side by side, and a line
    // never wraps: a wrapped line would break the scroll. On a narrow screen the text is clipped.
    private static List<string> Lines(IEnumerable<Group> groups, int width)
    {
        int keyWidth = Math.Min(KeyWidth, Math.Max(8, width / 2));
        int actionWidth = Math.Max(1, width - keyWidth - 2);

        List<string> lines = new();

        foreach (Group group in groups)
        {
            if (lines.Count > 0)
                lines.Add(new string(' ', width));

            lines.Add($"[bold yellow]{Markup.Escape(TuiText.Clip(group.Title, width).PadRight(width))}[/]");

            foreach ((string keys, string action) in group.Rows)
            {
                string key = TuiText.Clip(keys, keyWidth).PadRight(keyWidth);
                string text = TuiText.Clip(action, actionWidth).PadRight(actionWidth);
                lines.Add($"[aqua]{Markup.Escape(key)}[/]  {Markup.Escape(text)}");
            }
        }

        return lines;
    }

    // Cuts text into lines of at most `width` characters at the spaces. A word longer than the line
    // is cut.
    private static List<string> WordWrap(string text, int width)
    {
        List<string> lines = new();
        string line = "";

        foreach (string word in text.Split(' ', StringSplitOptions.RemoveEmptyEntries))
        {
            if (line.Length > 0 && line.Length + 1 + word.Length > width)
            {
                lines.Add(line);
                line = "";
            }

            line = line.Length == 0 ? word : line + " " + word;

            while (line.Length > width)
            {
                lines.Add(line[..width]);
                line = line[width..];
            }
        }

        if (line.Length > 0)
            lines.Add(line);

        return lines;
    }

    private static List<string> Merge(List<string> left, List<string> right)
    {
        string blank = new(' ', ColumnWidth);
        int count = Math.Max(left.Count, right.Count);

        return Enumerable.Range(0, count)
            .Select(i => $"{left.ElementAtOrDefault(i) ?? blank}    {right.ElementAtOrDefault(i) ?? blank}")
            .ToList();
    }
}
