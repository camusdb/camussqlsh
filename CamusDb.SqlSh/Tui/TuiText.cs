/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;

namespace CamusDb.SqlSh.Tui;

/// <summary>Text helpers that the panes of the full-screen mode share.</summary>
internal static class TuiText
{
    // Spectre's Tree wraps a node that is wider than its pane instead of clipping it, which
    // breaks the one-row-per-entry mapping that selection depends on. Every label is cut to
    // the pane width so a long name costs one row, not two.
    public static string Clip(string text, int width)
    {
        if (width <= 1)
            return "";

        return text.Length <= width ? text : text[..Math.Max(1, width - 1)] + "…";
    }

    // A cell is drawn on one line, so a line break or a Tab in a value is shown as one cell.
    public static string OneLine(string text) =>
        text.Replace("\r\n", "↵").Replace('\n', '↵').Replace('\r', '↵').Replace('\t', ' ');

    // Cuts text into lines of at most `width` characters. A line break in the text starts a
    // new line. A Tab is shown as four spaces.
    public static List<string> Wrap(string text, int width)
    {
        List<string> lines = new();
        width = Math.Max(1, width);

        foreach (string part in text.Replace("\r\n", "\n").Replace('\r', '\n').Replace("\t", "    ").Split('\n'))
        {
            if (part.Length == 0)
            {
                lines.Add("");
                continue;
            }

            for (int i = 0; i < part.Length; i += width)
                lines.Add(part.Substring(i, Math.Min(width, part.Length - i)));
        }

        return lines;
    }

    // The formatter escapes a value for Spectre markup. The grid stores plain text, so the
    // escape is undone here: Markup.Escape doubles every bracket, and nothing else.
    public static string Unescape(string markup) => markup.Replace("[[", "[").Replace("]]", "]");

    // Spectre 0.57.2 exposes no Style-to-markup conversion this build can see, so the
    // markup tag is built from the parts the shell's highlighter actually sets.
    public static string Describe(Style style)
    {
        string colour = style.Foreground.ToString();

        return style.Decoration == Decoration.None ? colour : $"{style.Decoration} {colour}".ToLowerInvariant();
    }

    public static Style Border(bool focused) => focused ? new Style(Color.Yellow) : new Style(Color.Grey35);

    public static int SafeWidth()
    {
        try { return Math.Max(60, Console.WindowWidth); }
        catch { return 120; }
    }

    public static int SafeHeight()
    {
        try { return Math.Max(16, Console.WindowHeight); }
        catch { return 40; }
    }
}
