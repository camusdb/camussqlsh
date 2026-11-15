/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RadLine;
using Spectre.Console;
using Spectre.Console.Rendering;
using System.Text;

namespace CamusDb.SqlSh.Tui;

/// <summary>Draws the query editor of a tab, with SQL colors and the cursor.</summary>
internal sealed class EditorRenderer
{
    private readonly IHighlighter _highlighter;

    // Markup tags for the tokens that the highlighter cannot colour one word at a time.
    private readonly string? _stringTag;
    private readonly string? _identifierTag;
    private readonly string? _commentTag;

    public EditorRenderer(IHighlighter highlighter)
    {
        _highlighter = highlighter;

        // The shell's highlighter colours a whole string or comment by its regexes, and the prompt
        // gives it whole strings and comments. The editor lexer finds those tokens itself, so it asks
        // the highlighter once for the style of a minimal sample of each kind. A string such as '--x'
        // then stays a string, and does not become a comment.
        _stringTag = highlighter.Highlight("''") is { } str ? TuiText.Describe(str) : null;
        _identifierTag = highlighter.Highlight("``") is { } ident ? TuiText.Describe(ident) : null;
        _commentTag = highlighter.Highlight("--") is { } comment ? TuiText.Describe(comment) : null;
    }

    /// <summary>
    /// The editor panel. <paramref name="header"/> is markup: the tab strip. The renderer moves the
    /// window of the buffer so that the cursor stays in view.
    /// </summary>
    public IRenderable Render(EditorBuffer editor, int height, int width, bool focused, string header)
    {
        int visible = Math.Max(1, height - 2);
        editor.Visible = visible;

        if (editor.Row < editor.Top) editor.Top = editor.Row;
        if (editor.Row >= editor.Top + visible) editor.Top = editor.Row - visible + 1;

        // Two border cells, two padding cells, three for the line number, two for the gap.
        int textWidth = Math.Max(10, width - 9);

        // A line longer than the pane must scroll rather than hide its tail, so the window follows
        // the cursor. Every line scrolls together, which keeps the columns aligned.
        LineBuffer current = editor.Current;

        if (current.Position < editor.Left)
            editor.Left = current.Position;

        if (current.Position >= editor.Left + textWidth)
            editor.Left = current.Position - textWidth + 1;

        // A two-column Grid splits the pane width between its columns, which left the code column far
        // narrower than the space available. The line number is part of the same markup string
        // instead, so the text gets every cell that is left.
        List<IRenderable> rendered = new();
        List<LineBuffer> lines = editor.Lines;

        // A string or a comment that opens above the window colours the lines below it, so the lexer
        // reads every line from the top.
        SqlLexState state = default;

        for (int i = 0; i < editor.Top && i < lines.Count; i++)
            SqlLineLexer.Tokenize(lines[i].Content, ref state);

        for (int i = editor.Top; i < Math.Min(lines.Count, editor.Top + visible); i++)
        {
            bool onCursor = i == editor.Row && focused;
            List<SqlToken> tokens = SqlLineLexer.Tokenize(lines[i].Content, ref state);
            rendered.Add(new Markup($"[grey]{i + 1,3}[/]  {RenderLine(lines[i], tokens, onCursor, editor.Left, textWidth)}"));
        }

        if (rendered.Count == 0)
            rendered.Add(new Markup(string.Empty));

        return new Panel(new Rows(rendered))
        {
            Header = new PanelHeader(header),
            Border = BoxBorder.Rounded,
            BorderStyle = TuiText.Border(focused),
            Expand = true,
            Height = height,
        };
    }

    // The Live region owns the screen, so the hardware cursor cannot be placed inside a Panel. The
    // cursor is drawn as an inverted cell instead.
    //
    // Tokenizing starts at the beginning of the line even when the window does not, so a keyword that
    // straddles the left edge keeps its colour.
    private string RenderLine(LineBuffer line, List<SqlToken> tokens, bool showCursor, int left, int width)
    {
        string text = line.Content;
        int cursor = Math.Clamp(line.Position, 0, text.Length);

        StringBuilder markup = new();

        foreach ((int start, int length, SqlTokenKind kind) in tokens)
        {
            if (start >= left + width)
                break;

            string token = text.Substring(start, length);

            // `is { }` produces a non-null Style whether the highlighter's return is a nullable
            // reference or a nullable value type, so this compiles either way.
            string? tag = kind switch
            {
                SqlTokenKind.String => _stringTag,
                SqlTokenKind.Identifier => _identifierTag,
                SqlTokenKind.Comment => _commentTag,
                _ => _highlighter.Highlight(token) is { } style ? TuiText.Describe(style) : null,
            };

            for (int j = 0; j < token.Length; j++)
            {
                int at = start + j;

                if (at < left || at >= left + width)
                    continue;

                // A pasted Tab is kept in the text but drawn as one cell, so the cursor column stays
                // the character index.
                string ch = Markup.Escape(token[j] == '\t' ? " " : token[j].ToString());

                if (showCursor && at == cursor)
                    markup.Append($"[black on white]{ch}[/]");
                else if (tag is not null)
                    markup.Append($"[{tag}]{ch}[/]");
                else
                    markup.Append(ch);
            }
        }

        if (showCursor && cursor >= text.Length && cursor < left + width)
            markup.Append("[black on white] [/]");

        return markup.ToString();
    }
}
