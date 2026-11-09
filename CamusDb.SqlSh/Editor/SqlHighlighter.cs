/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RadLine;
using Spectre.Console;

// Colors the lines of the prompt editor. A string, a block comment or a $$ function body can start
// on an earlier line, so each line is read with the lines above it, by the lexer that the
// full-screen editor uses. Words and other tokens get their colors from the word highlighter.
internal sealed class SqlHighlighter : ILineHighlighter
{
    private readonly IHighlighter words;
    private readonly Style stringStyle;
    private readonly Style identifierStyle;
    private readonly Style commentStyle;

    public SqlHighlighter(IHighlighter words, Style stringStyle, Style identifierStyle, Style commentStyle)
    {
        this.words = words;
        this.stringStyle = stringStyle;
        this.identifierStyle = identifierStyle;
        this.commentStyle = commentStyle;
    }

    public Style? Highlight(string token)
    {
        return words.Highlight(token);
    }

    public IEnumerable<(string Text, Style? Style)> HighlightLine(IReadOnlyList<string> lines, int lineIndex)
    {
        SqlLexState state = default;

        for (int i = 0; i < lineIndex; i++)
            SqlLineLexer.Tokenize(lines[i], ref state);

        string text = lines[lineIndex];
        int at = 0;

        foreach ((int start, int length, SqlTokenKind kind) in SqlLineLexer.Tokenize(text, ref state))
        {
            if (start > at)
                yield return (text[at..start], null);

            string token = text.Substring(start, length);

            Style? style = kind switch
            {
                SqlTokenKind.String => stringStyle,
                SqlTokenKind.Identifier => identifierStyle,
                SqlTokenKind.Comment => commentStyle,
                _ => string.IsNullOrWhiteSpace(token) ? null : words.Highlight(token),
            };

            yield return (token, style);
            at = start + length;
        }

        if (at < text.Length)
            yield return (text[at..], null);
    }
}
