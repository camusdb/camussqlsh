
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

// One statement pulled out of a script, with the line it started on so an error can point at it.
// Start and End are offsets into everything fed so far: the first character of the statement and
// its terminator (the last character, when the text ran out first). Both are inclusive.
public readonly record struct SqlStatement(string Sql, bool Vertical, int Line, int Start = 0, int End = 0);

// Splits SQL text into statements, tracking where a ';' actually terminates one: not inside a
// string, a `quoted identifier`, or a comment. Holding the state in an object (rather than
// re-scanning a whole string) lets callers feed a file line by line instead of loading it whole,
// and lets the prompt ask whether what's been typed so far is finished.
public sealed class SqlScanner
{
    private readonly StringBuilder current = new();

    private bool inSingleQuote, inDoubleQuote, inBacktick, inLineComment, inBlockComment;
    private int parenDepth;

    // The line the scanner is reading, and the one the pending statement's first real character
    // sat on — leading blank lines and comments shouldn't shift the reported location.
    private int line = 1;
    private int statementLine = 1;

    // Offsets for SqlStatement.Start and End. `fed` counts the characters of earlier chunks, and
    // `at` is the offset of the character being read.
    private int fed;
    private int at;
    private int statementStart;

    private bool hasContent;
    private char lastContentChar;

    /// <summary>Inside a string literal or a quoted identifier, so the statement can't be over.</summary>
    public bool InsideQuotes => inSingleQuote || inDoubleQuote || inBacktick;

    /// <summary>Inside an unterminated /* … */. A line comment self-terminates, so it doesn't count.</summary>
    public bool InsideBlockComment => inBlockComment;

    public int ParenDepth => parenDepth;

    /// <summary>The pending text holds something other than whitespace and comments.</summary>
    public bool HasPendingContent => hasContent;

    /// <summary>Last non-whitespace character outside a comment, for spotting a trailing comma.</summary>
    public char LastContentChar => lastContentChar;

    /// <summary>
    /// Consumes a chunk of SQL and yields whatever statements it completed. Two-character tokens
    /// (--, /*, */, \G, doubled quotes) are looked up within the chunk, so callers must not split a
    /// chunk mid-token; feeding whole lines is always safe, since none of them can span a newline.
    /// </summary>
    public IEnumerable<SqlStatement> Feed(string text)
    {
        for (int i = 0; i < text.Length; i++)
        {
            at = fed + i;

            char c = text[i];
            char next = i + 1 < text.Length ? text[i + 1] : '\0';

            if (inLineComment)
            {
                Skip(c);

                if (c == '\n')
                    inLineComment = false;

                continue;
            }

            if (inBlockComment)
            {
                Skip(c);

                if (c == '*' && next == '/')
                {
                    Skip(next);
                    i++;
                    inBlockComment = false;
                }

                continue;
            }

            if (InsideQuotes)
            {
                // Backslash escapes a character inside a string literal, but a backtick-quoted
                // identifier has no escape other than doubling.
                if (c == '\\' && next != '\0' && !inBacktick)
                {
                    Append(c, content: true);
                    Append(next, content: true);
                    i++;
                    continue;
                }

                char delimiter = inSingleQuote ? '\'' : inDoubleQuote ? '"' : '`';

                if (c == delimiter)
                {
                    // A doubled delimiter ('' "" ``) is a literal one, not the end of the quote.
                    if (next == delimiter)
                    {
                        Append(c, content: true);
                        Append(next, content: true);
                        i++;
                        continue;
                    }

                    inSingleQuote = inDoubleQuote = inBacktick = false;
                }

                Append(c, content: true);
                continue;
            }

            // -- only opens a comment when whitespace follows it, so `a--b` stays arithmetic. The end
            // of a chunk counts as whitespace: callers feed lines, and a trailing -- ends the line.
            if (c == '-' && next == '-' && (i + 2 >= text.Length || char.IsWhiteSpace(text[i + 2])))
            {
                OpenComment();
                Skip(c);
                Skip(next);
                inLineComment = true;
                i++;
                continue;
            }

            if (c == '#')
            {
                OpenComment();
                Skip(c);
                inLineComment = true;
                continue;
            }

            if (c == '/' && next == '*')
            {
                OpenComment();
                Skip(c);
                Skip(next);
                inBlockComment = true;
                i++;
                continue;
            }

            if (c is '\'' or '"' or '`')
            {
                inSingleQuote = c == '\'';
                inDoubleQuote = c == '"';
                inBacktick = c == '`';
                Append(c, content: true);
                continue;
            }

            // MySQL-style \G terminator: ends the statement and requests vertical output.
            if (c == '\\' && next == 'G')
            {
                i++;
                at = fed + i;

                if (TryTake(vertical: true, out SqlStatement verticalStatement))
                    yield return verticalStatement;

                continue;
            }

            if (c == ';')
            {
                if (TryTake(vertical: false, out SqlStatement statement))
                    yield return statement;

                continue;
            }

            if (c == '(')
                parenDepth++;
            else if (c == ')' && parenDepth > 0)
                parenDepth--;

            Append(c, content: true);
        }

        fed += text.Length;
    }

    /// <summary>Yields a final statement left unterminated at the end of the input, if any.</summary>
    public IEnumerable<SqlStatement> Flush()
    {
        at = Math.Max(0, fed - 1);

        if (TryTake(vertical: false, out SqlStatement statement))
            yield return statement;
    }

    private void Append(char c, bool content)
    {
        current.Append(c);

        if (content && !char.IsWhiteSpace(c))
        {
            if (!hasContent)
            {
                statementLine = line;
                statementStart = at;
                hasContent = true;
            }

            lastContentChar = c;
        }

        if (c == '\n')
            line++;
    }

    // Comment text is dropped rather than forwarded: servers disagree on which comment syntaxes they
    // accept (CamusDB rejects '#'), and a dump full of them shouldn't fail on that. Newlines survive
    // so the statement keeps its shape and the server's "line N, col M" still lands on the right row.
    private void Skip(char c)
    {
        if (c == '\n')
        {
            current.Append(c);
            line++;
        }
    }

    // A comment can sit between two tokens (a/*x*/b), so leave a separator where it was.
    private void OpenComment()
    {
        current.Append(' ');
    }

    private bool TryTake(bool vertical, out SqlStatement statement)
    {
        string sql = current.ToString().Trim();
        int firstLine = statementLine;
        bool any = hasContent;

        current.Clear();
        parenDepth = 0;
        hasContent = false;
        lastContentChar = '\0';
        statementLine = line;

        // Nothing but whitespace and comments: an empty ";;", or a trailing comment at end of file.
        if (!any)
        {
            statement = default;
            return false;
        }

        statement = new SqlStatement(sql, vertical, firstLine, Start: statementStart, End: at);
        return true;
    }
}
