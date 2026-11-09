/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

// What the line lexer is inside of at a point in the text. A string, a block comment or a dollar
// quote can continue on the next line, so this state carries from one line to the next. Dollar is
// the text of a $$ … $$ dollar quote, the C# body of CREATE FUNCTION.
internal enum SqlLexMode { Code, Single, Double, Backtick, Block, Dollar }

internal enum SqlTokenKind { Word, Plain, String, Identifier, Comment }

internal readonly record struct SqlToken(int Start, int Length, SqlTokenKind Kind);

// The state at the end of a line. DollarQuote is the quote that opened the dollar-quoted text
// ($$ or $tag$), because only the same quote ends it. The default is plain code.
internal record struct SqlLexState(SqlLexMode Mode, string? DollarQuote);

// Splits the lines of the editors into tokens to color. The prompt editor and the full-screen
// editor use it, so a statement has the same colors in both.
internal static class SqlLineLexer
{
    // Splits one line into tokens. The rules are those of SqlScanner, which splits the statements
    // that run: a backslash escapes a character in a string but not in a `quoted identifier`, a
    // doubled delimiter is a literal one, -- opens a comment only before white space or the end of
    // the line, and a $$ or $tag$ quote opens raw text that only the same quote ends. A shell
    // command such as \d or /plan at the start of the line is one word, so it keeps its color.
    // `state` comes in as the state at the start of the line and goes out as the state at its end.
    internal static List<SqlToken> Tokenize(string text, ref SqlLexState state)
    {
        List<SqlToken> tokens = new();
        int n = text.Length;
        int i = 0;

        while (i < n)
        {
            int start = i;

            if (state.Mode == SqlLexMode.Code)
            {
                char c = text[i];
                char next = i + 1 < n ? text[i + 1] : '\0';

                if ((c == '-' && next == '-' && (i + 2 >= n || char.IsWhiteSpace(text[i + 2]))) ||
                    (c == '#' && SqlScanner.IsHashComment(text, i)))
                {
                    tokens.Add(new SqlToken(i, n - i, SqlTokenKind.Comment));
                    break;
                }

                if (c == '/' && next == '*')
                {
                    state.Mode = SqlLexMode.Block;
                    i += 2;
                }
                else if (c is '\'' or '"' or '`')
                {
                    state.Mode = c == '\'' ? SqlLexMode.Single : c == '"' ? SqlLexMode.Double : SqlLexMode.Backtick;
                    i++;
                }
                else if (c == '$' && SqlScanner.DollarQuoteAt(text, i) is { } quote)
                {
                    state = new SqlLexState(SqlLexMode.Dollar, quote);
                    i += quote.Length;
                }
                else if (c is '\\' or '/' && char.IsLetter(next) && string.IsNullOrWhiteSpace(text[..i]))
                {
                    i++;

                    while (i < n && IsWordChar(text[i]))
                        i++;

                    tokens.Add(new SqlToken(start, i - start, SqlTokenKind.Word));
                    continue;
                }
                else if (IsWordChar(c))
                {
                    while (i < n && IsWordChar(text[i]))
                        i++;

                    tokens.Add(new SqlToken(start, i - start, SqlTokenKind.Word));
                    continue;
                }
                else
                {
                    tokens.Add(new SqlToken(start, 1, SqlTokenKind.Plain));
                    i++;
                    continue;
                }
            }

            // A comment or a quoted text is open here. It opened on this line, or it continues from
            // the line before.
            SqlTokenKind kind = state.Mode switch
            {
                SqlLexMode.Block => SqlTokenKind.Comment,
                SqlLexMode.Backtick => SqlTokenKind.Identifier,
                _ => SqlTokenKind.String,
            };

            if (state.Mode == SqlLexMode.Dollar)
            {
                string quote = state.DollarQuote!;
                int end = text.IndexOf(quote, i, StringComparison.Ordinal);

                if (end < 0)
                {
                    i = n;
                }
                else
                {
                    i = end + quote.Length;
                    state = default;
                }
            }
            else if (state.Mode == SqlLexMode.Block)
            {
                int end = text.IndexOf("*/", i, StringComparison.Ordinal);

                if (end < 0)
                {
                    i = n;
                }
                else
                {
                    i = end + 2;
                    state = default;
                }
            }
            else
            {
                char quote = state.Mode == SqlLexMode.Single ? '\'' : state.Mode == SqlLexMode.Double ? '"' : '`';

                while (i < n)
                {
                    char c = text[i];

                    if (c == '\\' && quote != '`' && i + 1 < n)
                    {
                        i += 2;
                        continue;
                    }

                    if (c == quote)
                    {
                        if (i + 1 < n && text[i + 1] == quote)
                        {
                            i += 2;
                            continue;
                        }

                        i++;
                        state = default;
                        break;
                    }

                    i++;
                }
            }

            tokens.Add(new SqlToken(start, i - start, kind));
        }

        return tokens;
    }

    private static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c == '_';
}
