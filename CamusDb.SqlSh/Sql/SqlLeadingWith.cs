/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.RegularExpressions;

/// <summary>
/// Reads past the <c>WITH</c> clause at the start of a statement. The server writes each common table
/// expression (CTE) out at parse time, so the statement after the clause is the one that it runs:
/// <c>WITH s AS (…) SELECT …</c> is a query, and <c>WITH s AS (…) UPDATE …</c> is a write. The shell
/// chooses how to run a statement from its first word, so it reads that word after the clause.
///
/// <para>The scan reads <c>WITH [RECURSIVE] name [(columns)] AS [[NOT] MATERIALIZED] (query) [, …]</c>,
/// as CamusDB.Client does. When the text does not have that form, the scan returns the text from the
/// <c>WITH</c> keyword, and the server reports the syntax error.</para>
/// </summary>
internal static class SqlLeadingWith
{
    /// <summary>
    /// The statement after a leading <c>WITH</c> clause, with leading white space and comments removed.
    /// A statement with no <c>WITH</c> clause comes back with leading white space and comments removed only.
    /// </summary>
    internal static string Skip(string sql)
    {
        int start = SkipTrivia(sql, 0);
        if (!IsKeywordAt(sql, start, "with", out int i))
            return sql[start..];

        i = SkipTrivia(sql, i);
        if (IsKeywordAt(sql, i, "recursive", out int next))
            i = SkipTrivia(sql, next);

        while (true)
        {
            if (!TrySkipName(sql, ref i))
                return sql[start..];

            i = SkipTrivia(sql, i);
            if (i < sql.Length && sql[i] == '(')
            {
                i = SkipParenthesized(sql, i);
                if (i < 0)
                    return sql[start..];

                i = SkipTrivia(sql, i);
            }

            if (!IsKeywordAt(sql, i, "as", out i))
                return sql[start..];

            i = SkipTrivia(sql, i);
            if (IsKeywordAt(sql, i, "not", out next))
                i = SkipTrivia(sql, next);
            if (IsKeywordAt(sql, i, "materialized", out next))
                i = SkipTrivia(sql, next);

            if (i >= sql.Length || sql[i] != '(')
                return sql[start..];

            i = SkipParenthesized(sql, i);
            if (i < 0)
                return sql[start..];

            i = SkipTrivia(sql, i);
            if (i < sql.Length && sql[i] == ',')
            {
                i = SkipTrivia(sql, i + 1);
                continue;
            }

            return sql[i..];
        }
    }

    // A CTE definition: the WITH or the comma before it, the name, an optional column list, AS, and an
    // optional [NOT] MATERIALIZED before the parenthesis. The completion reads the names from the
    // statement typed so far, so a regular expression is enough: a wrong name costs one extra candidate.
    private static readonly Regex Definition = new(
        @"(?:\bwith(?:\s+recursive)?|,)\s+(?<name>`[^`]+`|[A-Za-z_][A-Za-z0-9_]*)\s*(?:\([^()]*\)\s*)?as\s+(?:not\s+)?(?:materialized\s*)?\(",
        RegexOptions.IgnoreCase | RegexOptions.CultureInvariant | RegexOptions.Compiled);

    /// <summary>
    /// The CTE names that <paramref name="statement"/> defines, as written (a name in backticks keeps them).
    /// A nested <c>WITH</c> counts too. Empty when the statement has no <c>WITH</c>.
    /// </summary>
    internal static string[] DefinedNames(string statement)
    {
        if (statement.IndexOf("with", StringComparison.OrdinalIgnoreCase) < 0)
            return [];

        return Definition.Matches(statement)
                         .Select(m => m.Groups["name"].Value)
                         .Distinct(StringComparer.OrdinalIgnoreCase)
                         .ToArray();
    }

    // The index of the first character at or after i that is not white space and not in a comment.
    private static int SkipTrivia(string text, int i)
    {
        while (i < text.Length)
        {
            char c = text[i];
            if (char.IsWhiteSpace(c))
            {
                i++;
            }
            else if (c == '-' && i + 1 < text.Length && text[i + 1] == '-')
            {
                int end = text.IndexOf('\n', i);
                i = end < 0 ? text.Length : end + 1;
            }
            else if (c == '/' && i + 1 < text.Length && text[i + 1] == '*')
            {
                int end = text.IndexOf("*/", i + 2, StringComparison.Ordinal);
                i = end < 0 ? text.Length : end + 2;
            }
            else
            {
                break;
            }
        }

        return i;
    }

    // Whether the keyword starts at i as a whole word. `end` is the index after it.
    private static bool IsKeywordAt(string text, int i, string keyword, out int end)
    {
        end = i + keyword.Length;
        return end <= text.Length &&
               string.Compare(text, i, keyword, 0, keyword.Length, StringComparison.OrdinalIgnoreCase) == 0 &&
               (end == text.Length || !IsNameChar(text[end]));
    }

    // Moves past a plain name or a name in backticks. False when no name starts at i.
    private static bool TrySkipName(string text, ref int i)
    {
        if (i >= text.Length)
            return false;

        if (text[i] == '`')
        {
            i = SkipDelimited(text, i, '`');
            return true;
        }

        if (!IsNameChar(text[i]) || char.IsAsciiDigit(text[i]))
            return false;

        while (i < text.Length && IsNameChar(text[i]))
            i++;

        return true;
    }

    private static bool IsNameChar(char c) => char.IsLetterOrDigit(c) || c == '_';

    // The index after the ')' that closes the '(' at `open`, or -1 when the text ends first. Parentheses
    // in literals, delimited names, dollar-quoted text and comments do not count.
    private static int SkipParenthesized(string text, int open)
    {
        int depth = 0;
        int i = open;
        while (i < text.Length)
        {
            char c = text[i];
            if (c is '\'' or '"' or '`')
            {
                i = SkipDelimited(text, i, c);
                continue;
            }

            if (c == '$' && DollarQuoteLength(text, i) > 0)
            {
                i = SkipDollarQuoted(text, i);
                continue;
            }

            if ((c == '-' && i + 1 < text.Length && text[i + 1] == '-') || (c == '/' && i + 1 < text.Length && text[i + 1] == '*'))
            {
                i = SkipTrivia(text, i);
                continue;
            }

            if (c == '(')
            {
                depth++;
            }
            else if (c == ')')
            {
                depth--;
                if (depth == 0)
                    return i + 1;
            }

            i++;
        }

        return -1;
    }

    // The index after the literal or delimited name that opens at `open`. A backslash escapes the next
    // character in a string, and a doubled quote stands for one quote. The text can end first.
    private static int SkipDelimited(string text, int open, char quote)
    {
        int j = open + 1;
        while (j < text.Length)
        {
            char ch = text[j];
            if (ch == '\\' && quote != '`')
            {
                j += 2;
                continue;
            }

            if (ch == quote)
            {
                if (j + 1 < text.Length && text[j + 1] == quote)
                {
                    j += 2;
                    continue;
                }

                return j + 1;
            }

            j++;
        }

        return text.Length;
    }

    // The length of the dollar quote that opens at `start`: $$, or $tag$. Zero when none opens there.
    private static int DollarQuoteLength(string text, int start)
    {
        if (start >= text.Length || text[start] != '$')
            return 0;

        int i = start + 1;
        if (i < text.Length && (char.IsAsciiLetter(text[i]) || text[i] == '_'))
        {
            while (i < text.Length && (char.IsAsciiLetterOrDigit(text[i]) || text[i] == '_'))
                i++;
        }

        return i < text.Length && text[i] == '$' ? i + 1 - start : 0;
    }

    // The index after the dollar-quoted text that opens at `start`. With no closing quote, the rest of
    // the text is in it.
    private static int SkipDollarQuoted(string text, int start)
    {
        int length = DollarQuoteLength(text, start);
        string quote = text.Substring(start, length);
        int close = text.IndexOf(quote, start + length, StringComparison.Ordinal);
        return close < 0 ? text.Length : close + length;
    }
}
