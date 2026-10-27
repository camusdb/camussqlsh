
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using static SqlKind;

// Reads SQL text through SqlScanner: statements out of a script, and whether what the prompt holds so
// far is a whole statement.
internal static class SqlScript
{
    // Streams statements out of a reader one line at a time. Feeding whole lines keeps the scanner's
    // two-character lookahead (`--`, `/*`, `\G`, doubled quotes) inside a single call, since none of
    // those tokens can straddle a line break.
    internal static async IAsyncEnumerable<SqlStatement> ReadStatements(TextReader reader)
    {
        SqlScanner scanner = new();

        // One folder per script so quote state survives across lines: a string literal spanning
        // lines keeps its interior characters unfenced from folding.
        SmartQuoteFolder folder = new();

        while (true)
        {
            string? line = await reader.ReadLineAsync();

            // Fold smart quotes per line so -f files and `source` benefit the way the prompt does.
            IEnumerable<SqlStatement> statements = line is null
                ? scanner.Flush()
                : scanner.Feed(folder.Fold(line) + "\n");

            foreach (SqlStatement statement in statements)
                yield return statement;

            if (line is null)
                yield break;
        }
    }

    // True while what's been typed so far can't stand on its own, so the prompt should keep reading.
    // Shares the splitter's scanner: whether a ';' terminates a statement and whether a statement is
    // finished are the same question, and answering them differently is how a ';' inside a comment or a
    // `quoted identifier` ends up confusing the shell.
    internal static bool IsIncomplete(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return false;

        SqlScanner scanner = new();

        // Only what dangles after the last ';' matters here — statements it completed are already whole.
        foreach (SqlStatement _ in scanner.Feed(input))
        {
        }

        if (scanner.InsideQuotes || scanner.InsideBlockComment || scanner.ParenDepth > 0)
            return true;

        return scanner.HasPendingContent && scanner.LastContentChar == ',';
    }
}
