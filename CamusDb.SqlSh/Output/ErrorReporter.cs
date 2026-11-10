
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Text;
using System.Text.RegularExpressions;

internal static class ErrorReporter
{
    // Reports a failed statement: the exception, the server's error code, a caret under the offending
    // token when the message carries a location, and — for the authorization codes — the action that
    // actually fixes it, since "insufficient privilege" alone doesn't say which grant is missing.
    internal static void WriteStatementError(Exception ex, string? sql)
    {
        string errorCode = ex is CamusException ce ? ce.Code : $"0x{ex.HResult:X8}";

        AnsiConsole.MarkupLine("[red]{0}[/] ([grey]{1}[/]): {2}", Markup.Escape(ex.GetType().Name), Markup.Escape(errorCode), Markup.Escape(ex.Message));
        WriteErrorCaret(sql, ex.Message);

        switch (errorCode)
        {
            case "CADB0516":
                AnsiConsole.MarkupLine("[grey58]Not authenticated. Start the shell with[/] [cyan]-u <user>[/] [grey58](and[/] [cyan]-p[/][grey58]), or re-run it if the token was revoked.[/]");
                break;

            case "CADB0517":
                AnsiConsole.MarkupLine("[grey58]Authenticated, but missing a privilege on some table the statement touches. Grant it with[/] [cyan]GRANT … ON db.* TO <user>[/][grey58], as a superuser.[/]");
                break;

            case "CADB0519":
                AnsiConsole.MarkupLine("[grey58]The server requires TLS for credential-bearing requests. Use an[/] [cyan]https://[/] [grey58]endpoint.[/]");
                break;

            // An inline function or inline trigger body that does not compile, or that uses a
            // refused type or member. Each finding names a line and a column of the body, not of
            // the statement.
            case "CADB0838" or "CADB0839":
                WriteBodyCarets(sql, ex.Message);
                break;
        }

        AnsiConsole.WriteLine();
    }

    // Startup connection failures are the one place where a raw stack trace helps nobody: the causes
    // are few and each has a concrete fix, so name the likely one.
    internal static void WriteConnectionError(Exception ex)
    {
        string code = ex is CamusException ce ? ce.Code : "";

        AnsiConsole.MarkupLine("[red]Connection failed[/]{0}: {1}\n",
            code.Length > 0 ? $" ([grey]{Markup.Escape(code)}[/])" : "",
            Markup.Escape(ex.Message));

        switch (code)
        {
            case "CADB0516":
                AnsiConsole.MarkupLine("[grey58]Authentication failed. Check[/] [cyan]-u[/][grey58]/[/][cyan]-p[/][grey58], or that the user exists on this server.[/]");
                break;

            case "CADB0518":
                AnsiConsole.MarkupLine("[grey58]Too many login attempts for this account; the server rate-limits logins per minute. Wait and retry.[/]");
                break;

            // Both ends raise this code. The driver refuses before the password leaves this process;
            // the server refuses on arrival. Each end is waived separately, so name both fixes.
            case "CADB0519":
                AnsiConsole.MarkupLine("[grey58]Credentials are refused over plaintext: the password and the bearer token would cross the network in the clear.[/]");
                AnsiConsole.MarkupLine("[grey58]Use an[/] [cyan]https://[/] [grey58]endpoint.[/]");
                AnsiConsole.MarkupLine("[grey58]When TLS terminates in front of the server, pass[/] [cyan]--allow-insecure-credentials[/] [grey58]here, and start the server with[/] [cyan]--require-tls-when-auth-enabled false[/][grey58].[/]");
                break;
        }
    }

    // Renders a Rust-compiler-style pointer under the offending line when the error
    // message carries a "(line N, col M)" location, e.g.:
    //
    //     create table x1 (id uuid primary key default(gen_uuid_v7()), name string(20) ...
    //                                                  ^
    private static void WriteErrorCaret(string? sql, string message)
    {
        if (string.IsNullOrEmpty(sql))
            return;

        Match m = Regex.Match(message, @"line\s+(\d+),\s*col\s+(\d+)", RegexOptions.IgnoreCase);
        if (!m.Success)
            return;

        if (!int.TryParse(m.Groups[1].Value, out int line) || !int.TryParse(m.Groups[2].Value, out int col))
            return;

        string[] lines = sql.Replace("\r\n", "\n").Split('\n');
        if (line < 1 || line > lines.Length)
            return;

        AnsiConsole.WriteLine();

        // Columns are 1-based.
        WriteCaret(lines[line - 1], col - 1, note: null);
    }

    // The most findings of one inline function that get a caret. The message names all of them.
    private const int MaxBodyCarets = 3;

    // A failed CREATE FUNCTION or CREATE TRIGGER … LANGUAGE csharp names each problem as
    // "(line,column): message", where line 1 is the text right after the opening $$. The positions are moved to the statement, so the caret
    // lands under the body text that the user typed:
    //
    //     Function 'slugify' does not compile: (2,12): CS0103: The name 'z' does not exist …
    //
    //       return z.Trim();
    //              ^ CS0103: The name 'z' does not exist in the current context
    private static void WriteBodyCarets(string? sql, string message)
    {
        if (string.IsNullOrEmpty(sql))
            return;

        Match head = Regex.Match(
            message,
            @"(?<kind>Function|Trigger) '(?<name>[^']+)' (?:does not compile|is refused): (?<findings>\(\d+,\d+\): .*)$",
            RegexOptions.Singleline);

        if (!head.Success)
            return;

        string? statement = FindCreateStatement(sql, head.Groups["kind"].Value, head.Groups["name"].Value);

        if (statement is null || FindDollarBody(statement) is not int bodyStart)
            return;

        string[] lines = statement.Split('\n');

        // The line and the column of the first character of the body, both 0-based.
        int bodyLine = statement.AsSpan(0, bodyStart).Count('\n');
        int bodyColumn = bodyStart - (statement.LastIndexOf('\n', Math.Max(0, bodyStart - 1)) + 1);

        MatchCollection findings = Regex.Matches(
            head.Groups["findings"].Value,
            @"\((?<line>\d+),(?<col>\d+)\): (?<text>.*?)(?=; \(\d+,\d+\): |$)",
            RegexOptions.Singleline);

        int written = 0;
        int lastTarget = -1;

        foreach (Match finding in findings)
        {
            if (written == MaxBodyCarets)
                break;

            if (!int.TryParse(finding.Groups["line"].Value, out int line) || !int.TryParse(finding.Groups["col"].Value, out int col) || line < 1)
                continue;

            int target = bodyLine + line - 1;

            if (target >= lines.Length)
                continue;

            int caret = (line == 1 ? bodyColumn : 0) + col - 1;

            if (written == 0)
                AnsiConsole.WriteLine();

            // Two findings on one line share the source line, and each gets its own caret line.
            if (target != lastTarget)
                AnsiConsole.WriteLine("  " + lines[target]);

            WriteCaretLine(lines[target], caret, finding.Groups["text"].Value);
            lastTarget = target;
            written++;
        }
    }

    // The statement of `sql` that creates the function or the trigger `name`, with its line breaks
    // as "\n". `kind` is "Function" or "Trigger", as the message names it. The scanner splits the
    // text the way the shell sent it. Its comments are blank, but every line keeps its place, and
    // the body is raw text.
    private static string? FindCreateStatement(string sql, string kind, string name)
    {
        string pattern = kind == "Trigger"
            ? @"^create\s+trigger\s+`?(?<name>[A-Za-z_][A-Za-z0-9_]*)`?"
            : @"^create\s+(?:or\s+replace\s+)?function\s+`?(?<name>[A-Za-z_][A-Za-z0-9_]*)`?";

        foreach ((string statement, bool _) in SqlKind.EscapeStringIntoLines(sql.Replace("\r\n", "\n")))
        {
            Match match = Regex.Match(statement, pattern, RegexOptions.IgnoreCase);

            if (match.Success && string.Equals(match.Groups["name"].Value, name, StringComparison.OrdinalIgnoreCase))
                return statement;
        }

        return null;
    }

    // The offset of the first character after the opening dollar quote of a statement, or null when
    // it has none. A '$' inside a string or a quoted identifier opens no quote.
    private static int? FindDollarBody(string statement)
    {
        char inside = '\0';

        for (int i = 0; i < statement.Length; i++)
        {
            char c = statement[i];

            if (inside != '\0')
            {
                if (c == '\\' && inside != '`')
                    i++;
                else if (c == inside && i + 1 < statement.Length && statement[i + 1] == inside)
                    i++;
                else if (c == inside)
                    inside = '\0';

                continue;
            }

            if (c is '\'' or '"' or '`')
                inside = c;
            else if (c == '$' && SqlScanner.DollarQuoteAt(statement, i) is { } quote)
                return i + quote.Length;
        }

        return null;
    }

    // Writes a source line with a caret under the 0-based column `caret`, and a note after the caret.
    private static void WriteCaret(string source, int caret, string? note)
    {
        AnsiConsole.WriteLine("  " + source);
        WriteCaretLine(source, caret, note);
    }

    // Writes the caret line for a source line that is already written.
    private static void WriteCaretLine(string source, int caret, string? note)
    {
        // Clamp into the line so a caret always renders.
        caret = Math.Clamp(caret, 0, source.Length);

        // Preserve tabs in the padding so the caret stays aligned with the source.
        StringBuilder pad = new();
        for (int i = 0; i < caret; i++)
            pad.Append(source[i] == '\t' ? '\t' : ' ');

        if (string.IsNullOrEmpty(note))
            AnsiConsole.MarkupLine("  [red]{0}^[/]", pad.ToString());
        else
            AnsiConsole.MarkupLine("  [red]{0}^[/] [grey58]{1}[/]", pad.ToString(), Markup.Escape(note));
    }
}
