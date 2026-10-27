
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

        string source = lines[line - 1];

        // Columns are 1-based; clamp into the line so a caret always renders.
        int caret = Math.Clamp(col - 1, 0, source.Length);

        // Preserve tabs in the padding so the caret stays aligned with the source.
        StringBuilder pad = new();
        for (int i = 0; i < caret; i++)
            pad.Append(source[i] == '\t' ? '\t' : ' ');

        AnsiConsole.WriteLine();
        AnsiConsole.WriteLine("  " + source);
        AnsiConsole.MarkupLine("  [red]{0}^[/]", pad.ToString());
    }
}
