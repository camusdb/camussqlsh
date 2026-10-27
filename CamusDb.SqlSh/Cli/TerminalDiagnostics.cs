
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RadLine;
using Spectre.Console;

// The two diagnostic modes that run instead of the shell: --diagnose-terminal and --debug-keys.
internal static class TerminalDiagnostics
{
    // Prints why the rich editor is on or off.
    internal static void WriteCapabilities(bool forceRich)
    {
        Capabilities caps = AnsiConsole.Console.Profile.Capabilities;
        bool supported = LineEditor.IsSupported(AnsiConsole.Console);
        AnsiConsole.MarkupLine(supported
            ? "[green]Rich editor supported.[/] Terminal capabilities:"
            : "[yellow]Rich editor disabled[/] (falling back to plain prompt). Terminal capabilities:");
        AnsiConsole.MarkupLine("  IsTerminal  : {0}", AnsiConsole.Console.Profile.Out.IsTerminal);
        AnsiConsole.MarkupLine("  Ansi        : {0}", caps.Ansi);
        AnsiConsole.MarkupLine("  Interactive : {0}", caps.Interactive);
        AnsiConsole.MarkupLine("  TERM        : {0}", Markup.Escape(Environment.GetEnvironmentVariable("TERM") ?? "(unset)"));
        AnsiConsole.MarkupLine("  NO_COLOR    : {0}", Markup.Escape(Environment.GetEnvironmentVariable("NO_COLOR") ?? "(unset)"));
        AnsiConsole.MarkupLine("  ForceRich   : {0}", forceRich);

        if (!supported)
        {
            AnsiConsole.WriteLine();
            AnsiConsole.MarkupLine("[grey]If your terminal supports ANSI (e.g. Rio), force it with[/] [cyan]--force-rich[/] [grey]or[/] [cyan]CAMUS_FORCE_RICH=1[/][grey],[/]");
            AnsiConsole.MarkupLine("[grey]or set[/] [cyan]TERM=xterm-256color[/][grey].[/]");
        }
    }

    // Prints each key the terminal delivers, so a paste that the editor mishandles can be reported.
    internal static void EchoKeys()
    {
        Console.Write("\u001b[?2004h"); // enable bracketed paste, like the real editor does
        Console.WriteLine("Key diagnostic. Paste your multi-line SQL now. Press Ctrl+D to finish.\n");
        int n = 0;
        while (true)
        {
            ConsoleKeyInfo k = Console.ReadKey(intercept: true);
            if (k.Key == ConsoleKey.D && (k.Modifiers & ConsoleModifiers.Control) != 0)
                break;
            Console.WriteLine($"#{n++,-3} Key={k.Key}({(int)k.Key})  Char=0x{(int)k.KeyChar:X2}  Mods={k.Modifiers}");
        }
        Console.Write("\u001b[?2004l");
        Console.WriteLine("\nDone. Copy everything above and share it.");
    }
}
