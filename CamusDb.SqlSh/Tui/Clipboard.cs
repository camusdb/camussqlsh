/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// The terminal owns the clipboard, so a copy goes through it: OSC 52 asks the terminal to put base64
/// text on the system clipboard. Ghostty, Kitty, WezTerm and iTerm2 support it. iTerm2 needs
/// "Applications in terminal may access clipboard" turned on. The shell cannot tell whether the
/// terminal did it, so the returned status says what was sent.
/// </summary>
internal static class Clipboard
{
    public static string Copy(string text, string what)
    {
        string payload = Convert.ToBase64String(Encoding.UTF8.GetBytes(text));
        Console.Write($"\u001b]52;c;{payload}\u0007");

        return $"Copied {what} to the clipboard ({text.Length} characters).";
    }
}
