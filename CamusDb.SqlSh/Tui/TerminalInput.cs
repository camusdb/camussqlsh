/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text;

namespace CamusDb.SqlSh.Tui;

/// <summary>One input event: a key, or the text of a bracketed paste.</summary>
internal readonly record struct TerminalEvent(ConsoleKeyInfo Key, string? Paste)
{
    public bool IsPaste => Paste is not null;
}

/// <summary>
/// Reads keys from the terminal and decodes the escape sequences that .NET does not decode.
///
/// <para>Console.ReadKey cannot tell Shift+Enter from Enter: a terminal sends a bare CR for both,
/// and .NET reports Key=Enter, Mods=None. xterm's modifyOtherKeys mode fixes that by encoding a
/// modified key as CSI 27 ; &lt;mods&gt; ; &lt;code&gt; ~. .NET does not parse that form, so it
/// arrives as Escape followed by the raw bytes, and the decoder turns it back into a key. The decoder
/// also reads the CSI &lt;code&gt; ; &lt;mods&gt; u form of the Kitty keyboard protocol.</para>
///
/// <para>The shell does not ask for the Kitty protocol. With it on, Ghostty and Kitty send F1, F2
/// and F4 as CSI P, CSI Q and CSI S, and .NET reads those with the SCO key table as F4, F5 and F7.
/// A real F5 also arrives as F5, so the shell cannot correct the key after .NET reads it. Thus F1
/// refreshed the catalog and F2 ran the statement. modifyOtherKeys keeps the usual function key
/// codes.</para>
///
/// <para>Without bracketed paste, a paste arrives as plain keys: a pasted Tab moved the focus to the
/// next pane, and the rest of the paste went into the wrong pane. With it, the terminal wraps the
/// paste in ESC[200~ … ESC[201~, and this class collects the text between them.</para>
/// </summary>
internal sealed class TerminalInput
{
    private bool _pasting;
    private readonly StringBuilder _paste = new();

    // modifyOtherKeys level 2 reports every modified key, Shift+Enter included. A terminal that does
    // not understand the request ignores it, and Shift+Enter stays a plain Enter there. CTRL+R and F5
    // work everywhere either way. CSI > 4 m puts modifyOtherKeys back to the terminal's default.
    public static void EnableModes()
    {
        Console.Write("\u001b[>4;2m");
        Console.Write("\u001b[?2004h");
    }

    public static void DisableModes()
    {
        Console.Write("\u001b[?2004l");
        Console.Write("\u001b[>4m");
    }

    /// <summary>
    /// Reads the next event. Null when the bytes read were a sequence with no key of its own, or a key
    /// inside a paste that is not over yet.
    /// </summary>
    public TerminalEvent? Read()
    {
        ConsoleKeyInfo? key = NextKey(out string? pasted);

        if (pasted is not null)
            return new TerminalEvent(default, pasted);

        if (key is not { } k)
            return null;

        if (_pasting)
        {
            TakePaste(k);
            return null;
        }

        return new TerminalEvent(k, null);
    }

    // A Mac keyboard needs Fn for Home, End, PageUp and PageDown, so each has a second key that does
    // not. Ctrl+A and Ctrl+E are the readline keys for the start and the end of a line. Option+Left
    // and Option+Right arrive as ESC b and ESC f from most Mac terminals, and are the word moves
    // there. Option+Up and Option+Down page.
    public static ConsoleKeyInfo Translate(ConsoleKeyInfo k)
    {
        bool ctrl = (k.Modifiers & ConsoleModifiers.Control) != 0;
        bool alt = (k.Modifiers & ConsoleModifiers.Alt) != 0;

        return k.Key switch
        {
            ConsoleKey.A when ctrl => new ConsoleKeyInfo('\0', ConsoleKey.Home, false, false, false),
            ConsoleKey.E when ctrl => new ConsoleKeyInfo('\0', ConsoleKey.End, false, false, false),
            ConsoleKey.B when alt => new ConsoleKeyInfo('\0', ConsoleKey.LeftArrow, false, true, false),
            ConsoleKey.F when alt => new ConsoleKeyInfo('\0', ConsoleKey.RightArrow, false, true, false),
            ConsoleKey.UpArrow when alt => new ConsoleKeyInfo('\0', ConsoleKey.PageUp, false, false, false),
            ConsoleKey.DownArrow when alt => new ConsoleKeyInfo('\0', ConsoleKey.PageDown, false, false, false),
            _ => k,
        };
    }

    // A terminal writes an escape sequence in one burst, but the burst can arrive in two reads.
    // A short wait tells a sequence that is still on its way from a lone Escape key.
    private static bool MoreInput()
    {
        for (int i = 0; i < 25; i++)
        {
            if (Console.KeyAvailable)
                return true;

            Thread.Sleep(1);
        }

        return Console.KeyAvailable;
    }

    private ConsoleKeyInfo? NextKey(out string? pasted)
    {
        pasted = null;
        ConsoleKeyInfo k = Console.ReadKey(intercept: true);

        // A lone Escape has nothing behind it, so it stays an Escape.
        if (k.Key != ConsoleKey.Escape || !MoreInput())
            return k;

        char next = Console.ReadKey(intercept: true).KeyChar;

        // ESC O <letter> is SS3, the form of F1 to F4 and of the arrows in application mode. .NET reads
        // it by itself only when the whole sequence is in one read, so it can arrive here in parts.
        if (next == 'O' && MoreInput())
        {
            char letter = Console.ReadKey(intercept: true).KeyChar;
            return SpecialKey(letter, 1);
        }

        // ESC followed by anything other than '[' is the terminal's way of writing Alt+key.
        if (next != '[')
            return new ConsoleKeyInfo(next, KeyOf(next), false, true, false);

        StringBuilder body = new();
        char final = '\0';

        // A CSI sequence runs until a byte in the 0x40-0x7E range closes it.
        while (MoreInput())
        {
            char c = Console.ReadKey(intercept: true).KeyChar;

            if (c >= '\u0040' && c <= '\u007e')
            {
                final = c;
                break;
            }

            body.Append(c);
        }

        return Decode(body.ToString(), final, out pasted);
    }

    private ConsoleKeyInfo? Decode(string body, char final, out string? pasted)
    {
        pasted = null;

        // The bracketed-paste markers. They carry no key, only the start and the end of a paste.
        if (final == '~' && body == "200")
        {
            _pasting = true;
            _paste.Clear();
            return null;
        }

        if (final == '~' && body == "201")
        {
            _pasting = false;
            pasted = EndPaste();
            return null;
        }

        string[] parts = body.Split(';');

        // CSI [1 [; <mods>]] X — an arrow, Home, End or F1 to F4, with or without modifiers, in the
        // xterm form. A Mac terminal sends Option+Up as CSI 1;3A, for example. .NET reads most of these
        // by itself, and they arrive here only when the sequence comes in two reads.
        if (final is 'A' or 'B' or 'C' or 'D' or 'H' or 'F' or 'P' or 'Q' or 'R' or 'S'
            && (body.Length == 0 || (parts.Length is 1 or 2 && parts[0] == "1")))
        {
            int letterMods = parts.Length == 2 && int.TryParse(parts[1], out int m1) ? m1 : 1;
            return SpecialKey(final, letterMods);
        }

        // CSI <number> [; <mods>] ~ — the keys of the editing block and the function keys: Insert,
        // Delete, PageUp and PageDown, and F1 to F12. Ctrl+PageUp and Ctrl+PageDown change tabs,
        // where the terminal sends them.
        if (final == '~' && parts.Length is 1 or 2 && int.TryParse(parts[0], out int number)
            && TildeKey(number) is { } tildeKey)
        {
            int tildeMods = parts.Length == 2 && int.TryParse(parts[1], out int m2) ? m2 : 1;
            return WithModifiers(tildeKey, tildeMods);
        }

        int code;
        int mods;

        if (final == 'u')
        {
            // CSI <code> ; <mods> u — the Kitty form.
            if (!int.TryParse(parts[0], out code))
                return null;

            mods = parts.Length > 1 && int.TryParse(parts[1], out int m) ? m : 1;
        }
        else if (final == '~' && parts.Length == 3 && parts[0] == "27")
        {
            // CSI 27 ; <mods> ; <code> ~ — the older xterm modifyOtherKeys form.
            if (!int.TryParse(parts[2], out code) || !int.TryParse(parts[1], out mods))
                return null;
        }
        else
        {
            // Some other sequence. Swallow it rather than reporting a key nobody pressed.
            return null;
        }

        // The modifier field is a bitmask offset by one: 1 shift, 2 alt, 4 control.
        int bits = Math.Max(0, mods - 1);

        char ch = code is >= 32 and < 0x110000 ? (char)code : '\0';

        return new ConsoleKeyInfo(
            ch,
            KeyOf(code),
            (bits & 1) != 0,
            (bits & 2) != 0,
            (bits & 4) != 0);
    }

    // The key that a letter names at the end of an SS3 or a CSI sequence. Null for a letter that names
    // no key here.
    private static ConsoleKeyInfo? SpecialKey(char letter, int mods)
    {
        ConsoleKey? key = letter switch
        {
            'A' => ConsoleKey.UpArrow,
            'B' => ConsoleKey.DownArrow,
            'C' => ConsoleKey.RightArrow,
            'D' => ConsoleKey.LeftArrow,
            'H' => ConsoleKey.Home,
            'F' => ConsoleKey.End,
            'P' => ConsoleKey.F1,
            'Q' => ConsoleKey.F2,
            'R' => ConsoleKey.F3,
            'S' => ConsoleKey.F4,
            _ => null,
        };

        return key is { } k ? WithModifiers(k, mods) : null;
    }

    // The key of CSI <number> ~, in the xterm and VT220 numbering.
    private static ConsoleKey? TildeKey(int number) => number switch
    {
        1 or 7 => ConsoleKey.Home,
        2 => ConsoleKey.Insert,
        3 => ConsoleKey.Delete,
        4 or 8 => ConsoleKey.End,
        5 => ConsoleKey.PageUp,
        6 => ConsoleKey.PageDown,
        11 => ConsoleKey.F1,
        12 => ConsoleKey.F2,
        13 => ConsoleKey.F3,
        14 => ConsoleKey.F4,
        15 => ConsoleKey.F5,
        17 => ConsoleKey.F6,
        18 => ConsoleKey.F7,
        19 => ConsoleKey.F8,
        20 => ConsoleKey.F9,
        21 => ConsoleKey.F10,
        23 => ConsoleKey.F11,
        24 => ConsoleKey.F12,
        _ => null,
    };

    // The modifier field is a bitmask offset by one: 1 shift, 2 alt, 4 control.
    private static ConsoleKeyInfo WithModifiers(ConsoleKey key, int mods)
    {
        int bits = Math.Max(0, mods - 1);
        return new ConsoleKeyInfo('\0', key, (bits & 1) != 0, (bits & 2) != 0, (bits & 4) != 0);
    }

    private static ConsoleKey KeyOf(int code) => code switch
    {
        8 or 127 => ConsoleKey.Backspace,
        9 => ConsoleKey.Tab,
        13 => ConsoleKey.Enter,
        27 => ConsoleKey.Escape,
        32 => ConsoleKey.Spacebar,
        >= '0' and <= '9' => ConsoleKey.D0 + (code - '0'),
        >= 'a' and <= 'z' => ConsoleKey.A + (code - 'a'),
        >= 'A' and <= 'Z' => ConsoleKey.A + (code - 'A'),
        _ => 0,
    };

    // Every key inside a paste is text, even Tab and Enter. A pasted newline arrives as CR (Enter),
    // as LF (Enter with Control), or as both, so the key character is kept as it is and EndPaste
    // folds the variants into one line break.
    private void TakePaste(ConsoleKeyInfo k)
    {
        char c = k.KeyChar != '\0'
            ? k.KeyChar
            : k.Key switch
            {
                ConsoleKey.Enter => '\r',
                ConsoleKey.Tab => '\t',
                _ => '\0',
            };

        if (c != '\0')
            _paste.Append(c);
    }

    // The pasted text with one kind of line break. Null when nothing that is SQL text is left.
    private string? EndPaste()
    {
        // macOS apps can put Unicode line and paragraph separators on the clipboard.
        string text = _paste.ToString()
            .Replace("\r\n", "\n")
            .Replace('\r', '\n')
            .Replace('\u2028', '\n')
            .Replace('\u2029', '\n');

        _paste.Clear();

        // Other control characters have no place in SQL text and would break the drawing.
        text = new string(text.Where(c => c is '\n' or '\t' || !char.IsControl(c)).ToArray());

        return text.Length == 0 ? null : text;
    }
}
