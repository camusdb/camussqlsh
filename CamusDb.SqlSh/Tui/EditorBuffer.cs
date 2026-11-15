/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RadLine;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// The text of one query editor and its cursor. Each line is a RadLine <see cref="LineBuffer"/>, which
/// is pure state with no console of its own. Every tab has one buffer.
/// </summary>
internal sealed class EditorBuffer
{
    public readonly List<LineBuffer> Lines = new() { new LineBuffer(string.Empty) };

    /// <summary>The line of the cursor.</summary>
    public int Row;

    /// <summary>The first line in the window, and the first column. The renderer moves them.</summary>
    public int Top;
    public int Left;

    /// <summary>Visible lines in the window, for PageUp and PageDown. The renderer sets it.</summary>
    public int Visible = 1;

    // A completion session. It survives repeated CTRL+N presses so they cycle, and any other key
    // ends it.
    private string[] _matches = [];
    private int _matchIndex;
    private int _matchStart;
    private int _matchRow = -1;

    public LineBuffer Current => Lines[Math.Min(Row, Lines.Count - 1)];

    public string Text => string.Join('\n', Lines.Select(l => l.Content));

    public bool IsEmpty => Lines.All(l => string.IsNullOrWhiteSpace(l.Content));

    /// <summary>The offset of the cursor in <see cref="Text"/>.</summary>
    public int CursorOffset => Lines.Take(Row).Sum(l => l.Length + 1) + Current.Position;

    /// <summary>The first line with text, for the title of a tab. Empty when the buffer has no text.</summary>
    public string FirstLine => Lines.Select(l => l.Content.Trim()).FirstOrDefault(l => l.Length > 0) ?? "";

    /// <summary>Replaces the text. The cursor goes to the end.</summary>
    public void SetText(string text)
    {
        Lines.Clear();

        foreach (string line in text.Replace("\r\n", "\n").Split('\n'))
            Lines.Add(new LineBuffer(line));

        Row = Lines.Count - 1;
        Lines[Row].Move(Lines[Row].Length);
        Top = 0;
        Left = 0;
        EndCompletion();
    }

    public void Clear()
    {
        Lines.Clear();
        Lines.Add(new LineBuffer(string.Empty));
        Row = 0;
        Top = 0;
        Left = 0;
        EndCompletion();
    }

    // LineBuffer.Insert writes at the cursor and leaves the cursor where it was, because RadLine's
    // InsertCommand moves it as a separate step. Typing needs both.
    public static void Type(LineBuffer line, string text)
    {
        line.Insert(text);
        line.Move(line.Position + text.Length);
    }

    /// <summary>Inserts text that can hold line breaks at the cursor. The cursor ends after the text.</summary>
    public void InsertText(string text)
    {
        EndCompletion();

        string[] parts = text.Split('\n');
        LineBuffer line = Current;

        if (parts.Length == 1)
        {
            Type(line, text);
            return;
        }

        // The text after the cursor moves to the end of the last inserted line.
        string tail = line.Content[line.Position..];
        if (tail.Length > 0)
            line.Clear(line.Position, tail.Length);

        Type(line, parts[0]);

        for (int i = 1; i < parts.Length; i++)
        {
            Row++;
            Lines.Insert(Row, new LineBuffer(parts[i]));
        }

        LineBuffer last = Lines[Row];
        int end = last.Length;
        last.Move(end);
        last.Insert(tail); // Insert leaves the cursor before the tail, which is where it belongs.
        last.Move(end);
    }

    private static bool IsWordChar(char c) => char.IsLetterOrDigit(c) || c == '_';

    // The start of the word before the cursor: back over any non-word characters, then back over
    // the word. Ctrl+Left, Option+Left and Option+Backspace all use it.
    private static int WordStartBefore(string text, int position)
    {
        int p = position;

        while (p > 0 && !IsWordChar(text[p - 1]))
            p--;

        while (p > 0 && IsWordChar(text[p - 1]))
            p--;

        return p;
    }

    private static int WordEndAfter(string text, int position)
    {
        int p = position;

        while (p < text.Length && !IsWordChar(text[p]))
            p++;

        while (p < text.Length && IsWordChar(text[p]))
            p++;

        return p;
    }

    // Moves to another line and keeps the column where the line is long enough.
    private void MoveToRow(int row, int? column = null)
    {
        int c = column ?? Current.Position;
        Row = Math.Clamp(row, 0, Lines.Count - 1);
        Lines[Row].Move(Math.Min(c, Lines[Row].Length));
    }

    public void HandleKey(ConsoleKeyInfo k)
    {
        LineBuffer line = Current;
        bool ctrl = (k.Modifiers & ConsoleModifiers.Control) != 0;
        bool alt = (k.Modifiers & ConsoleModifiers.Alt) != 0;

        // Option+Backspace on a Mac, Ctrl+W in readline: delete the word before the cursor.
        // At the start of a line it joins the line to the one above, as Backspace does.
        if ((k.Key == ConsoleKey.Backspace && (alt || ctrl)) || (ctrl && k.Key == ConsoleKey.W))
        {
            if (!line.AtBeginning)
            {
                int start = WordStartBefore(line.Content, line.Position);
                line.Clear(start, line.Position - start);
                line.Move(start);
                return;
            }

            k = new ConsoleKeyInfo('\0', ConsoleKey.Backspace, false, false, false);
        }

        // Word moves. At the edge of a line they go on to the next or the previous line.
        if (k.Key == ConsoleKey.LeftArrow && (alt || ctrl))
        {
            if (line.AtBeginning && Row > 0)
                MoveToRow(Row - 1, int.MaxValue);
            else
                line.Move(WordStartBefore(line.Content, line.Position));
            return;
        }

        if (k.Key == ConsoleKey.RightArrow && (alt || ctrl))
        {
            if (line.AtEnd && Row < Lines.Count - 1)
                MoveToRow(Row + 1, 0);
            else
                line.Move(WordEndAfter(line.Content, line.Position));
            return;
        }

        // Ctrl+Home and Ctrl+End go to the start and the end of the whole editor.
        if (ctrl && k.Key == ConsoleKey.Home) { MoveToRow(0, 0); return; }
        if (ctrl && k.Key == ConsoleKey.End) { MoveToRow(Lines.Count - 1, int.MaxValue); return; }

        switch (k.Key)
        {
            case ConsoleKey.PageUp: MoveToRow(Row - Visible); return;
            case ConsoleKey.PageDown: MoveToRow(Row + Visible); return;

            case ConsoleKey.Enter:
            {
                // Split the line at the cursor, the way any editor does.
                string tail = line.Content[line.Position..];
                if (tail.Length > 0)
                    line.Clear(line.Position, tail.Length);
                Lines.Insert(Row + 1, new LineBuffer(tail));
                Row++;
                Lines[Row].Move(0);
                return;
            }

            case ConsoleKey.Backspace:
                if (!line.AtBeginning)
                {
                    line.Clear(line.Position - 1, 1);
                    line.Move(line.Position - 1);
                }
                else if (Row > 0)
                {
                    // Joining lines: the cursor lands where the two meet.
                    LineBuffer above = Lines[Row - 1];
                    int join = above.Length;
                    above.Move(above.Length);
                    above.Insert(line.Content); // cursor stays at the join, which is what we want
                    Lines.RemoveAt(Row);
                    Row--;
                    Lines[Row].Move(join);
                }
                return;

            case ConsoleKey.Delete:
                if (!line.AtEnd)
                    line.Clear(line.Position, 1);
                else if (Row < Lines.Count - 1)
                {
                    int join = line.Length;
                    line.Insert(Lines[Row + 1].Content);
                    Lines.RemoveAt(Row + 1);
                    line.Move(join);
                }
                return;

            case ConsoleKey.LeftArrow: line.Move(Math.Max(0, line.Position - 1)); return;
            case ConsoleKey.RightArrow: line.Move(Math.Min(line.Length, line.Position + 1)); return;
            case ConsoleKey.Home: line.Move(0); return;
            case ConsoleKey.End: line.Move(line.Length); return;

            case ConsoleKey.UpArrow:
                if (Row > 0) MoveToRow(Row - 1);
                return;

            case ConsoleKey.DownArrow:
                if (Row < Lines.Count - 1) MoveToRow(Row + 1);
                return;
        }

        // A key with Control is never text. With modifyOtherKeys or the Kitty protocol, a terminal
        // sends CTRL+K as a sequence whose character is 'k', and that must not type a k.
        if (!ctrl && !char.IsControl(k.KeyChar))
            Type(line, k.KeyChar.ToString());
    }

    // ---------------------------------------------------------------- completion

    public void EndCompletion()
    {
        _matches = [];
        _matchRow = -1;
    }

    /// <summary>
    /// Puts the next (or the previous) completion candidate in place of the word at the cursor.
    /// Returns the status line to show.
    /// </summary>
    public string Complete(SqlCompletion completion, bool forward)
    {
        LineBuffer line = Current;

        // A repeated CTRL+N on the same word steps to the next candidate rather than recomputing
        // the list, which is what makes cycling work.
        if (_matches.Length > 0 && _matchRow == Row)
        {
            _matchIndex = (_matchIndex + (forward ? 1 : _matches.Length - 1)) % _matches.Length;
            ReplaceWord(line, _matchStart, _matches[_matchIndex]);
            return DescribeMatches();
        }

        int end = line.Position;
        int start = end;

        // A qualified target is one word, as it is at the prompt: DROP INDEX <table>.<index> and SHOW
        // RANGES FROM INDEX <table>@<index>, where the primary index is ~pk. Without '.', '@' and '~'
        // the word would be the index half alone, and SqlCompletion could not tell which table it is in.
        while (start > 0 && (char.IsLetterOrDigit(line.Content[start - 1]) || line.Content[start - 1] is '_' or '.' or '@' or '~'))
            start--;

        string word = line.Content[start..end];

        // The prefix is the whole statement up to the word, so SqlCompletion can tell a table
        // position from a keyword position exactly as it does at the prompt.
        string prefix = string.Join('\n', Lines.Take(Row).Select(l => l.Content).Append(line.Content[..start]));
        string suffix = line.Content[end..];

        IEnumerable<string>? all = completion.GetCompletions(prefix, word, suffix);

        // RadLine matches candidates with an ordinal StartsWith, so the same rule applies here.
        string[] matches = all is null
            ? []
            : word.Length == 0
                ? all.ToArray()
                : all.Where(c => c.StartsWith(word, StringComparison.OrdinalIgnoreCase)).ToArray();

        if (matches.Length == 0)
            return $"No completion for '{word}'";

        _matches = matches;
        _matchIndex = forward ? 0 : matches.Length - 1;
        _matchStart = start;
        _matchRow = Row;

        ReplaceWord(line, start, matches[_matchIndex]);
        return DescribeMatches();
    }

    private string DescribeMatches()
    {
        return _matches.Length == 1
            ? $"1 completion: {_matches[0]}"
            : $"{_matchIndex + 1}/{_matches.Length}: {string.Join(' ', _matches.Take(12))}"
              + (_matches.Length > 12 ? " …" : "");
    }

    private static void ReplaceWord(LineBuffer line, int start, string word)
    {
        if (line.Position > start)
            line.Clear(start, line.Position - start);

        line.Move(start);
        Type(line, word);
    }
}
