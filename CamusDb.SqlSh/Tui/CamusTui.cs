/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using RadLine;
using Spectre.Console;
using Spectre.Console.Rendering;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using static SqlKind;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// A full-screen mode for the shell. Three panes share one Spectre <see cref="Layout"/>:
/// a catalog tree on the left, a query editor at the top right, and a result grid below it.
/// Spectre draws. This class owns the state, the focus ring and the key loop, because
/// Spectre has no input or focus model of its own.
///
/// One <see cref="LiveDisplay"/> region covers the whole screen. Every frame rebuilds the
/// layout from scratch, so a terminal resize needs no special handling. The loop polls
/// <see cref="Console.KeyAvailable"/> instead of blocking in ReadKey, so the screen stays
/// live while a query runs and ESC can cancel it.
///
/// Statements are classified by <see cref="SqlKind"/>, the same code the line-editor shell
/// uses, so a statement runs in the same place and against the same connection in both modes.
/// </summary>
internal sealed class CamusTui
{
    private enum Pane { Catalog, Editor, Results }

    private enum RowKind { Database, Table, Column, Note }

    // What the editor lexer is inside of at a point in the text. A string or a block comment
    // can continue on the next line, so this state carries from one line to the next.
    private enum Lex { Code, Single, Double, Backtick, Block }

    private enum TokenKind { Word, Plain, String, Identifier, Comment }

    // One entry in the messages area: the outcome of one statement, or an error from paging.
    private sealed record Message(DateTime At, string Sql, string Text, bool Error);

    // What one run of the editor reports. Each entry is one line of the status bar and one
    // message. An entry is an error when the statement failed.
    private sealed class RunLog
    {
        public readonly List<(string Text, bool Error)> Entries = new();

        public bool Failed => Entries.Any(e => e.Error);

        public void Add(string text) => Entries.Add((text, false));

        public void Fail(string text) => Entries.Add((text, true));
    }

    private const int MaxMessages = 500;

    // One row of the flattened catalog tree. The tree is stored flat because selection is an
    // index, and Spectre's Tree carries no selection state to move. A Note row is a placeholder,
    // such as "…" while a list loads. Its Table is empty when it sits under a database.
    private sealed record CatalogRow(RowKind Kind, string Database, string Table = "", string Name = "", string Type = "");

    private const int LimitRows = 500;

    // Rows fetched per trip to the reader once the first page is on screen.
    private const int PageRows = 200;

    // How close the visible window may come to the end of the buffer before the next page
    // is fetched. Large enough that a held-down arrow key does not outrun the fetch.
    private const int PageMargin = 60;

    private readonly Func<ColumnValue, string> _format;
    private readonly IHighlighter _highlighter;
    private readonly SqlCompletion? _completion;
    private readonly string _queryPath;

    // Markup tags for the tokens that the highlighter cannot colour one word at a time.
    private readonly string? _stringTag;
    private readonly string? _identifierTag;
    private readonly string? _commentTag;

    // The connection is replaced by `use`, so none of these are readonly.
    private CamusConnection _connection;
    private string _connectionString;
    // Null when the session has no current database.
    private string? _database;
    private CamusTransaction? _transaction;

    // The session that owns the terminal, so the prompt's SIGINT handler can hand the
    // terminal back. Null outside --tui.
    internal static CamusTui? Active { get; private set; }

    private Pane _focus = Pane.Editor;
    private bool _quit;
    private bool _help;

    // Set when a quit waits for the user to confirm the rollback of an open transaction.
    private bool _confirmQuit;

    // Set once ShutdownAsync starts, so the finally block and the SIGINT handler do not both run it.
    private int _shutDown;

    private bool _savedTreatControlC;

    // A bracketed paste collects here and goes into the editor in one piece when it ends.
    private bool _pasting;
    private readonly StringBuilder _paste = new();

    // Catalog. The databases come from SHOW DATABASES. A database's tables load the first time it
    // expands, and a table's columns load the first time the table expands, one DESCRIBE each.
    // Background loads write these while the draw loop reads them, so the maps are concurrent and
    // the database list is replaced whole, never changed in place.
    private IReadOnlyList<string> _databases = [];
    private readonly ConcurrentDictionary<string, List<string>> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, List<(string Name, string Type)>> _columns = new(StringComparer.OrdinalIgnoreCase);

    // The expanded nodes: a database by its name, a table by TableKey.
    private readonly ConcurrentDictionary<string, bool> _expanded = new(StringComparer.OrdinalIgnoreCase);
    private bool _refreshing;
    private int _catalogSel;
    private int _catalogTop;

    // Editor. Each line is a RadLine LineBuffer, which is pure state with no console of its own.
    private readonly List<LineBuffer> _lines = new() { new LineBuffer(string.Empty) };
    private int _row;
    private int _editorTop;
    private int _editorLeft;

    // A Tab-completion session. It survives repeated Tab presses so they cycle, and any
    // other key ends it.
    private string[] _matches = [];
    private int _matchIndex;
    private int _matchStart;
    private int _matchRow = -1;

    // Results. The cells hold plain text: markup escaping happens when a cell is drawn, after it
    // is clipped, so a clip never cuts an escaped "[[" in half. _selRow and _selCol are the
    // selected cell, and the window (_rowTop, _colLeft) follows it.
    private string[] _headers = [];
    private readonly List<string[]> _rows = new();
    private int _rowTop;
    private int _colLeft;
    private int _selRow;
    private int _selCol;
    private int _visibleRows = 1;

    // The value view shows the full text of the selected cell, or with _rowView the whole row,
    // one column under the other. It takes the place of the grid.
    private bool _valueView;
    private bool _rowView;
    private int _viewTop;
    private int _viewVisible = 1;

    // The messages area keeps the outcome of every statement, so a script's log is not lost
    // when the status line is too short for it. It takes the place of the grid too.
    private readonly List<Message> _messages = new();
    private bool _showMessages;
    private int _messageTop;
    private bool _messageFollow = true;
    private int _messageVisible = 1;

    // Visible rows of the editor and the catalog, for PageUp and PageDown.
    private int _editorVisible = 1;
    private int _catalogVisible = 1;
    private string _status = "Ready. CTRL+R runs the statement at the cursor, CTRL+G runs them all. F1 lists the keys.";
    private bool _statusIsError;
    private bool _running;
    private CancellationTokenSource? _cancel;

    // The open reader behind the grid. Rows arrive a page at a time as the user scrolls,
    // so a wide scan costs one page of memory rather than the whole result.
    private CamusCommand? _pageCommand;
    private CamusDataReader? _pageReader;
    private bool _pageDone = true;
    private bool _fetching;

    private bool _limitOn = true;

    // Bumped by background work (a DESCRIBE, a query, a page fetch) so the draw loop knows
    // to repaint even though no key was pressed.
    private int _revision;

    private CamusTui(
        CamusConnection connection,
        string connectionString,
        Func<ColumnValue, string> format,
        IHighlighter highlighter,
        SqlCompletion? completion,
        string queryPath)
    {
        _connection = connection;
        _connectionString = connectionString;
        _database = GetConnValue(connectionString, "Database") is { Length: > 0 } db ? db : null;
        _format = format;
        _highlighter = highlighter;
        _completion = completion;
        _queryPath = queryPath;

        // The shell's highlighter colours a whole string or comment by its regexes, and the
        // prompt gives it whole strings and comments. The editor lexer finds those tokens itself,
        // so it asks the highlighter once for the style of a minimal sample of each kind.
        // A string such as '--x' then stays a string, and does not become a comment.
        _stringTag = highlighter.Highlight("''") is { } str ? Describe(str) : null;
        _identifierTag = highlighter.Highlight("``") is { } ident ? Describe(ident) : null;
        _commentTag = highlighter.Highlight("--") is { } comment ? Describe(comment) : null;
    }

    internal static async Task RunAsync(
        CamusConnection connection,
        string connectionString,
        Func<ColumnValue, string> format,
        IHighlighter highlighter,
        SqlCompletion? completion,
        string queryPath)
    {
        CamusTui tui = new(connection, connectionString, format, highlighter, completion, queryPath);
        await tui.LoopAsync();
    }

    private async Task LoopAsync()
    {
        if (Console.IsInputRedirected)
        {
            AnsiConsole.MarkupLine("[red]--tui needs a terminal.[/] Standard input is redirected.");
            return;
        }

        LoadQuery();

        if (_database is not null)
            _expanded[_database] = true;

        await LoadCatalogAsync();
        SelectCatalogRow(r => r.Kind == RowKind.Database && IsCurrent(r.Database));

        Active = this;

        // Ctrl+C is a key here, not a signal. As a signal it reached the prompt's handler, which
        // ends the process at once: the finally block below never ran, so the terminal kept the
        // keyboard protocol, the cursor stayed hidden, and the TUI's transaction was not rolled back.
        _savedTreatControlC = Console.TreatControlCAsInput;
        Console.TreatControlCAsInput = true;

        AnsiConsole.Cursor.Hide();
        EnableKeyDisambiguation();
        EnableBracketedPaste();

        try
        {
            await AnsiConsole.Live(Build())
                .AutoClear(false)
                .Overflow(VerticalOverflow.Crop)
                .StartAsync(async ctx =>
                {
                    int drawn = -1;

                    while (!_quit)
                    {
                        if (Console.KeyAvailable)
                        {
                            if (NextKey() is { } key)
                            {
                                if (_pasting)
                                    TakePaste(key);
                                else
                                    OnKey(key);
                            }

                            _revision++;
                        }
                        else if (drawn != _revision)
                        {
                            drawn = _revision;
                            ctx.UpdateTarget(Build());
                        }
                        else
                        {
                            await Task.Delay(15);
                        }
                    }
                });
        }
        finally
        {
            await ShutdownAsync();
            Active = null;
        }
    }

    // Gives the terminal back and ends the session. It runs from the finally block of the loop,
    // and from the prompt's SIGINT handler when a SIGINT comes from outside the terminal.
    // The terminal is restored first, because the SIGINT handler gives this method a few
    // seconds only.
    internal async Task ShutdownAsync()
    {
        if (Interlocked.Exchange(ref _shutDown, 1) == 1)
            return;

        _cancel?.Cancel();
        SaveQuery();
        DisableBracketedPaste();
        DisableKeyDisambiguation();
        Console.TreatControlCAsInput = _savedTreatControlC;
        AnsiConsole.Cursor.Show();

        await ClosePageAsync();

        // An open transaction is the user's to finish, but leaving it open after the screen
        // is gone would strand locks with nothing left to release them.
        if (Interlocked.Exchange(ref _transaction, null) is { } transaction)
        {
            AnsiConsole.MarkupLine("[yellow]Rolling back the active transaction…[/]");

            try
            {
                await transaction.RollbackAsync();
            }
            catch (Exception ex)
            {
                AnsiConsole.MarkupLine("[red]{0}[/]", Markup.Escape(ex.Message));
            }
        }
    }

    // ---------------------------------------------------------------- keys

    // Console.ReadKey cannot tell Shift+Enter from Enter: a terminal sends a bare CR for both,
    // and .NET reports Key=Enter, Mods=None. The keyboard protocol that Kitty introduced fixes
    // that by encoding the key and its modifiers as CSI <code> ; <mods> u, and Ghostty, Kitty,
    // WezTerm and recent iTerm2 all speak it. .NET does not parse that form, so it arrives as
    // Escape followed by the raw bytes, and the decoder below turns it back into a key.
    //
    // Flag 1 asks only for disambiguation, which is the smallest request that separates
    // Shift+Enter from Enter. A terminal that does not understand the request ignores it, and
    // Shift+Enter stays a plain Enter there. F5 works everywhere either way.
    private static void EnableKeyDisambiguation() => Console.Write("\u001b[>1u");

    private static void DisableKeyDisambiguation() => Console.Write("\u001b[<u");

    // Without bracketed paste, a paste arrives as plain keys: a pasted Tab moved the focus to the
    // next pane, and the rest of the paste went into the wrong pane. With it, the terminal wraps
    // the paste in ESC[200~ … ESC[201~, and the decoder below collects the text between them.
    private static void EnableBracketedPaste() => Console.Write("\u001b[?2004h");

    private static void DisableBracketedPaste() => Console.Write("\u001b[?2004l");

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

    // Returns null when the bytes read were a sequence with no key of its own to report.
    private ConsoleKeyInfo? NextKey()
    {
        ConsoleKeyInfo k = Console.ReadKey(intercept: true);

        // A lone Escape has nothing behind it, so it stays an Escape.
        if (k.Key != ConsoleKey.Escape || !MoreInput())
            return k;

        char next = Console.ReadKey(intercept: true).KeyChar;

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

        return Decode(body.ToString(), final);
    }

    private ConsoleKeyInfo? Decode(string body, char final)
    {
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
            EndPaste();
            return null;
        }

        string[] parts = body.Split(';');

        // CSI 1 ; <mods> X — an arrow, Home or End with modifiers, in the xterm form. A Mac
        // terminal sends Option+Up as CSI 1;3A, for example.
        if (final is 'A' or 'B' or 'C' or 'D' or 'H' or 'F' && parts.Length == 2 && parts[0] == "1"
            && int.TryParse(parts[1], out int arrowMods))
        {
            int arrowBits = Math.Max(0, arrowMods - 1);

            ConsoleKey arrow = final switch
            {
                'A' => ConsoleKey.UpArrow,
                'B' => ConsoleKey.DownArrow,
                'C' => ConsoleKey.RightArrow,
                'D' => ConsoleKey.LeftArrow,
                'H' => ConsoleKey.Home,
                _ => ConsoleKey.End,
            };

            return new ConsoleKeyInfo('\0', arrow, (arrowBits & 1) != 0, (arrowBits & 2) != 0, (arrowBits & 4) != 0);
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

    // ---------------------------------------------------------------- input

    private void OnKey(ConsoleKeyInfo k)
    {
        bool ctrl = (k.Modifiers & ConsoleModifiers.Control) != 0;

        // A quit that waits for confirmation takes the next key. Only a second quit key confirms.
        // Any other key keeps the session and is not used for anything else, because the user
        // did not type it for the pane.
        if (_confirmQuit)
        {
            _confirmQuit = false;

            if (ctrl && (k.Key == ConsoleKey.Q || k.Key == ConsoleKey.C))
            {
                _quit = true;
            }
            else
            {
                _status = "Quit cancelled. The transaction is still open.";
                _statusIsError = false;
            }

            return;
        }

        // Completion is CTRL+N and CTRL+P rather than Tab. Tab has to stay the pane ring:
        // binding it to completion inside the editor left no forward way out of that pane.
        // Both are real control characters, so every terminal delivers them.
        bool completing = _focus == Pane.Editor && ctrl && (k.Key == ConsoleKey.N || k.Key == ConsoleKey.P);

        if (!completing)
            EndCompletion();

        if (completing)
        {
            Complete(k.Key == ConsoleKey.N);
            return;
        }

        // Shift+Enter runs, matching every SQL editor people arrive from. It reaches here only
        // on a terminal that speaks the disambiguating keyboard protocol; F5 is the fallback.
        // Enter with Control is left alone on purpose: that is how a pasted newline arrives,
        // so binding it to run would make a pasted script execute itself.
        if (k.Key == ConsoleKey.Enter && (k.Modifiers & ConsoleModifiers.Shift) != 0) { RunCurrent(); return; }

        // Ctrl+C stops a running query, the same as ESC. When nothing runs, it quits, as it
        // does at the prompt.
        if (ctrl && k.Key == ConsoleKey.C)
        {
            if (_running) { _cancel?.Cancel(); _status = "Cancelling…"; }
            else RequestQuit();
            return;
        }

        if (ctrl && k.Key == ConsoleKey.Q) { RequestQuit(); return; }
        if (k.Key == ConsoleKey.F1) { _help = !_help; return; }
        // Each run has a control key beside its function key. On a Mac keyboard a function key
        // needs Fn too, but a control key is a real control character that every terminal sends.
        if (k.Key == ConsoleKey.F5 || (ctrl && k.Key == ConsoleKey.R)) { RunCurrent(); return; }
        if (k.Key == ConsoleKey.F6 || (ctrl && k.Key == ConsoleKey.G)) { StartRun(Sql); return; }
        if (k.Key == ConsoleKey.F2) { _limitOn = !_limitOn; return; }
        if (k.Key == ConsoleKey.F4) { _ = RefreshFromKeyAsync(); return; }
        if (ctrl && k.Key == ConsoleKey.S) { SaveQuery(); _status = $"Saved to {_queryPath}"; _statusIsError = false; return; }

        // The editor reopens with the previous session's text, so there has to be a way to
        // empty it in one keystroke. CTRL+L is what a terminal user reaches for to clear, and
        // CTRL+U is the readline habit for the same thing.
        if (ctrl && (k.Key == ConsoleKey.L || k.Key == ConsoleKey.U)) { ClearEditor(); return; }

        if (k.Key == ConsoleKey.Escape)
        {
            if (_running) { _cancel?.Cancel(); _status = "Cancelling…"; }
            else if (_help) _help = false;
            else if (_valueView) _valueView = false;
            else if (_showMessages) _showMessages = false;
            return;
        }

        k = Translate(k);

        if (k.Key == ConsoleKey.Tab)
        {
            int step = (k.Modifiers & ConsoleModifiers.Shift) != 0 ? 2 : 1;
            _focus = (Pane)(((int)_focus + step) % 3);
            return;
        }

        switch (_focus)
        {
            case Pane.Catalog: CatalogKey(k); break;
            case Pane.Editor: EditorKey(k); break;
            case Pane.Results: ResultsKey(k); break;
        }
    }

    // A Mac keyboard needs Fn for Home, End, PageUp and PageDown, so each has a second key that
    // does not. Ctrl+A and Ctrl+E are the readline keys for the start and the end of a line.
    // Option+Left and Option+Right arrive as ESC b and ESC f from most Mac terminals, and are
    // the word moves there. Option+Up and Option+Down page.
    private static ConsoleKeyInfo Translate(ConsoleKeyInfo k)
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

    // Quitting rolls back an open transaction, which can lose work. So with a transaction open,
    // the first quit key only asks, and a second one confirms.
    private void RequestQuit()
    {
        if (_transaction is null)
        {
            _quit = true;
            return;
        }

        _confirmQuit = true;
        _help = false;
    }

    private void CatalogKey(ConsoleKeyInfo k)
    {
        List<CatalogRow> rows = FlatCatalog();
        if (rows.Count == 0)
            return;

        CatalogRow row = rows[Math.Min(_catalogSel, rows.Count - 1)];

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: _catalogSel = Math.Min(rows.Count - 1, _catalogSel + 1); break;
            case ConsoleKey.UpArrow: _catalogSel = Math.Max(0, _catalogSel - 1); break;
            case ConsoleKey.Home: _catalogSel = 0; break;
            case ConsoleKey.End: _catalogSel = rows.Count - 1; break;
            case ConsoleKey.PageDown: _catalogSel = Math.Min(rows.Count - 1, _catalogSel + _catalogVisible); break;
            case ConsoleKey.PageUp: _catalogSel = Math.Max(0, _catalogSel - _catalogVisible); break;

            case ConsoleKey.RightArrow:
                Expand(row);
                break;

            // Enter on another database switches to it, as `use` does. On every other row it
            // expands, as Right does.
            case ConsoleKey.Enter:
                if (row.Kind == RowKind.Database && !IsCurrent(row.Database))
                    StartRun($"use `{row.Database.Replace("`", "``")}`");
                else
                    Expand(row);
                break;

            case ConsoleKey.LeftArrow:
                Collapse(row);
                break;

            // Space drops the selected name into the editor at the cursor, which is the
            // whole point of having the catalog next to the editor.
            case ConsoleKey.Spacebar:
                string? name = row.Kind switch
                {
                    RowKind.Database => row.Database,
                    RowKind.Table => row.Table,
                    RowKind.Column => row.Name,
                    _ => null,
                };

                if (name is not null)
                    Type(Current, name);
                break;
        }
    }

    private bool IsCurrent(string database) => string.Equals(database, _database, StringComparison.OrdinalIgnoreCase);

    private static string TableKey(string database, string table) => database + "\u001f" + table;

    private void Expand(CatalogRow row)
    {
        if (row.Kind == RowKind.Database && _expanded.TryAdd(row.Database, true) && !_tables.ContainsKey(row.Database))
            _ = LoadTablesAsync(row.Database);
        else if (row.Kind == RowKind.Table && _expanded.TryAdd(TableKey(row.Database, row.Table), true)
                 && !_columns.ContainsKey(TableKey(row.Database, row.Table)))
            _ = DescribeAsync(row.Database, row.Table);
    }

    // Left closes the selected node. On a row that is already closed, or that has no children,
    // it closes the parent and moves the selection up to the parent.
    private void Collapse(CatalogRow row)
    {
        string tableKey = TableKey(row.Database, row.Table);

        switch (row.Kind)
        {
            case RowKind.Database:
                _expanded.TryRemove(row.Database, out _);
                break;

            case RowKind.Table when _expanded.TryRemove(tableKey, out _):
                break;

            case RowKind.Column:
            case RowKind.Note when row.Table.Length > 0:
                _expanded.TryRemove(tableKey, out _);
                SelectCatalogRow(r => r.Kind == RowKind.Table && r.Database == row.Database && r.Table == row.Table);
                break;

            default:
                _expanded.TryRemove(row.Database, out _);
                SelectCatalogRow(r => r.Kind == RowKind.Database && r.Database == row.Database);
                break;
        }
    }

    private void SelectCatalogRow(Func<CatalogRow, bool> match)
    {
        int index = FlatCatalog().FindIndex(r => match(r));

        if (index >= 0)
            _catalogSel = index;
    }

    private LineBuffer Current => _lines[Math.Min(_row, _lines.Count - 1)];

    private void ClearEditor()
    {
        _lines.Clear();
        _lines.Add(new LineBuffer(string.Empty));
        _row = 0;
        _editorTop = 0;
        _editorLeft = 0;
        _focus = Pane.Editor;
    }

    // LineBuffer.Insert writes at the cursor and leaves the cursor where it was, because
    // RadLine's InsertCommand moves it as a separate step. Typing needs both.
    private static void Type(LineBuffer line, string text)
    {
        line.Insert(text);
        line.Move(line.Position + text.Length);
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
        _row = Math.Clamp(row, 0, _lines.Count - 1);
        _lines[_row].Move(Math.Min(c, _lines[_row].Length));
    }

    private void EditorKey(ConsoleKeyInfo k)
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
            if (line.AtBeginning && _row > 0)
                MoveToRow(_row - 1, int.MaxValue);
            else
                line.Move(WordStartBefore(line.Content, line.Position));
            return;
        }

        if (k.Key == ConsoleKey.RightArrow && (alt || ctrl))
        {
            if (line.AtEnd && _row < _lines.Count - 1)
                MoveToRow(_row + 1, 0);
            else
                line.Move(WordEndAfter(line.Content, line.Position));
            return;
        }

        // Ctrl+Home and Ctrl+End go to the start and the end of the whole editor.
        if (ctrl && k.Key == ConsoleKey.Home) { MoveToRow(0, 0); return; }
        if (ctrl && k.Key == ConsoleKey.End) { MoveToRow(_lines.Count - 1, int.MaxValue); return; }

        switch (k.Key)
        {
            case ConsoleKey.PageUp: MoveToRow(_row - _editorVisible); return;
            case ConsoleKey.PageDown: MoveToRow(_row + _editorVisible); return;

            case ConsoleKey.Enter:
            {
                // Split the line at the cursor, the way any editor does.
                string tail = line.Content[line.Position..];
                if (tail.Length > 0)
                    line.Clear(line.Position, tail.Length);
                _lines.Insert(_row + 1, new LineBuffer(tail));
                _row++;
                _lines[_row].Move(0);
                return;
            }

            case ConsoleKey.Backspace:
                if (!line.AtBeginning)
                {
                    line.Clear(line.Position - 1, 1);
                    line.Move(line.Position - 1);
                }
                else if (_row > 0)
                {
                    // Joining lines: the cursor lands where the two meet.
                    LineBuffer above = _lines[_row - 1];
                    int join = above.Length;
                    above.Move(above.Length);
                    above.Insert(line.Content); // cursor stays at the join, which is what we want
                    _lines.RemoveAt(_row);
                    _row--;
                    _lines[_row].Move(join);
                }
                return;

            case ConsoleKey.Delete:
                if (!line.AtEnd)
                    line.Clear(line.Position, 1);
                else if (_row < _lines.Count - 1)
                {
                    int join = line.Length;
                    line.Insert(_lines[_row + 1].Content);
                    _lines.RemoveAt(_row + 1);
                    line.Move(join);
                }
                return;

            case ConsoleKey.LeftArrow: line.Move(Math.Max(0, line.Position - 1)); return;
            case ConsoleKey.RightArrow: line.Move(Math.Min(line.Length, line.Position + 1)); return;
            case ConsoleKey.Home: line.Move(0); return;
            case ConsoleKey.End: line.Move(line.Length); return;

            case ConsoleKey.UpArrow:
                if (_row > 0) { int c = line.Position; _row--; _lines[_row].Move(Math.Min(c, _lines[_row].Length)); }
                return;

            case ConsoleKey.DownArrow:
                if (_row < _lines.Count - 1) { int c = line.Position; _row++; _lines[_row].Move(Math.Min(c, _lines[_row].Length)); }
                return;
        }

        if (!char.IsControl(k.KeyChar))
            Type(line, k.KeyChar.ToString());
    }

    private void ResultsKey(ConsoleKeyInfo k)
    {
        if (_valueView)
        {
            ValueViewKey(k);
            return;
        }

        if (_showMessages)
        {
            MessagesKey(k);
            return;
        }

        int last = Math.Max(0, _rows.Count - 1);
        int lastCol = Math.Max(0, _headers.Length - 1);

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: _selRow = Math.Min(last, _selRow + 1); break;
            case ConsoleKey.UpArrow: _selRow = Math.Max(0, _selRow - 1); break;
            case ConsoleKey.PageDown: _selRow = Math.Min(last, _selRow + _visibleRows); break;
            case ConsoleKey.PageUp: _selRow = Math.Max(0, _selRow - _visibleRows); break;
            case ConsoleKey.Home: _selRow = 0; _selCol = 0; break;
            case ConsoleKey.End: _selRow = last; break;
            // Horizontal movement is by whole column, not by character. A wide result set
            // is read column by column, and this keeps every header aligned with its cells.
            case ConsoleKey.RightArrow: _selCol = Math.Min(lastCol, _selCol + 1); break;
            case ConsoleKey.LeftArrow: _selCol = Math.Max(0, _selCol - 1); break;

            case ConsoleKey.Enter:
                if (_rows.Count > 0)
                {
                    _valueView = true;
                    _viewTop = 0;
                }
                break;

            default:
                switch (k.KeyChar)
                {
                    case 'c': CopyCell(); break;
                    case 'C': CopyRow(); break;
                    case 'm' or 'M': _showMessages = true; break;
                }
                break;
        }

        MaybeFetchPage();
    }

    private void ValueViewKey(ConsoleKeyInfo k)
    {
        int last = Math.Max(0, _rows.Count - 1);
        int lastCol = Math.Max(0, _headers.Length - 1);

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: _viewTop++; break;
            case ConsoleKey.UpArrow: _viewTop = Math.Max(0, _viewTop - 1); break;
            case ConsoleKey.PageDown: _viewTop += _viewVisible; break;
            case ConsoleKey.PageUp: _viewTop = Math.Max(0, _viewTop - _viewVisible); break;
            case ConsoleKey.Home: _viewTop = 0; break;
            case ConsoleKey.End: _viewTop = int.MaxValue; break;

            // Left and Right go to the next value: the next column of the row in the cell view,
            // the next row in the row view. Both views keep the grid selection in step.
            case ConsoleKey.LeftArrow:
                if (_rowView) _selRow = Math.Max(0, _selRow - 1); else _selCol = Math.Max(0, _selCol - 1);
                _viewTop = 0;
                break;

            case ConsoleKey.RightArrow:
                if (_rowView) _selRow = Math.Min(last, _selRow + 1); else _selCol = Math.Min(lastCol, _selCol + 1);
                _viewTop = 0;
                MaybeFetchPage();
                break;

            case ConsoleKey.Enter: _valueView = false; break;

            default:
                switch (k.KeyChar)
                {
                    case 'v' or 'V': _rowView = !_rowView; _viewTop = 0; break;
                    case 'c': if (_rowView) CopyRow(vertical: true); else CopyCell(); break;
                    case 'C': CopyRow(); break;
                }
                break;
        }
    }

    private void MessagesKey(ConsoleKeyInfo k)
    {
        // Moving up stops the view from following new messages. Going back to the end, with
        // Down or End, makes it follow again. The draw sets _messageFollow when it reaches the end.
        switch (k.Key)
        {
            case ConsoleKey.UpArrow: _messageFollow = false; _messageTop = Math.Max(0, _messageTop - 1); break;
            case ConsoleKey.DownArrow: _messageTop++; break;
            case ConsoleKey.PageUp: _messageFollow = false; _messageTop = Math.Max(0, _messageTop - _messageVisible); break;
            case ConsoleKey.PageDown: _messageTop += _messageVisible; break;
            case ConsoleKey.Home: _messageFollow = false; _messageTop = 0; break;
            case ConsoleKey.End: _messageFollow = true; break;

            default:
                switch (k.KeyChar)
                {
                    case 'm' or 'M': _showMessages = false; break;
                    case 'c': CopyMessages(); break;
                }
                break;
        }
    }

    // ---------------------------------------------------------------- clipboard

    // The terminal owns the clipboard, so the copy goes through it: OSC 52 asks the terminal to
    // put base64 text on the system clipboard. Ghostty, Kitty, WezTerm and iTerm2 support it.
    // iTerm2 needs "Applications in terminal may access clipboard" turned on. The shell cannot
    // tell whether the terminal did it, so the status says what was sent.
    private void Copy(string text, string what)
    {
        string payload = Convert.ToBase64String(Encoding.UTF8.GetBytes(text));
        Console.Write($"\u001b]52;c;{payload}\u0007");

        _status = $"Copied {what} to the clipboard ({text.Length} characters).";
        _statusIsError = false;
    }

    private string[]? SelectedRow => _selRow < _rows.Count ? _rows[_selRow] : null;

    private void CopyCell()
    {
        if (SelectedRow is { } row && _selCol < row.Length)
            Copy(row[_selCol], $"{_headers.ElementAtOrDefault(_selCol)} of row {_selRow + 1}");
    }

    // The row as one tab-separated line, which pastes into a spreadsheet as cells. The vertical
    // form is "name: value" lines, the same as the row view.
    private void CopyRow(bool vertical = false)
    {
        if (SelectedRow is not { } row)
            return;

        string text = vertical
            ? string.Join('\n', row.Select((value, i) => $"{_headers.ElementAtOrDefault(i)}: {value}"))
            : string.Join('\t', row);

        Copy(text, $"row {_selRow + 1}");
    }

    private void CopyMessages()
    {
        Message[] messages;

        lock (_messages)
            messages = _messages.ToArray();

        Copy(
            string.Join('\n', messages.Select(m => $"{m.At:HH:mm:ss} {(m.Error ? "ERROR " : "")}{Abbreviate(m.Sql, 200)} — {m.Text}")),
            "the messages");
    }

    // ---------------------------------------------------------------- paste

    // Every key inside a paste is text, even Tab and Enter. A pasted newline arrives as CR
    // (Enter), as LF (Enter with Control), or as both, so the key character is kept as it is
    // and EndPaste folds the variants into one line break.
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

    private void EndPaste()
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

        if (text.Length == 0)
            return;

        // A paste always goes into the editor, whatever pane has the focus.
        EndCompletion();
        _focus = Pane.Editor;
        InsertText(text);
    }

    // Inserts text that can hold line breaks at the cursor. The cursor ends after the text.
    private void InsertText(string text)
    {
        string[] parts = text.Split('\n');
        LineBuffer line = Current;

        if (parts.Length == 1)
        {
            Type(line, text);
            return;
        }

        // The text after the cursor moves to the end of the last pasted line.
        string tail = line.Content[line.Position..];
        if (tail.Length > 0)
            line.Clear(line.Position, tail.Length);

        Type(line, parts[0]);

        for (int i = 1; i < parts.Length; i++)
        {
            _row++;
            _lines.Insert(_row, new LineBuffer(parts[i]));
        }

        LineBuffer last = _lines[_row];
        int end = last.Length;
        last.Move(end);
        last.Insert(tail); // Insert leaves the cursor before the tail, which is where it belongs.
        last.Move(end);
    }

    // ---------------------------------------------------------------- completion

    private void EndCompletion()
    {
        _matches = [];
        _matchRow = -1;
    }

    private void Complete(bool forward)
    {
        if (_completion is null)
            return;

        LineBuffer line = Current;

        // A repeated CTRL+N on the same word steps to the next candidate rather than
        // recomputing the list, which is what makes cycling work.
        if (_matches.Length > 0 && _matchRow == _row)
        {
            _matchIndex = (_matchIndex + (forward ? 1 : _matches.Length - 1)) % _matches.Length;
            ReplaceWord(line, _matchStart, _matches[_matchIndex]);
            ReportMatches();
            return;
        }

        int end = line.Position;
        int start = end;

        while (start > 0 && (char.IsLetterOrDigit(line.Content[start - 1]) || line.Content[start - 1] == '_'))
            start--;

        string word = line.Content[start..end];

        // The prefix is the whole statement up to the word, so SqlCompletion can tell a
        // table position from a keyword position exactly as it does at the prompt.
        string prefix = string.Join('\n', _lines.Take(_row).Select(l => l.Content).Append(line.Content[..start]));
        string suffix = line.Content[end..];

        IEnumerable<string>? all = _completion.GetCompletions(prefix, word, suffix);

        if (all is null)
            return;

        // RadLine matches candidates with an ordinal StartsWith, so the same rule applies here.
        string[] matches = word.Length == 0
            ? all.ToArray()
            : all.Where(c => c.StartsWith(word, StringComparison.OrdinalIgnoreCase)).ToArray();

        if (matches.Length == 0)
        {
            _status = $"No completion for '{word}'";
            _statusIsError = false;
            return;
        }

        _matches = matches;
        _matchIndex = forward ? 0 : matches.Length - 1;
        _matchStart = start;
        _matchRow = _row;

        ReplaceWord(line, start, matches[_matchIndex]);
        ReportMatches();
    }

    private void ReportMatches()
    {
        _statusIsError = false;
        _status = _matches.Length == 1
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

    // ---------------------------------------------------------------- data

    private string Sql => string.Join('\n', _lines.Select(l => l.Content));

    private void LoadQuery()
    {
        try
        {
            if (!File.Exists(_queryPath))
                return;

            string[] saved = File.ReadAllLines(_queryPath);

            if (saved.Length == 0)
                return;

            _lines.Clear();

            foreach (string line in saved)
                _lines.Add(new LineBuffer(line));

            _row = _lines.Count - 1;
            _lines[_row].Move(_lines[_row].Length);
        }
        catch
        {
            // An unreadable scratch file is not worth refusing to start over.
        }
    }

    private void SaveQuery()
    {
        try
        {
            File.WriteAllLines(_queryPath, _lines.Select(l => l.Content));
        }
        catch
        {
            // Same: the editor text is a convenience, not the user's data.
        }
    }

    // Reloads the database list. The table lists of the expanded databases and the columns of
    // the expanded tables load again too. The cached lists of closed nodes are dropped, so they
    // load again when they open. The old lists stay on screen until the new ones arrive.
    private async Task LoadCatalogAsync()
    {
        List<string> databases = new();

        try
        {
            await using CamusConnection sys = await ConnectionHelper.OpenAsync(GetEndpointConnectionString(_connectionString));
            databases.AddRange(await ReadNamesAsync(sys, "show databases"));
        }
        catch
        {
            // No access to the database list. The current database still shows below.
        }

        if (_database is not null && !databases.Contains(_database, StringComparer.OrdinalIgnoreCase))
            databases.Add(_database);

        databases.Sort(StringComparer.OrdinalIgnoreCase);
        _databases = databases;

        foreach (string key in _tables.Keys)
            if (!_expanded.ContainsKey(key) || !databases.Contains(key, StringComparer.OrdinalIgnoreCase))
                _tables.TryRemove(key, out _);

        foreach (string key in _columns.Keys)
            if (!_expanded.ContainsKey(key))
                _columns.TryRemove(key, out _);

        foreach (string database in databases.Where(d => _expanded.ContainsKey(d)))
        {
            await LoadTablesAsync(database);

            foreach (string table in _tables.GetValueOrDefault(database) ?? [])
                if (_expanded.ContainsKey(TableKey(database, table)))
                    await DescribeAsync(database, table);
        }

        _revision++;
    }

    private async Task LoadTablesAsync(string database)
    {
        List<string> tables = new();

        try
        {
            await WithDatabaseAsync(database, async connection =>
            {
                foreach (string statement in new[] { "show tables", "show views", "show materialized views" })
                {
                    try
                    {
                        tables.AddRange(await ReadNamesAsync(connection, statement));
                    }
                    catch
                    {
                        // A server without views. Show whatever did load.
                    }
                }
            });
        }
        catch
        {
            // The database did not open. It stays expanded, with no tables under it.
        }

        tables.Sort(StringComparer.OrdinalIgnoreCase);
        _tables[database] = tables;
        _revision++;
    }

    // The first column of every row, for the SHOW statements that list names.
    private static async Task<List<string>> ReadNamesAsync(CamusConnection connection, string statement)
    {
        List<string> names = new();

        await using CamusCommand cmd = connection.CreateSelectCommand(statement);
        cmd.CommandTimeout = 10;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            string? name = row.Values.FirstOrDefault().StrValue;
            if (!string.IsNullOrWhiteSpace(name))
                names.Add(name);
        }

        return names;
    }

    // The current database is read on the session's connection. Another database is read on a
    // connection of its own that closes at once, because the catalog only looks at it.
    private async Task WithDatabaseAsync(string database, Func<CamusConnection, Task> work)
    {
        if (IsCurrent(database))
        {
            await work(_connection);
            return;
        }

        await using CamusConnection other = await ConnectionHelper.OpenAsync(SwapDatabase(_connectionString, database));
        await work(other);
    }

    // DESCRIBE runs off the key loop so the screen never freezes on a slow catalog read.
    private async Task DescribeAsync(string database, string table)
    {
        List<(string, string)> cols = new();

        try
        {
            await WithDatabaseAsync(database, async connection =>
            {
                await using CamusCommand cmd = connection.CreateSelectCommand($"describe {table}");
                cmd.CommandTimeout = 10;

                CamusDataReader reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
                    string[] keys = row.Keys.ToArray();

                    // Column order is the server's to change, so ask by name and fall back to
                    // position only when the expected name is absent.
                    string name = Pick(row, keys, 0, "field", "column", "name") ?? "?";
                    string type = Pick(row, keys, 1, "type", "datatype") ?? "";

                    cols.Add((name, type));
                }
            });
        }
        catch
        {
            // Leave the table expanded but empty rather than dropping the user back a level.
        }

        _columns[TableKey(database, table)] = cols;
        _revision++;
    }

    private static string? Pick(Dictionary<string, ColumnValue> row, string[] keys, int fallbackIndex, params string[] names)
    {
        // ReadCurrentRow builds an ordinal dictionary, but DESCRIBE capitalizes its column names.
        foreach (string name in names)
            foreach (string key in keys)
                if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase)
                    && !string.IsNullOrWhiteSpace(row[key].StrValue))
                    return row[key].StrValue;

        return fallbackIndex < keys.Length ? row[keys[fallbackIndex]].StrValue : null;
    }

    // ---------------------------------------------------------------- execution

    // Runs only the statement at the cursor. The editor text comes back from the last session,
    // so running the whole editor every time ran its old INSERT and DDL statements again too.
    // The cursor belongs to a statement from its first character up to the character after its
    // terminator, so a cursor at the end of "select 1;" runs that statement. A cursor after
    // the last statement runs the last statement.
    private void RunCurrent()
    {
        string text = Sql;
        int cursor = _lines.Take(_row).Sum(l => l.Length + 1) + Current.Position;

        // Folding smart quotes keeps every character at its offset, and a folded quote opens a
        // string here just as it does when the statement runs.
        SqlScanner scanner = new();
        List<SqlStatement> statements = scanner.Feed(NormalizeSmartQuotes(text)).ToList();
        statements.AddRange(scanner.Flush());

        if (statements.Count == 0)
            return;

        SqlStatement pick = statements.FirstOrDefault(s => cursor <= s.End + 1, statements[^1]);

        StartRun(text[pick.Start..(pick.End + 1)]);
    }

    private void StartRun(string text)
    {
        if (_running)
            return;

        text = text.Trim();

        if (text.Length == 0)
            return;

        _running = true;
        _cancel = new CancellationTokenSource();
        _status = "Running…";
        _statusIsError = false;
        _revision++;

        _ = RunScriptAsync(text, _cancel.Token);
    }

    // The editor holds a script, not a single statement, so it is split the same way -e is.
    // Each statement reports its own outcome; the last one that returns rows fills the grid.
    private async Task RunScriptAsync(string text, CancellationToken token)
    {
        RunLog log = new();
        string current = "";
        int grid = _gridVersion;

        try
        {
            await ClosePageAsync();

            foreach ((string sql, bool _) in EscapeStringIntoLines(NormalizeSmartQuotes(text)))
            {
                if (token.IsCancellationRequested)
                {
                    log.Add("cancelled");
                    AddMessage("", "cancelled", false);
                    break;
                }

                current = sql;
                int before = log.Entries.Count;

                await RunStatementAsync(sql, log, token);

                foreach ((string entry, bool error) in log.Entries.Skip(before))
                    AddMessage(sql, entry, error);
            }
        }
        catch (Exception ex)
        {
            log.Fail(ex.Message);
            AddMessage(current, ex.Message, true);
        }
        finally
        {
            _status = log.Entries.Count == 0 ? "Nothing to run." : string.Join("   ·   ", log.Entries.Select(e => e.Text));
            _statusIsError = log.Failed;

            // A failed run opens the messages, because the status line can be too short for the
            // error. A run that fills the grid shows the grid.
            if (log.Failed)
            {
                _showMessages = true;
                _messageFollow = true;
            }
            else if (_gridVersion != grid)
            {
                _showMessages = false;
            }

            _running = false;
            _revision++;
        }
    }

    private void AddMessage(string sql, string text, bool error)
    {
        lock (_messages)
        {
            _messages.Add(new Message(DateTime.Now, sql, text, error));

            if (_messages.Count > MaxMessages)
                _messages.RemoveRange(0, _messages.Count - MaxMessages);
        }

        _revision++;
    }

    private async Task RunStatementAsync(string sql, RunLog log, CancellationToken token)
    {
        if (string.IsNullOrWhiteSpace(sql))
            return;

        // USE belongs to the shell, not the server: the database lives in the connection string.
        if (IsUseDatabase(sql, out string database))
        {
            if (_transaction is not null)
                throw new InvalidOperationException("There's an active transaction, please commit or rollback before switching databases");

            await SwitchDatabaseAsync(database);
            log.Add($"database changed to {database}");
            return;
        }

        // Backups are node-wide REST admin calls with rich table output of their own. They stay
        // at the prompt rather than getting a second, worse rendering here.
        if (IsBackupCommand(sql, out _))
        {
            log.Add("backup commands are only available at the prompt, not in --tui");
            return;
        }

        bool needsDb = !IsServerLevelDDL(sql) && !IsSystemLevelQuery(sql);

        if (needsDb && !HasDatabase(_connectionString))
        {
            log.Fail("no database selected — run `use <database>`");
            return;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();

        if (IsSystemLevelQuery(sql))
        {
            await using CamusConnection sys = await ConnectionHelper.OpenAsync(GetEndpointConnectionString(_connectionString));
            log.Add(await QueryAsync(sys, sql, log, token));
        }
        else if (IsQueryable(sql))
        {
            log.Add(await QueryAsync(_connection, sql, log, token));
        }
        else if (IsServerLevelDDL(sql))
        {
            await using CamusConnection sys = await ConnectionHelper.OpenAsync(GetEndpointConnectionString(_connectionString));
            await DdlAsync(sys, sql);
            log.Add($"OK ({stopwatch.Elapsed})");

            // CREATE, DROP and RENAME DATABASE change the database list in the catalog.
            if (IsDatabaseDDL(sql))
                await LoadCatalogAsync();
        }
        else if (IsDDL(sql))
        {
            await DdlAsync(_connection, sql);
            log.Add($"OK ({stopwatch.Elapsed})");

            // A CREATE or DROP changes what the catalog should show.
            if (ChangesTableSet(sql))
                await RefreshCatalogAsync();
            else if (ChangesIndexSet(sql))
                _completion?.InvalidateIndexes();
        }
        else if (IsBeginTx(sql))
        {
            if (_transaction is not null)
                throw new InvalidOperationException("There's an active transaction already");

            _transaction = await _connection.BeginTransactionAsync();
            log.Add($"transaction started ({stopwatch.Elapsed})");
        }
        else if (IsCommitTx(sql))
        {
            if (_transaction is null)
                throw new InvalidOperationException("There's no active transaction");

            try
            {
                await _transaction.CommitAsync();
                log.Add($"committed ({stopwatch.Elapsed})");
            }
            finally
            {
                _transaction = null;
            }
        }
        else if (IsRollbackTx(sql))
        {
            if (_transaction is null)
                throw new InvalidOperationException("There's no active transaction");

            try
            {
                await _transaction.RollbackAsync();
                log.Add($"rolled back ({stopwatch.Elapsed})");
            }
            finally
            {
                _transaction = null;
            }
        }
        else if (IsInsert(sql))
        {
            log.Add(await InsertAsync(sql, log, token));
        }
        else
        {
            await using CamusCommand cmd = _connection.CreateCamusCommand(sql);
            cmd.CommandTimeout = 60;
            cmd.Transaction = _transaction;

            int affected = await cmd.ExecuteNonQueryAsync();
            log.Add($"{affected} rows affected ({stopwatch.Elapsed})");
        }
    }

    private async Task SwitchDatabaseAsync(string database)
    {
        _connectionString = SwapDatabase(_connectionString, database);
        _connection = await ConnectionHelper.OpenAsync(_connectionString);
        _database = database;
        _expanded[database] = true;

        await RefreshCatalogAsync();
        SelectCatalogRow(r => r.Kind == RowKind.Database && IsCurrent(r.Database));
    }

    private async Task RefreshCatalogAsync()
    {
        await LoadCatalogAsync();

        if (_completion is not null)
            await _completion.RefreshTablesAsync(_connection);
    }

    // F4. The catalog does not see tables that another session creates, so it must be possible
    // to load it again by hand.
    private async Task RefreshFromKeyAsync()
    {
        if (_refreshing)
            return;

        _refreshing = true;
        _status = "Refreshing the catalog…";
        _statusIsError = false;
        _revision++;

        try
        {
            await RefreshCatalogAsync();
            _status = $"Catalog refreshed: {_databases.Count} databases.";
        }
        catch (Exception ex)
        {
            _status = ex.Message;
            _statusIsError = true;
            AddMessage("", ex.Message, true);
        }
        finally
        {
            _refreshing = false;
            _revision++;
        }
    }

    private static async Task DdlAsync(CamusConnection connection, string sql)
    {
        using CamusCommand cmd = connection.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;

        await cmd.ExecuteDDLAsync();
    }

    // Reads the first page and leaves the reader open, so scrolling can pull the rest.
    private async Task<string> QueryAsync(CamusConnection connection, string sql, RunLog log, CancellationToken token)
    {
        await ClosePageAsync();

        Stopwatch stopwatch = Stopwatch.StartNew();

        CamusCommand cmd = connection.CreateSelectCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = _transaction;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        TimeSpan first = stopwatch.Elapsed;

        _pageCommand = cmd;
        _pageReader = reader;
        _pageDone = false;

        NewGrid();

        int wanted = _limitOn ? Math.Min(LimitRows, PageRows) : PageRows;
        int got = await FetchAsync(wanted, log, token);

        return _pageDone
            ? $"{_rows.Count} rows in {stopwatch.Elapsed}"
            : $"{got} rows so far (first in {first}) — scroll for more";
    }

    // An INSERT runs through a reader, so an INSERT … RETURNING can show its rows in the grid. The
    // server sends a reader with columns only for a RETURNING list. Any other INSERT leaves the grid
    // as it is and logs the count, as before. The RETURNING rows are already in memory on the client,
    // so they are read now and the reader closes at once.
    private async Task<string> InsertAsync(string sql, RunLog log, CancellationToken token)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        await using CamusCommand cmd = _connection.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = _transaction;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();
        int affected = reader.RecordsAffected;

        if (reader.FieldCount == 0)
            return $"{affected} rows affected ({stopwatch.Elapsed})";

        await ClosePageAsync();

        _pageReader = reader;
        _pageDone = false;

        NewGrid();

        // The display cap still applies, so the grid can show fewer rows than the count.
        await FetchAsync(int.MaxValue, log, token);

        return $"{affected} rows affected ({stopwatch.Elapsed})";
    }

    // A new result replaces the grid. _gridVersion tells RunScriptAsync that the grid changed.
    private int _gridVersion;

    private void NewGrid()
    {
        _headers = [];
        _rows.Clear();
        _rowTop = 0;
        _colLeft = 0;
        _selRow = 0;
        _selCol = 0;
        _valueView = false;
        _gridVersion++;
    }

    // The formatter escapes a value for Spectre markup. The grid stores plain text, so the
    // escape is undone here: Markup.Escape doubles every bracket, and nothing else.
    private static string Unescape(string markup) => markup.Replace("[[", "[").Replace("]]", "]");

    // Pulls up to `count` more rows off the open reader. Returns how many arrived. A read error
    // goes into `log`, and the result keeps the rows that arrived before it.
    private async Task<int> FetchAsync(int count, RunLog log, CancellationToken token)
    {
        if (_pageReader is null || _pageDone)
            return 0;

        int got = 0;

        try
        {
            while (got < count)
            {
                if (token.IsCancellationRequested)
                    break;

                if (_limitOn && _rows.Count >= LimitRows)
                {
                    // The cap is a display cap, not a SQL LIMIT. Rows past it are never
                    // read, which is what keeps a wide scan from filling memory.
                    _pageDone = true;
                    break;
                }

                if (!await _pageReader.ReadAsync())
                {
                    _pageDone = true;
                    break;
                }

                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(_pageReader);

                if (_headers.Length == 0)
                    _headers = row.Keys.ToArray();

                _rows.Add(row.Values.Select(v => Unescape(_format(v))).ToArray());
                got++;
            }
        }
        catch (Exception ex)
        {
            _pageDone = true;
            log.Fail(ex.Message);
        }

        if (_pageDone)
            await ClosePageAsync();

        _revision++;
        return got;
    }

    // Called after every scroll. Fetching starts before the window reaches the end of the
    // buffer, so a held-down arrow key does not stall waiting for rows.
    private void MaybeFetchPage()
    {
        if (_pageDone || _fetching || _running || _pageReader is null)
            return;

        if (_selRow + _visibleRows + PageMargin < _rows.Count)
            return;

        _fetching = true;

        _ = Task.Run(async () =>
        {
            try
            {
                RunLog log = new();
                int got = await FetchAsync(PageRows, log, _cancel?.Token ?? CancellationToken.None);

                if (log.Failed)
                {
                    string error = log.Entries.First(e => e.Error).Text;
                    AddMessage("", error, true);
                    _status = error;
                    _statusIsError = true;
                }
                else if (_pageDone)
                {
                    _status = $"{_rows.Count} rows (end of result)";
                    _statusIsError = false;
                }
                else if (got > 0)
                {
                    _status = $"{_rows.Count} rows so far — scroll for more";
                    _statusIsError = false;
                }
            }
            finally
            {
                _fetching = false;
                _revision++;
            }
        });
    }

    private async Task ClosePageAsync()
    {
        _pageReader = null;
        _pageDone = true;

        if (_pageCommand is not null)
        {
            CamusCommand cmd = _pageCommand;
            _pageCommand = null;

            try
            {
                await cmd.DisposeAsync();
            }
            catch
            {
                // A reader torn down mid-scan is expected when a new query replaces it.
            }
        }
    }

    // ---------------------------------------------------------------- render

    private List<CatalogRow> FlatCatalog()
    {
        List<CatalogRow> rows = new();

        foreach (string database in _databases)
        {
            rows.Add(new CatalogRow(RowKind.Database, database));

            if (!_expanded.ContainsKey(database))
                continue;

            if (!_tables.TryGetValue(database, out List<string>? tables))
            {
                rows.Add(new CatalogRow(RowKind.Note, database, Name: "…"));
                continue;
            }

            if (tables.Count == 0)
                rows.Add(new CatalogRow(RowKind.Note, database, Name: "(no tables)"));

            foreach (string table in tables)
            {
                rows.Add(new CatalogRow(RowKind.Table, database, table));

                string key = TableKey(database, table);

                if (!_expanded.ContainsKey(key))
                    continue;

                if (_columns.TryGetValue(key, out List<(string Name, string Type)>? cols))
                    foreach ((string name, string type) in cols)
                        rows.Add(new CatalogRow(RowKind.Column, database, table, name, type));
                else
                    rows.Add(new CatalogRow(RowKind.Note, database, table, "…"));
            }
        }

        return rows;
    }

    private Layout Build()
    {
        int width = SafeWidth();
        int height = SafeHeight();

        int body = Math.Max(6, height - 1);
        // The value view is for reading a long value, so the editor gives it most of its room.
        int editorHeight = _valueView ? 5 : Math.Max(5, body / 2);

        int leftWidth = Math.Clamp(width / 4, 20, 40);
        int rightWidth = Math.Max(20, width - leftWidth);

        return new Layout("root").SplitRows(
            new Layout("body").Size(body).SplitColumns(
                new Layout("left").Size(leftWidth).Update(RenderCatalog(body, leftWidth)),
                new Layout("right").SplitRows(
                    new Layout("editor").Size(editorHeight).Update(RenderEditor(editorHeight, rightWidth)),
                    new Layout("results").Update(RenderResults(body - editorHeight, rightWidth)))),
            new Layout("footer").Size(1).Update(RenderFooter()));
    }

    private static int SafeWidth()
    {
        try { return Math.Max(60, Console.WindowWidth); }
        catch { return 120; }
    }

    private static int SafeHeight()
    {
        try { return Math.Max(16, Console.WindowHeight); }
        catch { return 40; }
    }

    private Style BorderOf(Pane pane) => _focus == pane ? new Style(Color.Yellow) : new Style(Color.Grey35);

    // Spectre's Tree wraps a node that is wider than its pane instead of clipping it, which
    // breaks the one-row-per-entry mapping that selection depends on. Every label is cut to
    // the pane width here so a long name costs one row, not two.
    private static string Clip(string text, int width)
    {
        if (width <= 1)
            return "";

        return text.Length <= width ? text : text[..Math.Max(1, width - 1)] + "…";
    }

    private IRenderable RenderCatalog(int height, int width)
    {
        List<CatalogRow> rows = FlatCatalog();

        if (_catalogSel >= rows.Count)
            _catalogSel = Math.Max(0, rows.Count - 1);

        // Two border rows, one header row for the server node.
        int visible = Math.Max(1, height - 4);
        _catalogVisible = visible;

        if (_catalogSel < _catalogTop) _catalogTop = _catalogSel;
        if (_catalogSel >= _catalogTop + visible) _catalogTop = _catalogSel - visible + 1;
        _catalogTop = Math.Clamp(_catalogTop, 0, Math.Max(0, rows.Count - 1));

        // Panel borders take two cells and Panel padding takes two more.
        int inner = Math.Max(4, width - 4);

        Grid grid = new();
        grid.AddColumn(new GridColumn().NoWrap());

        string server = GetConnValue(_connectionString, "Endpoint") ?? "server";
        server = server.Replace("https://", "").Replace("http://", "").TrimEnd('/');
        grid.AddRow($"[bold]{Markup.Escape(Clip(server, inner))}[/]");

        for (int i = _catalogTop; i < Math.Min(rows.Count, _catalogTop + visible); i++)
        {
            CatalogRow row = rows[i];
            bool selected = i == _catalogSel;

            // The selected name is drawn inverted. Each case below says what goes before and
            // after the name, so the name gets the cells that are left.
            string Name(string text, int room, string colour) => selected
                ? $"[black on aqua]{Markup.Escape(Clip(text, room))}[/]"
                : $"[{colour}]{Markup.Escape(Clip(text, room))}[/]";

            string label;

            switch (row.Kind)
            {
                case RowKind.Database:
                {
                    // "▶ " before the name. The current database has " ●" after it.
                    string marker = _expanded.ContainsKey(row.Database) ? "▼" : "▶";
                    bool current = IsCurrent(row.Database);
                    label = $"{marker} {Name(row.Database, inner - 4, current ? "bold white" : "white")}"
                            + (current ? " [green]●[/]" : "");
                    break;
                }

                case RowKind.Table:
                {
                    // "  ▶ " before the name, " t" after it.
                    string marker = _expanded.ContainsKey(TableKey(row.Database, row.Table)) ? "▼" : "▶";
                    label = $"  {marker} {Name(row.Table, inner - 6, "white")} [grey]t[/]";
                    break;
                }

                case RowKind.Column:
                {
                    // "    ├ " before the name, then a space and the type after it.
                    string type = Clip(row.Type, 7);
                    label = $"    ├ {Name(row.Name, Math.Max(3, inner - 7 - type.Length), "grey78")} [grey]{Markup.Escape(type)}[/]";
                    break;
                }

                default:
                {
                    string indent = row.Table.Length > 0 ? "    " : "  ";
                    label = $"{indent}{Name(row.Name, inner - indent.Length, "grey")}";
                    break;
                }
            }

            grid.AddRow(label);
        }

        if (rows.Count == 0)
            grid.AddRow("[grey](no databases)[/]");

        return new Panel(grid)
        {
            Header = new PanelHeader("Data Catalog"),
            Border = BoxBorder.Rounded,
            BorderStyle = BorderOf(Pane.Catalog),
            Expand = true,
            Height = height,
        };
    }

    private IRenderable RenderEditor(int height, int width)
    {
        int visible = Math.Max(1, height - 2);
        _editorVisible = visible;

        if (_row < _editorTop) _editorTop = _row;
        if (_row >= _editorTop + visible) _editorTop = _row - visible + 1;

        // Two border cells, two padding cells, three for the line number, two for the gap.
        int textWidth = Math.Max(10, width - 9);

        // A line longer than the pane must scroll rather than hide its tail, so the window
        // follows the cursor. Every line scrolls together, which keeps the columns aligned.
        LineBuffer current = Current;

        if (current.Position < _editorLeft)
            _editorLeft = current.Position;

        if (current.Position >= _editorLeft + textWidth)
            _editorLeft = current.Position - textWidth + 1;

        // A two-column Grid splits the pane width between its columns, which left the code
        // column far narrower than the space available. The line number is part of the same
        // markup string instead, so the text gets every cell that is left.
        List<IRenderable> rendered = new();

        // A string or a comment that opens above the window colours the lines below it, so the
        // lexer reads every line from the top.
        Lex state = Lex.Code;

        for (int i = 0; i < _editorTop && i < _lines.Count; i++)
            Tokenize(_lines[i].Content, ref state);

        for (int i = _editorTop; i < Math.Min(_lines.Count, _editorTop + visible); i++)
        {
            bool onCursor = i == _row && _focus == Pane.Editor;
            List<(int Start, int Length, TokenKind Kind)> tokens = Tokenize(_lines[i].Content, ref state);
            rendered.Add(new Markup($"[grey]{i + 1,3}[/]  {RenderLine(_lines[i], tokens, onCursor, _editorLeft, textWidth)}"));
        }

        if (rendered.Count == 0)
            rendered.Add(new Markup(string.Empty));

        return new Panel(new Rows(rendered))
        {
            Header = new PanelHeader("Query Editor"),
            Border = BoxBorder.Rounded,
            BorderStyle = BorderOf(Pane.Editor),
            Expand = true,
            Height = height,
        };
    }

    // The Live region owns the screen, so the hardware cursor cannot be placed inside a
    // Panel. The cursor is drawn as an inverted cell instead.
    //
    // Tokenizing starts at the beginning of the line even when the window does not, so a
    // keyword that straddles the left edge keeps its colour.
    private string RenderLine(LineBuffer line, List<(int Start, int Length, TokenKind Kind)> tokens, bool showCursor, int left, int width)
    {
        string text = line.Content;
        int cursor = Math.Clamp(line.Position, 0, text.Length);

        StringBuilder markup = new();

        foreach ((int start, int length, TokenKind kind) in tokens)
        {
            if (start >= left + width)
                break;

            string token = text.Substring(start, length);

            // `is { }` produces a non-null Style whether the highlighter's return is a
            // nullable reference or a nullable value type, so this compiles either way.
            string? tag = kind switch
            {
                TokenKind.String => _stringTag,
                TokenKind.Identifier => _identifierTag,
                TokenKind.Comment => _commentTag,
                _ => _highlighter.Highlight(token) is { } style ? Describe(style) : null,
            };

            for (int j = 0; j < token.Length; j++)
            {
                int at = start + j;

                if (at < left || at >= left + width)
                    continue;

                // A pasted Tab is kept in the text but drawn as one cell, so the cursor
                // column stays the character index.
                string ch = Markup.Escape(token[j] == '\t' ? " " : token[j].ToString());

                if (showCursor && at == cursor)
                    markup.Append($"[black on white]{ch}[/]");
                else if (tag is not null)
                    markup.Append($"[{tag}]{ch}[/]");
                else
                    markup.Append(ch);
            }
        }

        if (showCursor && cursor >= text.Length && cursor < left + width)
            markup.Append("[black on white] [/]");

        return markup.ToString();
    }

    // Splits one line into tokens to colour. The rules are those of SqlScanner, which splits the
    // statements that run: a backslash escapes a character in a string but not in a `quoted
    // identifier`, a doubled delimiter is a literal one, and -- opens a comment only before
    // white space or the end of the line. `state` comes in as the state at the start of the line
    // and goes out as the state at its end.
    private static List<(int Start, int Length, TokenKind Kind)> Tokenize(string text, ref Lex state)
    {
        List<(int, int, TokenKind)> tokens = new();
        int n = text.Length;
        int i = 0;

        while (i < n)
        {
            int start = i;

            if (state == Lex.Code)
            {
                char c = text[i];
                char next = i + 1 < n ? text[i + 1] : '\0';

                if ((c == '-' && next == '-' && (i + 2 >= n || char.IsWhiteSpace(text[i + 2]))) || c == '#')
                {
                    tokens.Add((i, n - i, TokenKind.Comment));
                    break;
                }

                if (c == '/' && next == '*')
                {
                    state = Lex.Block;
                    i += 2;
                }
                else if (c is '\'' or '"' or '`')
                {
                    state = c == '\'' ? Lex.Single : c == '"' ? Lex.Double : Lex.Backtick;
                    i++;
                }
                else if (IsWordChar(c))
                {
                    while (i < n && IsWordChar(text[i]))
                        i++;

                    tokens.Add((start, i - start, TokenKind.Word));
                    continue;
                }
                else
                {
                    tokens.Add((start, 1, TokenKind.Plain));
                    i++;
                    continue;
                }
            }

            // A comment or a quoted text is open here. It opened on this line, or it
            // continues from the line before.
            TokenKind kind = state switch
            {
                Lex.Block => TokenKind.Comment,
                Lex.Backtick => TokenKind.Identifier,
                _ => TokenKind.String,
            };

            if (state == Lex.Block)
            {
                int end = text.IndexOf("*/", i, StringComparison.Ordinal);

                if (end < 0)
                {
                    i = n;
                }
                else
                {
                    i = end + 2;
                    state = Lex.Code;
                }
            }
            else
            {
                char quote = state == Lex.Single ? '\'' : state == Lex.Double ? '"' : '`';

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
                        state = Lex.Code;
                        break;
                    }

                    i++;
                }
            }

            tokens.Add((start, i - start, kind));
        }

        return tokens;
    }

    // Spectre 0.57.2 exposes no Style-to-markup conversion this build can see, so the
    // markup tag is built from the parts the shell's highlighter actually sets.
    private static string Describe(Style style)
    {
        string colour = style.Foreground.ToString();

        return style.Decoration == Decoration.None ? colour : $"{style.Decoration} {colour}".ToLowerInvariant();
    }

    private IRenderable RenderResults(int height, int width)
    {
        if (_valueView && SelectedRow is not null)
            return RenderValueView(height, width);

        _valueView = false;

        if (_showMessages)
            return RenderMessages(height, width);

        // Two border rows, one header row, one status row.
        int visible = Math.Max(1, height - 4);
        _visibleRows = visible;

        _selRow = Math.Clamp(_selRow, 0, Math.Max(0, _rows.Count - 1));
        _selCol = Math.Clamp(_selCol, 0, Math.Max(0, _headers.Length - 1));

        if (_selRow < _rowTop) _rowTop = _selRow;
        if (_selRow >= _rowTop + visible) _rowTop = _selRow - visible + 1;

        List<string[]> window = _rows.Skip(_rowTop).Take(visible).ToList();

        // The window shows only the columns that fit, so Spectre never squeezes a column to
        // nothing. It moves right until the selected column is one of them.
        int inner = Math.Max(10, width - 4);

        if (_selCol < _colLeft) _colLeft = _selCol;
        while (_selCol >= _colLeft + ColumnsThatFit(window, _colLeft, inner)) _colLeft++;

        int count = ColumnsThatFit(window, _colLeft, inner);

        Table table = new()
        {
            Border = TableBorder.None,
            Expand = true,
        };

        if (_headers.Length == 0)
        {
            table.AddColumn(new TableColumn("[grey](no result)[/]"));
        }
        else
        {
            for (int c = _colLeft; c < _colLeft + count; c++)
                table.AddColumn(new TableColumn($"[bold yellow]{Markup.Escape(Clip(_headers[c], 24))}[/]").NoWrap());

            bool focused = _focus == Pane.Results;

            for (int r = 0; r < window.Count; r++)
            {
                string[] cells = new string[count];

                for (int c = 0; c < count; c++)
                {
                    string[] row = window[r];
                    string text = Markup.Escape(Clip(OneLine(row.ElementAtOrDefault(_colLeft + c) ?? ""), 40));
                    bool selected = focused && _rowTop + r == _selRow && _colLeft + c == _selCol;
                    cells[c] = selected ? $"[black on aqua]{text}[/]" : text;
                }

                table.AddRow(cells);
            }
        }

        string more = _pageDone ? "" : "+";

        string title = _rows.Count > 0
            ? $"Query Results ({_rows.Count}{more} rows)  row {_selRow + 1}  col {_selCol + 1}/{_headers.Length}"
            : "Query Results";

        return new Panel(new Rows(table, new Markup(Caption(width))))
        {
            Header = new PanelHeader(title),
            Border = BoxBorder.Rounded,
            BorderStyle = BorderOf(Pane.Results),
            Expand = true,
            Height = height,
        };
    }

    // The status line under the grid. When the status is too long for the line, the line says
    // that M shows all of it.
    private string Caption(int width)
    {
        if (_running)
            return "[yellow]running…[/]  ESC cancels";

        string colour = _statusIsError ? "red" : "grey";
        int room = Math.Max(10, width - 4);

        if (_status.Length <= room)
            return $"[{colour}]{Markup.Escape(_status)}[/]";

        return $"[{colour}]{Markup.Escape(Clip(_status, room - 13))}[/] [aqua]M[/] [grey]messages[/]";
    }

    // A cell is drawn on one line, so a line break or a Tab in a value is shown as one cell.
    private static string OneLine(string text) =>
        text.Replace("\r\n", "↵").Replace('\n', '↵').Replace('\r', '↵').Replace('\t', ' ');

    // How many columns, from `first`, fit in `room` cells. A column is as wide as its clipped
    // header or its widest clipped cell in the window, plus Spectre's one cell of padding on
    // each side. At least one column always shows.
    private int ColumnsThatFit(List<string[]> window, int first, int room)
    {
        int used = 0;
        int count = 0;

        for (int c = first; c < _headers.Length; c++)
        {
            int widest = Math.Min(24, _headers[c].Length);

            foreach (string[] row in window)
                widest = Math.Max(widest, Math.Min(40, OneLine(row.ElementAtOrDefault(c) ?? "").Length));

            used += widest + 2;

            if (used > room && count > 0)
                break;

            count++;
        }

        return Math.Max(1, count);
    }

    // Cuts text into lines of at most `width` characters. A line break in the text starts a
    // new line. A Tab is shown as four spaces.
    private static List<string> Wrap(string text, int width)
    {
        List<string> lines = new();
        width = Math.Max(1, width);

        foreach (string part in text.Replace("\r\n", "\n").Replace('\r', '\n').Replace("\t", "    ").Split('\n'))
        {
            if (part.Length == 0)
            {
                lines.Add("");
                continue;
            }

            for (int i = 0; i < part.Length; i += width)
                lines.Add(part.Substring(i, Math.Min(width, part.Length - i)));
        }

        return lines;
    }

    private IRenderable RenderValueView(int height, int width)
    {
        string[] row = SelectedRow!;
        int inner = Math.Max(10, width - 4);

        // Each line is markup. The text in it is escaped after the wrap, for the same reason
        // as in the grid.
        List<string> lines = new();

        if (_rowView)
        {
            for (int c = 0; c < _headers.Length; c++)
            {
                string name = Markup.Escape(Clip(_headers[c], inner));
                lines.Add(c == _selCol ? $"[black on aqua]{name}[/]" : $"[bold yellow]{name}[/]");

                foreach (string line in Wrap(row.ElementAtOrDefault(c) ?? "", inner - 2))
                    lines.Add("  " + Markup.Escape(line));
            }
        }
        else
        {
            foreach (string line in Wrap(row.ElementAtOrDefault(_selCol) ?? "", inner))
                lines.Add(Markup.Escape(line));
        }

        // Two border rows and one row for the key line.
        int visible = Math.Max(1, height - 3);
        _viewVisible = visible;
        _viewTop = Math.Clamp(_viewTop, 0, Math.Max(0, lines.Count - visible));

        List<IRenderable> shown = lines.Skip(_viewTop).Take(visible).Select(l => (IRenderable)new Markup(l)).ToList();

        while (shown.Count < visible)
            shown.Add(new Markup(""));

        string keys = _rowView
            ? "[grey]←/→ row · ↑/↓ scroll · V cell view · C copy · ESC close[/]"
            : "[grey]←/→ column · ↑/↓ scroll · V row view · C copy · ESC close[/]";

        shown.Add(new Markup(keys));

        string position = lines.Count > visible ? $"  lines {_viewTop + 1}-{Math.Min(lines.Count, _viewTop + visible)} of {lines.Count}" : "";

        string title = _rowView
            ? $"Row {_selRow + 1} of {_rows.Count}{position}"
            : $"{_headers.ElementAtOrDefault(_selCol)}  row {_selRow + 1}  col {_selCol + 1}/{_headers.Length}{position}";

        return new Panel(new Rows(shown))
        {
            Header = new PanelHeader(Markup.Escape(title)),
            Border = BoxBorder.Rounded,
            BorderStyle = BorderOf(Pane.Results),
            Expand = true,
            Height = height,
        };
    }

    private IRenderable RenderMessages(int height, int width)
    {
        Message[] messages;

        lock (_messages)
            messages = _messages.ToArray();

        int inner = Math.Max(10, width - 4);

        // One line for the time and the statement, then the outcome below it, wrapped.
        List<string> lines = new();

        foreach (Message m in messages)
        {
            string time = $"[grey]{m.At:HH:mm:ss}[/]";
            string sql = m.Sql.Length > 0 ? Markup.Escape(Abbreviate(m.Sql, Math.Max(10, inner - 9))) : "";
            lines.Add($"{time} {sql}");

            string colour = m.Error ? "red" : "white";

            foreach (string line in Wrap(m.Text, inner - 2))
                lines.Add($"  [{colour}]{Markup.Escape(line)}[/]");
        }

        // Two border rows and one row for the key line.
        int visible = Math.Max(1, height - 3);
        _messageVisible = visible;

        int bottom = Math.Max(0, lines.Count - visible);

        if (_messageFollow || _messageTop >= bottom)
        {
            _messageTop = bottom;
            _messageFollow = true;
        }

        List<IRenderable> shown = lines.Skip(_messageTop).Take(visible).Select(l => (IRenderable)new Markup(l)).ToList();

        if (lines.Count == 0)
            shown.Add(new Markup("[grey](no messages)[/]"));

        while (shown.Count < visible)
            shown.Add(new Markup(""));

        shown.Add(new Markup("[grey]↑/↓ scroll · C copy all · M or ESC back to the results[/]"));

        return new Panel(new Rows(shown))
        {
            Header = new PanelHeader($"Messages ({messages.Length})"),
            Border = BoxBorder.Rounded,
            BorderStyle = BorderOf(Pane.Results),
            Expand = true,
            Height = height,
        };
    }

    private IRenderable RenderFooter()
    {
        if (_confirmQuit)
        {
            return new Markup(
                "[black on red] TX OPEN [/] [white]Quitting rolls back the open transaction.[/]  " +
                "[aqua]CTRL+Q[/] Roll back and quit  [aqua]any other key[/] Stay");
        }

        if (_help)
        {
            return new Markup(
                "[grey]TAB pane · SHIFT+TAB back · CTRL+N/CTRL+P complete · CTRL+R, F5 or SHIFT+ENTER run statement · CTRL+G or F6 run all · " +
                "ESC or CTRL+C cancel · F2 limit · F4 refresh catalog · CTRL+S save · CTRL+L or CTRL+U clear · SPACE (catalog) insert name · " +
                "→/← (catalog) expand · ENTER (catalog) use database · ENTER (results) view value · C/SHIFT+C (results) copy cell/row · " +
                "M (results) messages · CTRL+A/CTRL+E line start/end · OPTION+←/→ word · OPTION+↑/↓ page · CTRL+Q quit[/]");
        }

        string limit = _limitOn ? $"[black on yellow] LIMIT {LimitRows} [/]" : "[grey] no limit [/]";
        string tx = _transaction is not null ? "  [black on red] TX OPEN [/]" : "";

        return new Markup(
            $"[aqua]CTRL+Q[/] Quit  [aqua]F1[/] Help  [aqua]CTRL+R[/] Run  [aqua]CTRL+G[/] Run all  [aqua]TAB[/] Pane  {limit}{tx}");
    }
}
