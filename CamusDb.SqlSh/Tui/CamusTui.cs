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

    // One row of the flattened catalog tree. The tree is stored flat because selection is an
    // index, and Spectre's Tree carries no selection state to move.
    private sealed record CatalogRow(string Table, string? Column, string? Type);

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

    // The connection is replaced by `use`, so none of these are readonly.
    private CamusConnection _connection;
    private string _connectionString;
    private string _database;
    private CamusTransaction? _transaction;

    private Pane _focus = Pane.Editor;
    private bool _quit;
    private bool _help;

    // Catalog. _columns is filled lazily, one DESCRIBE per table, the first time it expands.
    private readonly List<string> _tables = new();
    private readonly Dictionary<string, List<(string Name, string Type)>> _columns = new(StringComparer.OrdinalIgnoreCase);
    private readonly HashSet<string> _expanded = new(StringComparer.OrdinalIgnoreCase);
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

    // Results.
    private string[] _headers = [];
    private readonly List<string[]> _rows = new();
    private int _rowTop;
    private int _colLeft;
    private int _visibleRows = 1;
    private string _status = "Ready. F5 runs the statements. F1 lists the keys.";
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
        _database = GetConnValue(connectionString, "Database") is { Length: > 0 } db ? db : "(none)";
        _format = format;
        _highlighter = highlighter;
        _completion = completion;
        _queryPath = queryPath;
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
        await LoadTablesAsync();

        AnsiConsole.Cursor.Hide();
        EnableKeyDisambiguation();

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
                                OnKey(key);

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
            _cancel?.Cancel();
            await ClosePageAsync();
            SaveQuery();
            DisableKeyDisambiguation();
            AnsiConsole.Cursor.Show();
        }

        // An open transaction is the user's to finish, but leaving it open after the screen
        // is gone would strand locks with nothing left to release them.
        if (_transaction is not null)
        {
            AnsiConsole.MarkupLine("[yellow]Rolling back the active transaction…[/]");

            try
            {
                await _transaction.RollbackAsync();
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

    // Returns null when the bytes read were a sequence with no key of its own to report.
    private static ConsoleKeyInfo? NextKey()
    {
        ConsoleKeyInfo k = Console.ReadKey(intercept: true);

        // A lone Escape has nothing behind it, so it stays an Escape.
        if (k.Key != ConsoleKey.Escape || !Console.KeyAvailable)
            return k;

        char next = Console.ReadKey(intercept: true).KeyChar;

        // ESC followed by anything other than '[' is the terminal's way of writing Alt+key.
        if (next != '[')
            return new ConsoleKeyInfo(next, KeyOf(next), false, true, false);

        StringBuilder body = new();
        char final = '\0';

        // A CSI sequence runs until a byte in the 0x40-0x7E range closes it.
        while (Console.KeyAvailable)
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

    private static ConsoleKeyInfo? Decode(string body, char final)
    {
        string[] parts = body.Split(';');

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
        if (k.Key == ConsoleKey.Enter && (k.Modifiers & ConsoleModifiers.Shift) != 0) { StartRun(); return; }

        if (ctrl && k.Key == ConsoleKey.Q) { _quit = true; return; }
        if (k.Key == ConsoleKey.F1) { _help = !_help; return; }
        if (k.Key == ConsoleKey.F5 || (ctrl && k.Key == ConsoleKey.R)) { StartRun(); return; }
        if (k.Key == ConsoleKey.F2) { _limitOn = !_limitOn; return; }
        if (ctrl && k.Key == ConsoleKey.S) { SaveQuery(); _status = $"Saved to {_queryPath}"; _statusIsError = false; return; }

        // The editor reopens with the previous session's text, so there has to be a way to
        // empty it in one keystroke. CTRL+L is what a terminal user reaches for to clear, and
        // CTRL+U is the readline habit for the same thing.
        if (ctrl && (k.Key == ConsoleKey.L || k.Key == ConsoleKey.U)) { ClearEditor(); return; }

        if (k.Key == ConsoleKey.Escape)
        {
            if (_running) { _cancel?.Cancel(); _status = "Cancelling…"; }
            else if (_help) _help = false;
            return;
        }

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

    private void CatalogKey(ConsoleKeyInfo k)
    {
        List<CatalogRow> rows = FlatCatalog();
        if (rows.Count == 0)
            return;

        int at = Math.Min(_catalogSel, rows.Count - 1);

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: _catalogSel = Math.Min(rows.Count - 1, _catalogSel + 1); break;
            case ConsoleKey.UpArrow: _catalogSel = Math.Max(0, _catalogSel - 1); break;
            case ConsoleKey.Home: _catalogSel = 0; break;
            case ConsoleKey.End: _catalogSel = rows.Count - 1; break;

            case ConsoleKey.RightArrow:
            case ConsoleKey.Enter:
                if (rows[at].Column is null && _expanded.Add(rows[at].Table))
                    _ = DescribeAsync(rows[at].Table);
                break;

            case ConsoleKey.LeftArrow:
                _expanded.Remove(rows[at].Table);
                break;

            // Space drops the selected name into the editor at the cursor, which is the
            // whole point of having the catalog next to the editor.
            case ConsoleKey.Spacebar:
                Type(Current, rows[at].Column ?? rows[at].Table);
                break;
        }
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

    private void EditorKey(ConsoleKeyInfo k)
    {
        LineBuffer line = Current;

        switch (k.Key)
        {
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
        int last = Math.Max(0, _rows.Count - 1);

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: _rowTop = Math.Min(last, _rowTop + 1); break;
            case ConsoleKey.UpArrow: _rowTop = Math.Max(0, _rowTop - 1); break;
            case ConsoleKey.PageDown: _rowTop = Math.Min(last, _rowTop + _visibleRows); break;
            case ConsoleKey.PageUp: _rowTop = Math.Max(0, _rowTop - _visibleRows); break;
            case ConsoleKey.Home: _rowTop = 0; _colLeft = 0; break;
            case ConsoleKey.End: _rowTop = last; break;
            // Horizontal movement is by whole column, not by character. A wide result set
            // is read column by column, and this keeps every header aligned with its cells.
            case ConsoleKey.RightArrow: _colLeft = Math.Min(Math.Max(0, _headers.Length - 1), _colLeft + 1); break;
            case ConsoleKey.LeftArrow: _colLeft = Math.Max(0, _colLeft - 1); break;
        }

        MaybeFetchPage();
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

    private async Task LoadTablesAsync()
    {
        _tables.Clear();
        _columns.Clear();
        _expanded.Clear();
        _catalogSel = 0;
        _catalogTop = 0;

        foreach (string statement in new[] { "show tables", "show views", "show materialized views" })
        {
            try
            {
                await using CamusCommand cmd = _connection.CreateSelectCommand(statement);
                cmd.CommandTimeout = 10;

                CamusDataReader reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
                    string? name = row.Values.FirstOrDefault().StrValue;
                    if (!string.IsNullOrWhiteSpace(name))
                        _tables.Add(name);
                }
            }
            catch
            {
                // A server without views, or no database selected. Show whatever did load.
            }
        }

        _tables.Sort(StringComparer.OrdinalIgnoreCase);
        _revision++;
    }

    // DESCRIBE runs off the key loop so the screen never freezes on a slow catalog read.
    private async Task DescribeAsync(string table)
    {
        if (_columns.ContainsKey(table))
            return;

        List<(string, string)> cols = new();

        try
        {
            await using CamusCommand cmd = _connection.CreateSelectCommand($"describe {table}");
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
        }
        catch
        {
            // Leave the table expanded but empty rather than dropping the user back a level.
        }

        _columns[table] = cols;
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

    private void StartRun()
    {
        if (_running)
            return;

        string text = Sql.Trim();

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
        List<string> log = new();

        try
        {
            await ClosePageAsync();

            foreach ((string sql, bool _) in EscapeStringIntoLines(NormalizeSmartQuotes(text)))
            {
                if (token.IsCancellationRequested)
                {
                    log.Add("cancelled");
                    break;
                }

                await RunStatementAsync(sql, log, token);
            }

            _statusIsError = false;
        }
        catch (Exception ex)
        {
            log.Add(ex.Message);
            _statusIsError = true;
        }
        finally
        {
            _status = log.Count == 0 ? "Nothing to run." : string.Join("   ·   ", log);
            _running = false;
            _revision++;
        }
    }

    private async Task RunStatementAsync(string sql, List<string> log, CancellationToken token)
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
            log.Add("no database selected — run `use <database>`");
            _statusIsError = true;
            return;
        }

        Stopwatch stopwatch = Stopwatch.StartNew();

        if (IsSystemLevelQuery(sql))
        {
            await using CamusConnection sys = await ConnectionHelper.OpenAsync(GetEndpointConnectionString(_connectionString));
            log.Add(await QueryAsync(sys, sql, token));
        }
        else if (IsQueryable(sql))
        {
            log.Add(await QueryAsync(_connection, sql, token));
        }
        else if (IsServerLevelDDL(sql))
        {
            await using CamusConnection sys = await ConnectionHelper.OpenAsync(GetEndpointConnectionString(_connectionString));
            await DdlAsync(sys, sql);
            log.Add($"OK ({stopwatch.Elapsed})");
        }
        else if (IsDDL(sql))
        {
            await DdlAsync(_connection, sql);
            log.Add($"OK ({stopwatch.Elapsed})");

            // A CREATE or DROP changes what the catalog should show.
            if (ChangesTableSet(sql))
                await RefreshCatalogAsync();
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

        if (_completion is not null)
            await _completion.RefreshTablesAsync(_connection);

        await RefreshCatalogAsync();
    }

    private async Task RefreshCatalogAsync()
    {
        await LoadTablesAsync();

        if (_completion is not null)
            await _completion.RefreshTablesAsync(_connection);
    }

    private static async Task DdlAsync(CamusConnection connection, string sql)
    {
        using CamusCommand cmd = connection.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;

        await cmd.ExecuteDDLAsync();
    }

    // Reads the first page and leaves the reader open, so scrolling can pull the rest.
    private async Task<string> QueryAsync(CamusConnection connection, string sql, CancellationToken token)
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

        _headers = [];
        _rows.Clear();
        _rowTop = 0;
        _colLeft = 0;

        int wanted = _limitOn ? Math.Min(LimitRows, PageRows) : PageRows;
        int got = await FetchAsync(wanted, token);

        return _pageDone
            ? $"{_rows.Count} rows in {stopwatch.Elapsed}"
            : $"{got} rows so far (first in {first}) — scroll for more";
    }

    // Pulls up to `count` more rows off the open reader. Returns how many arrived.
    private async Task<int> FetchAsync(int count, CancellationToken token)
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

                _rows.Add(row.Values.Select(_format).ToArray());
                got++;
            }
        }
        catch (Exception ex)
        {
            _pageDone = true;
            _status = ex.Message;
            _statusIsError = true;
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

        if (_rowTop + _visibleRows + PageMargin < _rows.Count)
            return;

        _fetching = true;

        _ = Task.Run(async () =>
        {
            try
            {
                int got = await FetchAsync(PageRows, _cancel?.Token ?? CancellationToken.None);

                if (_pageDone)
                    _status = $"{_rows.Count} rows (end of result)";
                else if (got > 0)
                    _status = $"{_rows.Count} rows so far — scroll for more";
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

        foreach (string table in _tables)
        {
            rows.Add(new CatalogRow(table, null, null));

            if (!_expanded.Contains(table))
                continue;

            if (_columns.TryGetValue(table, out List<(string Name, string Type)>? cols))
                foreach ((string name, string type) in cols)
                    rows.Add(new CatalogRow(table, name, type));
            else
                rows.Add(new CatalogRow(table, "…", null));
        }

        return rows;
    }

    private Layout Build()
    {
        int width = SafeWidth();
        int height = SafeHeight();

        int body = Math.Max(6, height - 1);
        int editorHeight = Math.Max(5, body / 2);

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

        // Two border rows, one header row for the database node.
        int visible = Math.Max(1, height - 4);

        if (_catalogSel < _catalogTop) _catalogTop = _catalogSel;
        if (_catalogSel >= _catalogTop + visible) _catalogTop = _catalogSel - visible + 1;
        _catalogTop = Math.Clamp(_catalogTop, 0, Math.Max(0, rows.Count - 1));

        // Panel borders take two cells and Panel padding takes two more.
        int inner = Math.Max(4, width - 4);

        Grid grid = new();
        grid.AddColumn(new GridColumn().NoWrap());

        grid.AddRow($"[bold]{Markup.Escape(Clip(_database, inner - 3))}[/] [grey]db[/]");

        for (int i = _catalogTop; i < Math.Min(rows.Count, _catalogTop + visible); i++)
        {
            CatalogRow row = rows[i];
            bool selected = i == _catalogSel && rows.Count > 0;

            string label;

            if (row.Column is null)
            {
                string marker = _expanded.Contains(row.Table) ? "▼" : "▶";
                // " ▶ " ahead of the name, " t" after it.
                string name = Markup.Escape(Clip(row.Table, inner - 5));
                label = selected
                    ? $" {marker} [black on aqua]{name}[/] [grey]t[/]"
                    : $" {marker} [white]{name}[/] [grey]t[/]";
            }
            else
            {
                // "   ├ " ahead of the name, then a space and the type after it.
                string type = Clip(row.Type ?? "", 7);
                string name = Markup.Escape(Clip(row.Column, Math.Max(3, inner - 6 - type.Length)));
                type = Markup.Escape(type);
                label = selected
                    ? $"   ├ [black on aqua]{name}[/] [grey]{type}[/]"
                    : $"   ├ [grey78]{name}[/] [grey]{type}[/]";
            }

            grid.AddRow(label);
        }

        if (rows.Count == 0)
            grid.AddRow("[grey](no tables)[/]");

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

        for (int i = _editorTop; i < Math.Min(_lines.Count, _editorTop + visible); i++)
        {
            bool onCursor = i == _row && _focus == Pane.Editor;
            rendered.Add(new Markup($"[grey]{i + 1,3}[/]  {RenderLine(_lines[i], onCursor, _editorLeft, textWidth)}"));
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
    private string RenderLine(LineBuffer line, bool showCursor, int left, int width)
    {
        string text = line.Content;
        int cursor = Math.Clamp(line.Position, 0, text.Length);

        StringBuilder markup = new();
        int i = 0;

        // Tokenizing starts at the beginning of the line even when the window does not, so a
        // keyword that straddles the left edge keeps its colour.
        while (i < text.Length && i < left + width)
        {
            int start = i;

            if (char.IsLetterOrDigit(text[i]) || text[i] == '_')
                while (i < text.Length && (char.IsLetterOrDigit(text[i]) || text[i] == '_')) i++;
            else
                i++;

            string token = text[start..i];

            // `is { }` produces a non-null Style whether the highlighter's return is a
            // nullable reference or a nullable value type, so this compiles either way.
            string? tag = _highlighter.Highlight(token) is { } style ? Describe(style) : null;

            for (int j = 0; j < token.Length; j++)
            {
                int at = start + j;

                if (at < left || at >= left + width)
                    continue;

                string ch = Markup.Escape(token[j].ToString());

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

    // Spectre 0.57.2 exposes no Style-to-markup conversion this build can see, so the
    // markup tag is built from the parts the shell's highlighter actually sets.
    private static string Describe(Style style)
    {
        string colour = style.Foreground.ToString();

        return style.Decoration == Decoration.None ? colour : $"{style.Decoration} {colour}".ToLowerInvariant();
    }

    private IRenderable RenderResults(int height, int width)
    {
        // Two border rows, one header row, one status row.
        int visible = Math.Max(1, height - 4);
        _visibleRows = visible;

        if (_rowTop > 0 && _rowTop >= _rows.Count)
            _rowTop = Math.Max(0, _rows.Count - 1);

        Table table = new()
        {
            Border = TableBorder.None,
            Expand = true,
        };

        string[] shown = _headers.Skip(_colLeft).ToArray();

        if (shown.Length == 0)
        {
            table.AddColumn(new TableColumn("[grey](no result)[/]"));
        }
        else
        {
            foreach (string header in shown)
                table.AddColumn(new TableColumn($"[bold yellow]{Markup.Escape(Clip(header, 24))}[/]").NoWrap());

            foreach (string[] row in _rows.Skip(_rowTop).Take(visible))
                table.AddRow(row.Skip(_colLeft).Select(c => Clip(c, 40)).ToArray());
        }

        string caption = _running
            ? "[yellow]running…[/]  ESC cancels"
            : _statusIsError
                ? $"[red]{Markup.Escape(Clip(_status, width - 4))}[/]"
                : $"[grey]{Markup.Escape(Clip(_status, width - 4))}[/]";

        string more = _pageDone ? "" : "+";

        string title = _rows.Count > 0
            ? $"Query Results ({_rows.Count}{more} rows)  rows {_rowTop + 1}-{Math.Min(_rows.Count, _rowTop + visible)}  cols {_colLeft + 1}-{_headers.Length}"
            : "Query Results";

        return new Panel(new Rows(table, new Markup(caption)))
        {
            Header = new PanelHeader(title),
            Border = BoxBorder.Rounded,
            BorderStyle = BorderOf(Pane.Results),
            Expand = true,
            Height = height,
        };
    }

    private IRenderable RenderFooter()
    {
        if (_help)
        {
            return new Markup(
                "[grey]TAB pane · SHIFT+TAB back · CTRL+N/CTRL+P complete · SHIFT+ENTER or F5 run · ESC cancel · " +
                "F2 limit · CTRL+S save · CTRL+L or CTRL+U clear · SPACE (catalog) insert name · →/← (catalog) expand · CTRL+Q quit[/]");
        }

        string limit = _limitOn ? $"[black on yellow] LIMIT {LimitRows} [/]" : "[grey] no limit [/]";
        string tx = _transaction is not null ? "  [black on red] TX OPEN [/]" : "";

        return new Markup(
            $"[aqua]CTRL+Q[/] Quit  [aqua]F1[/] Help  [aqua]SHIFT+ENTER[/]/[aqua]F5[/] Run  [aqua]TAB[/] Pane  {limit}{tx}");
    }
}
