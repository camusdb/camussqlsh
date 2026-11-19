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
using System.Text;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// A full-screen mode for the shell. Three panes share one Spectre <see cref="Layout"/>: a catalog
/// tree on the left, a query editor at the top right, and a result grid below it. The editor and the
/// grid belong to the active tab. Each tab has its own text, rows and messages, and all tabs share one
/// connection and one transaction.
///
/// <para>Spectre draws. This class owns the focus ring, the tabs and the key loop, because Spectre has
/// no input or focus model of its own. The other parts are:</para>
/// <list type="bullet">
/// <item><see cref="TuiSession"/>: the connection, the current database and the transaction.</item>
/// <item><see cref="CatalogPane"/>: the Data Catalog.</item>
/// <item><see cref="QueryTab"/>: an <see cref="EditorBuffer"/> and a <see cref="ResultView"/>.</item>
/// <item><see cref="StatementRunner"/>: runs the statements of a tab, one run at a time.</item>
/// <item><see cref="EditorRenderer"/> and <see cref="ResultsRenderer"/>: draw the active tab.</item>
/// <item><see cref="TerminalInput"/>: decodes keys and pastes. <see cref="TabStore"/>: saves the tabs.</item>
/// </list>
///
/// <para>One <see cref="LiveDisplay"/> region covers the whole screen. Every frame rebuilds the layout
/// from scratch, so a terminal resize needs no special handling. The loop polls
/// <see cref="Console.KeyAvailable"/> instead of blocking in ReadKey, so the screen stays live while a
/// query runs and ESC can cancel it.</para>
/// </summary>
internal sealed class CamusTui
{
    private enum Pane { Catalog, Editor, Results }

    // Alt+1 to Alt+9 select a tab, so nine tabs can each have a key.
    private const int MaxTabs = 9;

    // The width of a tab title in the tab strip.
    private const int TitleWidth = 16;

    private readonly TuiSession _session;
    private readonly TuiContext _context;
    private readonly CatalogPane _catalog;
    private readonly StatementRunner _runner;
    private readonly SqlCompletion? _completion;
    private readonly EditorRenderer _editorRenderer;
    private readonly TabStore _store;
    private readonly TerminalInput _input = new();

    private readonly List<QueryTab> _tabs = new();
    private int _active;

    // The session that owns the terminal, so the prompt's SIGINT handler can hand the terminal back.
    // Null outside --tui.
    internal static CamusTui? Active { get; private set; }

    private Pane _focus = Pane.Editor;
    private bool _quit;

    // The F1 help panel. While it is open, it takes the keys: see HelpKey.
    private bool _help;
    private int _helpTop;
    private int _helpVisible = 1;

    // Set when a quit waits for the user to confirm the rollback of an open transaction.
    private bool _confirmQuit;

    // Set when the close of a tab with text waits for the user to confirm.
    private bool _confirmClose;

    // Set once ShutdownAsync starts, so the finally block and the SIGINT handler do not both run it.
    private int _shutDown;

    private bool _savedTreatControlC;

    private CamusTui(
        CamusConnection connection,
        string connectionString,
        Func<ColumnValue, string> format,
        IHighlighter highlighter,
        SqlCompletion? completion,
        TabStore store,
        bool readOnly)
    {
        _session = new TuiSession(connection, connectionString);
        _context = new TuiContext(format) { ReadOnly = readOnly };
        _completion = completion;
        _catalog = new CatalogPane(_session, _context, completion);
        _runner = new StatementRunner(_session, _context, _catalog, completion);
        _editorRenderer = new EditorRenderer(highlighter);
        _store = store;
    }

    internal static async Task RunAsync(
        CamusConnection connection,
        string connectionString,
        Func<ColumnValue, string> format,
        IHighlighter highlighter,
        SqlCompletion? completion,
        string tabsPath,
        string legacyQueryPath,
        bool readOnly)
    {
        CamusTui tui = new(connection, connectionString, format, highlighter, completion, new TabStore(tabsPath, legacyQueryPath), readOnly);
        await tui.LoopAsync();
    }

    private QueryTab Tab => _tabs[_active];

    private async Task LoopAsync()
    {
        if (Console.IsInputRedirected)
        {
            AnsiConsole.MarkupLine("[red]--tui needs a terminal.[/] Standard input is redirected.");
            return;
        }

        LoadTabs();
        await _catalog.StartAsync();

        Active = this;

        // Ctrl+C is a key here, not a signal. As a signal it reached the prompt's handler, which ends
        // the process at once: the finally block below never ran, so the terminal kept the keyboard
        // protocol, the cursor stayed hidden, and the TUI's transaction was not rolled back.
        _savedTreatControlC = Console.TreatControlCAsInput;
        Console.TreatControlCAsInput = true;

        AnsiConsole.Cursor.Hide();
        TerminalInput.EnableModes();

        try
        {
            await AnsiConsole.Live(Build())
                .AutoClear(false)
                .Overflow(VerticalOverflow.Crop)
                .StartAsync(async ctx =>
                {
                    int drawn = -1;
                    (int Width, int Height) drawnSize = (0, 0);

                    while (!_quit)
                    {
                        // A resize sends no key, so the loop compares the size to the last frame and
                        // draws again when it changed. Without this, parts of the old frame stayed on
                        // the screen until the next key.
                        (int Width, int Height) size = (TuiText.SafeWidth(), TuiText.SafeHeight());

                        if (size != drawnSize)
                        {
                            drawnSize = size;
                            _context.Repaint();
                        }

                        if (Console.KeyAvailable)
                        {
                            if (_input.Read() is { } input)
                            {
                                if (input.IsPaste)
                                    OnPaste(input.Paste!);
                                else
                                    OnKey(input.Key);
                            }

                            _context.Repaint();
                        }
                        else if (drawn != _context.Revision)
                        {
                            drawn = _context.Revision;
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

    // Gives the terminal back and ends the session. It runs from the finally block of the loop, and
    // from the prompt's SIGINT handler when a SIGINT comes from outside the terminal. The terminal is
    // restored first, because the SIGINT handler gives this method a few seconds only.
    internal async Task ShutdownAsync()
    {
        if (Interlocked.Exchange(ref _shutDown, 1) == 1)
            return;

        _runner.Cancel();
        SaveTabs();
        TerminalInput.DisableModes();
        Console.TreatControlCAsInput = _savedTreatControlC;
        AnsiConsole.Cursor.Show();

        foreach (QueryTab tab in _tabs)
            await tab.Results.ClosePageAsync();

        // An open transaction is the user's to finish, but leaving it open after the screen is gone
        // would strand locks with nothing left to release them.
        if (_session.TakeTransaction() is { } transaction)
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

    // ---------------------------------------------------------------- tabs

    private void LoadTabs()
    {
        (List<string> texts, int active) = _store.Load();

        foreach (string text in texts.Take(MaxTabs))
            _tabs.Add(new QueryTab(_context, text));

        if (_tabs.Count == 0)
            _tabs.Add(new QueryTab(_context));

        _active = Math.Clamp(active, 0, _tabs.Count - 1);
    }

    private void SaveTabs() => _store.Save(_tabs.Select(t => t.Editor.Text), _active);

    private void NewTab()
    {
        if (_tabs.Count >= MaxTabs)
        {
            Tab.Results.SetStatus($"There can be at most {MaxTabs} tabs. CTRL+D closes the active tab.");
            return;
        }

        Tab.Editor.EndCompletion();
        _tabs.Insert(_active + 1, new QueryTab(_context));
        _active++;
        _focus = Pane.Editor;
    }

    // Closing a tab with text asks first, because the text is lost. A tab with a run in progress does
    // not close: the run writes to it. The last tab does not close either: it becomes empty.
    private void CloseTab(bool confirmed)
    {
        QueryTab tab = Tab;

        if (_context.Running && _runner.RunningTab == tab)
        {
            tab.Results.SetStatus("A statement runs in this tab. ESC cancels it.", error: true);
            return;
        }

        if (!confirmed && !tab.Editor.IsEmpty)
        {
            _confirmClose = true;
            _help = false;
            return;
        }

        _ = tab.Results.ClosePageAsync();
        _tabs.RemoveAt(_active);

        if (_tabs.Count == 0)
            _tabs.Add(new QueryTab(_context));

        _active = Math.Min(_active, _tabs.Count - 1);
    }

    private void SelectTab(int index)
    {
        if (index < 0 || index >= _tabs.Count || index == _active)
            return;

        Tab.Editor.EndCompletion();
        _active = index;
    }

    private void NextTab(int step) => SelectTab((_active + step + _tabs.Count) % _tabs.Count);

    // ---------------------------------------------------------------- input

    private void OnPaste(string text)
    {
        // A paste always goes into the editor of the active tab, whatever pane has the focus. It
        // closes the help, so the text is in view.
        _help = false;
        _focus = Pane.Editor;
        Tab.Editor.InsertText(text);
    }

    private void OnKey(ConsoleKeyInfo k)
    {
        bool ctrl = (k.Modifiers & ConsoleModifiers.Control) != 0;
        bool alt = (k.Modifiers & ConsoleModifiers.Alt) != 0;

        // A quit that waits for confirmation takes the next key. Only a second quit key confirms. Any
        // other key keeps the session and is not used for anything else, because the user did not
        // type it for the pane.
        if (_confirmQuit)
        {
            _confirmQuit = false;

            if (ctrl && (k.Key == ConsoleKey.Q || k.Key == ConsoleKey.C))
                _quit = true;
            else
                Tab.Results.SetStatus("Quit cancelled. The transaction is still open.");

            return;
        }

        // The close of a tab with text takes the next key in the same way.
        if (_confirmClose)
        {
            _confirmClose = false;

            if (ctrl && k.Key == ConsoleKey.D)
                CloseTab(confirmed: true);
            else
                Tab.Results.SetStatus("The tab is still open.");

            return;
        }

        // The help covers the panes, so a key typed while it is open must not go to a pane that the
        // user cannot see. Only CTRL+C and CTRL+Q keep their meaning, and the keys of the help work.
        if (_help && !(ctrl && k.Key is ConsoleKey.C or ConsoleKey.Q))
        {
            HelpKey(k);
            return;
        }

        // Completion is CTRL+N and CTRL+P rather than Tab. Tab has to stay the pane ring: binding it
        // to completion inside the editor left no forward way out of that pane. Both are real control
        // characters, so every terminal delivers them.
        bool completing = _focus == Pane.Editor && ctrl && (k.Key == ConsoleKey.N || k.Key == ConsoleKey.P);

        if (!completing)
            Tab.Editor.EndCompletion();

        if (completing)
        {
            if (_completion is not null)
                Tab.Results.SetStatus(Tab.Editor.Complete(_completion, k.Key == ConsoleKey.N));
            return;
        }

        // Shift+Enter runs, matching every SQL editor people arrive from. It reaches here only on a
        // terminal that speaks the disambiguating keyboard protocol; F5 is the fallback. Enter with
        // Control is left alone on purpose: that is how a pasted newline arrives, so binding it to run
        // would make a pasted script execute itself.
        if (k.Key == ConsoleKey.Enter && (k.Modifiers & ConsoleModifiers.Shift) != 0) { _runner.RunCurrent(Tab); return; }

        // Ctrl+C stops a running query, the same as ESC. When nothing runs, it quits, as it does at
        // the prompt.
        if (ctrl && k.Key == ConsoleKey.C)
        {
            if (_context.Running) CancelRun();
            else RequestQuit();
            return;
        }

        if (ctrl && k.Key == ConsoleKey.Q) { RequestQuit(); return; }
        if (k.Key == ConsoleKey.F1) { _help = true; _helpTop = 0; return; }
        // Each run has a control key beside its function key. On a Mac keyboard a function key needs
        // Fn too, but a control key is a real control character that every terminal sends.
        if (k.Key == ConsoleKey.F5 || (ctrl && k.Key == ConsoleKey.R)) { _runner.RunCurrent(Tab); return; }
        if (k.Key == ConsoleKey.F6 || (ctrl && k.Key == ConsoleKey.G)) { _runner.Start(Tab, Tab.Editor.Text); return; }
        if (k.Key == ConsoleKey.F2) { _context.LimitOn = !_context.LimitOn; return; }
        if (k.Key == ConsoleKey.F4) { _ = RefreshCatalogAsync(Tab); return; }

        if (ctrl && k.Key == ConsoleKey.S)
        {
            SaveTabs();
            Tab.Results.SetStatus($"Saved {_tabs.Count} {(_tabs.Count == 1 ? "tab" : "tabs")} to {_store.Path}");
            return;
        }

        // The editor reopens with the previous session's text, so there has to be a way to empty it
        // in one keystroke. CTRL+L is what a terminal user reaches for to clear, and CTRL+U is the
        // readline habit for the same thing.
        if (ctrl && (k.Key == ConsoleKey.L || k.Key == ConsoleKey.U)) { Tab.Editor.Clear(); _focus = Pane.Editor; return; }

        if (TabKey(k, ctrl, alt))
            return;

        if (k.Key == ConsoleKey.Escape)
        {
            if (_context.Running) CancelRun();
            else if (Tab.Results.ValueView) Tab.Results.ValueView = false;
            else if (Tab.Results.ShowMessages) Tab.Results.ShowMessages = false;
            return;
        }

        k = TerminalInput.Translate(k);

        if (k.Key == ConsoleKey.Tab)
        {
            int step = (k.Modifiers & ConsoleModifiers.Shift) != 0 ? 2 : 1;
            _focus = (Pane)(((int)_focus + step) % 3);
            return;
        }

        switch (_focus)
        {
            case Pane.Catalog:
                _catalog.HandleKey(k, sql => _runner.Start(Tab, sql), name => EditorBuffer.Type(Tab.Editor.Current, name));
                break;
            case Pane.Editor:
                Tab.Editor.HandleKey(k);
                break;
            case Pane.Results:
                Tab.Results.HandleKey(k);
                break;
        }
    }

    // The tab keys. CTRL+T opens a tab and CTRL+D closes one. CTRL+O goes to the next tab, and works
    // on every terminal. F7 and F8 go back and forward. CTRL+PageUp and CTRL+PageDown do the same
    // where the terminal sends them, and Alt+1 to Alt+9 go to one tab where Option or Alt sends ESC.
    private bool TabKey(ConsoleKeyInfo k, bool ctrl, bool alt)
    {
        if (ctrl && k.Key == ConsoleKey.T) { NewTab(); return true; }
        if (ctrl && k.Key == ConsoleKey.D) { CloseTab(confirmed: false); return true; }
        if (ctrl && k.Key == ConsoleKey.O) { NextTab(1); return true; }
        if (k.Key == ConsoleKey.F7 || (ctrl && k.Key == ConsoleKey.PageUp)) { NextTab(-1); return true; }
        if (k.Key == ConsoleKey.F8 || (ctrl && k.Key == ConsoleKey.PageDown)) { NextTab(1); return true; }

        if (alt && !ctrl && k.Key is >= ConsoleKey.D1 and <= ConsoleKey.D9)
        {
            SelectTab(k.Key - ConsoleKey.D1);
            return true;
        }

        return false;
    }

    private void HelpKey(ConsoleKeyInfo k)
    {
        switch (TerminalInput.Translate(k).Key)
        {
            case ConsoleKey.F1 or ConsoleKey.Escape or ConsoleKey.Enter or ConsoleKey.Q: _help = false; break;
            case ConsoleKey.UpArrow: _helpTop = Math.Max(0, _helpTop - 1); break;
            case ConsoleKey.DownArrow: _helpTop++; break;
            case ConsoleKey.PageUp: _helpTop = Math.Max(0, _helpTop - _helpVisible); break;
            case ConsoleKey.PageDown: _helpTop += _helpVisible; break;
            case ConsoleKey.Home: _helpTop = 0; break;
            case ConsoleKey.End: _helpTop = int.MaxValue; break;
        }
    }

    private void CancelRun()
    {
        _runner.Cancel();
        (_runner.RunningTab ?? Tab).Results.SetStatus("Cancelling…");
    }

    // Quitting rolls back an open transaction, which can lose work. So with a transaction open, the
    // first quit key only asks, and a second one confirms.
    private void RequestQuit()
    {
        if (_session.Transaction is null)
        {
            _quit = true;
            return;
        }

        _confirmQuit = true;
        _help = false;
    }

    // F4. The status and an error go to the tab that was active when the key was pressed.
    private async Task RefreshCatalogAsync(QueryTab tab)
    {
        tab.Results.SetStatus("Refreshing the catalog…");

        if (await _catalog.RefreshFromKeyAsync() is not { } outcome)
            return;

        tab.Results.SetStatus(outcome.Status, outcome.Error);

        if (outcome.Error)
            tab.Results.AddMessage("", outcome.Status, true);
    }

    // ---------------------------------------------------------------- render

    private Layout Build()
    {
        int width = TuiText.SafeWidth();
        int height = TuiText.SafeHeight();

        QueryTab tab = Tab;

        int body = Math.Max(6, height - 1);
        // The value view is for reading a long value, so the editor gives it most of its room.
        int editorHeight = tab.Results.ValueView ? 5 : Math.Max(5, body / 2);

        int leftWidth = Math.Clamp(width / 4, 20, 40);
        int rightWidth = Math.Max(20, width - leftWidth);

        if (_help)
        {
            return new Layout("root").SplitRows(
                new Layout("body").Size(body).Update(HelpView.Render(body, width, ref _helpTop, out _helpVisible)),
                new Layout("footer").Size(1).Update(RenderFooter()));
        }

        bool running = _context.Running && _runner.RunningTab == tab;

        IRenderable editor = _editorRenderer.Render(tab.Editor, editorHeight, rightWidth, _focus == Pane.Editor, TabStrip(rightWidth - 6));
        IRenderable results = ResultsRenderer.Render(tab.Results, body - editorHeight, rightWidth, _focus == Pane.Results, running);

        return new Layout("root").SplitRows(
            new Layout("body").Size(body).SplitColumns(
                new Layout("left").Size(leftWidth).Update(_catalog.Render(body, leftWidth, _focus == Pane.Catalog)),
                new Layout("right").SplitRows(
                    new Layout("editor").Size(editorHeight).Update(editor),
                    new Layout("results").Update(results))),
            new Layout("footer").Size(1).Update(RenderFooter()));
    }

    // The tabs, in the header of the editor panel: " 1 select * from t " for each tab. The active tab
    // is inverted, and a tab with a run in progress has a dot. When the tabs do not fit, the strip
    // shows the tabs near the active one, with ‹ and › for the tabs that it does not show.
    private string TabStrip(int width)
    {
        string[] labels = new string[_tabs.Count];

        for (int i = 0; i < _tabs.Count; i++)
        {
            bool running = _context.Running && _runner.RunningTab == _tabs[i];
            labels[i] = $" {i + 1} {_tabs[i].Title(TitleWidth)}{(running ? " ●" : "")} ";
        }

        int first = _active;
        int last = _active;
        int used = labels[_active].Length;

        // Grow the window to both sides while the next label fits. Two cells stay free for ‹ and ›.
        bool grew = true;

        while (grew)
        {
            grew = false;

            if (last + 1 < labels.Length && used + labels[last + 1].Length + 1 <= width - 4)
            {
                last++;
                used += labels[last].Length + 1;
                grew = true;
            }

            if (first > 0 && used + labels[first - 1].Length + 1 <= width - 4)
            {
                first--;
                used += labels[first].Length + 1;
                grew = true;
            }
        }

        StringBuilder strip = new();

        if (first > 0)
            strip.Append("[grey]‹[/] ");

        for (int i = first; i <= last; i++)
        {
            if (i > first)
                strip.Append("[grey35]│[/]");

            string label = Markup.Escape(labels[i]);
            strip.Append(i == _active ? $"[black on yellow]{label}[/]" : $"[grey]{label}[/]");
        }

        if (last < labels.Length - 1)
            strip.Append(" [grey]›[/]");

        return strip.ToString();
    }

    private IRenderable RenderFooter()
    {
        if (_confirmQuit)
        {
            return new Markup(
                "[black on red] TX OPEN [/] [white]Quitting rolls back the open transaction.[/]  " +
                "[aqua]CTRL+Q[/] Roll back and quit  [aqua]any other key[/] Stay");
        }

        if (_confirmClose)
        {
            return new Markup(
                "[black on yellow] CLOSE TAB [/] [white]The text of this tab is lost.[/]  " +
                "[aqua]CTRL+D[/] Close the tab  [aqua]any other key[/] Stay");
        }

        if (_help)
            return new Markup("[aqua]F1[/], [aqua]ESC[/] or [aqua]Q[/] Close the help  [aqua]↑/↓[/] Scroll  [aqua]CTRL+Q[/] Quit");

        string limit = _context.LimitOn ? $"[black on yellow] LIMIT {TuiContext.LimitRows} [/]" : "[grey] no limit [/]";
        string readOnly = _context.ReadOnly ? "  [black on green] READ-ONLY [/]" : "";
        string tx = _session.Transaction is not null ? "  [black on red] TX OPEN [/]" : "";

        // A run in another tab is not in sight, so the footer says where it is.
        int runningIndex = _runner.RunningTab is { } runningTab ? _tabs.IndexOf(runningTab) : -1;
        string elsewhere = _context.Running && runningIndex >= 0 && runningIndex != _active
            ? $"  [black on aqua] RUNNING IN TAB {runningIndex + 1} [/]"
            : "";

        return new Markup(
            $"[aqua]CTRL+Q[/] Quit  [aqua]F1[/] Help  [aqua]CTRL+R[/] Run  [aqua]CTRL+G[/] Run all  [aqua]CTRL+T[/] New tab  [aqua]TAB[/] Pane  {limit}{readOnly}{tx}{elsewhere}");
    }
}
