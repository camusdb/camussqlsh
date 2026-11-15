/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using static SqlKind;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// The results of one tab: the grid, the value view, the messages, the status line and the open
/// reader behind the grid. Every tab has one, so a tab keeps its rows when another tab runs.
///
/// <para>The cells hold plain text: markup escaping happens when a cell is drawn, after it is clipped,
/// so a clip never cuts an escaped "[[" in half. <see cref="SelRow"/> and <see cref="SelCol"/> are the
/// selected cell, and the window (<see cref="RowTop"/>, <see cref="ColLeft"/>) follows it.</para>
///
/// <para>Each method that changes the grid works on this view only. A run captures its tab when it
/// starts, so a change of tab during the run does not send the rows to the wrong grid.</para>
/// </summary>
internal sealed class ResultView
{
    private const int MaxMessages = 500;

    private readonly TuiContext _context;

    public ResultView(TuiContext context)
    {
        _context = context;
    }

    public string[] Headers = [];
    public readonly List<string[]> Rows = new();
    public int RowTop;
    public int ColLeft;
    public int SelRow;
    public int SelCol;
    public int VisibleRows = 1;

    // The value view shows the full text of the selected cell, or with RowView the whole row, one
    // column under the other. It takes the place of the grid.
    public bool ValueView;
    public bool RowView;
    public int ViewTop;
    public int ViewVisible = 1;

    // The messages area keeps the outcome of every statement of this tab, so a script's log is not
    // lost when the status line is too short for it. It takes the place of the grid too.
    private readonly List<TuiMessage> _messages = new();
    public bool ShowMessages;
    public int MessageTop;
    public bool MessageFollow = true;
    public int MessageVisible = 1;

    public string Status = "Ready. CTRL+R runs the statement at the cursor, CTRL+G runs them all. F1 lists the keys.";
    public bool StatusIsError;

    // The open reader behind the grid. Rows arrive a page at a time as the user scrolls, so a wide
    // scan costs one page of memory rather than the whole result.
    private CamusCommand? _pageCommand;
    private CamusDataReader? _pageReader;
    public bool PageDone { get; private set; } = true;
    private bool _fetching;

    /// <summary>A new result replaces the grid. The runner compares this number to see that it did.</summary>
    public int GridVersion { get; private set; }

    public string[]? SelectedRow => SelRow < Rows.Count ? Rows[SelRow] : null;

    public void SetStatus(string text, bool error = false)
    {
        Status = text;
        StatusIsError = error;
        _context.Repaint();
    }

    public TuiMessage[] Messages()
    {
        lock (_messages)
            return _messages.ToArray();
    }

    public void AddMessage(string sql, string text, bool error)
    {
        lock (_messages)
        {
            _messages.Add(new TuiMessage(DateTime.Now, sql, text, error));

            if (_messages.Count > MaxMessages)
                _messages.RemoveRange(0, _messages.Count - MaxMessages);
        }

        _context.Repaint();
    }

    // ---------------------------------------------------------------- reader

    /// <summary>Empties the grid and puts a new reader behind it.</summary>
    public void StartGrid(CamusCommand? command, CamusDataReader reader)
    {
        _pageCommand = command;
        _pageReader = reader;
        PageDone = false;

        Headers = [];
        Rows.Clear();
        RowTop = 0;
        ColLeft = 0;
        SelRow = 0;
        SelCol = 0;
        ValueView = false;
        GridVersion++;
    }

    /// <summary>
    /// Reads up to <paramref name="count"/> more rows from the open reader. Returns how many arrived.
    /// A read error goes into <paramref name="log"/>, and the grid keeps the rows that arrived before it.
    /// </summary>
    public async Task<int> FetchAsync(int count, RunLog log, CancellationToken token)
    {
        if (_pageReader is not { } reader || PageDone)
            return 0;

        int got = 0;

        try
        {
            while (got < count)
            {
                if (token.IsCancellationRequested)
                    break;

                if (_context.LimitOn && Rows.Count >= TuiContext.LimitRows)
                {
                    // The cap is a display cap, not a SQL LIMIT. Rows past it are never read, which
                    // is what keeps a wide scan from filling memory.
                    PageDone = true;
                    break;
                }

                if (!await reader.ReadAsync())
                {
                    PageDone = true;
                    break;
                }

                Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);

                if (Headers.Length == 0)
                    Headers = row.Keys.ToArray();

                Rows.Add(row.Values.Select(v => TuiText.Unescape(_context.Format(v))).ToArray());
                got++;
            }
        }
        catch (Exception ex)
        {
            PageDone = true;
            log.Fail(ex.Message);
        }

        if (PageDone)
            await ClosePageAsync();

        _context.Repaint();
        return got;
    }

    // Called after every scroll. Fetching starts before the window reaches the end of the buffer, so
    // a held-down arrow key does not stall waiting for rows. No page is read while a run is in
    // progress, because the run uses the same connection.
    private void MaybeFetchPage()
    {
        if (PageDone || _fetching || _context.Running || _pageReader is null)
            return;

        if (SelRow + VisibleRows + TuiContext.PageMargin < Rows.Count)
            return;

        _fetching = true;

        _ = Task.Run(async () =>
        {
            try
            {
                RunLog log = new();
                int got = await FetchAsync(TuiContext.PageRows, log, CancellationToken.None);

                if (log.Failed)
                {
                    string error = log.Entries.First(e => e.Error).Text;
                    AddMessage("", error, true);
                    SetStatus(error, error: true);
                }
                else if (PageDone)
                {
                    SetStatus($"{Rows.Count} rows (end of result)");
                }
                else if (got > 0)
                {
                    SetStatus($"{Rows.Count} rows so far — scroll for more");
                }
            }
            finally
            {
                _fetching = false;
                _context.Repaint();
            }
        });
    }

    public async Task ClosePageAsync()
    {
        _pageReader = null;
        PageDone = true;

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

    // ---------------------------------------------------------------- keys

    public void HandleKey(ConsoleKeyInfo k)
    {
        if (ValueView)
        {
            ValueViewKey(k);
            return;
        }

        if (ShowMessages)
        {
            MessagesKey(k);
            return;
        }

        int last = Math.Max(0, Rows.Count - 1);
        int lastCol = Math.Max(0, Headers.Length - 1);

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: SelRow = Math.Min(last, SelRow + 1); break;
            case ConsoleKey.UpArrow: SelRow = Math.Max(0, SelRow - 1); break;
            case ConsoleKey.PageDown: SelRow = Math.Min(last, SelRow + VisibleRows); break;
            case ConsoleKey.PageUp: SelRow = Math.Max(0, SelRow - VisibleRows); break;
            case ConsoleKey.Home: SelRow = 0; SelCol = 0; break;
            case ConsoleKey.End: SelRow = last; break;
            // Horizontal movement is by whole column, not by character. A wide result set is read
            // column by column, and this keeps every header aligned with its cells.
            case ConsoleKey.RightArrow: SelCol = Math.Min(lastCol, SelCol + 1); break;
            case ConsoleKey.LeftArrow: SelCol = Math.Max(0, SelCol - 1); break;

            case ConsoleKey.Enter:
                if (Rows.Count > 0)
                {
                    ValueView = true;
                    ViewTop = 0;
                }
                break;

            default:
                switch (k.KeyChar)
                {
                    case 'c': CopyCell(); break;
                    case 'C': CopyRow(); break;
                    case 'm' or 'M': ShowMessages = true; break;
                }
                break;
        }

        MaybeFetchPage();
    }

    private void ValueViewKey(ConsoleKeyInfo k)
    {
        int last = Math.Max(0, Rows.Count - 1);
        int lastCol = Math.Max(0, Headers.Length - 1);

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: ViewTop++; break;
            case ConsoleKey.UpArrow: ViewTop = Math.Max(0, ViewTop - 1); break;
            case ConsoleKey.PageDown: ViewTop += ViewVisible; break;
            case ConsoleKey.PageUp: ViewTop = Math.Max(0, ViewTop - ViewVisible); break;
            case ConsoleKey.Home: ViewTop = 0; break;
            case ConsoleKey.End: ViewTop = int.MaxValue; break;

            // Left and Right go to the next value: the next column of the row in the cell view, the
            // next row in the row view. Both views keep the grid selection in step.
            case ConsoleKey.LeftArrow:
                if (RowView) SelRow = Math.Max(0, SelRow - 1); else SelCol = Math.Max(0, SelCol - 1);
                ViewTop = 0;
                break;

            case ConsoleKey.RightArrow:
                if (RowView) SelRow = Math.Min(last, SelRow + 1); else SelCol = Math.Min(lastCol, SelCol + 1);
                ViewTop = 0;
                MaybeFetchPage();
                break;

            case ConsoleKey.Enter: ValueView = false; break;

            default:
                switch (k.KeyChar)
                {
                    case 'v' or 'V': RowView = !RowView; ViewTop = 0; break;
                    case 'c': if (RowView) CopyRow(vertical: true); else CopyCell(); break;
                    case 'C': CopyRow(); break;
                }
                break;
        }
    }

    private void MessagesKey(ConsoleKeyInfo k)
    {
        // Moving up stops the view from following new messages. Going back to the end, with Down or
        // End, makes it follow again. The renderer sets MessageFollow when it reaches the end.
        switch (k.Key)
        {
            case ConsoleKey.UpArrow: MessageFollow = false; MessageTop = Math.Max(0, MessageTop - 1); break;
            case ConsoleKey.DownArrow: MessageTop++; break;
            case ConsoleKey.PageUp: MessageFollow = false; MessageTop = Math.Max(0, MessageTop - MessageVisible); break;
            case ConsoleKey.PageDown: MessageTop += MessageVisible; break;
            case ConsoleKey.Home: MessageFollow = false; MessageTop = 0; break;
            case ConsoleKey.End: MessageFollow = true; break;

            default:
                switch (k.KeyChar)
                {
                    case 'm' or 'M': ShowMessages = false; break;
                    case 'c': CopyMessages(); break;
                }
                break;
        }
    }

    // ---------------------------------------------------------------- clipboard

    private void CopyCell()
    {
        if (SelectedRow is { } row && SelCol < row.Length)
            SetStatus(Clipboard.Copy(row[SelCol], $"{Headers.ElementAtOrDefault(SelCol)} of row {SelRow + 1}"));
    }

    // The row as one tab-separated line, which pastes into a spreadsheet as cells. The vertical form
    // is "name: value" lines, the same as the row view.
    private void CopyRow(bool vertical = false)
    {
        if (SelectedRow is not { } row)
            return;

        string text = vertical
            ? string.Join('\n', row.Select((value, i) => $"{Headers.ElementAtOrDefault(i)}: {value}"))
            : string.Join('\t', row);

        SetStatus(Clipboard.Copy(text, $"row {SelRow + 1}"));
    }

    private void CopyMessages()
    {
        SetStatus(Clipboard.Copy(
            string.Join('\n', Messages().Select(m => $"{m.At:HH:mm:ss} {(m.Error ? "ERROR " : "")}{Abbreviate(m.Sql, 200)} — {m.Text}")),
            "the messages"));
    }
}
