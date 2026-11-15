/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// The state that every part of the full-screen mode reads: the value formatter, the row cap, whether
/// a run is in progress, and the repaint counter.
///
/// <para>Background work (a DESCRIBE, a query, a page fetch) calls <see cref="Repaint"/> so the draw
/// loop knows to draw again, also when no key was pressed.</para>
/// </summary>
internal sealed class TuiContext
{
    /// <summary>The cap on the rows that the grid reads. A display cap, not a SQL LIMIT.</summary>
    public const int LimitRows = 500;

    /// <summary>Rows read in one trip to the reader after the first page is on screen.</summary>
    public const int PageRows = 200;

    /// <summary>
    /// How close the visible window can come to the end of the buffer before the next page is read.
    /// Large enough that a held-down arrow key does not outrun the fetch.
    /// </summary>
    public const int PageMargin = 60;

    private int _revision;
    private volatile bool _running;

    public TuiContext(Func<ColumnValue, string> format)
    {
        Format = format;
    }

    public Func<ColumnValue, string> Format { get; }

    /// <summary>True while the row cap is on. F2 changes it.</summary>
    public bool LimitOn { get; set; } = true;

    /// <summary>True while a run is in progress, in any tab. One run at a time is allowed.</summary>
    public bool Running
    {
        get => _running;
        set => _running = value;
    }

    public int Revision => Volatile.Read(ref _revision);

    public void Repaint() => Interlocked.Increment(ref _revision);
}
