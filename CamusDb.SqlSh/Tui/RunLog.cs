/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDb.SqlSh.Tui;

/// <summary>One entry in the messages area: the outcome of one statement, or an error from paging.</summary>
internal sealed record TuiMessage(DateTime At, string Sql, string Text, bool Error);

/// <summary>
/// What one run of the editor reports. Each entry is one part of the status line and one message.
/// An entry is an error when the statement failed.
/// </summary>
internal sealed class RunLog
{
    public readonly List<(string Text, bool Error)> Entries = new();

    public bool Failed => Entries.Any(e => e.Error);

    public void Add(string text) => Entries.Add((text, false));

    public void Fail(string text) => Entries.Add((text, true));
}
