/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// One tab of the full-screen mode: an editor and its results. All tabs share the session: one
/// connection, one current database and one transaction.
/// </summary>
internal sealed class QueryTab
{
    public QueryTab(TuiContext context, string text = "")
    {
        Results = new ResultView(context);

        if (text.Length > 0)
            Editor.SetText(text);
    }

    public EditorBuffer Editor { get; } = new();

    public ResultView Results { get; }

    /// <summary>
    /// The title in the tab strip: the first line with text, cut to <paramref name="width"/>
    /// characters, or "empty".
    /// </summary>
    public string Title(int width)
    {
        string first = Editor.FirstLine;

        return first.Length == 0 ? "empty" : TuiText.Clip(TuiText.OneLine(first), width);
    }
}
