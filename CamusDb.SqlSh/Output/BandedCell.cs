
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;
using Spectre.Console.Rendering;

// One table cell that fills its whole width, from border to border, with a background. A Spectre table
// has no row style, and a background on the cell markup colors the text only, not the padding and the
// space after a short value. So the column has no padding of its own: this cell draws the one space on
// each side and fills the line to the column width itself.
//
// With no background, the cell draws the same spaces and nothing else, so a header or a plain row lines
// up with a banded row.
internal sealed class BandedCell : Renderable
{
    private const int Gutter = 1;

    private readonly Markup content;

    private readonly Style? band;

    public BandedCell(string markup, Style? band)
    {
        content = new Markup(markup);
        this.band = band;
    }

    protected override Measurement Measure(RenderOptions options, int maxWidth)
    {
        Measurement inner = ((IRenderable)content).Measure(options, InnerWidth(maxWidth));

        return new Measurement(inner.Min + (2 * Gutter), inner.Max + (2 * Gutter));
    }

    protected override IEnumerable<Segment> Render(RenderOptions options, int maxWidth)
    {
        int innerWidth = InnerWidth(maxWidth);

        List<SegmentLine> lines = Segment.SplitLines(((IRenderable)content).Render(options, innerWidth));

        Style padStyle = band ?? Style.Plain;

        for (int i = 0; i < lines.Count; i++)
        {
            if (i > 0)
                yield return Segment.LineBreak;

            yield return new Segment(new string(' ', Gutter), padStyle);

            foreach (Segment segment in lines[i])
                yield return band is { } fill ? new Segment(segment.Text, segment.Style.Combine(fill)) : segment;

            int rest = maxWidth - Gutter - lines[i].CellCount();

            if (rest > 0)
                yield return new Segment(new string(' ', rest), padStyle);
        }
    }

    private static int InnerWidth(int maxWidth) => Math.Max(1, maxWidth - (2 * Gutter));
}
