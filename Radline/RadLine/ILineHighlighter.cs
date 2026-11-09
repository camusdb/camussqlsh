using System.Collections.Generic;
using Spectre.Console;

namespace RadLine
{
    /// <summary>
    /// A highlighter that colors a whole line, with the lines above it as context. It can color a
    /// string or a comment that starts on an earlier line. When the editor's highlighter implements
    /// this interface, the editor uses it in place of <see cref="IHighlighter.Highlight"/>.
    /// </summary>
    public interface ILineHighlighter : IHighlighter
    {
        /// <summary>
        /// Colors one line of the editor.
        /// </summary>
        /// <param name="lines">All the lines of the editor.</param>
        /// <param name="lineIndex">The index of the line to color.</param>
        /// <returns>
        /// The parts of <c>lines[lineIndex]</c>, in order, each with its style. Joined, the texts of
        /// the parts are the whole line.
        /// </returns>
        IEnumerable<(string Text, Style? Style)> HighlightLine(IReadOnlyList<string> lines, int lineIndex);
    }
}
