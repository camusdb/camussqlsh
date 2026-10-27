
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Globalization;

internal static class ValueFormatter
{
    // The display text of one cell, escaped for Spectre markup. Every ColumnType the driver can
    // return has a branch, so a value never falls through to the text of its CLR type.
    internal static string Format(ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Id => !string.IsNullOrEmpty(value.StrValue) ? value.StrValue! : "",
            ColumnType.String => !string.IsNullOrEmpty(value.StrValue) ? Markup.Escape(value.StrValue!) : "",
            ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float64 or ColumnType.Float32 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
            // The server sends the canonical decimal text, which can hold more digits than a decimal or
            // a double. It is shown as it arrives, so no digit is lost.
            ColumnType.Numeric => value.StrValue ?? "",
            ColumnType.Bool => value.BoolValue.ToString(),
            ColumnType.Uuid => FormatUuid(value),
            // The server sends a date as yyyy-MM-dd and a datetime in round-trip form, and that text is
            // what the parser reads back. LongValue holds the same instant in UTC ticks, so an older
            // server that omits the text is still rendered rather than dropped.
            ColumnType.Date => value.IsoValue ?? new DateTime(value.LongValue, DateTimeKind.Utc).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            ColumnType.DateTime => value.IsoValue ?? new DateTime(value.LongValue, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
            ColumnType.Bytes => FormatBytesLiteral(value.BytesValue),
            ColumnType.Array => FormatArray(value.ArrayValues),
            _ => "null"
        };
    }

    // The canonical string form the server sends, or the one rebuilt from the two 64-bit halves when
    // the reply carries only those.
    private static string FormatUuid(ColumnValue value)
    {
        if (!string.IsNullOrEmpty(value.UuidValue))
            return value.UuidValue!;

        return value.AsGuid().ToString("D");
    }

    // A bytes value as the x'…' literal the parser accepts, so a cell can be copied back into a
    // statement. A long value is clipped and its true length is named: a blob or an embedding runs to
    // megabytes, and the whole hex string would fill the terminal and slow the table down.
    private static string FormatBytesLiteral(byte[]? bytes)
    {
        if (bytes is null || bytes.Length == 0)
            return "x''";

        const int MaxShown = 32;

        string hex = Convert.ToHexStringLower(bytes, 0, Math.Min(bytes.Length, MaxShown));

        return bytes.Length <= MaxShown
            ? string.Concat("x'", hex, "'")
            : string.Format(CultureInfo.InvariantCulture, "x'{0}…' ({1} bytes)", hex, bytes.Length);
    }

    // An array value as [a, b, c]. Each element goes through Format, so a string element is
    // escaped and a nested value is rendered by its own type.
    //
    // The brackets are doubled because this text is markup: a Spectre table cell and the vertical
    // writer both read one, and a single '[' opens a style tag there.
    private static string FormatArray(List<ColumnValue>? elements)
    {
        if (elements is null || elements.Count == 0)
            return "[[]]";

        return string.Concat("[[", string.Join(", ", elements.Select(element => Format(element))), "]]");
    }
}
