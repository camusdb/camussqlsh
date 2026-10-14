/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CsvHelper;
using System.Buffers;
using System.Globalization;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;

/// <summary>
/// How -e and -f print a result set: the Spectre table that the prompt also uses, or a document
/// that another program can parse. Csv is for <c>\export</c> only; <c>--format</c> does not take it.
/// </summary>
internal enum OutputFormat
{
    Table,
    Json,
    Yaml,
    Csv
}

/// <summary>
/// Writes result sets as JSON, YAML or CSV, one document for each statement that returns rows. The
/// stream is standard output for <c>--format</c>, and a file for <c>\export</c>.
///
/// <para>A JSON document is an array of objects, one object for each row. Two or more result sets
/// give two or more arrays, one after the other, which is the stream that <c>jq</c> reads. A YAML
/// document is a sequence of mappings, and the documents are separated by <c>---</c>.</para>
///
/// <para>A value keeps its type where both formats have one: an integer, a float or a bool is a
/// number or a bool, and a SQL null is null. A numeric value is a string, because it can hold more
/// digits than a double, and a parser would round it. A bytes value is base64, and it is never
/// clipped the way the table clips it.</para>
///
/// <para>A CSV document has a header record with the column names, then one record for each row.
/// CSV has text only, so the types are lost, and a SQL null is an empty field.</para>
/// </summary>
internal sealed class StructuredResultWriter(OutputFormat format, Stream output)
{
    // Big enough that a large result set costs few writes, small enough that rows reach a pipe
    // while the query still streams.
    private const int FlushThreshold = 64 * 1024;

    private static readonly JsonWriterOptions JsonOptions = new()
    {
        Indented = true,
        // The output goes to a terminal or a pipe, not into HTML, so a name with an accent or a
        // '<' stays readable instead of becoming \u escapes.
        Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping
    };

    private static readonly UTF8Encoding Utf8NoBom = new(encoderShouldEmitUTF8Identifier: false);

    // YAML 1.1 reads these plain words as a bool or a null, so a column with one of these names
    // is quoted to stay a string.
    private static readonly HashSet<string> YamlReservedWords = new(StringComparer.OrdinalIgnoreCase)
    {
        "y", "n", "yes", "no", "true", "false", "on", "off", "null"
    };

    private int resultSets;

    public static bool TryParse(string? text, out OutputFormat format)
    {
        switch (text?.Trim().ToLowerInvariant())
        {
            case null or "" or "table":
                format = OutputFormat.Table;
                return true;

            case "json":
                format = OutputFormat.Json;
                return true;

            case "yaml" or "yml":
                format = OutputFormat.Yaml;
                return true;

            default:
                format = OutputFormat.Table;
                return false;
        }
    }

    /// <summary>
    /// Writes every row of <paramref name="reader"/> as one document and returns the row count.
    /// A result set with no rows is still a document: an empty array.
    /// </summary>
    public async Task<int> WriteAsync(CamusDataReader reader)
    {
        resultSets++;

        return format switch
        {
            OutputFormat.Json => await WriteJsonAsync(reader),
            OutputFormat.Csv => await WriteCsvAsync(reader),
            _ => await WriteYamlAsync(reader)
        };
    }

    private async Task<int> WriteJsonAsync(CamusDataReader reader)
    {
        int rows = 0;

        await using Utf8JsonWriter json = new(output, JsonOptions);

        json.WriteStartArray();

        while (await reader.ReadAsync())
        {
            json.WriteStartObject();

            foreach (KeyValuePair<string, ColumnValue> item in ConnectionHelper.ReadCurrentRow(reader))
            {
                json.WritePropertyName(item.Key);
                WriteJsonValue(json, ToPlainValue(item.Value));
            }

            json.WriteEndObject();
            rows++;

            if (json.BytesPending > FlushThreshold)
                await json.FlushAsync();
        }

        json.WriteEndArray();
        await json.FlushAsync();

        output.Write("\n"u8);
        await output.FlushAsync();

        return rows;
    }

    private async Task<int> WriteYamlAsync(CamusDataReader reader)
    {
        int rows = 0;

        await using StreamWriter yaml = new(output, Utf8NoBom, FlushThreshold, leaveOpen: true)
        {
            NewLine = "\n"
        };

        if (resultSets > 1)
            yaml.WriteLine("---");

        while (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            rows++;

            if (row.Count == 0)
            {
                yaml.WriteLine("- {}");
                continue;
            }

            bool first = true;

            foreach (KeyValuePair<string, ColumnValue> item in row)
            {
                yaml.Write(first ? "- " : "  ");
                yaml.Write(YamlKey(item.Key));
                yaml.Write(": ");
                yaml.WriteLine(YamlValue(ToPlainValue(item.Value)));
                first = false;
            }
        }

        if (rows == 0)
            yaml.WriteLine("[]");

        await yaml.FlushAsync();

        return rows;
    }

    // The header comes from the reader, not from the first row, so a result set with no rows still
    // names its columns. CsvHelper quotes a field that holds a comma, a quote, a line break or an
    // edge space.
    private async Task<int> WriteCsvAsync(CamusDataReader reader)
    {
        int rows = 0;

        await using StreamWriter text = new(output, Utf8NoBom, FlushThreshold, leaveOpen: true);
        await using CsvWriter csv = new(text, CultureInfo.InvariantCulture, leaveOpen: true);

        for (int i = 0; i < reader.FieldCount; i++)
            csv.WriteField(reader.GetName(i));

        await csv.NextRecordAsync();

        while (await reader.ReadAsync())
        {
            for (int i = 0; i < reader.FieldCount; i++)
                csv.WriteField(CsvValue(ToPlainValue(reader.GetColumnValue(i))));

            await csv.NextRecordAsync();
            rows++;
        }

        await csv.FlushAsync();

        return rows;
    }

    // One cell as the CLR value that the formats write: null, string, long, double, float, bool,
    // byte[] or a list of those. Every ColumnType has a branch, as in the table's FormatValue.
    private static object? ToPlainValue(ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Id or ColumnType.String => value.StrValue ?? "",
            ColumnType.Integer64 => value.LongValue,
            ColumnType.Float64 => value.FloatValue,
            // The driver holds a float32 as a double. The cast gives back the float, so the text is
            // 0.1 and not 0.10000000149011612.
            ColumnType.Float32 => (float)value.FloatValue,
            ColumnType.Numeric => value.StrValue,
            ColumnType.Bool => value.BoolValue,
            ColumnType.Uuid => !string.IsNullOrEmpty(value.UuidValue) ? value.UuidValue : value.AsGuid().ToString("D"),
            ColumnType.Date => value.IsoValue ?? new DateTime(value.LongValue, DateTimeKind.Utc).ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
            ColumnType.DateTime => value.IsoValue ?? new DateTime(value.LongValue, DateTimeKind.Utc).ToString("o", CultureInfo.InvariantCulture),
            ColumnType.Bytes => value.BytesValue ?? [],
            ColumnType.Array => value.ArrayValues?.Select(ToPlainValue).ToList() ?? [],
            _ => null
        };
    }

    private static void WriteJsonValue(Utf8JsonWriter json, object? value)
    {
        switch (value)
        {
            case null:
                json.WriteNullValue();
                break;

            case string text:
                json.WriteStringValue(text);
                break;

            case long number:
                json.WriteNumberValue(number);
                break;

            case double number when double.IsFinite(number):
                json.WriteNumberValue(number);
                break;

            case float number when float.IsFinite(number):
                json.WriteNumberValue(number);
                break;

            // JSON has no NaN or infinity, so these are the strings "NaN", "Infinity" and
            // "-Infinity": the spelling that System.Text.Json reads back as a number when
            // AllowNamedFloatingPointLiterals is set.
            case double or float:
                json.WriteStringValue(Convert.ToString(value, CultureInfo.InvariantCulture));
                break;

            case bool flag:
                json.WriteBooleanValue(flag);
                break;

            case byte[] bytes:
                json.WriteBase64StringValue(bytes);
                break;

            case List<object?> elements:
                json.WriteStartArray();

                foreach (object? element in elements)
                    WriteJsonValue(json, element);

                json.WriteEndArray();
                break;
        }
    }

    // One cell as CSV text. CSV has no null, so a SQL null is an empty field, the same as an empty
    // string. An array is its JSON text, so a reader can still tell the elements apart.
    private static string CsvValue(object? value)
    {
        return value switch
        {
            null => "",
            string text => text,
            long number => number.ToString(CultureInfo.InvariantCulture),
            double number => number.ToString("R", CultureInfo.InvariantCulture),
            float number => number.ToString("R", CultureInfo.InvariantCulture),
            bool flag => flag ? "true" : "false",
            byte[] bytes => Convert.ToBase64String(bytes),
            List<object?> elements => JsonText(elements),
            _ => ""
        };
    }

    private static string JsonText(object? value)
    {
        ArrayBufferWriter<byte> buffer = new();

        using (Utf8JsonWriter json = new(buffer, new JsonWriterOptions { Encoder = JsonOptions.Encoder }))
            WriteJsonValue(json, value);

        return Encoding.UTF8.GetString(buffer.WrittenSpan);
    }

    private static string YamlValue(object? value)
    {
        return value switch
        {
            null => "null",
            string text => YamlQuote(text),
            long number => number.ToString(CultureInfo.InvariantCulture),
            double number => YamlFloat(number, number.ToString("R", CultureInfo.InvariantCulture)),
            float number => YamlFloat(number, number.ToString("R", CultureInfo.InvariantCulture)),
            bool flag => flag ? "true" : "false",
            byte[] bytes => "!!binary " + YamlQuote(Convert.ToBase64String(bytes)),
            List<object?> elements => string.Concat("[", string.Join(", ", elements.Select(YamlValue)), "]"),
            _ => "null"
        };
    }

    // A float always carries a '.', so that YAML 1.1 (PyYAML, for one) reads 3.0 as a float and
    // not an int, and 1E+20 as a float and not a string.
    private static string YamlFloat(double number, string text)
    {
        if (double.IsNaN(number))
            return ".nan";

        if (double.IsInfinity(number))
            return number > 0 ? ".inf" : "-.inf";

        if (text.Contains('.'))
            return text;

        int exponent = text.IndexOf('E');

        return exponent < 0 ? text + ".0" : text.Insert(exponent, ".0");
    }

    // A column name stays plain when no YAML parser can read it as anything but a string;
    // otherwise it is quoted like a string value.
    private static string YamlKey(string name)
    {
        bool plain = name.Length > 0
            && (char.IsAsciiLetter(name[0]) || name[0] == '_')
            && name.All(c => char.IsAsciiLetterOrDigit(c) || c == '_')
            && !YamlReservedWords.Contains(name);

        return plain ? name : YamlQuote(name);
    }

    // A double-quoted scalar. Every string is quoted, so a value such as "no", "1.0" or "~" stays
    // a string. Control characters and the Unicode line breaks are escaped, because YAML does not
    // allow them raw.
    private static string YamlQuote(string text)
    {
        StringBuilder quoted = new(text.Length + 2);

        quoted.Append('"');

        foreach (char c in text)
        {
            switch (c)
            {
                case '"':
                    quoted.Append("\\\"");
                    break;

                case '\\':
                    quoted.Append("\\\\");
                    break;

                case '\n':
                    quoted.Append("\\n");
                    break;

                case '\r':
                    quoted.Append("\\r");
                    break;

                case '\t':
                    quoted.Append("\\t");
                    break;

                default:
                    if (char.IsControl(c) || c is (char)0x2028 or (char)0x2029 or (char)0xFEFF)
                        quoted.Append("\\u").Append(((int)c).ToString("x4", CultureInfo.InvariantCulture));
                    else
                        quoted.Append(c);
                    break;
            }
        }

        quoted.Append('"');

        return quoted.ToString();
    }
}
