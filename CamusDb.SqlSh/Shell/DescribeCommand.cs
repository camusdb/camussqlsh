/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;

// The kinds of object that a `\d` command lists. `\dt`, `\dv`, `\dm`, `\ds` and `\di` pick one each,
// and the letters combine the psql way: `\dtv` lists the tables and the views.
[Flags]
internal enum RelationKinds
{
    None = 0,
    Tables = 1,
    Views = 2,
    MaterializedViews = 4,
    Sequences = 8,
    Indexes = 16,

    // What a bare `\d` lists. Indexes are left out, as psql leaves them out.
    Relations = Tables | Views | MaterializedViews | Sequences,
}

// One parsed `\d` command. With no kinds and a pattern, it describes each object that the pattern
// names. In all other cases it lists the objects of the kinds, filtered by the pattern.
internal readonly record struct DescribeRequest(RelationKinds Kinds, bool Verbose, string? Pattern);

// The psql-style `\d` family. The server has no statement that does this, so the shell builds the
// answer from SHOW TABLES, SHOW VIEWS, SHOW MATERIALIZED VIEWS, SHOW SEQUENCES, SHOW COLUMNS,
// SHOW INDEXES and the SHOW CREATE statements.
internal static class DescribeCommand
{
    private enum RelationKind
    {
        Table,
        View,
        MaterializedView,
        Sequence,
    }

    // One relation of the database, with the row of the SHOW statement that listed it. The row of
    // SHOW SEQUENCES holds the settings of the sequence, and the row of SHOW MATERIALIZED VIEWS
    // holds whether the view holds data.
    private sealed record Relation(string Name, RelationKind Kind, Dictionary<string, ColumnValue> Row);

    private sealed record IndexInfo(string Name, string Table, bool Unique, string[] Columns, string[] Directions, string Include, string Type, string? Comment);

    // The parts of a SHOW CREATE TABLE statement that SHOW COLUMNS and SHOW INDEXES do not give.
    private sealed class TableDefinition
    {
        public Dictionary<string, string> ColumnComments { get; } = new(StringComparer.OrdinalIgnoreCase);

        public List<(string Name, string Definition)> Checks { get; } = [];

        public List<(string Name, string Definition)> ForeignKeys { get; } = [];

        public List<(string Name, string Definition)> OtherConstraints { get; } = [];

        public string? Comment { get; set; }

        public string? Options { get; set; }
    }

    internal static async Task ExecuteAsync(CamusConnection connection, DescribeRequest request)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();

        List<Relation> relations = await LoadRelationsAsync(connection);

        if (request.Kinds == RelationKinds.None && request.Pattern is not null)
            await DescribeAsync(connection, relations, request);
        else
            await ListAsync(connection, relations, request, stopwatch);
    }

    // `\df [pattern]` and `\df+ [pattern]`: the inline functions of the current database, from SHOW
    // FUNCTIONS. A row in the `unloading` state is the code of a dropped or replaced function that a
    // running statement still holds, so it is a detail of the node and only `\df+` shows it. `\df+`
    // also adds the compile state on this node and the CREATE FUNCTION statement of each function.
    internal static async Task ListFunctionsAsync(CamusConnection connection, bool verbose, string? pattern)
    {
        Stopwatch stopwatch = Stopwatch.StartNew();
        Regex? filter = pattern is null ? null : ToRegex(pattern);

        List<Dictionary<string, ColumnValue>> rows = [];

        foreach (Dictionary<string, ColumnValue> row in await QueryAsync(connection, "show functions"))
        {
            string name = Text(row, "name") ?? "";
            bool unloading = string.Equals(Text(row, "state"), "unloading", StringComparison.OrdinalIgnoreCase);

            if (name.Length == 0 || (unloading && !verbose))
                continue;

            if (filter is null || filter.IsMatch(name))
                rows.Add(row);
        }

        if (rows.Count == 0)
        {
            if (pattern is null)
                AnsiConsole.MarkupLine("Did not find any functions.\n");
            else
                AnsiConsole.MarkupLine("Did not find any functions named \"{0}\".\n", Markup.Escape(pattern));

            return;
        }

        Table table = StartTable("List of functions");
        table.AddColumn("Name");
        table.AddColumn("Arguments");
        table.AddColumn("Returns");
        table.AddColumn("Null input");
        table.AddColumn("Language");
        table.AddColumn("Owner");

        if (verbose)
        {
            table.AddColumn("Generation");
            table.AddColumn("State");
        }

        foreach (Dictionary<string, ColumnValue> row in rows)
        {
            List<string> cells =
            [
                Markup.Escape(Text(row, "name") ?? ""),
                Markup.Escape(Text(row, "arguments") ?? ""),
                Markup.Escape(Text(row, "returns") ?? ""),
                Markup.Escape(Text(row, "null_input") ?? ""),
                Markup.Escape(Text(row, "language") ?? ""),
                Markup.Escape(Text(row, "owner") ?? ""),
            ];

            if (verbose)
            {
                cells.Add(Markup.Escape(Text(row, "generation") ?? ""));
                cells.Add(Markup.Escape(Text(row, "state") ?? ""));
            }

            table.AddRow(cells.ToArray());
        }

        AnsiConsole.Write(table);

        if (verbose)
        {
            foreach (Dictionary<string, ColumnValue> row in rows)
            {
                if (string.Equals(Text(row, "state"), "unloading", StringComparison.OrdinalIgnoreCase))
                    continue;

                // A function name is always a plain identifier, so it goes into the statement bare.
                string name = Text(row, "name")!;
                List<Dictionary<string, ColumnValue>> created = await QueryAsync(connection, $"show create function {name}");
                string? statement = created.Count > 0 ? Text(created[0], "create function") : null;

                if (string.IsNullOrEmpty(statement))
                    continue;

                AnsiConsole.MarkupLine("[bold]Source of \"{0}\":[/]", Markup.Escape(name));

                foreach (string line in statement.Replace("\r\n", "\n").Split('\n'))
                    AnsiConsole.MarkupLine("    {0}", Markup.Escape(line));
            }
        }

        ResultWriter.WriteRowsInSet(rows.Count, stopwatch.Elapsed);
    }

    private static async Task DescribeAsync(CamusConnection connection, List<Relation> relations, DescribeRequest request)
    {
        Regex pattern = ToRegex(request.Pattern!);

        List<Relation> matches = relations.Where(r => pattern.IsMatch(r.Name)).ToList();

        if (matches.Count == 0)
        {
            AnsiConsole.MarkupLine("[red]Did not find any relation named \"{0}\".[/]\n", Markup.Escape(request.Pattern!));
            return;
        }

        foreach (Relation relation in matches)
        {
            switch (relation.Kind)
            {
                case RelationKind.Table:
                    await DescribeTableAsync(connection, relation, request.Verbose);
                    break;

                case RelationKind.View:
                    await DescribeViewAsync(connection, relation, request.Verbose);
                    break;

                case RelationKind.MaterializedView:
                    await DescribeMaterializedViewAsync(connection, relation, request.Verbose);
                    break;

                case RelationKind.Sequence:
                    DescribeSequence(relation, request.Verbose);
                    break;
            }

            AnsiConsole.WriteLine();
        }
    }

    // `\d` with no name, and `\dt`, `\dv`, `\dm`, `\ds`, `\di` and their combinations.
    private static async Task ListAsync(CamusConnection connection, List<Relation> relations, DescribeRequest request, Stopwatch stopwatch)
    {
        RelationKinds kinds = request.Kinds == RelationKinds.None ? RelationKinds.Relations : request.Kinds;
        Regex? pattern = request.Pattern is null ? null : ToRegex(request.Pattern);

        List<(string Name, string Type, string Table)> rows = [];

        foreach (Relation relation in relations)
        {
            if (!kinds.HasFlag(ToFlag(relation.Kind)))
                continue;

            if (pattern is not null && !pattern.IsMatch(relation.Name))
                continue;

            rows.Add((relation.Name, TypeName(relation.Kind), ""));
        }

        // SHOW INDEXES answers for one relation at a time, so this costs one statement for each table
        // and each materialized view. The pattern filters the index names, as it does in psql.
        if (kinds.HasFlag(RelationKinds.Indexes))
        {
            foreach (Relation relation in relations)
            {
                if (relation.Kind is not (RelationKind.Table or RelationKind.MaterializedView))
                    continue;

                foreach (IndexInfo index in await LoadIndexesAsync(connection, relation.Name))
                {
                    if (pattern is null || pattern.IsMatch(index.Name))
                        rows.Add((index.Name, "index", index.Table));
                }
            }
        }

        if (rows.Count == 0)
        {
            string what = NounFor(kinds);

            if (request.Pattern is null)
                AnsiConsole.MarkupLine("Did not find any {0}.\n", what);
            else
                AnsiConsole.MarkupLine("Did not find any {0} named \"{1}\".\n", what, Markup.Escape(request.Pattern));

            return;
        }

        // Lowercase first, then ordinal: OrdinalIgnoreCase compares uppercase letters, and those sort
        // before '_', so `robots` came before `robot_counts`.
        rows.Sort((a, b) =>
        {
            int byName = string.CompareOrdinal(a.Name.ToLowerInvariant(), b.Name.ToLowerInvariant());
            return byName != 0 ? byName : string.CompareOrdinal(a.Table.ToLowerInvariant(), b.Table.ToLowerInvariant());
        });

        bool withTable = kinds.HasFlag(RelationKinds.Indexes);

        Table table = StartTable("List of " + NounFor(kinds));
        table.AddColumn("Name");
        table.AddColumn("Type");

        if (withTable)
            table.AddColumn("Table");

        foreach ((string name, string type, string tableName) in rows)
        {
            if (withTable)
                table.AddRow(Markup.Escape(name), type, Markup.Escape(tableName));
            else
                table.AddRow(Markup.Escape(name), type);
        }

        AnsiConsole.Write(table);
        ResultWriter.WriteRowsInSet(rows.Count, stopwatch.Elapsed);
    }

    private static async Task DescribeTableAsync(CamusConnection connection, Relation relation, bool verbose)
    {
        List<Dictionary<string, ColumnValue>> columns = await QueryAsync(connection, $"show columns from {Quote(relation.Name)}");
        List<IndexInfo> indexes = await LoadIndexesAsync(connection, relation.Name);
        TableDefinition definition = await LoadTableDefinitionAsync(connection, relation.Name);

        bool withExtra = columns.Any(c => !string.IsNullOrEmpty(Text(c, "Extra")));

        Table table = StartTable($"Table \"{relation.Name}\"");
        table.AddColumn("Column");
        table.AddColumn("Type");
        table.AddColumn("Nullable");
        table.AddColumn("Default");

        if (withExtra)
            table.AddColumn("Extra");

        if (verbose)
            table.AddColumn("Description");

        foreach (Dictionary<string, ColumnValue> column in columns)
        {
            string name = Text(column, "Field") ?? "";

            List<string> cells =
            [
                Markup.Escape(name),
                Markup.Escape(Text(column, "Type") ?? ""),
                string.Equals(Text(column, "Null"), "NO", StringComparison.OrdinalIgnoreCase) ? "not null" : "",
                Markup.Escape(ColumnDefault(column)),
            ];

            if (withExtra)
                cells.Add(Markup.Escape(Text(column, "Extra") ?? ""));

            if (verbose)
                cells.Add(Markup.Escape(definition.ColumnComments.GetValueOrDefault(name) ?? ""));

            table.AddRow(cells.ToArray());
        }

        AnsiConsole.Write(table);

        WriteIndexes(indexes);
        WriteSection("Check constraints:", definition.Checks);
        WriteSection("Foreign-key constraints:", definition.ForeignKeys);
        WriteSection("Constraints:", definition.OtherConstraints);

        if (verbose)
        {
            WriteDetail("Description", definition.Comment);
            WriteDetail("Options", definition.Options);
        }
    }

    private static async Task DescribeViewAsync(CamusConnection connection, Relation relation, bool verbose)
    {
        WriteViewColumns($"View \"{relation.Name}\"", await QueryAsync(connection, $"show columns from {Quote(relation.Name)}"));

        if (verbose)
            WriteDefinition(await LoadViewDefinitionAsync(connection, $"show create view {Quote(relation.Name)}"));
    }

    private static async Task DescribeMaterializedViewAsync(CamusConnection connection, Relation relation, bool verbose)
    {
        WriteViewColumns($"Materialized view \"{relation.Name}\"", await QueryAsync(connection, $"show columns from {Quote(relation.Name)}"));
        WriteIndexes(await LoadIndexesAsync(connection, relation.Name));

        string? populated = Text(relation.Row, "populated");

        if (populated is not null)
            WriteDetail("Populated", string.Equals(populated, "true", StringComparison.OrdinalIgnoreCase) ? "yes" : "no");

        WriteDetail("Refreshed at", Text(relation.Row, "refreshed_at"));

        if (verbose)
            WriteDefinition(await LoadViewDefinitionAsync(connection, $"show create materialized view {Quote(relation.Name)}"));
    }

    // SHOW SEQUENCES already holds every setting, so a sequence needs no more statements.
    private static void DescribeSequence(Relation relation, bool verbose)
    {
        Table table = StartTable($"Sequence \"{relation.Name}\"");

        (string Header, string Key)[] settings =
        [
            ("Start", "start_value"),
            ("Minimum", "min_value"),
            ("Maximum", "max_value"),
            ("Increment", "increment"),
            ("Cache", "cache"),
            ("Reserved up to", "reserved_upto"),
        ];

        foreach ((string header, string _) in settings)
            table.AddColumn(header);

        table.AddRow(settings.Select(s => Markup.Escape(Text(relation.Row, s.Key) ?? "")).ToArray());

        AnsiConsole.Write(table);

        WriteDetail("Owned by", Text(relation.Row, "owned_by"));

        if (verbose)
            WriteDetail("Description", Text(relation.Row, "comment"));
    }

    private static async Task<List<Relation>> LoadRelationsAsync(CamusConnection connection)
    {
        List<Relation> relations = [];

        (string Sql, RelationKind Kind)[] sources =
        [
            ("show tables", RelationKind.Table),
            ("show views", RelationKind.View),
            ("show materialized views", RelationKind.MaterializedView),
            ("show sequences", RelationKind.Sequence),
        ];

        foreach ((string sql, RelationKind kind) in sources)
        {
            // The first column of each statement holds the name, whatever the column is called.
            foreach (Dictionary<string, ColumnValue> row in await QueryAsync(connection, sql))
            {
                string? name = row.Count > 0 ? Text(row.Values.First()) : null;

                if (!string.IsNullOrEmpty(name))
                    relations.Add(new Relation(name, kind, row));
            }
        }

        return relations;
    }

    private static async Task<List<IndexInfo>> LoadIndexesAsync(CamusConnection connection, string relation)
    {
        List<IndexInfo> indexes = [];

        foreach (Dictionary<string, ColumnValue> row in await QueryAsync(connection, $"show indexes from {Quote(relation)}"))
        {
            indexes.Add(new IndexInfo(
                Name: Text(row, "Key_name") ?? "",
                Table: Text(row, "Table") ?? relation,
                Unique: Text(row, "Non_unique") == "0",
                Columns: SplitList(Text(row, "Columns")),
                Directions: SplitList(Text(row, "Directions")),
                Include: Text(row, "Include") ?? "",
                Type: Text(row, "Index_type") ?? "",
                Comment: Text(row, "Comment")));
        }

        return indexes;
    }

    private static async Task<TableDefinition> LoadTableDefinitionAsync(CamusConnection connection, string table)
    {
        List<Dictionary<string, ColumnValue>> rows = await QueryAsync(connection, $"show create table {Quote(table)}");

        // The second column holds the statement: `Create Table`.
        string? ddl = rows.Count > 0 && rows[0].Count > 1 ? Text(rows[0].Values.ElementAt(1)) : null;

        return ddl is null ? new TableDefinition() : ParseTableDefinition(ddl);
    }

    // The query of a view, without the `CREATE [MATERIALIZED] VIEW <name> AS` in front of it.
    private static async Task<string?> LoadViewDefinitionAsync(CamusConnection connection, string sql)
    {
        List<Dictionary<string, ColumnValue>> rows = await QueryAsync(connection, sql);

        string? ddl = rows.Count > 0 && rows[0].Count > 1 ? Text(rows[0].Values.ElementAt(1)) : null;

        if (ddl is null)
            return null;

        Match match = Regex.Match(
            ddl,
            """^\s*CREATE\s+(?:MATERIALIZED\s+)?VIEW\s+(?:`(?:``|[^`])*`|\S+)\s+AS\s+(?<query>.*?)\s*;?\s*$""",
            RegexOptions.IgnoreCase | RegexOptions.Singleline);

        return match.Success ? match.Groups["query"].Value : ddl;
    }

    // Reads the column comments, the constraints, the table comment and the WITH options out of the
    // statement that SHOW CREATE TABLE gives, for example:
    //
    //   CREATE TABLE `robots` ( `id` OID NOT NULL COMMENT 'Key', ..., PRIMARY KEY (`id`),
    //   CONSTRAINT `robots_owner_id_fkey` FOREIGN KEY (`owner_id`) REFERENCES `owners` (`id`))
    //   COMMENT 'Robot catalog' WITH (ttl_expiration_expression = 'expires_at');
    private static TableDefinition ParseTableDefinition(string ddl)
    {
        TableDefinition definition = new();

        int open = IndexOfTopLevel(ddl, '(', 0);
        if (open < 0)
            return definition;

        int close = IndexOfTopLevel(ddl, ')', open + 1, depthStart: 1);
        if (close < 0)
            return definition;

        foreach (string item in SplitTopLevel(ddl[(open + 1)..close]))
        {
            if (item.StartsWith('`'))
            {
                string? name = ReadIdentifier(item, 0, out int end);

                if (name is not null && FindTrailingComment(item[end..]) is { } columnComment)
                    definition.ColumnComments[name] = columnComment;

                continue;
            }

            Match constraint = Regex.Match(
                item,
                """^CONSTRAINT\s+(?:`(?<name>(?:``|[^`])*)`|(?<name>\S+))\s+(?<body>.*)$""",
                RegexOptions.IgnoreCase | RegexOptions.Singleline);

            if (!constraint.Success)
                continue;

            string constraintName = constraint.Groups["name"].Value.Replace("``", "`");
            string body = UnquoteSimpleIdentifiers(constraint.Groups["body"].Value);

            if (body.StartsWith("CHECK", StringComparison.OrdinalIgnoreCase))
                definition.Checks.Add((constraintName, body));
            else if (body.StartsWith("FOREIGN", StringComparison.OrdinalIgnoreCase))
                definition.ForeignKeys.Add((constraintName, body));
            else
                definition.OtherConstraints.Add((constraintName, body));
        }

        string tail = ddl[(close + 1)..].Trim().TrimEnd(';').Trim();

        Match comment = Regex.Match(tail, """^COMMENT\s+""", RegexOptions.IgnoreCase);
        if (comment.Success && ReadStringLiteral(tail, comment.Length, out int afterComment) is { } tableComment)
        {
            definition.Comment = tableComment;
            tail = tail[afterComment..].Trim();
        }

        Match options = Regex.Match(tail, """^WITH\s*\((?<options>.*)\)$""", RegexOptions.IgnoreCase | RegexOptions.Singleline);
        if (options.Success)
            definition.Options = options.Groups["options"].Value.Trim();

        return definition;
    }

    private static void WriteViewColumns(string title, List<Dictionary<string, ColumnValue>> columns)
    {
        Table table = StartTable(title);
        table.AddColumn("Column");
        table.AddColumn("Type");

        foreach (Dictionary<string, ColumnValue> column in columns)
            table.AddRow(Markup.Escape(Text(column, "Field") ?? ""), Markup.Escape(Text(column, "Type") ?? ""));

        AnsiConsole.Write(table);
    }

    // Writes the indexes the psql way, the primary key first:
    //     "~pk" PRIMARY KEY (id)
    //     "robots_year_idx" (year DESC) INCLUDE (price)
    private static void WriteIndexes(List<IndexInfo> indexes)
    {
        if (indexes.Count == 0)
            return;

        AnsiConsole.MarkupLine("[bold]Indexes:[/]");

        foreach (IndexInfo index in indexes.OrderBy(i => i.Name == "~pk" ? 0 : 1))
        {
            StringBuilder line = new();
            line.Append('"').Append(index.Name).Append('"');

            if (index.Name == "~pk")
                line.Append(" PRIMARY KEY");
            else if (index.Unique)
                line.Append(" UNIQUE");

            if (index.Type.Length > 0 && !index.Type.Equals("ORDERED", StringComparison.OrdinalIgnoreCase))
                line.Append(' ').Append(index.Type);

            line.Append(" (");

            for (int i = 0; i < index.Columns.Length; i++)
            {
                if (i > 0)
                    line.Append(", ");

                line.Append(index.Columns[i]);

                if (i < index.Directions.Length && index.Directions[i].Equals("DESC", StringComparison.OrdinalIgnoreCase))
                    line.Append(" DESC");
            }

            line.Append(')');

            if (index.Include.Length > 0)
                line.Append(" INCLUDE (").Append(string.Join(", ", SplitList(index.Include))).Append(')');

            AnsiConsole.MarkupLine("    {0}", Markup.Escape(line.ToString()));
        }
    }

    private static void WriteSection(string title, List<(string Name, string Definition)> items)
    {
        if (items.Count == 0)
            return;

        AnsiConsole.MarkupLine("[bold]{0}[/]", Markup.Escape(title));

        foreach ((string name, string body) in items)
            AnsiConsole.MarkupLine("    {0}", Markup.Escape($"\"{name}\" {body}"));
    }

    private static void WriteDetail(string label, string? value)
    {
        if (string.IsNullOrEmpty(value))
            return;

        AnsiConsole.MarkupLine("[bold]{0}:[/] {1}", Markup.Escape(label), Markup.Escape(value));
    }

    private static void WriteDefinition(string? query)
    {
        if (string.IsNullOrEmpty(query))
            return;

        AnsiConsole.MarkupLine("[bold]View definition:[/]");
        AnsiConsole.MarkupLine("    {0}", Markup.Escape(query));
    }

    // Writes the title on its own line, then returns the table to fill. A Spectre table title wraps
    // to the width of the table, and a narrow table would break `Materialized view "x"` in two.
    private static Table StartTable(string title)
    {
        AnsiConsole.MarkupLine("[bold]{0}[/]", Markup.Escape(title));

        return new Table
        {
            Border = TableBorder.Square,
        };
    }

    private static async Task<List<Dictionary<string, ColumnValue>>> QueryAsync(CamusConnection connection, string sql)
    {
        await using CamusCommand cmd = connection.CreateSelectCommand(sql);

        cmd.CommandTimeout = 60;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        List<Dictionary<string, ColumnValue>> rows = [];

        while (await reader.ReadAsync())
            rows.Add(ConnectionHelper.ReadCurrentRow(reader));

        return rows;
    }

    // A default of NULL is no default at all, as psql shows it.
    private static string ColumnDefault(Dictionary<string, ColumnValue> column)
    {
        string? value = Text(column, "Default");

        return value is null || value == "NULL" ? "" : value;
    }

    private static string? Text(Dictionary<string, ColumnValue> row, string column)
    {
        return row.TryGetValue(column, out ColumnValue value) ? Text(value) : null;
    }

    // The plain text of a cell, without the Spectre escapes that ValueFormatter adds. Null for a SQL
    // null.
    private static string? Text(ColumnValue value)
    {
        return value.Type switch
        {
            ColumnType.Id or ColumnType.String or ColumnType.Numeric or ColumnType.Jsonb => value.StrValue,
            ColumnType.Integer64 => value.LongValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Float64 or ColumnType.Float32 => value.FloatValue.ToString(CultureInfo.InvariantCulture),
            ColumnType.Bool => value.BoolValue ? "true" : "false",
            ColumnType.Date or ColumnType.DateTime => value.IsoValue,
            _ => null,
        };
    }

    private static string[] SplitList(string? value)
    {
        return string.IsNullOrEmpty(value)
            ? []
            : value.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
    }

    private static string Quote(string name) => "`" + name.Replace("`", "``") + "`";

    // A psql pattern: `*` is any run of characters and `?` is one character. A name in backticks or
    // double quotes is taken as it is, with no wildcards. CamusDB names are not case-sensitive, so
    // the match is not either.
    internal static Regex ToRegex(string pattern)
    {
        string trimmed = pattern.Trim();

        if (trimmed.Length >= 2 && (trimmed[0] == '`' || trimmed[0] == '"') && trimmed[^1] == trimmed[0])
        {
            string quote = trimmed[0].ToString();
            string literal = trimmed[1..^1].Replace(quote + quote, quote);
            return new Regex("^" + Regex.Escape(literal) + "$", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
        }

        StringBuilder regex = new("^");

        foreach (char c in trimmed)
        {
            regex.Append(c switch
            {
                '*' => ".*",
                '?' => ".",
                _ => Regex.Escape(c.ToString()),
            });
        }

        regex.Append('$');

        return new Regex(regex.ToString(), RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);
    }

    private static RelationKinds ToFlag(RelationKind kind) => kind switch
    {
        RelationKind.Table => RelationKinds.Tables,
        RelationKind.View => RelationKinds.Views,
        RelationKind.MaterializedView => RelationKinds.MaterializedViews,
        _ => RelationKinds.Sequences,
    };

    private static string TypeName(RelationKind kind) => kind switch
    {
        RelationKind.Table => "table",
        RelationKind.View => "view",
        RelationKind.MaterializedView => "materialized view",
        _ => "sequence",
    };

    // The words for the list title and for the "Did not find any …" message.
    private static string NounFor(RelationKinds kinds) => kinds switch
    {
        RelationKinds.Tables => "tables",
        RelationKinds.Views => "views",
        RelationKinds.MaterializedViews => "materialized views",
        RelationKinds.Sequences => "sequences",
        RelationKinds.Indexes => "indexes",
        _ => "relations",
    };

    // The position of `target` at nesting depth 0, outside quotes, from `start`. `depthStart` is the
    // depth at `start`: 1 to find the parenthesis that closes one already open.
    private static int IndexOfTopLevel(string text, char target, int start, int depthStart = 0)
    {
        int depth = depthStart;

        for (int i = start; i < text.Length; i++)
        {
            char c = text[i];

            if (c is '\'' or '`' or '"')
            {
                i = SkipQuoted(text, i);
                continue;
            }

            if (c == '(')
            {
                if (target == '(' && depth == 0)
                    return i;

                depth++;
            }
            else if (c == ')')
            {
                depth--;

                if (target == ')' && depth == 0)
                    return i;
            }
        }

        return -1;
    }

    // Splits the body of a CREATE TABLE at the commas that are outside parentheses and quotes.
    private static List<string> SplitTopLevel(string body)
    {
        List<string> items = [];
        int depth = 0;
        int start = 0;

        for (int i = 0; i < body.Length; i++)
        {
            char c = body[i];

            if (c is '\'' or '`' or '"')
            {
                i = SkipQuoted(body, i);
                continue;
            }

            if (c == '(')
                depth++;
            else if (c == ')')
                depth--;
            else if (c == ',' && depth == 0)
            {
                items.Add(body[start..i].Trim());
                start = i + 1;
            }
        }

        items.Add(body[start..].Trim());
        items.RemoveAll(string.IsNullOrEmpty);

        return items;
    }

    // The position of the quote that closes the one at `open`. A doubled quote is a literal one, and
    // a backslash escapes the next character in a string, but not in a backtick-quoted name.
    private static int SkipQuoted(string text, int open)
    {
        char quote = text[open];

        for (int i = open + 1; i < text.Length; i++)
        {
            if (text[i] == '\\' && quote != '`')
            {
                i++;
                continue;
            }

            if (text[i] != quote)
                continue;

            if (i + 1 < text.Length && text[i + 1] == quote)
            {
                i++;
                continue;
            }

            return i;
        }

        return text.Length - 1;
    }

    private static string? ReadIdentifier(string text, int start, out int end)
    {
        end = start;

        if (start >= text.Length || text[start] != '`')
            return null;

        end = SkipQuoted(text, start) + 1;

        return text[(start + 1)..(end - 1)].Replace("``", "`");
    }

    // The comment at the end of a column definition: `... NOT NULL COMMENT 'text'`.
    private static string? FindTrailingComment(string definition)
    {
        for (int i = 0; i < definition.Length; i++)
        {
            char c = definition[i];

            if (c is '\'' or '`' or '"')
            {
                i = SkipQuoted(definition, i);
                continue;
            }

            if (i > 0 && !char.IsWhiteSpace(definition[i - 1]))
                continue;

            Match match = Regex.Match(definition[i..], """^COMMENT\s+""", RegexOptions.IgnoreCase);

            if (match.Success && ReadStringLiteral(definition, i + match.Length, out int end) is { } comment &&
                definition[end..].Trim().Length == 0)
                return comment;
        }

        return null;
    }

    // Reads the string literal at `start`: a plain 'text' with '' for a quote, or an E'text' escape
    // string, the two forms that SHOW CREATE TABLE writes. `end` is the position after it.
    private static string? ReadStringLiteral(string text, int start, out int end)
    {
        end = start;
        bool escapes = false;

        if (start < text.Length && (text[start] == 'E' || text[start] == 'e'))
        {
            escapes = true;
            start++;
        }

        if (start >= text.Length || text[start] != '\'')
            return null;

        StringBuilder value = new();

        for (int i = start + 1; i < text.Length; i++)
        {
            char c = text[i];

            if (c == '\\' && escapes && i + 1 < text.Length)
            {
                char next = text[++i];
                value.Append(next switch
                {
                    'n' => '\n',
                    't' => '\t',
                    'r' => '\r',
                    '0' => '\0',
                    _ => next,
                });
                continue;
            }

            if (c == '\'')
            {
                if (i + 1 < text.Length && text[i + 1] == '\'')
                {
                    value.Append('\'');
                    i++;
                    continue;
                }

                end = i + 1;
                return value.ToString();
            }

            value.Append(c);
        }

        return null;
    }

    // `owner_id` becomes owner_id, but a name that needs its backticks keeps them. Quoted strings are
    // left as they are.
    private static string UnquoteSimpleIdentifiers(string text)
    {
        StringBuilder result = new(text.Length);

        for (int i = 0; i < text.Length; i++)
        {
            char c = text[i];

            if (c is not ('\'' or '`' or '"'))
            {
                result.Append(c);
                continue;
            }

            int close = SkipQuoted(text, i);
            string quoted = text[i..(close + 1)];

            if (c == '`' && Regex.IsMatch(quoted, """^`[A-Za-z_][A-Za-z0-9_]*`$"""))
                result.Append(quoted, 1, quoted.Length - 2);
            else
                result.Append(quoted);

            i = close;
        }

        return result.ToString();
    }
}
