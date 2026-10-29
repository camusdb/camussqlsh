/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using System.Globalization;
using System.Text.RegularExpressions;

/// <summary>The family of a plan operator. The flow colours and describes a node by it.</summary>
internal enum PlanNodeKind
{
    Source,
    Join,
    Filter,
    Aggregate,
    Sort,
    Limit,
    Project,
    Exchange,
    SetOperation,
    Other,
}

/// <summary>
/// A query plan read back from the rows of an EXPLAIN result set.
///
/// <para>The server walks a plan that is one chain: every operator has a single input, and it
/// emits the chain root first. A join is a link in that chain whose left input is the next row;
/// its right input never gets a row of its own, so the join's detail text is the only place that
/// names it (see <see cref="QueryPlanNode.SideInput"/>).</para>
///
/// <para>A set operation (UNION, INTERSECT, EXCEPT) is the one place where the plan branches. It is
/// the source of its chain, and each of its two operands is a chain of its own under an
/// <c>operand</c> row (see <see cref="QueryPlanNode.Branches"/>).</para>
/// </summary>
internal sealed class QueryPlan
{
    /// <summary><c>physical</c>, <c>logical</c> or <c>analyze</c>, as the <c>stage</c> column gives it.</summary>
    public required string Stage { get; init; }

    /// <summary>True when the rows carry measured counters, which only EXPLAIN (ANALYZE) emits.</summary>
    public bool IsAnalyze { get; init; }

    /// <summary>The operators in data-flow order: the first one reads the source, the last one feeds the result.</summary>
    public required IReadOnlyList<QueryPlanNode> Nodes { get; init; }

    /// <summary>Every operator of the plan: the main chain and, under each set operation, the chains of its operands.</summary>
    public IEnumerable<QueryPlanNode> AllNodes => Flatten(Nodes);

    /// <summary>The trailing rows that describe the whole plan, such as <c>plan-info</c> and <c>cache</c>.</summary>
    public IReadOnlyList<QueryPlanNote> Notes { get; init; } = [];

    /// <summary>The sum of every node's own estimated cost. A node's cost is not cumulative.</summary>
    public double TotalEstimatedCost { get; init; }

    /// <summary>Measured wall-clock time of the whole plan. Only EXPLAIN (ANALYZE) reports it.</summary>
    public double? TotalTimeMs { get; init; }

    internal static IEnumerable<QueryPlanNode> Flatten(IEnumerable<QueryPlanNode> nodes) =>
        nodes.SelectMany(n => n.Branches.SelectMany(b => Flatten(b.Nodes)).Prepend(n));
}

internal sealed class QueryPlanNode
{
    /// <summary>The canonical operator name, such as <c>table-scan</c> or <c>hash-join</c>.</summary>
    public required string Operator { get; init; }

    /// <summary>The text that was inside the operator's parentheses. Empty when it had none.</summary>
    public string Detail { get; init; } = "";

    public PlanNodeKind Kind { get; init; }

    /// <summary>The table, index or alias this operator works on, when the detail names one.</summary>
    public string? Target { get; init; }

    /// <summary>
    /// The name of a join's second input, which has no row of its own in the plan: <c>Right input</c>,
    /// or <c>Subquery</c> for a semi-join or an anti-join. Null for an operator that is not a join.
    /// </summary>
    public string? SideInput { get; init; }

    /// <summary>What the detail text tells about the second input, such as its alias or its index.</summary>
    public string? SideDetail { get; init; }

    public long? EstimatedRows { get; init; }

    public double? EstimatedCost { get; init; }

    public long? ActualRows { get; init; }

    public long? RowsRead { get; init; }

    /// <summary>Wall-clock time of the whole plan. The server reports it on the root node only.</summary>
    public double? ActualTimeMs { get; init; }

    public long? KvLookups { get; init; }

    public long? KvScanEntries { get; init; }

    /// <summary>
    /// The span rows that EXPLAIN (ANALYZE) emits after a gather (<c>gather-span</c>) or after a join
    /// that ran a broadcast probe (<c>broadcast-span</c>), one per span.
    /// </summary>
    public List<QueryPlanSpan> Spans { get; } = [];

    /// <summary>The left and the right operand of a set operation. Empty for any other operator.</summary>
    public List<QueryPlanBranch> Branches { get; } = [];
}

/// <summary>
/// One operand of a set operation: the <c>operand</c> row, which under EXPLAIN (ANALYZE) counts the
/// rows the operand returned, and the operand's own chain in data-flow order.
/// </summary>
internal sealed record QueryPlanBranch(QueryPlanNode Operand, IReadOnlyList<QueryPlanNode> Nodes)
{
    /// <summary><c>left</c> or <c>right</c>, as the detail of the operand row gives it.</summary>
    public string Side => SideOf(Operand);

    internal static string SideOf(QueryPlanNode operand) => operand.Detail.Split(',')[0].Trim();
}

internal sealed record QueryPlanSpan(string Detail, long? ActualRows, long? RowsRead);

internal sealed record QueryPlanNote(string Name, string Detail);

/// <summary>
/// Reads the rows of an EXPLAIN result set back into a <see cref="QueryPlan"/>.
///
/// <para>The input is the server's own rendering (ExplainExecutor and PlanRenderer): one row per
/// operator with the columns <c>stage</c>, <c>node</c>, <c>detail</c>, <c>estimated_rows</c> and
/// <c>estimated_cost</c>, plus the measured columns that EXPLAIN (ANALYZE) adds. After the operators
/// come rows that describe the whole plan (<c>plan-info</c>, <c>distribution</c>, <c>cache</c>), and
/// ANALYZE puts span rows after a gather or a broadcast join. The parser keeps those apart, because
/// they are not steps of the flow.</para>
///
/// <para>The web console has the same parser over its own row type. Keep the two in step when the
/// server's EXPLAIN shape changes.</para>
/// </summary>
internal static class QueryPlanParser
{
    private static readonly HashSet<string> NoteRows = new(StringComparer.OrdinalIgnoreCase)
    {
        "plan-info", "distribution", "cache",
    };

    private static readonly HashSet<string> SpanRows = new(StringComparer.OrdinalIgnoreCase)
    {
        "gather-span", "broadcast-span",
    };

    // A detail is "key=value, key=value". A value can hold an expression with commas, so each pattern
    // anchors its key at the start of the detail or after ", " and stops the value at the next comma.
    private static readonly Regex TableKey = DetailKey("table");
    private static readonly Regex IndexKey = DetailKey("index");
    private static readonly Regex AliasKey = DetailKey("alias");
    private static readonly Regex RightKey = DetailKey("right");
    private static readonly Regex InnerKey = DetailKey("inner");
    private static readonly Regex RightOrderKey = DetailKey("right-order");

    /// <summary>
    /// Returns the plan, or null when the columns are not the EXPLAIN shape or no operator row came
    /// back. A null result leaves the caller with the plain table.
    /// </summary>
    public static QueryPlan? TryParse(IReadOnlyList<Dictionary<string, ColumnValue>> rows)
    {
        if (rows.Count == 0)
            return null;

        // The reader names the columns as the server sends them. The lookup ignores case, so the
        // parser does not depend on the server's spelling of a name.
        Dictionary<string, string> columns = new(StringComparer.OrdinalIgnoreCase);
        foreach (string name in rows[0].Keys)
            columns.TryAdd(name, name);

        if (!columns.ContainsKey("node") || !columns.ContainsKey("detail"))
            return null;

        List<QueryPlanNode> nodes = [];
        List<QueryPlanNote> notes = [];
        string stage = "";
        bool isAnalyze = columns.ContainsKey("actual_rows");

        foreach (Dictionary<string, ColumnValue> row in rows)
        {
            string name = Text(row, columns, "node") ?? "";
            string detail = Text(row, columns, "detail") ?? "";

            if (stage.Length == 0)
                stage = Text(row, columns, "stage") ?? "";

            if (NoteRows.Contains(name))
            {
                notes.Add(new QueryPlanNote(name, detail));
                continue;
            }

            if (SpanRows.Contains(name))
            {
                QueryPlanSpan span = new(detail, Long(row, columns, "actual_rows"), Long(row, columns, "rows_read"));
                if (nodes.Count > 0)
                    nodes[^1].Spans.Add(span);
                else
                    notes.Add(new QueryPlanNote(name, detail));
                continue;
            }

            if (name.Length == 0)
                continue;

            nodes.Add(CreateNode(name, detail, row, columns));
        }

        if (nodes.Count == 0)
            return null;

        List<QueryPlanNode> flow;

        if (nodes.Any(n => n.Kind == PlanNodeKind.SetOperation))
        {
            int position = 0;
            flow = ReadChain(nodes, ref position, branch: false);
        }
        else
        {
            // A plan comes root first, so the source is the last row. The FROM-less shape is the one
            // exception: it lists constant-source, then project, then limit. The source is the only
            // node with no input, so whichever end holds it is where the flow starts.
            flow = nodes;
            if (!(flow[0].Kind == PlanNodeKind.Source && flow[^1].Kind != PlanNodeKind.Source))
                flow.Reverse();
        }

        return new QueryPlan
        {
            Stage = stage.Length == 0 ? (isAnalyze ? "analyze" : "physical") : stage,
            IsAnalyze = isAnalyze,
            Nodes = flow,
            Notes = notes,
            TotalEstimatedCost = QueryPlan.Flatten(flow).Sum(n => n.EstimatedCost ?? 0),
            TotalTimeMs = flow.Select(n => n.ActualTimeMs).FirstOrDefault(t => t is not null),
        };
    }

    /// <summary>
    /// Reads one chain from the rows, which come root first in a depth-first walk, and returns it in
    /// data-flow order. A set operation is followed by its <c>operand(left)</c> row and the left
    /// chain, then by its <c>operand(right)</c> row and the right chain.
    ///
    /// <para>The chain of an operand ends at its source, or at a nested set operation once both of
    /// its operands are read. The main chain reads every row that is left, so a shape the parser
    /// does not know still shows each of its operators.</para>
    /// </summary>
    private static List<QueryPlanNode> ReadChain(List<QueryPlanNode> rows, ref int position, bool branch)
    {
        List<QueryPlanNode> chain = [];

        while (position < rows.Count)
        {
            QueryPlanNode node = rows[position];

            // In an operand's chain, the next operand row belongs to the set operation above it.
            if (branch && IsOperand(node))
                break;

            position++;
            chain.Add(node);

            if (node.Kind == PlanNodeKind.SetOperation)
            {
                ReadBranch(rows, ref position, node, "left");
                ReadBranch(rows, ref position, node, "right");
            }

            if (branch && node.Kind is PlanNodeKind.Source or PlanNodeKind.SetOperation)
                break;
        }

        chain.Reverse();
        return chain;
    }

    /// <summary>
    /// Reads one operand of <paramref name="setOperation"/> when the next row is its operand row.
    /// The side must match, so a nested set operation never takes the right operand of the one
    /// above it.
    /// </summary>
    private static void ReadBranch(List<QueryPlanNode> rows, ref int position, QueryPlanNode setOperation, string side)
    {
        if (position >= rows.Count || !IsOperand(rows[position]))
            return;

        QueryPlanNode operand = rows[position];
        if (!string.Equals(QueryPlanBranch.SideOf(operand), side, StringComparison.OrdinalIgnoreCase))
            return;

        position++;
        setOperation.Branches.Add(new QueryPlanBranch(operand, ReadChain(rows, ref position, branch: true)));
    }

    private static bool IsOperand(QueryPlanNode node) =>
        string.Equals(node.Operator, "operand", StringComparison.OrdinalIgnoreCase);

    private static QueryPlanNode CreateNode(string name, string detail, Dictionary<string, ColumnValue> row, Dictionary<string, string> columns)
    {
        PlanNodeKind kind = Classify(name);
        (string? side, string? sideDetail) = DescribeSideInput(name, detail);

        return new QueryPlanNode
        {
            Operator = name,
            Detail = detail,
            Kind = kind,
            Target = kind == PlanNodeKind.Source ? DescribeTarget(name, detail) : null,
            SideInput = side,
            SideDetail = sideDetail,
            EstimatedRows = Long(row, columns, "estimated_rows"),
            EstimatedCost = Double(row, columns, "estimated_cost"),
            ActualRows = Long(row, columns, "actual_rows"),
            RowsRead = Long(row, columns, "rows_read"),
            ActualTimeMs = Double(row, columns, "actual_time_ms"),
            KvLookups = Long(row, columns, "kv_lookups"),
            KvScanEntries = Long(row, columns, "kv_scan_entries"),
        };
    }

    internal static PlanNodeKind Classify(string name) => name.ToLowerInvariant() switch
    {
        "table-scan" or "index-lookup" or "index-range-scan" or "index-in-list"
            or "derived-table-scan" or "function-scan" or "constant-source" => PlanNodeKind.Source,
        "nested-loop-join" or "index-nested-loop-join" or "hash-join" or "merge-join"
            or "semi-join" or "anti-join" or "null-aware-anti-join" => PlanNodeKind.Join,
        "filter" or "having-filter" => PlanNodeKind.Filter,
        "aggregate" or "distinct" => PlanNodeKind.Aggregate,
        "sort" or "topk" => PlanNodeKind.Sort,
        "limit" => PlanNodeKind.Limit,
        "project" => PlanNodeKind.Project,
        "gather" => PlanNodeKind.Exchange,
        "union" or "intersect" or "except" => PlanNodeKind.SetOperation,
        _ => PlanNodeKind.Other,
    };

    private static string? DescribeTarget(string name, string detail) => name.ToLowerInvariant() switch
    {
        "table-scan" => Value(TableKey, detail),
        "index-lookup" or "index-range-scan" or "index-in-list" => Value(IndexKey, detail),
        "derived-table-scan" => Value(AliasKey, detail),
        "function-scan" => FunctionName(detail),
        _ => null,
    };

    /// <summary>
    /// The function a <c>function-scan</c> calls: its detail starts with the call. The one-row source of
    /// a FROM-less operand is <c>*single_row*()</c>, which is not a name a developer wrote, so it is left out.
    /// </summary>
    private static string? FunctionName(string detail)
    {
        int paren = detail.IndexOf('(');
        string name = paren < 0 ? "" : detail[..paren].Trim();
        return name.Length == 0 || name.StartsWith('*') ? null : name;
    }

    /// <summary>
    /// Names a join's second input from the detail text. Only the facts each operator states without
    /// doubt are used. A hash join's <c>build=</c> is not one of them: when the left side builds, it
    /// names the left input, so it says nothing about which alias the right input is.
    /// </summary>
    private static (string? Side, string? Detail) DescribeSideInput(string name, string detail)
    {
        switch (name.ToLowerInvariant())
        {
            case "nested-loop-join":
                return ("Right input", Value(RightKey, detail));

            case "index-nested-loop-join":
                return ("Right input", Value(IndexKey, detail) is { } lookupIndex ? $"looked up in index {lookupIndex}" : null);

            case "hash-join":
                return ("Right input", null);

            case "merge-join":
                return ("Right input", Value(RightOrderKey, detail) switch
                {
                    null => null,
                    "sort" => "sorted before the merge",
                    string order => $"read in order from {order.Replace("forced-index=", "index ", StringComparison.Ordinal)}",
                });

            case "semi-join":
            case "anti-join":
            case "null-aware-anti-join":
                {
                    string? inner = Value(InnerKey, detail);
                    string? index = Value(IndexKey, detail);
                    return ("Subquery", inner is null ? null : index is null ? inner : $"{inner} via index {index}");
                }

            default:
                return (null, null);
        }
    }

    private static Regex DetailKey(string key) => new(
        $@"(?:^|,\s){Regex.Escape(key)}=(?<value>[^,]+)",
        RegexOptions.CultureInvariant | RegexOptions.Compiled);

    private static string? Value(Regex key, string detail)
    {
        Match match = key.Match(detail);
        return match.Success ? match.Groups["value"].Value.Trim() : null;
    }

    private static ColumnValue? Cell(Dictionary<string, ColumnValue> row, Dictionary<string, string> columns, string column) =>
        columns.TryGetValue(column, out string? name) && row.TryGetValue(name, out ColumnValue value) ? value : null;

    private static string? Text(Dictionary<string, ColumnValue> row, Dictionary<string, string> columns, string column) =>
        Cell(row, columns, column) is { Type: ColumnType.String or ColumnType.Id } value ? value.StrValue : null;

    private static long? Long(Dictionary<string, ColumnValue> row, Dictionary<string, string> columns, string column) =>
        Cell(row, columns, column) switch
        {
            { Type: ColumnType.Integer64 } value => value.LongValue,
            { Type: ColumnType.Float64 or ColumnType.Float32 } value when double.IsFinite(value.FloatValue) => (long)value.FloatValue,
            { Type: ColumnType.Numeric or ColumnType.String } value when long.TryParse(value.StrValue,
                NumberStyles.Integer, CultureInfo.InvariantCulture, out long parsed) => parsed,
            _ => null,
        };

    private static double? Double(Dictionary<string, ColumnValue> row, Dictionary<string, string> columns, string column) =>
        Cell(row, columns, column) switch
        {
            { Type: ColumnType.Float64 or ColumnType.Float32 } value => value.FloatValue,
            { Type: ColumnType.Integer64 } value => value.LongValue,
            { Type: ColumnType.Numeric or ColumnType.String } value when double.TryParse(value.StrValue,
                NumberStyles.Float, CultureInfo.InvariantCulture, out double parsed) => parsed,
            _ => null,
        };
}
