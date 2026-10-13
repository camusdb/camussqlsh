/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using Spectre.Console;
using Spectre.Console.Rendering;
using System.Globalization;

/// <summary>
/// Draws an EXPLAIN result as a flow: the source at the top, each operator below the one that feeds
/// it, and the result at the bottom. An edge carries the number of rows its upper node produces.
///
/// <para>This is the terminal form of the web console's Flow view. Each operator is a panel in the
/// colour of its family. A join's second input has no row in the plan, so it is a dashed box beside
/// the join, or a line in the join's panel when the terminal is too narrow for the box.</para>
/// </summary>
internal static class QueryPlanFlow
{
    /// <summary>The widest a node panel gets, so the flow stays readable on a wide terminal.</summary>
    private const int MaxNodeWidth = 76;

    /// <summary>The narrowest a node panel gets before the side boxes move into the panels.</summary>
    private const int MinNodeWidth = 40;

    private const int SideWidth = 30;

    /// <summary>The columns between a join's panel and its side box, which the link arrow uses.</summary>
    private const int LinkWidth = 4;

    /// <summary>From this row count up, an edge is drawn heavy, so a wide stream stands out.</summary>
    private const long HeavyEdgeRows = 10_000;

    private const string EdgeColor = "grey50";

    private sealed record Glyphs(
        string Shaft,
        string HeavyShaft,
        string Arrow,
        string Link,
        string BarFull,
        string BarEmpty,
        string Separator,
        string Times,
        string Bullet);

    private static readonly Glyphs UnicodeGlyphs = new("│", "┃", "▼", "◀╌╌", "━", "─", "·", "×", "•");

    private static readonly Glyphs AsciiGlyphs = new("|", "#", "v", "<--", "=", "-", "-", "x", "*");

    public static void Write(IAnsiConsole console, QueryPlan plan)
    {
        Glyphs glyphs = console.Profile.Capabilities.Unicode ? UnicodeGlyphs : AsciiGlyphs;
        int width = console.Profile.Width;

        bool hasSideInputs = plan.Nodes.Any(n => n.SideInput is not null);
        bool sideBeside = hasSideInputs && width >= MinNodeWidth + LinkWidth + SideWidth;
        int nodeWidth = Math.Max(Math.Min(MaxNodeWidth, sideBeside ? width - LinkWidth - SideWidth : width), 20);
        int center = nodeWidth / 2;

        QueryPlanNode? hottest = Hottest(plan);

        List<IRenderable> lines = [Summary(plan, glyphs), Text.Empty];

        for (int i = 0; i < plan.Nodes.Count; i++)
        {
            QueryPlanNode node = plan.Nodes[i];

            if (i > 0)
                lines.AddRange(Edge(plan.Nodes[i - 1], center, glyphs));

            Panel panel = NodePanel(plan, node, node == hottest, nodeWidth, inlineSide: !sideBeside, glyphs);

            lines.Add(sideBeside && node.SideInput is not null
                ? WithSideBox(panel, node, nodeWidth, glyphs)
                : panel);
        }

        lines.AddRange(Edge(plan.Nodes[^1], center, glyphs));
        lines.Add(new Padder(
            new Panel(new Markup("[bold]Result[/]")) { Border = BoxBorder.Rounded, BorderStyle = Style.Parse(EdgeColor) },
            new Padding(Math.Max(center - 4, 0), 0, 0, 0)));

        if (plan.Notes.Count > 0)
        {
            Grid notes = new();
            notes.AddColumn(new GridColumn { NoWrap = true, Padding = new Padding(0, 0, 2, 0) });
            notes.AddColumn(new GridColumn { Padding = new Padding(0) });

            foreach (QueryPlanNote note in plan.Notes)
                notes.AddRow(new Markup($"[grey]{Markup.Escape(NoteLabel(note.Name))}[/]"), new Text(note.Detail));

            lines.Add(Text.Empty);
            lines.Add(notes);
        }

        console.Write(new Rows(lines));
    }

    /// <summary>The line over the flow: the stage, the operator count, the total cost and, with ANALYZE, the time.</summary>
    private static Markup Summary(QueryPlan plan, Glyphs glyphs)
    {
        List<string> parts =
        [
            $"[grey85 on grey23] {Markup.Escape(plan.Stage)} [/]",
            $"{plan.Nodes.Count} operator{(plan.Nodes.Count == 1 ? "" : "s")}",
        ];

        if (plan.TotalEstimatedCost > 0)
            parts.Add($"estimated cost [bold]{FormatCost(plan.TotalEstimatedCost)}[/]");

        if (plan.TotalTimeMs is double ms)
            parts.Add($"measured time [bold]{FormatMs(ms)}[/]");

        parts.Add("[grey]rows flow from top to bottom[/]");

        return new Markup(string.Join($" [grey]{glyphs.Separator}[/] ", parts));
    }

    private static Panel NodePanel(QueryPlan plan, QueryPlanNode node, bool hot, int width, bool inlineSide, Glyphs glyphs)
    {
        string color = KindColor(node.Kind);
        List<IRenderable> content = [];

        string head = $"[bold]{Markup.Escape(node.Operator)}[/]";
        if (node.Target is not null)
            head += $"  [{color}]{Markup.Escape(node.Target)}[/]";
        if (hot)
            head += "  [red]Highest cost[/]";
        if (EstimateMiss(node, glyphs) is string miss)
            head += $"  [yellow]{miss}[/]";
        content.Add(new Markup(head));

        if (Describe(node.Operator) is string description)
            content.Add(new Markup($"[grey]{Markup.Escape(description)}[/]"));

        if (node.Detail.Length > 0)
            content.Add(new Text(node.Detail));

        if (inlineSide && node.SideInput is not null)
        {
            string side = $"[{color}]{glyphs.Link[..1]} {Markup.Escape(node.SideInput)}[/]";
            if (node.SideDetail is not null)
                side += $"[grey]: {Markup.Escape(node.SideDetail)}[/]";
            content.Add(new Markup(side));
        }

        double? share = CostShare(plan, node);

        if (Metrics(node, share) is string metrics)
            content.Add(new Markup(metrics));

        // The panel takes two columns for its borders and two for its padding.
        if (share is double fraction)
            content.Add(new Markup(CostBar(fraction, width - 4, color, glyphs)));

        foreach (QueryPlanSpan span in node.Spans)
            content.Add(new Markup($"[grey]{glyphs.Bullet} {Markup.Escape(span.Detail)}[/]"));

        return new Panel(new Rows(content))
        {
            Header = new PanelHeader($"[{color} bold] {KindLabel(node.Kind)} [/]"),
            Border = BoxBorder.Rounded,
            BorderStyle = hot ? new Style(Color.Red) : Style.Parse(color),
            Padding = new Padding(1, 0, 1, 0),
            Width = width,
        };
    }

    /// <summary>
    /// The join's panel with its second input in a dashed box beside it. The link arrow sits on the
    /// second line, which is the first line of text in both boxes.
    /// </summary>
    private static Grid WithSideBox(Panel panel, QueryPlanNode node, int nodeWidth, Glyphs glyphs)
    {
        string color = KindColor(node.Kind);
        string text = $"[{color} bold]{Markup.Escape(node.SideInput!)}[/]";
        if (node.SideDetail is not null)
            text += $"\n{Markup.Escape(node.SideDetail)}";

        Panel side = new(new Markup(text))
        {
            Border = BoxBorder.Dashed,
            BorderStyle = Style.Parse(color),
            Padding = new Padding(1, 0, 1, 0),
        };

        Grid grid = new();
        grid.AddColumn(new GridColumn { Width = nodeWidth, Padding = new Padding(0) });
        grid.AddColumn(new GridColumn { Width = LinkWidth, NoWrap = true, Padding = new Padding(0) });
        grid.AddColumn(new GridColumn { Width = SideWidth, Padding = new Padding(0) });
        grid.AddRow(panel, new Markup($"\n[{EdgeColor}] {glyphs.Link}[/]"), side);

        return grid;
    }

    /// <summary>The arrow out of <paramref name="from"/>. Its label and its weight give the rows that node produces.</summary>
    private static IEnumerable<IRenderable> Edge(QueryPlanNode from, int center, Glyphs glyphs)
    {
        long? rows = from.ActualRows ?? from.EstimatedRows;
        string shaft = rows >= HeavyEdgeRows ? glyphs.HeavyShaft : glyphs.Shaft;
        string pad = new(' ', center);
        string label = EdgeLabel(from, glyphs) is string text ? $" [grey]{text}[/]" : "";

        yield return new Markup($"{pad}[{EdgeColor}]{shaft}[/]{label}");
        yield return new Markup($"{pad}[{EdgeColor}]{glyphs.Arrow}[/]");
    }

    private static string? EdgeLabel(QueryPlanNode from, Glyphs glyphs)
    {
        if (from.ActualRows is long actual)
        {
            string text = $"{FormatCount(actual)} row{(actual == 1 ? "" : "s")}";
            return from.EstimatedRows is long estimated ? $"{text} {glyphs.Separator} est. {FormatCount(estimated)}" : text;
        }

        return from.EstimatedRows is long rows ? $"est. {FormatCount(rows)} row{(rows == 1 ? "" : "s")}" : null;
    }

    /// <summary>
    /// The one operator that costs more than each of the others. With one costed operator, or with
    /// a tie at the top, there is nothing to single out.
    /// </summary>
    private static QueryPlanNode? Hottest(QueryPlan plan)
    {
        List<QueryPlanNode> costed = plan.Nodes
            .Where(n => n.EstimatedCost > 0)
            .OrderByDescending(n => n.EstimatedCost)
            .ToList();

        return costed.Count > 1 && costed[0].EstimatedCost > costed[1].EstimatedCost ? costed[0] : null;
    }

    /// <summary>The node's part of the plan's estimated cost. Null for a node that costs nothing.</summary>
    private static double? CostShare(QueryPlan plan, QueryPlanNode node) =>
        plan.TotalEstimatedCost > 0 && node.EstimatedCost is double cost and > 0 ? cost / plan.TotalEstimatedCost : null;

    private static string CostBar(double share, int width, string color, Glyphs glyphs)
    {
        int filled = Math.Clamp((int)Math.Round(share * width), 1, width);

        return $"[{color}]{string.Concat(Enumerable.Repeat(glyphs.BarFull, filled))}[/]" +
               $"[grey35]{string.Concat(Enumerable.Repeat(glyphs.BarEmpty, width - filled))}[/]";
    }

    private static string? Metrics(QueryPlanNode node, double? share)
    {
        List<string> parts = [];

        if (share is double s)
            parts.Add($"[grey]est. cost[/] [bold]{FormatCost(node.EstimatedCost!.Value)}[/] [grey]({FormatShare(s)})[/]");

        if (Counter(node, node.RowsRead) is long read)
            parts.Add($"[grey]rows read[/] [bold]{FormatCount(read)}[/]");

        if (Counter(node, node.KvLookups) is long lookups)
            parts.Add($"[grey]KV lookups[/] [bold]{FormatCount(lookups)}[/]");

        if (Counter(node, node.KvScanEntries) is long entries)
            parts.Add($"[grey]KV scan entries[/] [bold]{FormatCount(entries)}[/]");

        return parts.Count == 0 ? null : string.Join("   ", parts);
    }

    /// <summary>
    /// ANALYZE reports the storage counters on every operator, and they are 0 on each one that
    /// does not touch storage. A zero is news on a source only, so the other operators leave it out.
    /// </summary>
    private static long? Counter(QueryPlanNode node, long? value) =>
        value is long v && (v > 0 || node.Kind == PlanNodeKind.Source) ? v : null;

    /// <summary>
    /// A label when the measured rows are ten times the estimate or a tenth of it. Small counts are
    /// left out: 2 rows against an estimate of 25 is not worth a developer's attention.
    /// </summary>
    private static string? EstimateMiss(QueryPlanNode node, Glyphs glyphs)
    {
        if (node.ActualRows is not long actual || node.EstimatedRows is not long estimated)
            return null;

        long high = Math.Max(actual, estimated);
        long low = Math.Max(Math.Min(actual, estimated), 1);
        if (high < 100 || high / (double)low < 10)
            return null;

        return string.Format(CultureInfo.InvariantCulture, "Estimate off {0:0}{1}", high / (double)low, glyphs.Times);
    }

    private static string FormatCount(long count) => count.ToString("N0", CultureInfo.InvariantCulture);

    private static string FormatCost(double cost) =>
        cost.ToString(cost >= 100 ? "N0" : "#,0.##", CultureInfo.InvariantCulture);

    private static string FormatShare(double share) =>
        share < 0.005 ? "<1%" : string.Format(CultureInfo.InvariantCulture, "{0:0}%", share * 100);

    private static string FormatMs(double ms) => ms switch
    {
        < 1 => string.Format(CultureInfo.InvariantCulture, "{0:0.##}μs", ms * 1000),
        < 1000 => string.Format(CultureInfo.InvariantCulture, "{0:0.##}ms", ms),
        _ => string.Format(CultureInfo.InvariantCulture, "{0:0.###}s", ms / 1000),
    };

    /// <summary>The web console's colour for each family, so a plan reads the same in both tools.</summary>
    private static string KindColor(PlanNodeKind kind) => kind switch
    {
        PlanNodeKind.Source => "#4ea1ff",
        PlanNodeKind.Join => "#b388ff",
        PlanNodeKind.Filter => "#ffb74d",
        PlanNodeKind.Aggregate => "#4dd0e1",
        PlanNodeKind.Sort => "#f48fb1",
        PlanNodeKind.Limit => "#aed581",
        PlanNodeKind.Project => "#2ecc71",
        PlanNodeKind.Exchange => "#ff8a65",
        _ => "grey",
    };

    private static string KindLabel(PlanNodeKind kind) => kind switch
    {
        PlanNodeKind.Source => "SOURCE",
        PlanNodeKind.Join => "JOIN",
        PlanNodeKind.Filter => "FILTER",
        PlanNodeKind.Aggregate => "AGGREGATE",
        PlanNodeKind.Sort => "SORT",
        PlanNodeKind.Limit => "LIMIT",
        PlanNodeKind.Project => "OUTPUT",
        PlanNodeKind.Exchange => "EXCHANGE",
        _ => "STEP",
    };

    private static string? Describe(string op) => op.ToLowerInvariant() switch
    {
        "table-scan" => "Reads every row of the table.",
        "index-lookup" => "Finds the rows for one exact index key.",
        "index-range-scan" => "Reads a range of keys from an index.",
        "index-in-list" => "Looks up each value of an IN list in an index.",
        "derived-table-scan" => "Reads the rows of a subquery in FROM.",
        "constant-source" => "Makes one row without reading a table.",
        "filter" => "Keeps only the rows that match the condition.",
        "having-filter" => "Keeps only the groups that match HAVING.",
        "aggregate" => "Groups the rows and computes the aggregates.",
        "distinct" => "Removes duplicate rows.",
        "sort" => "Sorts all the rows.",
        "topk" => "Keeps only the first k rows in sort order, without a full sort.",
        "limit" => "Stops after the row limit.",
        "project" => "Computes the output columns.",
        "nested-loop-join" => "Compares each left row with each right row.",
        "index-nested-loop-join" => "For each left row, finds the matching right rows through an index.",
        "hash-join" => "Puts one input in a hash table, then probes it with the other input.",
        "merge-join" => "Merges two inputs that are sorted on the join keys.",
        "semi-join" => "Keeps each row that has a match in the subquery (IN, EXISTS).",
        "anti-join" => "Keeps each row that has no match in the subquery (NOT IN, NOT EXISTS).",
        "null-aware-anti-join" => "Keeps each row that has no match in the subquery, with the NULL rules of NOT IN.",
        "gather" => "Collects the rows from the partitions of the cluster.",
        _ => null,
    };

    private static string NoteLabel(string name) => name.ToLowerInvariant() switch
    {
        "plan-info" => "Plan info",
        "distribution" => "Distribution",
        "cache" => "Result cache",
        _ => name,
    };
}
