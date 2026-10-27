
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Globalization;

// The output of the `backup …` commands.
internal static class BackupReport
{
    internal static void WriteUsage()
    {
        AnsiConsole.MarkupLine("[white]Backup commands[/] [grey58](node-wide: every database on the server is captured)[/]");
        AnsiConsole.MarkupLine("  [cyan]backup full[/]                            take a full backup");
        AnsiConsole.MarkupLine("  [cyan]backup incremental <parent-backup-id>[/]  chain an incremental onto a backup");
        AnsiConsole.MarkupLine("  [cyan]backup coordinated[/]                     take a cluster-wide consistent backup");
        AnsiConsole.MarkupLine("  [cyan]backup list[/]                            list the node's backup catalog");
        AnsiConsole.MarkupLine("  [cyan]backup chain <leaf-backup-id>[/]          resolve and validate a restore chain");
        AnsiConsole.MarkupLine("  [cyan]backup gc preview[/]                      report what retention would reclaim");
        AnsiConsole.MarkupLine("  [cyan]backup gc[/]                              run retention now\n");
    }

    // One backup, reported field by field: a take is a single result, and the columns of a catalog listing
    // would leave most of the width empty.
    internal static void WriteInfo(CamusBackupInfo info, TimeSpan elapsed)
    {
        List<(string Name, string Value)> fields =
        [
            ("Backup Id", info.BackupId),
            ("Type", info.ActualKind is { Length: > 0 } kind ? kind : info.Type),
            ("Created (UTC)", FormatUtc(info.CreatedAtUtc)),
            ("Parent", info.ParentBackupId ?? "(none)"),
            ("Partitions", info.PartitionCount.ToString(CultureInfo.InvariantCulture)),
        ];

        if (info.ClusterId is { Length: > 0 } clusterId)
            fields.Add(("Cluster", clusterId));

        if (info.CoordinatorNode is { Length: > 0 } coordinator)
            fields.Add(("Coordinator", coordinator));

        // The coordinated cut's HLC — the point in time every partition was captured at. An embedded
        // single node reports an all-zero cut, which says nothing worth a line of its own.
        if (info.ClusterSnapshotPhysical is > 0)
            fields.Add(("Snapshot HLC", $"{info.ClusterSnapshotNode}:{info.ClusterSnapshotPhysical}:{info.ClusterSnapshotCounter}"));

        int nameWidth = fields.Max(f => f.Name.Length);

        foreach ((string name, string value) in fields)
            AnsiConsole.MarkupLine("[blue]{0}[/]: {1}", Markup.Escape(name.PadLeft(nameWidth)), Markup.Escape(value));

        // The call succeeded but cost a full image rather than an increment — worth saying out loud, since
        // the whole point of asking for an incremental was to avoid that.
        if (info.WasSubstituted)
        {
            AnsiConsole.MarkupLine(
                "[yellow]Requested {0}, the server took {1}: {2}[/]",
                Markup.Escape(info.RequestedKind ?? "?"),
                Markup.Escape(info.ActualKind ?? "?"),
                Markup.Escape(info.SubstitutionReason ?? "no reason reported"));
        }

        AnsiConsole.MarkupLine("Backup OK ({0})\n", elapsed);
    }

    internal static void WriteList(IReadOnlyList<CamusBackupInfo> backups, TimeSpan elapsed)
    {
        if (backups.Count > 0)
        {
            Table table = new()
            {
                Border = TableBorder.Square
            };

            table.AddColumn("Backup Id");
            table.AddColumn("Type");
            table.AddColumn("Created (UTC)");
            table.AddColumn("Parent");
            table.AddColumn("Partitions");
            table.AddColumn("Status");

            foreach (CamusBackupInfo info in backups)
            {
                table.AddRow(
                    Markup.Escape(info.BackupId),
                    Markup.Escape(info.ActualKind is { Length: > 0 } kind ? kind : info.Type),
                    Markup.Escape(info.IsInvalid ? "" : FormatUtc(info.CreatedAtUtc)),
                    Markup.Escape(info.ParentBackupId ?? ""),
                    Markup.Escape(info.IsInvalid ? "" : info.PartitionCount.ToString(CultureInfo.InvariantCulture)),
                    DescribeStatus(info));
            }

            AnsiConsole.Write(table);
        }

        AnsiConsole.MarkupLine("[blue]{0}[/] backups in set ({1})\n", backups.Count, elapsed);
    }

    // A listing fails open on a single unreadable manifest, so an entry can be present but meaningless;
    // say which, rather than printing a row of blanks with no explanation.
    private static string DescribeStatus(CamusBackupInfo info)
    {
        if (info.IsInvalid)
            return $"[red]invalid: {Markup.Escape(info.InvalidReason ?? "unreadable manifest")}[/]";

        if (info.WasSubstituted)
            return $"[yellow]substituted from {Markup.Escape(info.RequestedKind ?? "?")}[/]";

        return "[green]ok[/]";
    }

    // The window a point-in-time restore may target. The server reports it on the chain's root, not its
    // leaf, so it is a property of the resolved chain and belongs under the table rather than in a column.
    internal static void WriteRecoverableWindow(IReadOnlyList<CamusBackupInfo> chain)
    {
        if (chain.Count == 0)
            return;

        CamusBackupInfo root = chain[0];

        // A chain the server reports no coverage for — a freshly taken backup on an idle node has none —
        // comes back as null or as epoch 0. Printing "1970-01-01" for that would read as a real window.
        if (root.MinRecoverablePhysicalMs is not long from || root.MaxRecoverablePhysicalMs is not long to || from <= 0 || to <= 0)
            return;

        AnsiConsole.MarkupLine(
            "Recoverable window (UTC): [white]{0}[/] .. [white]{1}[/]\n",
            Markup.Escape(FormatEpochMs(from)),
            Markup.Escape(FormatEpochMs(to)));
    }

    internal static void WriteGcResult(CamusBackupGcResult result, TimeSpan elapsed)
    {
        List<CamusBackupGcDeletion> deletions = result.RetentionDeletions ?? [];
        List<CamusBackupGcOrphan> orphans = result.OrphanReclamations ?? [];

        if (deletions.Count > 0)
        {
            Table table = new()
            {
                Border = TableBorder.Square
            };

            table.AddColumn("Backup Id");
            table.AddColumn("Type");
            table.AddColumn("Created (UTC)");
            table.AddColumn("Size");
            table.AddColumn("Reason");

            foreach (CamusBackupGcDeletion deletion in deletions)
            {
                table.AddRow(
                    Markup.Escape(deletion.BackupId),
                    Markup.Escape(deletion.Type),
                    Markup.Escape(FormatUtc(deletion.CreatedAtUtc)),
                    Markup.Escape(FormatBytes(deletion.Bytes)),
                    Markup.Escape(deletion.Reason));
            }

            AnsiConsole.Write(table);
        }

        if (orphans.Count > 0)
        {
            Table table = new()
            {
                Border = TableBorder.Square
            };

            table.AddColumn("Orphan");
            table.AddColumn("Kind");
            table.AddColumn("Reason");

            foreach (CamusBackupGcOrphan orphan in orphans)
            {
                table.AddRow(
                    Markup.Escape(orphan.Name),
                    orphan.IsDirectory ? "directory" : "file",
                    Markup.Escape(orphan.Reason));
            }

            AnsiConsole.Write(table);
        }

        // A preview deleted nothing; saying "reclaimed" there would be a lie an operator acts on.
        string verb = result.Applied ? "reclaimed" : "would reclaim";

        AnsiConsole.MarkupLine(
            "{0}: [blue]{1}[/] backups, [blue]{2}[/] orphans, [blue]{3}[/] {4} ({5})\n",
            result.Applied ? "Retention applied" : "Retention preview",
            deletions.Count,
            orphans.Count,
            Markup.Escape(FormatBytes(result.BytesReclaimed)),
            verb,
            elapsed);
    }

    private static string FormatUtc(DateTime value)
        => value.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);

    private static string FormatEpochMs(long epochMs)
        => FormatUtc(DateTimeOffset.FromUnixTimeMilliseconds(epochMs).UtcDateTime);

    private static string FormatBytes(long bytes)
    {
        string[] units = ["B", "KB", "MB", "GB", "TB", "PB"];

        double size = bytes;
        int unit = 0;

        while (size >= 1024 && unit < units.Length - 1)
        {
            size /= 1024;
            unit++;
        }

        return unit == 0
            ? $"{bytes} B"
            : string.Format(CultureInfo.InvariantCulture, "{0:0.##} {1}", size, units[unit]);
    }
}
