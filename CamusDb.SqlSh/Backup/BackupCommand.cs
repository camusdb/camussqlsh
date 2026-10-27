
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using System.Diagnostics;

internal static class BackupCommand
{
    // Runs one `backup …` subcommand against the node's backup admin API. A usage mistake is reported and
    // swallowed the way `use` does it; a server-side failure is left to propagate, so it reaches
    // WriteStatementError with its CADB code and aborts a script the way a failed statement does.
    internal static async Task ExecuteAsync(CamusConnection connection, string arguments)
    {
        string[] parts = arguments.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length == 0 || string.Equals(parts[0], "help", StringComparison.InvariantCultureIgnoreCase))
        {
            BackupReport.WriteUsage();
            return;
        }

        string subcommand = parts[0].ToLowerInvariant();
        Stopwatch stopwatch = Stopwatch.StartNew();

        switch (subcommand)
        {
            case "full":
            case "coordinated":
            {
                if (parts.Length > 1)
                {
                    AnsiConsole.MarkupLine("[red]Usage: backup {0}[/]\n", subcommand);
                    return;
                }

                CamusBackupInfo info = subcommand == "full"
                    ? await connection.Backups.TakeFullBackupAsync()
                    : await connection.Backups.TakeCoordinatedBackupAsync();

                BackupReport.WriteInfo(info, stopwatch.Elapsed);
                return;
            }

            case "incremental":
            {
                if (parts.Length != 2)
                {
                    AnsiConsole.MarkupLine("[red]Usage: backup incremental <parent-backup-id>[/]\n");
                    return;
                }

                CamusBackupInfo info = await connection.Backups.TakeIncrementalBackupAsync(TrimBackupId(parts[1]));

                BackupReport.WriteInfo(info, stopwatch.Elapsed);
                return;
            }

            case "list":
            {
                if (parts.Length > 1)
                {
                    AnsiConsole.MarkupLine("[red]Usage: backup list[/]\n");
                    return;
                }

                IReadOnlyList<CamusBackupInfo> catalog = await connection.Backups.ListBackupsAsync();

                BackupReport.WriteList(catalog, stopwatch.Elapsed);
                return;
            }

            case "chain":
            {
                if (parts.Length != 2)
                {
                    AnsiConsole.MarkupLine("[red]Usage: backup chain <leaf-backup-id>[/]\n");
                    return;
                }

                IReadOnlyList<CamusBackupInfo> chain = await connection.Backups.GetChainAsync(TrimBackupId(parts[1]));

                BackupReport.WriteList(chain, stopwatch.Elapsed);
                BackupReport.WriteRecoverableWindow(chain);
                return;
            }

            case "gc":
            {
                bool preview = parts.Length == 2 &&
                    (string.Equals(parts[1], "preview", StringComparison.InvariantCultureIgnoreCase) ||
                     string.Equals(parts[1], "--dry-run", StringComparison.InvariantCultureIgnoreCase));

                if (parts.Length > 1 && !preview)
                {
                    AnsiConsole.MarkupLine("[red]Usage: backup gc [[preview]][/]\n");
                    return;
                }

                CamusBackupGcResult result = preview
                    ? await connection.Backups.PreviewGarbageCollectionAsync()
                    : await connection.Backups.CollectGarbageAsync();

                BackupReport.WriteGcResult(result, stopwatch.Elapsed);
                return;
            }

            default:
                AnsiConsole.MarkupLine("[red]Unknown backup command: {0}[/]\n", Markup.Escape(parts[0]));
                BackupReport.WriteUsage();
                return;
        }
    }

    // Backup ids are GUIDs, but they're routinely copied out of a quoted context; accept those spellings
    // rather than sending the quotes on to the server as part of the id.
    private static string TrimBackupId(string backupId) => backupId.Trim().Trim('\'', '"', '`');
}
