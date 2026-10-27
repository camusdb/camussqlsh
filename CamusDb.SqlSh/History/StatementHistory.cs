
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;
using System.Text.RegularExpressions;

// The statement history file. It is per-user state, so it lives under the user's own profile with
// owner-only permissions — never in the shared temp directory, where a fixed name would let any
// other local user read or clobber it (and where a path-separator typo once named the file with
// a literal ':' in it). --no-history skips the file entirely.
internal sealed class StatementHistory
{
    private const int MaxHistoryEntries = 2000;

    private readonly string path;

    private readonly List<string> entries;

    private StatementHistory(string path, List<string> entries)
    {
        this.path = path;
        this.entries = entries;
    }

    public IReadOnlyList<string> Entries => entries;

    // Opens the history at the user's state directory. Returns null, with a warning, when the
    // directory cannot be used: history is a convenience, and an unusable state directory shouldn't
    // cost the session.
    internal static async Task<StatementHistory?> TryLoadAsync()
    {
        try
        {
            UserPaths.EnsureStateDirectory();
            return await LoadAsync(UserPaths.HistoryPath);
        }
        catch (Exception ex)
        {
            Console.Error.WriteLine($"Warning: statement history is disabled ({ex.Message}).");
            return null;
        }
    }

    private static async Task<StatementHistory> LoadAsync(string historyPath)
    {
        List<string>? history = [];

        if (File.Exists(historyPath))
        {
            try
            {
                string historyText = await File.ReadAllTextAsync(historyPath);
                history = JsonSerializer.Deserialize<List<string>>(historyText);
            }
            catch
            {
                Console.WriteLine("Found invalid history");
            }
        }

        history ??= [];
        history = RemoveAdjacentDuplicates(history);

        // Same cap on load: a file grown elsewhere (or by an older version) costs one trim now
        // rather than an ever-larger file on every save.
        if (history.Count > MaxHistoryEntries)
            history = history[^MaxHistoryEntries..];

        return new StatementHistory(historyPath, history);
    }

    // Adds a statement for the file. A statement carrying a plaintext password (CREATE/ALTER USER …
    // IDENTIFIED BY '…') is kept out of it, because the file outlives the session.
    internal void Add(string sql)
    {
        if (CarriesPassword(sql))
            return;

        if (entries.Count > 0 && string.Equals(entries[^1], sql, StringComparison.Ordinal))
            return;

        entries.Add(sql);
    }

    internal async Task SaveAsync()
    {
        // Cap the file so a long session (or heavy -e scripting) can't grow it without bound; the
        // oldest entries drop off.
        List<string> saved = entries.Count > MaxHistoryEntries
            ? entries[^MaxHistoryEntries..]
            : entries;

        Directory.CreateDirectory(Path.GetDirectoryName(path)!);

        // Write to a sibling temp file, then rename over the target: a crash never leaves a
        // half-written file, and the rename replaces (rather than follows) anything planted at the
        // target path. The temp file is created with owner-only permissions before any content is
        // written, so the history — which holds the user's SQL — is never briefly world-readable.
        string tempPath = path + ".tmp";
        string json = JsonSerializer.Serialize(saved);

        await using (FileStream stream = new(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
        {
            if (!OperatingSystem.IsWindows())
                File.SetUnixFileMode(stream.SafeFileHandle, UnixFileMode.UserRead | UnixFileMode.UserWrite);

            await using (StreamWriter writer = new(stream))
                await writer.WriteAsync(json);
        }

        File.Move(tempPath, path, overwrite: true);
    }

    // True when the statement inlines a password, i.e. CREATE USER / ALTER USER … IDENTIFIED
    // [WITH plugin] BY '…'.
    private static bool CarriesPassword(string sql)
    {
        return Regex.IsMatch(sql, @"\bidentified\s+(with\s+\S+\s+)?by\b", RegexOptions.IgnoreCase);
    }

    private static List<string> RemoveAdjacentDuplicates(IEnumerable<string> history)
    {
        List<string> result = [];

        foreach (string item in history)
        {
            if (result.Count == 0 ||
                !string.Equals(result[^1], item, StringComparison.Ordinal))
            {
                result.Add(item);
            }
        }

        return result;
    }
}
