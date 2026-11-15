/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Text.Json;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// Keeps the text of every tab between sessions, in one JSON file next to the history file:
/// <c>{"active": 1, "tabs": ["select 1;", "show tables;"]}</c>.
///
/// <para>Earlier versions kept one editor in <c>query.sql</c>. When the tabs file does not exist, that
/// file becomes the first tab. The old file is not changed or deleted.</para>
/// </summary>
internal sealed class TabStore
{
    private sealed class Saved
    {
        public int Active { get; set; }
        public List<string> Tabs { get; set; } = new();
    }

    private static readonly JsonSerializerOptions Options = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        WriteIndented = true,
    };

    private readonly string _path;
    private readonly string? _legacyPath;

    public TabStore(string path, string? legacyPath)
    {
        _path = path;
        _legacyPath = legacyPath;
    }

    public string Path => _path;

    /// <summary>
    /// The saved tab texts and the index of the active tab. An empty list when nothing is saved or
    /// the file cannot be read: the editor text is a convenience, not the user's data.
    /// </summary>
    public (List<string> Tabs, int Active) Load()
    {
        try
        {
            if (File.Exists(_path))
            {
                Saved? saved = JsonSerializer.Deserialize<Saved>(File.ReadAllText(_path), Options);

                if (saved is { Tabs.Count: > 0 })
                    return (saved.Tabs, Math.Clamp(saved.Active, 0, saved.Tabs.Count - 1));
            }
            else if (_legacyPath is not null && File.Exists(_legacyPath))
            {
                return ([File.ReadAllText(_legacyPath).TrimEnd('\r', '\n')], 0);
            }
        }
        catch
        {
            // An unreadable file is not worth refusing to start over.
        }

        return ([], 0);
    }

    public void Save(IEnumerable<string> tabs, int active)
    {
        try
        {
            string json = JsonSerializer.Serialize(new Saved { Active = active, Tabs = tabs.ToList() }, Options);
            File.WriteAllText(_path, json);
        }
        catch
        {
            // Same: the editor text is a convenience, not the user's data.
        }
    }
}
