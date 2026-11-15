/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using Spectre.Console.Rendering;
using System.Collections.Concurrent;
using static SqlKind;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// The Data Catalog pane: the tree of databases, tables and columns, how it loads, its keys and how
/// it is drawn.
///
/// <para>The databases come from SHOW DATABASES. A database's tables load the first time it expands,
/// and a table's columns load the first time the table expands, one DESCRIBE each. Background loads
/// write the maps while the draw loop reads them, so the maps are concurrent and the database list is
/// replaced whole, never changed in place.</para>
/// </summary>
internal sealed class CatalogPane
{
    private enum RowKind { Database, Table, Column, Note }

    // One row of the flattened tree. The tree is stored flat because selection is an index, and
    // Spectre's Tree carries no selection state to move. A Note row is a placeholder, such as "…"
    // while a list loads. Its Table is empty when it sits under a database.
    private sealed record CatalogRow(RowKind Kind, string Database, string Table = "", string Name = "", string Type = "");

    private readonly TuiSession _session;
    private readonly TuiContext _context;
    private readonly SqlCompletion? _completion;

    private IReadOnlyList<string> _databases = [];
    private readonly ConcurrentDictionary<string, List<string>> _tables = new(StringComparer.OrdinalIgnoreCase);
    private readonly ConcurrentDictionary<string, List<(string Name, string Type)>> _columns = new(StringComparer.OrdinalIgnoreCase);

    // The expanded nodes: a database by its name, a table by TableKey.
    private readonly ConcurrentDictionary<string, bool> _expanded = new(StringComparer.OrdinalIgnoreCase);

    private bool _refreshing;
    private int _selected;
    private int _top;
    private int _visible = 1;

    public CatalogPane(TuiSession session, TuiContext context, SqlCompletion? completion)
    {
        _session = session;
        _context = context;
        _completion = completion;
    }

    public int DatabaseCount => _databases.Count;

    /// <summary>Opens the current database and loads the tree, at start.</summary>
    public async Task StartAsync()
    {
        if (_session.Database is not null)
            _expanded[_session.Database] = true;

        await LoadAsync();
        SelectCurrentDatabase();
    }

    /// <summary>After <c>use</c>: opens the new current database, loads again and selects it.</summary>
    public async Task OnDatabaseChangedAsync()
    {
        if (_session.Database is not null)
            _expanded[_session.Database] = true;

        await RefreshAsync();
        SelectCurrentDatabase();
    }

    private void SelectCurrentDatabase() =>
        SelectRow(r => r.Kind == RowKind.Database && _session.IsCurrent(r.Database));

    /// <summary>Loads the tree again, and the relation names that the completion offers.</summary>
    public async Task RefreshAsync()
    {
        await LoadAsync();

        if (_completion is not null)
            await _completion.RefreshTablesAsync(_session.Connection);
    }

    /// <summary>
    /// F4. The catalog does not see tables that another session creates, so it must be possible to
    /// load it again by hand. Returns the status line to show, and whether it is an error.
    /// </summary>
    public async Task<(string Status, bool Error)?> RefreshFromKeyAsync()
    {
        if (_refreshing)
            return null;

        _refreshing = true;

        try
        {
            await RefreshAsync();
            return ($"Catalog refreshed: {_databases.Count} databases.", false);
        }
        catch (Exception ex)
        {
            return (ex.Message, true);
        }
        finally
        {
            _refreshing = false;
            _context.Repaint();
        }
    }

    // ---------------------------------------------------------------- loading

    // Reloads the database list. The table lists of the expanded databases and the columns of the
    // expanded tables load again too. The cached lists of closed nodes are dropped, so they load
    // again when they open. The old lists stay on screen until the new ones arrive.
    public async Task LoadAsync()
    {
        List<string> databases = new();

        try
        {
            await using CamusConnection sys = await _session.OpenEndpointAsync();
            databases.AddRange(await ReadNamesAsync(sys, "show databases"));
        }
        catch
        {
            // No access to the database list. The current database still shows below.
        }

        if (_session.Database is { } current && !databases.Contains(current, StringComparer.OrdinalIgnoreCase))
            databases.Add(current);

        databases.Sort(StringComparer.OrdinalIgnoreCase);
        _databases = databases;

        foreach (string key in _tables.Keys)
            if (!_expanded.ContainsKey(key) || !databases.Contains(key, StringComparer.OrdinalIgnoreCase))
                _tables.TryRemove(key, out _);

        foreach (string key in _columns.Keys)
            if (!_expanded.ContainsKey(key))
                _columns.TryRemove(key, out _);

        foreach (string database in databases.Where(d => _expanded.ContainsKey(d)))
        {
            await LoadTablesAsync(database);

            foreach (string table in _tables.GetValueOrDefault(database) ?? [])
                if (_expanded.ContainsKey(TableKey(database, table)))
                    await DescribeAsync(database, table);
        }

        _context.Repaint();
    }

    private async Task LoadTablesAsync(string database)
    {
        List<string> tables = new();

        try
        {
            await WithDatabaseAsync(database, async connection =>
            {
                foreach (string statement in new[] { "show tables", "show views", "show materialized views" })
                {
                    try
                    {
                        tables.AddRange(await ReadNamesAsync(connection, statement));
                    }
                    catch
                    {
                        // A server without views. Show whatever did load.
                    }
                }
            });
        }
        catch
        {
            // The database did not open. It stays expanded, with no tables under it.
        }

        tables.Sort(StringComparer.OrdinalIgnoreCase);
        _tables[database] = tables;
        _context.Repaint();
    }

    // The first column of every row, for the SHOW statements that list names.
    private static async Task<List<string>> ReadNamesAsync(CamusConnection connection, string statement)
    {
        List<string> names = new();

        await using CamusCommand cmd = connection.CreateSelectCommand(statement);
        cmd.CommandTimeout = 10;

        CamusDataReader reader = await cmd.ExecuteReaderAsync();

        while (await reader.ReadAsync())
        {
            Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
            string? name = row.Values.FirstOrDefault().StrValue;
            if (!string.IsNullOrWhiteSpace(name))
                names.Add(name);
        }

        return names;
    }

    // The current database is read on the session's connection. Another database is read on a
    // connection of its own that closes at once, because the catalog only looks at it.
    private async Task WithDatabaseAsync(string database, Func<CamusConnection, Task> work)
    {
        if (_session.IsCurrent(database))
        {
            await work(_session.Connection);
            return;
        }

        await using CamusConnection other = await _session.OpenDatabaseAsync(database);
        await work(other);
    }

    // DESCRIBE runs off the key loop so the screen never freezes on a slow catalog read.
    private async Task DescribeAsync(string database, string table)
    {
        List<(string, string)> cols = new();

        try
        {
            await WithDatabaseAsync(database, async connection =>
            {
                await using CamusCommand cmd = connection.CreateSelectCommand($"describe {table}");
                cmd.CommandTimeout = 10;

                CamusDataReader reader = await cmd.ExecuteReaderAsync();

                while (await reader.ReadAsync())
                {
                    Dictionary<string, ColumnValue> row = ConnectionHelper.ReadCurrentRow(reader);
                    string[] keys = row.Keys.ToArray();

                    // Column order is the server's to change, so ask by name and fall back to
                    // position only when the expected name is absent.
                    string name = Pick(row, keys, 0, "field", "column", "name") ?? "?";
                    string type = Pick(row, keys, 1, "type", "datatype") ?? "";

                    cols.Add((name, type));
                }
            });
        }
        catch
        {
            // Leave the table expanded but empty rather than dropping the user back a level.
        }

        _columns[TableKey(database, table)] = cols;
        _context.Repaint();
    }

    private static string? Pick(Dictionary<string, ColumnValue> row, string[] keys, int fallbackIndex, params string[] names)
    {
        // ReadCurrentRow builds an ordinal dictionary, but DESCRIBE capitalizes its column names.
        foreach (string name in names)
            foreach (string key in keys)
                if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase)
                    && !string.IsNullOrWhiteSpace(row[key].StrValue))
                    return row[key].StrValue;

        return fallbackIndex < keys.Length ? row[keys[fallbackIndex]].StrValue : null;
    }

    // ---------------------------------------------------------------- keys

    /// <summary>
    /// Handles a key of the catalog pane. <paramref name="run"/> runs a statement in the active tab,
    /// and <paramref name="insert"/> puts a name into the active editor.
    /// </summary>
    public void HandleKey(ConsoleKeyInfo k, Action<string> run, Action<string> insert)
    {
        List<CatalogRow> rows = FlatRows();
        if (rows.Count == 0)
            return;

        CatalogRow row = rows[Math.Min(_selected, rows.Count - 1)];

        switch (k.Key)
        {
            case ConsoleKey.DownArrow: _selected = Math.Min(rows.Count - 1, _selected + 1); break;
            case ConsoleKey.UpArrow: _selected = Math.Max(0, _selected - 1); break;
            case ConsoleKey.Home: _selected = 0; break;
            case ConsoleKey.End: _selected = rows.Count - 1; break;
            case ConsoleKey.PageDown: _selected = Math.Min(rows.Count - 1, _selected + _visible); break;
            case ConsoleKey.PageUp: _selected = Math.Max(0, _selected - _visible); break;

            case ConsoleKey.RightArrow:
                Expand(row);
                break;

            // Enter on another database switches to it, as `use` does. On every other row it
            // expands, as Right does.
            case ConsoleKey.Enter:
                if (row.Kind == RowKind.Database && !_session.IsCurrent(row.Database))
                    run($"use `{row.Database.Replace("`", "``")}`");
                else
                    Expand(row);
                break;

            case ConsoleKey.LeftArrow:
                Collapse(row);
                break;

            // Space drops the selected name into the editor at the cursor, which is the whole point
            // of having the catalog next to the editor.
            case ConsoleKey.Spacebar:
                string? name = row.Kind switch
                {
                    RowKind.Database => row.Database,
                    RowKind.Table => row.Table,
                    RowKind.Column => row.Name,
                    _ => null,
                };

                if (name is not null)
                    insert(name);
                break;
        }
    }

    private static string TableKey(string database, string table) => database + "\u001f" + table;

    private void Expand(CatalogRow row)
    {
        if (row.Kind == RowKind.Database && _expanded.TryAdd(row.Database, true) && !_tables.ContainsKey(row.Database))
            _ = LoadTablesAsync(row.Database);
        else if (row.Kind == RowKind.Table && _expanded.TryAdd(TableKey(row.Database, row.Table), true)
                 && !_columns.ContainsKey(TableKey(row.Database, row.Table)))
            _ = DescribeAsync(row.Database, row.Table);
    }

    // Left closes the selected node. On a row that is already closed, or that has no children, it
    // closes the parent and moves the selection up to the parent.
    private void Collapse(CatalogRow row)
    {
        string tableKey = TableKey(row.Database, row.Table);

        switch (row.Kind)
        {
            case RowKind.Database:
                _expanded.TryRemove(row.Database, out _);
                break;

            case RowKind.Table when _expanded.TryRemove(tableKey, out _):
                break;

            case RowKind.Column:
            case RowKind.Note when row.Table.Length > 0:
                _expanded.TryRemove(tableKey, out _);
                SelectRow(r => r.Kind == RowKind.Table && r.Database == row.Database && r.Table == row.Table);
                break;

            default:
                _expanded.TryRemove(row.Database, out _);
                SelectRow(r => r.Kind == RowKind.Database && r.Database == row.Database);
                break;
        }
    }

    private void SelectRow(Func<CatalogRow, bool> match)
    {
        int index = FlatRows().FindIndex(r => match(r));

        if (index >= 0)
            _selected = index;
    }

    // ---------------------------------------------------------------- render

    private List<CatalogRow> FlatRows()
    {
        List<CatalogRow> rows = new();

        foreach (string database in _databases)
        {
            rows.Add(new CatalogRow(RowKind.Database, database));

            if (!_expanded.ContainsKey(database))
                continue;

            if (!_tables.TryGetValue(database, out List<string>? tables))
            {
                rows.Add(new CatalogRow(RowKind.Note, database, Name: "…"));
                continue;
            }

            if (tables.Count == 0)
                rows.Add(new CatalogRow(RowKind.Note, database, Name: "(no tables)"));

            foreach (string table in tables)
            {
                rows.Add(new CatalogRow(RowKind.Table, database, table));

                string key = TableKey(database, table);

                if (!_expanded.ContainsKey(key))
                    continue;

                if (_columns.TryGetValue(key, out List<(string Name, string Type)>? cols))
                    foreach ((string name, string type) in cols)
                        rows.Add(new CatalogRow(RowKind.Column, database, table, name, type));
                else
                    rows.Add(new CatalogRow(RowKind.Note, database, table, "…"));
            }
        }

        return rows;
    }

    public IRenderable Render(int height, int width, bool focused)
    {
        List<CatalogRow> rows = FlatRows();

        if (_selected >= rows.Count)
            _selected = Math.Max(0, rows.Count - 1);

        // Two border rows, one header row for the server node.
        int visible = Math.Max(1, height - 4);
        _visible = visible;

        if (_selected < _top) _top = _selected;
        if (_selected >= _top + visible) _top = _selected - visible + 1;
        _top = Math.Clamp(_top, 0, Math.Max(0, rows.Count - 1));

        // Panel borders take two cells and Panel padding takes two more.
        int inner = Math.Max(4, width - 4);

        Grid grid = new();
        grid.AddColumn(new GridColumn().NoWrap());

        string server = GetConnValue(_session.ConnectionString, "Endpoint") ?? "server";
        server = server.Replace("https://", "").Replace("http://", "").TrimEnd('/');
        grid.AddRow($"[bold]{Markup.Escape(TuiText.Clip(server, inner))}[/]");

        for (int i = _top; i < Math.Min(rows.Count, _top + visible); i++)
        {
            CatalogRow row = rows[i];
            bool selected = i == _selected;

            // The selected name is drawn inverted. Each case below says what goes before and after
            // the name, so the name gets the cells that are left.
            string Name(string text, int room, string colour) => selected
                ? $"[black on aqua]{Markup.Escape(TuiText.Clip(text, room))}[/]"
                : $"[{colour}]{Markup.Escape(TuiText.Clip(text, room))}[/]";

            string label;

            switch (row.Kind)
            {
                case RowKind.Database:
                {
                    // "▶ " before the name. The current database has " ●" after it.
                    string marker = _expanded.ContainsKey(row.Database) ? "▼" : "▶";
                    bool current = _session.IsCurrent(row.Database);
                    label = $"{marker} {Name(row.Database, inner - 4, current ? "bold white" : "white")}"
                            + (current ? " [green]●[/]" : "");
                    break;
                }

                case RowKind.Table:
                {
                    // "  ▶ " before the name, " t" after it.
                    string marker = _expanded.ContainsKey(TableKey(row.Database, row.Table)) ? "▼" : "▶";
                    label = $"  {marker} {Name(row.Table, inner - 6, "white")} [grey]t[/]";
                    break;
                }

                case RowKind.Column:
                {
                    // "    ├ " before the name, then a space and the type after it.
                    string type = TuiText.Clip(row.Type, 7);
                    label = $"    ├ {Name(row.Name, Math.Max(3, inner - 7 - type.Length), "grey78")} [grey]{Markup.Escape(type)}[/]";
                    break;
                }

                default:
                {
                    string indent = row.Table.Length > 0 ? "    " : "  ";
                    label = $"{indent}{Name(row.Name, inner - indent.Length, "grey")}";
                    break;
                }
            }

            grid.AddRow(label);
        }

        if (rows.Count == 0)
            grid.AddRow("[grey](no databases)[/]");

        return new Panel(grid)
        {
            Header = new PanelHeader("Data Catalog"),
            Border = BoxBorder.Rounded,
            BorderStyle = TuiText.Border(focused),
            Expand = true,
            Height = height,
        };
    }
}
