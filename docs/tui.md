# Full-Screen Mode (`--tui`)

```shell
camus-cli northwind --tui
```

The screen has three panes:

1. **Data Catalog**, on the left. It lists the databases. A green dot (`●`) marks the current one.
   Expand a database to see its tables, and a table to see its columns.
2. **Query Editor**, at the top right. It has coloring and completion, as the prompt does.
3. **Query Results**, at the bottom right. It shows the rows of the last query and one log line
   for each statement.

A status bar shows the time, the row count and errors. A key bar is at the bottom.

<img width="808" height="507" alt="tui" src="https://github.com/user-attachments/assets/fe53b8df-8f8c-4b20-ba16-47d41e3d0851" />

The mode needs an ANSI terminal. If Spectre.Console does not know the `TERM` value, `--tui` exits
with an error. Add `--force-rich` to start it anyway. See
[Terminal detection](troubleshooting.md#terminal-detection).

## Keys in all panes

| Key | Action |
| --- | --- |
| `Tab` / `Shift+Tab` | Go to the next or previous pane. |
| `Ctrl+R`, `F5`, `Shift+Enter` | Run the statement at the cursor. |
| `Ctrl+G`, `F6` | Run all statements in the editor. |
| `Esc` | Cancel the running query, or close the help bar. |
| `Ctrl+C` | Cancel the running query. If no query runs, quit. |
| `F1` | Show or hide the key list. |
| `F2` | Turn the [row cap](#row-cap-and-paging) on or off. |
| `F4` | Load the Data Catalog again. |
| `Ctrl+S` | Save the editor text to the [query file](#query-file). |
| `Ctrl+L`, `Ctrl+U` | Empty the editor. |
| `Ctrl+Q` | Quit. |
| `Ctrl+A` / `Ctrl+E` | `Home` / `End`, without `Fn` on a Mac. |
| `Option+Up` / `Option+Down` | `PageUp` / `PageDown`, without `Fn` on a Mac. |

- The statement at the cursor ends one character after its `;`. After the last statement,
  `Ctrl+R` runs the last statement.
- With a transaction open, `Ctrl+Q` and `Ctrl+C` ask first, because quitting rolls back. Press
  the key again to roll back and quit.
- A paste goes into the editor at the cursor, with its tabs and line breaks. This needs bracketed
  paste, which most terminals support.
- `Shift+Enter` needs the disambiguating keyboard protocol (Ghostty, Kitty, WezTerm, recent
  iTerm2). Elsewhere it starts a new line. `Ctrl+R` works on all terminals.
- `Option+Up` and `Option+Down` need a terminal that sends them as modified arrows (Ghostty,
  Kitty, WezTerm).

## Data Catalog

| Key | Action |
| --- | --- |
| `Up` / `Down`, `PageUp` / `PageDown`, `Home` / `End` | Move the selection. |
| `Right` | Expand the database or table. |
| `Left` | Collapse it, or go to its parent and collapse that. |
| `Enter` | On a database that is not current, `use` it. Elsewhere, expand. |
| `Space` | Put the selected name into the editor. |

The database list comes from `show databases`. If the user cannot see it, the catalog shows only
the current database. Tables load when you expand a database. For a database that is not current,
the catalog uses a short-lived separate connection.

The catalog loads again after a statement that changes tables or databases in this session. Press
`F4` to see changes from other sessions.

## Query Editor

| Key | Action |
| --- | --- |
| `Enter` | Start a new line. |
| `Option+Left` / `Option+Right`, `Ctrl+Left` / `Ctrl+Right` | Move one word. |
| `Ctrl+Home` / `Ctrl+End` | Go to the start or end of the editor. |
| `Option+Backspace`, `Ctrl+W` | Delete the word before the cursor. |
| `Ctrl+N` / `Ctrl+P` | Go to the next or previous completion. `Tab` changes panes. |

The editor accepts the same input as the prompt, but `backup` works at the prompt only. `use`
connects again and loads the catalog again.

## Query Results

| Key | Action |
| --- | --- |
| Arrows, `PageUp` / `PageDown` | Move the selected cell. |
| `Home` / `End` | Go to the first cell, or the last row. |
| `Enter` | Show the full value of the cell. |
| `c` | Copy the cell value. |
| `Shift+C` | Copy the row, with a tab between the values. |
| `m` | Show the messages. `m` or `Esc` goes back. |

The grid cuts a value at 40 characters and shows a line break as `↵`. In the value view:

| Key | Action |
| --- | --- |
| `Up` / `Down`, `PageUp` / `PageDown` | Scroll. |
| `Left` / `Right` | The previous or next column. In the row view, the previous or next row. |
| `v` | Change between the cell view and the row view (all columns of the row). |
| `c` | Copy what the view shows. |
| `Esc`, `Enter` | Close. |

Copy uses OSC 52, an escape sequence that writes to the system clipboard. Ghostty, Kitty and
WezTerm support it. In iTerm2, turn on "Applications in terminal may access clipboard". Without
OSC 52, a copy has no effect. A `bytes` value is cut in the views, so its copy is cut too.

### Messages

The status line shows the outcome of the last run. A long outcome ends with `M messages`. The
messages view keeps the last 500 outcomes with their time and statement. `c` copies them all. A
failed run opens the messages view. A run with rows opens the grid.

## Row cap and paging

The grid reads 200 rows at a time, as you scroll. A cap of 500 rows is on at start. `F2` turns
it off, so you can page through the full result. The cap does not add a `LIMIT` to the statement.

## Query file

The editor text is in `query.sql`, next to the [history file](interactive-shell.md#history). It
loads at start and saves at exit and on `Ctrl+S`.
