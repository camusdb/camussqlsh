# Full-Screen Mode (`--tui`)

```shell
camus-cli northwind --tui
```

The screen has three panes:

1. **Data Catalog**, on the left. It lists the databases. A green dot (`●`) marks the current one.
   Expand a database to see its tables, and a table to see its columns.
2. **Query Editor**, at the top right. It has coloring and completion, as the prompt does. Its
   header shows the [tabs](#tabs).
3. **Query Results**, at the bottom right. It shows the rows of the last query of the active tab,
   and one log line for each statement.

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
| `Esc` | Cancel the running query, or close the help. |
| `Ctrl+C` | Cancel the running query. If no query runs, quit. |
| `F1` | Show the [help](#help) with all the keys. |
| `F2` | Turn the [row cap](#row-cap-and-paging) on or off. |
| `F4` | Load the Data Catalog again. |
| `Ctrl+S` | Save the text of all tabs to the [tabs file](#tabs-file). |
| `Ctrl+L`, `Ctrl+U` | Empty the editor of the active tab. |
| `Ctrl+T` | Open a new tab after the active tab. |
| `Ctrl+D` | Close the active tab. |
| `Ctrl+O`, `F8`, `Ctrl+PageDown` | Go to the next tab. |
| `F7`, `Ctrl+PageUp` | Go to the previous tab. |
| `Alt+1` to `Alt+9` | Go to tab 1 to 9. |
| `Ctrl+Q` | Quit. |
| `Ctrl+A` / `Ctrl+E` | `Home` / `End`, without `Fn` on a Mac. |
| `Option+Up` / `Option+Down` | `PageUp` / `PageDown`, without `Fn` on a Mac. |

- The statement at the cursor ends one character after its `;`. After the last statement,
  `Ctrl+R` runs the last statement.
- With a transaction open, `Ctrl+Q` and `Ctrl+C` ask first, because quitting rolls back. Press
  the key again to roll back and quit.
- A paste goes into the editor at the cursor, with its tabs and line breaks. This needs bracketed
  paste, which most terminals support.
- `Shift+Enter` needs a terminal that supports xterm's `modifyOtherKeys` mode, for example xterm
  and Ghostty. Elsewhere it starts a new line. `Ctrl+R` works on all terminals. The shell does not
  turn on the Kitty keyboard protocol, because .NET then reads `F1`, `F2` and `F4` as other keys.
- `Option+Up` and `Option+Down` need a terminal that sends them as modified arrows (Ghostty,
  Kitty, WezTerm).

## Help

`F1` shows all the keys in a panel at the center of the screen. The panel takes the place of the
panes while it is open. `F1`, `Esc`, `Enter` or `q` closes it. When the keys do not fit, the arrows,
`PageUp` and `PageDown` scroll them. Other keys have no effect while the help is open, except
`Ctrl+C` and `Ctrl+Q`. A paste closes the help and goes into the editor.

## Tabs

Each tab has its own editor text, result grid, status line and messages. The header of the editor
shows the tabs: a number and the first line of text. The active tab has a yellow background. A dot
(`●`) marks the tab where a statement runs.

- All tabs use one connection. Thus `use`, the current database and an open transaction apply to
  all tabs. A `BEGIN` in one tab and a `COMMIT` in another tab end the same transaction.
- One statement runs at a time. You can change tabs while it runs. Its rows go to the tab where it
  started. In another tab, the key bar shows `RUNNING IN TAB n`. `Esc` cancels the run from any tab.
- There can be at most 9 tabs.
- `Ctrl+D` on a tab with text asks first, because the text is lost. Press `Ctrl+D` again to close
  it. A tab where a statement runs does not close. When you close the last tab, an empty tab
  replaces it.
- `Ctrl+PageUp` and `Ctrl+PageDown` work only in terminals that send them. `Alt+1` to `Alt+9` need a
  terminal that sends Option or Alt as `Esc`. `Ctrl+O`, `F7` and `F8` work in all terminals.
- When the tabs do not fit in the header, it shows the tabs near the active tab, with `‹` and `›`
  for the other tabs.

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

## Tabs file

The text of all tabs and the active tab are in `tui-tabs.json`, next to the
[history file](interactive-shell.md#history). The file loads at start, and it saves at exit and on
`Ctrl+S`. The results of a tab are not saved.

Earlier versions kept one editor in `query.sql`. When `tui-tabs.json` does not exist, the text of
`query.sql` becomes the first tab. The shell does not change or delete `query.sql`.
