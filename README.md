# CamusDB SQL Shell

`camus-cli` is the command-line SQL shell for [CamusDB](https://github.com/camusdb/camusdb). It
connects to a CamusDB server through the .NET driver and gives you:

- An interactive SQL prompt with history, multiline editing, syntax coloring and Tab completion.
- A non-interactive mode that runs SQL (`-e`) or a `.sql` file (`-f`) and exits.
- A full-screen mode (`--tui`) with the catalog, the editor and the results on one screen.
- Workloads that create sample data and put load on a server (`camus-cli workload`).

This README tells you how to use `camus-cli`. For the SQL that CamusDB accepts, its functions, its
data types, its error codes and the configuration of the server, read the CamusDB documentation:
**<https://camusdb.github.io/docs/intro/>**.

<img width="809" height="503" alt="cli" src="https://github.com/user-attachments/assets/f50a6b6b-af7f-4212-9246-1bf5cf696f97" />

## Installation

Install the published .NET tool:

```shell
dotnet tool install --global CamusDB.SqlSh
```

Update an existing installation:

```shell
dotnet tool update --global CamusDB.SqlSh
```

## Connecting

Start the shell against a local server:

```shell
camus-cli
```

With no connection string, the shell tries gRPC on `http://localhost:5096` first. If that fails,
it uses REST on `http://localhost:5095`. The banner tells you the endpoint, the transport and the
database:

```text
Connected to http://localhost:5096 over gRPC, database: northwind
```

Give a database name as the first argument to make it the current database:

```shell
camus-cli northwind
```

With no database name, the session has no current database. Use `use <database>` to select one.

Use `-c` to give a full connection string:

```shell
camus-cli -c "Endpoint=http://db.example.com:5095;Database=northwind"
```

The connection string must have an `Endpoint`. `Endpoint` can be a comma-separated list of nodes.
When the connection string has no `Protocol` key, the shell tries gRPC and then REST on the same
endpoint. Set `Protocol=grpc` or `Protocol=rest` to use one transport only:

```shell
camus-cli -c "Endpoint=http://localhost:5096;Database=northwind;Protocol=grpc"
camus-cli -c "Endpoint=http://localhost:5095;Database=northwind;Protocol=rest"
```

`-c` accepts every key of the .NET driver's connection string. See the
[.NET driver documentation](https://camusdb.github.io/docs/dotnet-driver/) for the full list.

## Authentication

If the server has authentication on, give a user:

```shell
camus-cli northwind -u app                 # prompts: Password:
camus-cli northwind -u app -p              # -p with no value prompts too
CAMUS_PASSWORD=app-secret camus-cli northwind -u app
```

The shell sends the password one time to get a short-lived token. Every statement after that
sends the token, not the password. The banner names the user:

```text
Connected to http://localhost:5096 over gRPC, database: northwind, user: app
```

Other ways to give credentials:

- Set `CAMUS_USER` and `CAMUS_PASSWORD`.
- Put `User=` and `Password=` in the `-c` connection string. A flag wins over the same key in `-c`.
- Give a token that you got elsewhere with `--token`, or set `CAMUS_ACCESS_TOKEN`. The shell does
  not renew this token, so the session ends when the token expires.

Do not use `-p <password>` on a shared host. Every process on the machine can read a command
line. Use the prompt or `CAMUS_PASSWORD` instead.

### Plaintext endpoints

The shell refuses to send credentials to an `http://` endpoint that is not on this machine. It
stops before the password goes on the network:

```text
$ camus-cli northwind -c "Endpoint=http://db.internal:5095" -u app
Connection failed (CADB0519): This connection string configures credentials against the Endpoint
'http://db.internal:5095', which is neither https nor loopback.
```

Use an `https://` endpoint for a remote server. If TLS ends in front of the server and the last
connection is in a safe network, add `--allow-insecure-credentials`. The shell then writes a
warning to stderr and connects. Nothing checks that the network is safe.

For users, grants and the server settings for authentication, see
[Authentication](https://camusdb.github.io/docs/sql-authentication/).

## Command Line Options

```text
camus-cli [database] [options]
camus-cli workload <init|run> <bank|northwind|factory|tpcc|tpcb> [options]
```

| Option | Description |
| --- | --- |
| `[database]` | The current database at start. If you do not give it, the session has no current database. |
| `-c`, `--connection-source` | A full connection string. It must have `Endpoint`. |
| `-e`, `--execute` | Run the given SQL and exit. See [Non-Interactive Execution](#non-interactive-execution). |
| `-f`, `--file` | Run the statements in a `.sql` file and exit. The shell stops at the first error. Use `-f -` to read standard input. See [Running a .sql File](#running-a-sql-file). |
| `--format` | How `-e` and `-f` show rows: `table` (default), `json` or `yaml`. See [JSON and YAML Output](#json-and-yaml-output). |
| `-u`, `--user` | The user to authenticate as. See [Authentication](#authentication). |
| `-p`, `--password` | The password of that user. Give `-p` with no value to get a prompt. |
| `--token` | Use a bearer token instead of a user and a password. |
| `--allow-insecure-credentials` | Send credentials to an `http://` endpoint that is not on this machine. See [Plaintext endpoints](#plaintext-endpoints). |
| `--no-history` | Do not load or save the statement history. |
| `--tui` | Open the full-screen mode. See [Full-Screen Mode](#full-screen-mode---tui). |
| `--force-rich` | Use the rich line editor also when the shell does not know the terminal. See [Terminal Detection](#terminal-detection). |
| `--diagnose-terminal` | Print the terminal capabilities that the shell found, and exit. |
| `-h`, `--help` | Show help. |
| `-v`, `--version` | Show the version. |

Environment variables:

| Variable | Description |
| --- | --- |
| `CAMUS_FORCE_RICH` | Set to `1`, `true` or `yes` to do the same as `--force-rich`. |
| `CAMUS_USER` | The default for `-u`. |
| `CAMUS_PASSWORD` | The default for `-p`. Use it in scripts instead of `-p`. |
| `CAMUS_ACCESS_TOKEN` | The default for `--token`. |

Examples:

```shell
camus-cli
camus-cli northwind
camus-cli northwind -u app
camus-cli -c "Endpoint=http://localhost:5095;Database=northwind"
camus-cli northwind -e "select * from users"
camus-cli northwind -f schema.sql
camus-cli northwind --tui
camus-cli --version
```

## Interactive Shell

When the terminal supports ANSI, `camus-cli` starts a rich line editor. The prompt is:

```text
camus>
```

A statement that continues on more lines shows this prompt:

```text
   ->
```

### Shell commands

The shell runs these commands itself. It does not send them to the server.

| Command | Action |
| --- | --- |
| `use <database>` | Make `<database>` the current database. |
| `source <file> [--force]` | Run the statements in a file. See [Script Files](#script-files). |
| `show prepared [sql]` | Show the statements that the driver keeps prepared. See [Prepared statements](#prepared-statements). |
| `\plan [flow\|table]` | Set how `EXPLAIN` shows a plan. See [Query Plans](#query-plans-explain). |
| `backup …` | Take and manage backups. See [Backups](#backups). |
| `clear` | Clear the screen. |
| `exit`, `quit` | Leave the shell. |

All other text goes to the server as SQL.

### Switching databases

```sql
use northwind;
use `order details`;
```

The database is part of the connection string, so `use` opens the connection again with the new
database. Write the name bare, in `` `backticks` `` or in `"double quotes"`. Use quotes for a name
that is a keyword or that has spaces in it.

`use` works at the prompt, in script files, with `-e` and with `-f`. Thus a script can change the
database in the middle of the file. The shell refuses `use` in an open transaction.

### Transactions

The shell keeps track of the transaction that `begin` (or `start transaction`) opens:

- You can have only one open transaction at a time.
- `exit` and `quit` do not work while a transaction is open. Run `commit` or `rollback` first.
- If `commit` or `rollback` fails, the shell forgets the transaction, so you can start a new one.
- If a statement in the transaction fails, the shell also forgets the transaction.

For what a transaction does on the server, see
[Transactions](https://camusdb.github.io/docs/sql-transactions/).

### Prepared statements

After the driver sees the same SQL two times, it prepares that statement on the server and runs
it prepared from then on. You do not have to turn this on. A prepared statement returns the same
result as an inline one.

`show prepared` tells you what the driver keeps prepared:

```text
camus> show prepared
Prepared statements: 1 (MaxAutoPrepare=128, AutoPrepareMinUsages=2)
  prepared     select id from robots where year = 1984
(the statement you ran last)
```

Give a statement to ask about that statement instead of the last one:

```sql
show prepared select id from robots where year = 1984
```

`\prepared` is an alias for both forms.

A statement that you type usually shows as `inline`. It has its values in the SQL text, so each
run is different text. The statements that the driver prepares are usually the ones that a
`source` file repeats with the same text. Two connection string keys control this:

- `MaxAutoPrepare=` sets how many statements stay prepared. `0` turns the feature off.
- `AutoPrepareMinUsages=` sets how many runs come before the driver prepares a statement.

```shell
camus-cli -c "Endpoint=http://localhost:5095;Database=demo;MaxAutoPrepare=512;AutoPrepareMinUsages=1"
```

See [Prepared Statements](https://camusdb.github.io/docs/prepared-statements/) for more.

## Multiline Input

Press `Shift+Enter` to start a new line:

```sql
select
  id,
  name
from users
where active = true;
```

When you paste text that has more than one line, the editor makes each pasted `Enter` a new line.
It does not run the text immediately.

The shell also continues to collect lines when a statement is not complete:

| Incomplete form | Example |
| --- | --- |
| Open single quote | `select 'hello` |
| Open double quote | `select "name` |
| Open parenthesis | `select concat(` |
| Trailing comma | `select id,` |

Use semicolons to send more than one statement at a time. A semicolon in a quoted string does not
split the statement:

```sql
insert into users (id, name) values (1, 'Ada; Lovelace');
select * from users;
```

## Keyboard Shortcuts

| Key | Action |
| --- | --- |
| `Enter` | Run the current statement. |
| `Shift+Enter` | Start a new line. |
| `Up` | Go to the previous line. On the first line, go to the previous history item. |
| `Down` | Go to the next line. On the last line, go to the next history item. |
| `Left` / `Right` | Move the cursor. |
| `Ctrl+Left` / `Ctrl+Right` | Move one word. |
| `Home` / `End` | Go to the start or the end of the line. |
| `PageUp` / `PageDown` | Go to the first or the last line. |
| `Backspace` / `Delete` | Delete text. |
| `Tab` | Complete the current word. See [Autocompletion](#autocompletion). |
| `Ctrl+Tab` | Go to the previous completion. |

## Syntax Coloring

The editor gives a different color to each of these groups:

- SQL keywords.
- Function names.
- Shell commands.
- Constants, numbers and quoted strings.
- Comments.

The word lists follow the grammar of the server. Some colored words are not reserved by the
server, so you can still use them as table and column names.

## Autocompletion

Press `Tab` to complete the word at the cursor. Press it again to go to the next match. Press
`Ctrl+Tab` to go back.

The completion looks at the word before the cursor:

| After | The shell suggests |
| --- | --- |
| `from`, `into`, `update`, `join`, `table`, `view`, `desc`, `describe`, `truncate`, `references` | Table and view names of the current database. |
| `show statistics for` | Table names. |
| `show ranges from index` | `table@index` names. Before the `@` you get table names. After it you get the indexes of that table. |
| `alter`, `drop`, `show create` or `comment on` + `sequence` | Sequence names. |
| `set cluster setting`, `reset cluster setting` | Configuration keys. |
| Any other word | SQL keywords, functions and shell commands. |

```sql
select * from us⇥              -- completes to a table such as "users"
insert into ⇥                  -- cycles through all table names
show ranges from index users@⇥ -- cycles through "users@by_email", "users@~pk", …
drop sequence ⇥                -- cycles through all sequence names
sel⇥                           -- completes to "select"
```

The shell loads the names from the server. It loads them again at start, after `use`, and after a
statement that creates, drops or changes a table, a view or a sequence. It loads the index names
of a table at the first `Tab` that needs them. If that load is slow, the `Tab` completes nothing,
and the next `Tab` uses the names that the load got.

## Full-Screen Mode (`--tui`)

`--tui` replaces the prompt with a full-screen mode. The screen has three panes:

1. **Data Catalog**, on the left. It lists the tables of the current database. Expand a table to see its columns and their types.
2. **Query Editor**, at the top right. It colors the SQL and completes words as the prompt does.
3. **Query Results**, at the bottom right. It shows the rows of the last query and one log line for each statement.

A status bar below the panes shows the time, the row count and errors. A key bar is at the bottom
of the screen.

<img width="808" height="507" alt="tui" src="https://github.com/user-attachments/assets/fe53b8df-8f8c-4b20-ba16-47d41e3d0851" />

```shell
camus-cli northwind --tui
camus-cli -c "Endpoint=http://localhost:5095;Database=northwind" --tui
```

The mode needs an ANSI terminal. If Spectre.Console does not know the `TERM` value of the
terminal, `--tui` shows an error and exits. Add `--force-rich` to start it anyway. See
[Terminal Detection](#terminal-detection).

### Keys

These keys work in all panes:

| Key | Action |
| --- | --- |
| `Tab` | Go to the next pane. |
| `Shift+Tab` | Go to the previous pane. |
| `F5`, `Ctrl+R`, `Shift+Enter` | Run the statements in the editor. |
| `Esc` | Cancel the running query, or close the help bar. |
| `F1` | Show or hide the key list. |
| `F2` | Turn the row cap on or off. See [Row cap and paging](#row-cap-and-paging). |
| `Ctrl+S` | Save the editor text to the query file. |
| `Ctrl+L`, `Ctrl+U` | Empty the editor. |
| `Ctrl+Q` | Quit. |

`Shift+Enter` needs a terminal that supports the disambiguating keyboard protocol, for example
Ghostty, Kitty, WezTerm or a recent iTerm2. On other terminals, `Shift+Enter` starts a new line.
`F5` works on all terminals.

These keys work in the Data Catalog pane:

| Key | Action |
| --- | --- |
| `Up` / `Down` | Move the selection. |
| `Right` / `Enter` | Expand the selected table. |
| `Left` | Collapse the selected table. |
| `Home` / `End` | Go to the first or the last row. |
| `Space` | Put the selected name into the editor at the cursor. |

These keys work in the Query Editor pane:

| Key | Action |
| --- | --- |
| `Enter` | Start a new line. |
| `Up` / `Down` | Move between lines. |
| `Left` / `Right` | Move the cursor. |
| `Home` / `End` | Go to the start or the end of the line. |
| `Backspace` / `Delete` | Delete text. |
| `Ctrl+N` | Complete the current word, or go to the next match. |
| `Ctrl+P` | Go to the previous match. |

`Tab` moves between panes, so the editor uses `Ctrl+N` and `Ctrl+P` for completion.

These keys work in the Query Results pane:

| Key | Action |
| --- | --- |
| `Up` / `Down` | Scroll one row. |
| `PageUp` / `PageDown` | Scroll one screen. |
| `Left` / `Right` | Scroll one column. |
| `Home` | Go to the first row and the first column. |
| `End` | Go to the last row. |

### What the editor runs

`F5` runs all statements in the editor, in order. Put a semicolon between statements. The Query
Results pane shows one log line for each statement, and the grid shows the rows of the last query.

The editor accepts the same input as the prompt, with one exception: the `backup` commands work
at the prompt only. `use <database>` connects again and loads the catalog again. A statement that
creates or drops a table also loads the catalog again.

### Row cap and paging

The grid does not read the full result into memory. It reads the first 200 rows. When you scroll
near the end, it reads 200 more.

At start, a cap of 500 rows is on, so the grid stops at 500 rows. Press `F2` to turn the cap off
and page through the full result. The cap is for the display only. It does not add a `LIMIT` to
the statement.

### The query file

The shell keeps the editor text in `query.sql`, in the same directory as the history file (see
[History](#history)). The mode loads this file at start and saves it at exit. Press `Ctrl+S` to
save it at any time.

## Non-Interactive Execution

Use `-e` (or `--execute`) to run SQL and exit. This is useful for scripts, cron jobs and CI:

```shell
camus-cli northwind -e "select * from users"
camus-cli -c "Endpoint=http://localhost:5095;Database=northwind" -e "show tables"
```

The output is the same as at the prompt. You can give more than one statement, separated by
semicolons, and you can use `\G`:

```shell
camus-cli demo -e "insert into users (id, name) values (1, 'Ada'); select * from users"
camus-cli demo -e "select * from users\G"
camus-cli demo -e "select * from users" > users.txt
```

In this mode the shell does not print the connection banner. If a statement fails, the process
exits with status `1`.

### Running a .sql File

Use `-f` (or `--file`) to run all statements in a `.sql` file and exit:

```shell
camus-cli northwind -f schema.sql
camus-cli -c "Endpoint=http://localhost:5095;Database=northwind" -f seed.sql
```

The shell reads the file as `source` does (see [Script Files](#script-files)). It stops at the
first statement that fails. It prints the error, the statement and the line where the statement
starts, and it exits with status `1`.

Use `-` as the path to read the script from standard input:

```shell
cat schema.sql | camus-cli northwind -f -
camus-cli northwind -f - <<'SQL'
create table users (id int64 primary key, name string);
insert into users values (1, 'Ada');
SQL
```

You can use `-f` and `-e` together. The file runs first:

```shell
camus-cli demo -f seed.sql -e "select count(*) from users"
```

### JSON and YAML Output

Use `--format json` or `--format yaml` with `-e` or `-f` to get rows that another program can
read:

```shell
camus-cli demo -e "select id, name from users" --format json | jq '.[].name'
camus-cli demo -e "select id, name from users" --format yaml > users.yaml
```

```json
[
  {
    "id": 1,
    "name": "Ada"
  }
]
```

With these formats, standard output contains only the rows. The shell does not print the version
banner. It writes the `rows in set` and `Query OK` lines and errors to standard error. A statement that fails still
makes the process exit with status `1`.

Each statement that returns rows gives one document. A JSON document is an array of objects. A
YAML document is a sequence of mappings, and `---` separates two documents. A query with no rows
gives an empty array (`[]`). A statement that returns no rows, such as `create table`, gives no
document.

The values keep their types:

| SQL type | JSON | YAML |
| --- | --- | --- |
| `int64`, `float32`, `float64` | number | number |
| `bool` | `true` or `false` | `true` or `false` |
| `null` | `null` | `null` |
| `numeric` | string, so that no digit is lost | string |
| `string`, `uuid`, `date`, `timestamp` | string | string |
| `bytes` | base64 string, not clipped | `!!binary` |
| `array` | array | flow sequence (`[1, 2, 3]`) |

`\G` has no effect with these formats. The interactive shell and `--tui` always show tables, so
`--format json` and `--format yaml` without `-e` or `-f` is an error.

## Script Files

Use `source` to run a file of SQL statements from the prompt:

```sql
source ./seed.sql
```

The shell reads the file as a stream, so the size of the file is not important. The first
statement runs before the shell reads the rest of the file.

The shell splits statements at semicolons. It ignores a semicolon in:

- `'single-quoted strings'` and `"double-quoted strings"`.
- `` `quoted identifiers` ``.
- `-- line comments`, `# line comments` and `/* block comments */`.

The shell understands doubled quotes (`'it''s'`) and backslash escapes (`'it\'s'`). It removes
comments before it sends a statement to the server.

The shell stops at the first statement that fails. It tells you the file and the line where the
statement starts. Add `--force` to continue past failures and get a summary at the end:

```sql
source ./seed.sql --force
```

A failure in an open transaction stops the file also with `--force`, because the rest of the
transaction cannot run.

A file can contain `use` statements. A `use` in an open transaction stops the file.

## Output

The shell shows the rows of a query as a table:

```text
+----+------+
| id | name |
+----+------+
| 1  | Ada  |
+----+------+
1 rows in set (00:00:00.0123456)
```

For other statements, it shows the number of rows that changed:

```text
Query OK, 1 rows affected (00:00:00.0123456)
```

For an error, it shows the exception type, the error code and the message. If the message gives a
line and a column, the shell also shows that line of the statement with a `^` under the column.
For the authentication errors `CADB0516`, `CADB0517` and `CADB0519`, it also shows a hint. See
[Error Codes](https://camusdb.github.io/docs/error-codes/) for all codes.

The shell shows each value in the form that you can type back into a statement. For example, it
shows a `bytes` value as an `x'…'` literal, and it shows a `numeric` value with all its digits.

### Vertical Output (`\G`)

End a statement with `\G` instead of `;` to show each row vertically, with one column on each
line. This is useful for wide rows:

```sql
select * from users\G
```

```text
*************************** 1. row ***************************
  id: 1
name: Ada
1 rows in set (00:00:00.0123456)
```

`\G` works at the prompt, in `source` files, with `-e` and with `-f`. It also works in a list of
statements (`select 1; select 2\G`).

### Query Plans (`EXPLAIN`)

The shell shows the result of `EXPLAIN` as a flow. The source is at the top, and the result is at
the bottom. Each operator is a box in the color of its family: source, join, filter, aggregate,
sort, limit or output. An arrow between two boxes shows the number of rows that the upper operator
produces:

```text
 physical  · 2 operators · estimated cost 720 · rows flow from top to bottom

╭─ SOURCE ─────────────────────────────────────────────────────────────────╮
│ table-scan  title  Highest cost                                          │
│ Reads every row of the table.                                            │
│ table=title                                                              │
│ est. cost 600 (83%)                                                      │
│ ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━──────────── │
╰──────────────────────────────────────────────────────────────────────────╯
                                      │ est. 60 rows
                                      ▼
╭─ JOIN ───────────────────────────────────────────────────────────────────╮    ┌┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┐
│ index-nested-loop-join                                                   │ ◀╌╌┆ Right input                ┆
│ For each left row, finds the matching right rows through an index.       │    ┆ looked up in index         ┆
│ on=ci.movie_id = t.id, index=movie_id_cast_info, left=t.id,              │    ┆ movie_id_cast_info         ┆
│ right=movie_id                                                           │    └┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┄┘
│ est. cost 120 (17%)                                                      │
│ ━━━━━━━━━━━━──────────────────────────────────────────────────────────── │
╰──────────────────────────────────────────────────────────────────────────╯
                                      │ est. 60 rows
                                      ▼
                                  ╭────────╮
                                  │ Result │
                                  ╰────────╯

Plan info  shape=ddea1051bfce4953, schema-deps=[1@0, 7@0]
3 rows in set (00:00:00.0211825)
```

- A bar under each operator shows its part of the estimated cost of the plan. `Highest cost`
  marks the operator that costs more than each of the others.
- The second input of a join has no row in the plan. A dashed box beside the join shows what the
  plan tells about it. On a narrow terminal, this information is a line in the box of the join.
- With `EXPLAIN (ANALYZE)`, an arrow shows the measured rows and the estimate. Each operator also
  shows its storage counters. `Estimate off Nx` marks an operator whose measured rows are 10 times
  the estimate, or one tenth of it.

Use `\plan table` to show the rows of the plan as a table. Use `\plan flow` to show the flow again.
`\plan` without an argument shows the current setting. The setting stays until the shell exits.
The command works at the prompt, in `source` files, with `-e` and with `-f`.

`\G` and `--format json|yaml` always show the rows of the plan. The `--tui` mode also shows the
rows.

## History

The shell saves the statements that you run in a JSON file:

| Platform | Path |
| --- | --- |
| Any, when `XDG_STATE_HOME` is set | `$XDG_STATE_HOME/camusdb/history.json` |
| Linux and macOS | `~/.config/camusdb/history.json` |
| Windows | `%LOCALAPPDATA%\camusdb\history.json` |

Only your user can read the directory and the file. The file keeps the last 2,000 statements. The
shell loads the file at start and saves it when you exit or press `Ctrl+C`. If you run the same
statement two times in sequence, the shell keeps it one time.

A statement that has a password in it (`create user … identified by '…'`, `alter user …`) does
not go into the file. You can get it back with `Up` until the session ends.

Use `--no-history` to not use the file.

## Backups

The `backup` commands are shell commands. They use the backup API of the server:

```text
backup full                            take a full backup
backup incremental <parent-backup-id>  add an incremental backup to a chain
backup coordinated                     take a backup of the full cluster at one point
backup list                            list the backups of the node
backup chain <leaf-backup-id>          find and check a restore chain
backup gc preview                      show what retention would delete
backup gc                              run retention now
```

`backup` alone, or `backup help`, shows this list.

```text
camus> backup full
    Backup Id: 971a0a88-3d36-42c6-b36b-8d1e773f40c4
         Type: Full
Created (UTC): 2026-08-10 03:45:47
       Parent: (none)
   Partitions: 4
Backup OK (00:00:03.117)

camus> backup incremental 971a0a88-3d36-42c6-b36b-8d1e773f40c4
camus> backup chain 719b7b6b-281d-4979-bf63-b495a7d1bdaf
```

How to use the commands from the shell:

- The commands do not need a current database.
- The commands work at the prompt only. They do not work in `--tui`.
- The backup API uses HTTP only. With the default connection, the shell sends backup requests to
  `http://localhost:5095`. With `-c`, it uses the `Endpoint` of the connection string. If you set
  `Protocol=grpc` in `-c`, add `BackupEndpoint=` with the HTTP endpoint of the server.
- `backup coordinated` must go to the coordinator node. If `Endpoint` has more than one node, set
  `BackupEndpoint=` to the coordinator.
- Backup requests have their own timeout. Set it with `BackupTimeout=` (in seconds) in the
  connection string. The default is 300 seconds.
- If the server has authentication on, connect as a superuser.

The shell cannot restore a backup. For the server setup, retention and restore, see
[Backup and Restore](https://camusdb.github.io/docs/backup-and-restore/).

## Workloads

`camus-cli workload` creates sample data and puts load on a server. Use it to try CamusDB or to
measure it:

```text
camus-cli workload init <bank|northwind|factory|tpcc|tpcb> [options]
camus-cli workload run  <bank|northwind|factory|tpcc|tpcb> [options]
```

`init` creates the schema and the data. `run` runs the workload until the duration ends.

| Option | Description |
| --- | --- |
| `-c`, `--connection-source` | A connection string. The default is the same as for the shell. |
| `--database` | The target database. The default is `demo`. |
| `--rows N` | The number of rows that `init` makes. For `bank` and `tpcb` it is accounts, and for `tpcc` it is warehouses. The default is 1000. |
| `--concurrency N` | The number of parallel workers for `run`, and of parallel writers for `init`. The default is 64. |
| `--duration N` | The duration of `run`, in seconds. The default is 60. |
| `--locking MODE` | `optimistic` or `pessimistic`. The default is `optimistic`. |
| `--isolation LEVEL` | `serializable` or `read-committed`. The default is `serializable`. |
| `--no-prepare` | Run each statement inline, not prepared. |
| `-u`, `-p`, `--allow-insecure-credentials` | The same as for the shell. |

```shell
camus-cli workload init bank --database demo --rows 5000
camus-cli workload run northwind --concurrency 5 --duration 120
camus-cli workload init tpcc --database tpcc --rows 1
camus-cli workload run tpcc --concurrency 4 --duration 120
```

See [Workload Utility](https://camusdb.github.io/docs/workload-utility/) for more.

## Troubleshooting

### The shell does not exit

A transaction is open. Run `commit` or `rollback`, then run `exit`.

### The connection string is not valid

The connection string must have an `Endpoint` with a full URL, for example:

```text
Endpoint=http://localhost:5095;Database=northwind
```

### An old version runs

Look at the version:

```shell
camus-cli -v
```

Clear the NuGet caches and update the tool:

```shell
dotnet nuget locals all --clear
dotnet tool update --global CamusDB.SqlSh
```

### Terminal Detection

The rich editor (colors, multiline input and Tab completion) needs a terminal that supports ANSI
and is interactive. The shell finds these capabilities from the environment, mainly from the `TERM`
variable. Some terminals that support ANSI have a `TERM` value that Spectre.Console does not know.
For example, Rio sets `TERM=rio`. On these terminals, the shell uses a plain prompt.

Print the capabilities that the shell found:

```shell
camus-cli --diagnose-terminal
```

```text
Rich editor disabled (falling back to plain prompt). Terminal capabilities:
  IsTerminal  : True
  Ansi        : False
  Interactive : False
  TERM        : rio
  NO_COLOR    : (unset)
  ForceRich   : False
```

If the terminal supports ANSI, force the rich editor:

```shell
camus-cli --force-rich
# or, for all sessions:
export CAMUS_FORCE_RICH=1
```

You can also set a `TERM` value that Spectre.Console knows. This also helps other terminal
programs:

```shell
TERM=xterm-256color camus-cli
```

## Development

Build the shell:

```shell
dotnet build CamusDb.SqlSh/CamusDb.SqlSh.csproj
```

Run the line editor tests:

```shell
dotnet test Radline/RadLine.Tests/RadLine.Tests.csproj
```

Run from source:

```shell
dotnet run --project CamusDb.SqlSh/CamusDb.SqlSh.csproj -- northwind
dotnet run --project CamusDb.SqlSh/CamusDb.SqlSh.csproj -- -c "Endpoint=http://localhost:5095;Database=northwind"
```

Make the NuGet package. The package goes into `CamusDb.SqlSh/nupkg/`:

```shell
dotnet pack CamusDb.SqlSh/CamusDb.SqlSh.csproj -c Release
```

Install the local package:

```shell
dotnet tool update --global CamusDB.SqlSh --add-source ./CamusDb.SqlSh/nupkg
```

## License

`camus-cli` is released under the MIT License.
