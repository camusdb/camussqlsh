# CamusDB SQL Shell

`camus-cli` is the command-line SQL shell for [CamusDB](https://github.com/camusdb/camusdb). It connects to one CamusDB node through the .NET native protocol driver and provides an interactive SQL prompt with history, multiline editing, syntax coloring, Tab autocompletion, transactions, and script execution, plus a non-interactive mode for running SQL (`-e`/`--execute`) or a whole `.sql` file (`-f`/`--file`) and exiting. A full-screen mode (`--tui`) puts the catalog, the editor and the results on one screen.

<img width="809" height="503" alt="cli" src="https://github.com/user-attachments/assets/f50a6b6b-af7f-4212-9246-1bf5cf696f97" />

## Installation

Install the published NuGet tool:

```shell
dotnet tool install --global CamusDB.SqlSh
```

Update an existing installation:

```shell
dotnet tool update --global CamusDB.SqlSh
```

## Basic Usage

Start the shell with the default connection:

```shell
$ camus-cli
```

By default, the shell connects to:

```text
Endpoint=https://localhost:5095;Database=test
```

Open a specific database using the positional database argument:

```shell
$ camus-cli northwind
```

This connects to:

```text
Endpoint=https://localhost:5095;Database=northwind
```

Open a custom endpoint and database with a connection string:

```shell
$ camus-cli -c "Endpoint=http://localhost:5095;Database=northwind"
```

The connection string must include both `Endpoint` and `Database`.

## Authentication

CamusDB authentication is **off by default**, and a shell started without credentials behaves
exactly as before. Against a server started with `CAMUSDB_AUTH_ENABLED=true`, pass a user:

```shell
$ camus-cli northwind -u app                 # prompts: Password:
$ CAMUS_PASSWORD=app-secret camus-cli northwind -u app
```

The password is exchanged **once** for a short-lived bearer token; every statement then carries
the token, never the password, and the driver renews it before it expires. The banner names the
identity the session is acting as:

```text
Connected to http://localhost:5096 over gRPC, database: northwind, user: app
```

Credentials can also come from `CAMUS_USER` / `CAMUS_PASSWORD`, or straight from the connection
string (`-c "…;User=app;Password=app-secret"`). A flag wins over the same key inside `-c`. When
another process already holds a token, hand it over with `--token` instead of a password — it is
used verbatim and never renewed, so its expiry ends the session.

Prefer the environment or the interactive prompt over `-p` on a shared host: a command line is
readable by every process on the machine.

### Managing users and grants

User and grant administration is server-level — it needs no current database — and requires a
superuser. The shell routes these statements accordingly, so they work even before a database is
selected:

```sql
create user app identified by 'app-secret';
alter user app identified by 'rotated-secret';
grant select, insert on northwind.* to app;
grant select on northwind.orders to reader;
revoke insert on northwind.* from app;
show grants for app;
drop user app;
```

A statement that inlines a password (`IDENTIFIED … BY '…'`) is recalled with the Up arrow during
the session but is **not** written to the on-disk history file.

Two errors are worth recognizing:

| Code | Meaning |
| --- | --- |
| `CADB0516` | Not authenticated — missing, invalid, or expired credentials. The server returns the same code for a wrong password and an unknown user, so replies can't be used to enumerate accounts. |
| `CADB0517` | Authenticated, but missing a privilege on a table the statement touches — including tables reached through joins and subqueries. Fix it with a `GRANT`, not by re-authenticating. |

With authentication on, the server refuses credentials over plaintext (`CADB0519`) unless the peer
is loopback: use an `https://` endpoint against any remote deployment.

## Command Line Options

```text
camus-cli [database] [options]
```

| Option | Description |
| --- | --- |
| `[database]` | Optional database name. Defaults to `test` when no connection string is provided. |
| `-c`, `--connection-source` | Full CamusDB connection string. Must include `Endpoint` and `Database`. |
| `-e`, `--execute` | Execute the given SQL and exit without starting the interactive shell. See [Non-Interactive Execution](#non-interactive-execution). |
| `-f`, `--file` | Execute the statements in a `.sql` file and exit, stopping at the first error. Use `-f -` to read the script from standard input. See [Running a .sql File](#running-a-sql-file). |
| `-u`, `--user` | User to authenticate as. Only needed against a server with authentication enabled. See [Authentication](#authentication). |
| `-p`, `--password` | That user's password. When `-u` is given without it, the shell prompts (without echoing). Prefer the prompt or `CAMUS_PASSWORD`: on the command line the password is visible to every other process on the machine, and each use prints a warning. |
| `--token` | Use a bearer token obtained elsewhere instead of logging in with a password. |
| `--no-history` | Do not load or save the statement history. |
| `--tui` | Open the full-screen mode: catalog, editor and results in three panes. Needs an ANSI terminal. See [Full-Screen Mode](#full-screen-mode---tui). |
| `--force-rich` | Force the rich line editor (colors, multiline, Tab completion) even when the terminal's `TERM` value is not recognized. See [Terminal Detection](#terminal-detection). |
| `--diagnose-terminal` | Print the detected terminal capabilities and exit. Useful for diagnosing why the rich editor is disabled. |
| `-h`, `--help` | Show help. |
| `-v`, `--version` | Show version. |

Environment variables:

| Variable | Description |
| --- | --- |
| `CAMUS_FORCE_RICH` | Set to `1`, `true`, or `yes` to force the rich line editor (same as `--force-rich`). |
| `CAMUS_USER` | Default for `-u`. |
| `CAMUS_PASSWORD` | Default for `-p`. Preferred over `-p` in scripts, since a command line is visible to every process on the host. |
| `CAMUS_ACCESS_TOKEN` | Default for `--token`. |

Examples:

```shell
$ camus-cli
$ camus-cli northwind
$ camus-cli -c "Endpoint=http://localhost:5095;Database=northwind"
$ camus-cli northwind -e "select * from users"
$ camus-cli --version
$ camus-cli -v
$ camus-cli --help
$ camus-cli -h
```

## Interactive Shell

When the terminal supports ANSI rendering, `camus-cli` starts an enhanced interactive editor.

Prompt:

```text
camus>
```

Continuation prompt for multiline input:

```text
   ->
```

Exit the shell:

```sql
exit
```

or:

```sql
quit
```

If a transaction is active, the shell requires `commit` or `rollback` before exit.

Clear the screen:

```sql
clear
```

Run SQL from a file:

```sql
source ./schema.sql
```

Switch the current database:

```sql
use northwind;
use `order details`;
```

`use` is handled by the shell rather than the server — the database is part of the connection
string, so the shell reopens the connection against it. The name may be bare, `` `backticked` ``,
or `"quoted"`, which is how a name that collides with a keyword or contains spaces is written.
It works in script files and with `-e` as well as at the prompt, so a dump can switch databases
mid-file, and a session started without a database can select one from its first statement.

Take and administer backups:

```sql
backup full
backup list
```

Like `use`, the `backup` family is the shell's rather than the server's — see [Backups](#backups).

### Prepared statements

The driver registers a statement with the server once it has seen the same SQL a couple of
times, and runs it prepared from then on. Nothing has to be enabled, and a prepared
execution returns exactly what an inline one does.

`show prepared` reports what is currently registered:

```text
camus> show prepared
Prepared statements: 1 (MaxAutoPrepare=128, AutoPrepareMinUsages=2)
  prepared     select id from robots where year = 1984
(the statement you ran last)
```

Pass a statement to ask about that one instead of the last one you ran:

```sql
show prepared select id from robots where year = 1984
```

`\prepared` is an alias for both forms.

Typed statements usually report as `inline`: they carry their values in the SQL text, so
each execution is distinct text and never repeats often enough to be registered. The
statements that do get prepared are the ones a `source` file or an application repeats
verbatim. Both thresholds come from the connection string — `MaxAutoPrepare=` (how many
statements stay registered; `0` turns registration off) and `AutoPrepareMinUsages=` (how
many executions come first):

```shell
$ camus-cli -c "Endpoint=http://localhost:5095;Database=demo;MaxAutoPrepare=512;AutoPrepareMinUsages=1"
```

## Non-Interactive Execution

Pass `-e` (or `--execute`) with a SQL string to run it immediately and exit, without
starting the interactive prompt. This is useful for scripts, cron jobs, CI pipelines, and
one-off queries:

```shell
$ camus-cli northwind -e "select * from users"
$ camus-cli -c "Endpoint=http://localhost:5095;Database=northwind" -e "show tables"
```

The target database is taken from the positional `[database]` argument or the `-c`
connection string, exactly as in interactive mode.

Results are rendered the same way as in the interactive shell: query statements print a
table, while DDL and mutation statements print affected row counts.

You can pass several statements separated by semicolons; they run in order:

```shell
$ camus-cli demo -e "insert into users (id, name) values (gen_id(), 'Ada'); select * from users"
```

Vertical output with `\G` also works in this mode:

```shell
$ camus-cli demo -e "select * from users\G"
```

The process exits after the SQL completes, so `-e` can be combined with shell redirection
and piping like any other command:

```shell
$ camus-cli demo -e "select * from users" > users.txt
```

### Running a .sql File

Pass `-f` (or `--file`) with a path to run every statement in a `.sql` file and exit, so a
schema or a migration can be applied without opening the interactive console:

```shell
$ camus-cli northwind -f schema.sql
$ camus-cli -c "Endpoint=http://localhost:5095;Database=northwind" -f seed.sql
```

Statements are separated by semicolons and run in order, and `\G` and comments are handled
exactly as with `source` inside the shell — the file is streamed, so its size doesn't matter.
Execution stops at the first statement that fails: the error is printed with the offending
statement and the line it started on, the remaining statements are left unrun, and the process
exits with status `1`.

Use `-` as the path to read the script from standard input:

```shell
$ cat schema.sql | camus-cli northwind -f -
$ camus-cli northwind -f - <<'SQL'
create table users (id oid primary key, name string);
insert into users values (gen_id(), 'Ada');
SQL
```

`-f` and `-e` can be combined; the file runs first, so `-e` can read back what it wrote:

```shell
$ camus-cli demo -f seed.sql -e "select count(*) from users"
```

## Multiline Input

The shell supports multiline SQL input.

Use `Shift+Enter` to insert a new line manually:

```sql
select
  id,
  name
from users
where active = true;
```

Pasting multiline SQL is also supported. When a pasted statement contains multiple lines, the editor converts pasted `Enter` keys into new lines instead of immediately submitting the SQL.

The shell keeps collecting input when a statement is incomplete. A statement is considered incomplete when it has:

| Incomplete form | Example |
| --- | --- |
| Open single quote | `select 'hello` |
| Open double quote | `select "name` |
| Open parenthesis | `select concat(` |
| Trailing comma | `select id,` |

Multiple SQL statements can be submitted together when they are separated by semicolons:

```sql
insert into users (id, name) values (gen_id(), 'Ada');
select * from users;
```

Semicolons inside single or double quoted strings do not split statements.

## Keyboard Shortcuts

| Key | Action |
| --- | --- |
| `Enter` | Submit the current statement. |
| `Shift+Enter` | Insert a new line in multiline mode. |
| `Up` | Move to the previous line in multiline input, or previous history item from the first line. |
| `Down` | Move to the next line in multiline input, or next history item from the last line. |
| `Left` / `Right` | Move the cursor. |
| `Ctrl+Left` / `Ctrl+Right` | Move by word. |
| `Home` / `End` | Move to the beginning or end of the current line. |
| `PageUp` / `PageDown` | Move to the first or last line of multiline input. |
| `Backspace` / `Delete` | Delete text. |
| `Tab` | Autocomplete the current word (see [Autocompletion](#autocompletion)). |
| `Ctrl+Tab` | Cycle to the previous completion. |

## Full-Screen Mode (`--tui`)

`--tui` replaces the prompt with a full-screen mode. The screen holds three panes:

1. **Data Catalog**, on the left. It lists the tables of the current database. Expand a table to read its columns and their types.
2. **Query Editor**, at the top right. It holds the SQL. It colors the SQL with the same word list as the prompt, and it offers the same completions.
3. **Query Results**, at the bottom right. It shows the rows of the last query. It also keeps one log line for each statement that ran.

A status bar below the panes reports the elapsed time, the row count and any error. A key bar sits at the foot of the screen.

<img width="808" height="507" alt="tui" src="https://github.com/user-attachments/assets/fe53b8df-8f8c-4b20-ba16-47d41e3d0851" />

Start the mode with the `--tui` flag:

```shell
$ camus-cli northwind --tui
```

```shell
$ camus-cli -c "Endpoint=http://localhost:5095;Database=northwind" --tui
```

The mode needs an ANSI terminal. On a terminal whose `TERM` value Spectre.Console does not recognize, `--tui` prints an error and exits. Add `--force-rich` to start it anyway. See [Terminal Detection](#terminal-detection).

### Keys

These keys work in every pane:

| Key | Action |
| --- | --- |
| `Tab` | Move to the next pane. |
| `Shift+Tab` | Move to the previous pane. |
| `Shift+Enter` | Run the statements in the editor. See the note below. |
| `F5` | Run the statements in the editor. |
| `Ctrl+R` | Run the statements in the editor. |
| `Esc` | Cancel the query that runs now, or close the help bar. |
| `F1` | Show or hide the key list. |
| `F2` | Turn the row cap on or off. See [Row Cap and Paging](#row-cap-and-paging). |
| `Ctrl+S` | Save the editor text to the query file. |
| `Ctrl+L` | Empty the editor. |
| `Ctrl+U` | Empty the editor. |
| `Ctrl+Q` | Quit. |

These keys work in the Data Catalog pane:

| Key | Action |
| --- | --- |
| `Up` / `Down` | Move the selection. |
| `Right` / `Enter` | Expand the selected table. |
| `Left` | Collapse the selected table. |
| `Home` / `End` | Move to the first or last row. |
| `Space` | Insert the selected name into the editor at the cursor. |

These keys work in the Query Editor pane:

| Key | Action |
| --- | --- |
| `Enter` | Start a new line. |
| `Up` / `Down` | Move between lines. |
| `Left` / `Right` | Move the cursor. |
| `Home` / `End` | Move to the beginning or end of the current line. |
| `Backspace` / `Delete` | Delete text. |
| `Ctrl+N` | Complete the current word, or step to the next candidate. |
| `Ctrl+P` | Step to the previous candidate. |

These keys work in the Query Results pane:

| Key | Action |
| --- | --- |
| `Up` / `Down` | Scroll one row. |
| `PageUp` / `PageDown` | Scroll one screen. |
| `Left` / `Right` | Scroll one whole column. |
| `Home` | Move to the first row and the first column. |
| `End` | Move to the last row. |

`Tab` moves between panes, so the editor uses `Ctrl+N` and `Ctrl+P` for completion instead.

`Shift+Enter` needs a terminal that speaks the disambiguating keyboard protocol. Ghostty, Kitty, WezTerm and recent iTerm2 versions all speak it. On any other terminal, `Shift+Enter` starts a new line. `F5` runs the statements on every terminal.

### What the editor runs

`F5` runs every statement in the editor, in order. Separate the statements with semicolons. The Query Results pane keeps one log line for each statement. The grid shows the rows of the last query statement.

The editor accepts the same SQL as the prompt, which includes:

- Query statements, DDL and DML. See [SQL Execution](#sql-execution).
- `begin`, `commit` and `rollback`. See [Transactions](#transactions).
- `use <database>`, which reconnects and reloads the catalog.
- Server-level and system-level statements, such as `show databases`.

A statement that creates or drops a table reloads the catalog by itself.

Backup commands are the one exception. They run at the prompt only. In `--tui` they report an error. See [Backups](#backups).

### Row cap and paging

The results grid does not read a whole table into memory. It reads the first 200 rows, then reads 200 more each time you scroll near the end of what it holds.

A cap of 500 rows is on at start. With the cap on, the grid stops at 500 rows and never reads the rest. Press `F2` to turn the cap off, and the grid then pages through the whole result. The cap is a display cap. It is not a SQL `LIMIT`, so the statement itself is unchanged.

### The query file

The editor text is kept between sessions in a file under the user's own profile, next to the history file (see [History](#history)): `<state directory>/query.sql`. The state directory is created with owner-only permissions, so other local users cannot read the query buffer.

The mode loads this file at start, and saves it at exit. Press `Ctrl+S` to save it at any time. Press `Ctrl+L` or `Ctrl+U` to empty the editor.

## History

Executed statements are stored in a JSON history file under the user's own profile — `$XDG_STATE_HOME/camusdb/history.json` when that variable is set, otherwise `~/.config/camusdb/history.json` on Linux and macOS and `%LOCALAPPDATA%\camusdb\history.json` on Windows. The directory is created with owner-only permissions (0700 on Unix) and the file is written only for the owning user, so other local users cannot read it. The file holds at most the 2,000 most recent statements.

History is loaded when the shell starts and saved when the shell exits normally or receives `Ctrl+C`. Repeating the same command consecutively stores it only once. Statements that inline a password (`CREATE USER … IDENTIFIED BY '…'`, `ALTER USER …`) are kept out of the file — they stay recallable with `Up` for the rest of the session only. Run the shell with `--no-history` to skip the file entirely.

Use `Up` and `Down` to navigate history. In multiline input, `Up` and `Down` first move between lines; from the first or last line they navigate history.

## SQL Execution

The shell sends SQL to CamusDB and displays results in a table for query statements. It prints affected row counts for non-query statements and DDL.

Query statements include:

```sql
select * from users;
explain select * from users;
explain (logical) select * from users;
explain (physical) select * from users;
explain (analyze) select * from users;
show tables;
desc users;
describe users;
show statistics for users;
show ranges from table users;
show slow queries;
```

### Table statistics

`show statistics for <table>` prints what the optimizer believes about a table — the estimated row count, per-column minimum and maximum, histogram bucket counts, approximate distinct values, per-index entry counts, and how stale all of it is. `TABLE` is an optional noise word, so `show statistics for table users` is the same statement.

```sql
analyze users;                          -- collect histograms and distinct-value counts
show statistics for users;
show statistics for users\G             -- ten columns; vertical output is easier to read
```

One row per statistics target, discriminated by the `kind` column (`table`, `column`, `key`, `index`). A `null` means either "does not apply to this row" or "never collected" — `kind` tells you which. `last_analyzed` and `stale_mutations` describe the whole table and repeat on every row.

The values are the answering node's view and include mutations it has not flushed yet, so in a cluster two nodes may report different values for the same table. Materialized views have statistics of their own; a plain view does not.

### Ranges

CamusDB divides a table's row space, and each order-safe index space, across Raft partitions, and
splits them further as they grow. `show ranges` prints that layout. One row per span, in the order
the router searches them.

```sql
show ranges from table users;
show ranges from index users@by_email;
show ranges from table users\G            -- fifteen columns; vertical output is easier to read
```

`show range … for row (…)` prints the single span that holds one row:

```sql
show range from table users for row (1500);            -- by primary key
show range from index users@by_email for row ('a@example.com');
```

The two `for row` forms differ. On an **index** the key is computed from the values alone, so it
answers for a key that does not exist, and fewer values than the index has key columns are accepted
— a prefix still lands in one span. On a **table** the row key is ordered by the row id the engine
minted, not by the primary key, so the server point-reads the primary index to find the row. A
primary key no row carries is an error there, not an empty result. That probe takes no lock and
joins no read set, so the statement is safe inside a transaction.

Three columns are worth reading carefully. `routing` is `key_range` or `hash` **as this node routes
the space**; a hash-routed space has exactly one span with both bounds null. `leader` is a gossip
hint, and a null means unknown rather than "no leader". `replicas` empty means legacy full
replication, not "no replicas". Every column describes the node that answered, so two nodes may
legitimately disagree, and there is no cluster-wide form.

Read `partition_id`, not `span`, to talk about the same range across two runs: a split renumbers
every span after it, while the partition keeps its identity. `start_key` and `end_key` are decoded
in column terms, and `raw_start_key`/`raw_end_key` carry the encoded bounds; a bound that will not
decode falls back to its raw text rather than failing the statement.

Three spellings reach the primary index: `t@~pk`, `t@t_pkey` and `t@primary`. The target takes no
quoted identifiers — `` `my_table`@`my_index` `` does not parse. A plain view has no key space of
its own; ask for the ranges of the tables its body reads. The statement needs `select` on the
target, and it reports only: it never moves a range.

### Slow query log

The server can record every statement that ran at or over a duration you set, with the execution
facts that explain the duration. `show slow queries` reads that record back. Rows come back newest
first, so a bare statement answers "what just happened" without an `order by`.

```sql
show slow queries;
show slow queries like '%from orders%';
show slow queries\G                       -- fourteen columns; vertical output is easier to read
```

The log is **off by default**. Turn it on per node in the server's `config.yml`
(`slow_query_log_enabled`, `slow_query_log_threshold_ms`, `slow_query_log_max_entries`,
`slow_query_log_max_sql_length`), and confirm what a node resolved with `show variables like
'slow_query%'`. A node that started with the log off has no ring at all, so turning the setting on
at runtime records nothing — that one needs a restart.

Three columns answer most "why was this slow" questions without a second run. `full_scan` means the
plan read a whole relation instead of seeking through an index. `spilled` means a sort, grouping,
distinct or hash join outgrew its memory budget and wrote to disk. `rows_read` far above
`rows_returned` is the signature of a predicate no index serves.

`outcome` is `completed`, `abandoned` or `failed`. `abandoned` means the caller stopped reading
early, so `rows_returned` is a floor rather than a total; the work was still done, which is why the
entry is kept. `failed` carries an `error_code`, and a slow failure is usually the most interesting
entry in the log.

`seq` keeps counting past the ring's capacity. If the newest `seq` advanced between two readings by
more than the number of entries the ring holds, entries were overwritten in between and you are not
seeing everything that qualified.

The statement needs a **superuser**, the same bar as `show engine stats` and `show variables`, and a
sharper reason: the rows carry the literal SQL other users ran, so a predicate value from a table
the caller holds no grant on can appear verbatim. Like the other node-level statements it runs
without a current database, it reports only the node that answered, and its entries do not survive a
restart.

DDL statements include:

```sql
create table users (
  id object_id primary key,
  name string not null,
  active bool default true
);

create index users_name on users (name);
alter table users rename column name to full_name;
drop index users_name;
truncate table users;                   -- empties the table; the table itself stays
drop table users;
```

`truncate [table] <table>` deletes every row of a base table in one step. The cost does not grow
with the row count, because the server replaces the key space the rows live in. The table keeps its
name, its columns, its indexes and its comments. The `table` keyword is optional, so
`truncate users` does the same thing.

The server refuses `truncate` inside an explicit transaction. It commits a replicated schema change
that a later `rollback` cannot undo. Run `commit` or `rollback` first. The statement needs both the
`delete` and the `drop` privilege on the table.

Mutation statements include:

```sql
insert into users (id, name, active)
values (gen_id(), 'Ada Lovelace', true);

update users
set active = false
where name = 'Ada Lovelace';

delete from users
where active = false;
```

## Transactions

Start a transaction:

```sql
begin;
```

or:

```sql
start transaction;
```

Commit:

```sql
commit;
```

Rollback:

```sql
rollback;
```

Only one active transaction is allowed at a time. If `commit` or `rollback` fails, the shell clears its local transaction state so a new transaction can be started.

## Backups

`backup` drives the server's **online** backup and point-in-time-recovery administration: taking
backups, listing the catalog, resolving a restore chain, and running retention. All of it is safe
while the server serves traffic.

```text
backup full                            take a full backup
backup incremental <parent-backup-id>  chain an incremental onto a backup
backup coordinated                     take a cluster-wide consistent backup
backup list                            list the node's backup catalog
backup chain <leaf-backup-id>          resolve and validate a restore chain
backup gc preview                      report what retention would reclaim
backup gc                              run retention now
```

`backup` on its own (or `backup help`) prints that list.

A typical session — take a full backup, chain an incremental onto it, then check the chain would
actually restore:

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

`backup chain` is the validating read: a chain that could not be assembled is rejected here rather
than at restore time, so it doubles as a "would this backup actually restore?" check. It prints the
chain root-first and, underneath, the **recoverable window** a point-in-time restore may target —
the server reports that window for the whole chain on its root, not on the leaf.

Things worth knowing:

- **Backups are node-wide, not per-database.** Every database on a CamusDB server shares one storage
  node, so a backup captures all of them at once. Nothing here is scoped to the current database, and
  the commands work with no database selected.
- **The server must opt in.** Backups are off until `kahuna.backup_dir` is set in the server's
  `config.yml`; until then every command fails with `BackupNotConfigured` (HTTP 503).
- **Superuser only.** With authentication enabled, every command needs a superuser — connect with
  `-u`/`--token` as one. With authentication *disabled* the server restricts this surface to loopback
  callers, so a remote shell is refused rather than allowed to take an anonymous node-wide backup.
- **An incremental can silently become a full.** If the parent has aged past the retention floor the
  server takes a full backup instead; the command still succeeds and reports the substitution and its
  reason in yellow.
- **`backup coordinated` must reach the coordinator.** Another node refuses with
  `BackupNotCoordinator`. Pin `BackupEndpoint=` to the coordinator when `Endpoint=` is a multi-node
  pool.
- **The API is REST-only.** It has no SQL form and no gRPC service. The shell points its default gRPC
  connection at the well-known HTTP port for you; against a `-c` connection string with an explicit
  `Protocol=grpc`, add `BackupEndpoint=` naming the server's HTTP endpoint.
- **Backup requests use their own timeout** — `BackupTimeout=` in the connection string, 300 seconds
  by default, rather than the statement timeout, since a full backup copies a whole node's base image.

Retention runs automatically after each backup and on a periodic tick, so `backup gc` is only needed
to reclaim space immediately after tightening the limits. Preview it first — the preview deletes
nothing and reports what the configured limits would drop:

```text
camus> backup gc preview
Retention preview: 2 backups, 0 orphans, 1.41 GB would reclaim (00:00:00.041)
```

**Restore is not here.** A restore rebuilds into a *fresh* data root, after which the server is
stopped and a new one booted against it — there is no hot in-place restore, so it stays an operator
runbook step rather than something the shell can drive to completion. See the server's
`backups-and-point-in-time-recovery` guide.

## Syntax Coloring

The interactive editor colors SQL keywords, shell commands, constants, numbers, quoted strings, and supported function names.

Colored SQL keywords include:

```text
select update from where order by asc desc describe database table set create if exists default
primary key index indexes constraint limit insert into values delete alter rename column drop
null not string int64 float64 object_id oid bool boolean is on in or and between like ilike add
show use tables view views materialized refresh concurrently cascade owner no data columns group
join inner offset unique having explain analyze begin start transaction commit rollback as
distinct cast integer double engine stats statistics for variables cluster setting settings reset
truncate ranges range row slow queries
```

Colored shell commands:

```text
clear source use exit quit backup
```

Colored constants:

```text
true false
```

Colored aggregate functions:

```text
count max min avg sum
```

Colored scalar functions and aliases:

```text
gen_id
current_database current_user current_role is_superuser
current_timestamp now current_date date_add date_diff date_part date_trunc unix_timestamp from_unixtime
abs ceil ceiling floor sqrt pow power mod sign random round
length lower upper trim ltrim rtrim substring replace contains starts_with ends_with concat
json_valid json_type json_extract json_value json_contains json_array_length
to_string to_int64 to_float64 to_bool to_id str_id
octet_length vector_dims l2_distance inner_product cosine_distance
```

## Autocompletion

Press `Tab` to autocomplete the word under the cursor; press it again to cycle through
matches, and `Ctrl+Tab` to cycle backwards.

Completion is context-aware. When the word being typed follows a keyword that expects a
table or view name — `from`, `into`, `update`, `join`, `table`, `view`, `desc`,
`describe`, or `truncate` — the shell suggests the **table and view names** of the current
database. In
any other position it suggests the SQL keywords, functions, and shell commands.

The `for` of `show statistics for` counts as a table position too, decided by the word
before it: `show grants for` takes a user name, so `for` alone is not enough.

The `index` of `show ranges from index` takes a **qualified** `table@index` name, which the
editor treats as one word. Before the `@` the shell suggests table names; after it, the index
names of that table, loaded from `show indexes from <table>` on the first `Tab` that asks for
them. A press that arrives before the load answers completes nothing; the next one completes
from the cache.

```sql
select * from us⇥              -- completes to a table such as "users"
insert into ⇥                  -- cycles through all table names
show statistics for ⇥          -- cycles through all table names
show ranges from table ⇥       -- cycles through all table names
show ranges from index us⇥     -- completes the table half, such as "users"
show ranges from index users@⇥ -- cycles through "users@by_email", "users@~pk", …
sel⇥                           -- completes to "select"
```

Relation names are loaded from `show tables`, `show views` and `show materialized views`,
and refreshed automatically on startup, after a `use <database>` switch, and after a
statement that changes the set of relations (`create`/`drop table`, `create [or
replace]`/`drop`/`alter view`, and their materialized forms). Each of those refreshes also
drops the cached index names, because an index belongs to one table in one database. Index DDL
(`create [unique] index`, `drop index`, `alter table`) drops them on its own.

## Function Examples

ID:

```sql
insert into users (id, name) values (gen_id(), 'Grace Hopper');
```

Date and time:

```sql
select current_timestamp(), now(), current_date();
select date_add(current_timestamp(), 7, 'day');
select date_diff('2026-01-01T00:00:00Z', current_timestamp(), 'day');
select date_part('year', current_timestamp());
select date_trunc('day', current_timestamp());
select unix_timestamp(), from_unixtime(1767225600);
```

Math:

```sql
select abs(-10), ceil(1.2), floor(1.8), round(1.234, 2);
select sqrt(16), pow(2, 8), mod(10, 3), sign(-42), random();
```

Strings:

```sql
select length(name), lower(name), upper(name), trim(name) from users;
select substring(name, 1, 3), replace(name, 'Ada', 'A.') from users;
select contains(name, 'Ada'), starts_with(name, 'A'), ends_with(name, 'e') from users;
select concat(first_name, ' ', last_name) from users;
```

JSON:

```sql
select json_valid(payload), json_type(payload) from events;
select json_extract(payload, '$.user.id'), json_value(payload, '$.user.name') from events;
select json_contains(payload, '{"active":true}'), json_array_length(payload, '$.items') from events;
```

Casting:

```sql
select cast(score as integer) from scores;
select to_string(score), to_int64(score), to_float64(score), to_bool(active), to_id(id_text) from scores;
```

Vectors:

```sql
select octet_length(embedding), vector_dims(embedding) from docs;
select id, l2_distance(embedding, 0x0000803F0000004000004040) as distance
from docs order by distance limit 10;
select id from docs order by cosine_distance(embedding, 0x0000803F0000004000004040) limit 10;
select id from docs order by inner_product(embedding, 0x0000803F0000004000004040) desc limit 10;
```

A vector is a `bytes` value. It holds tightly packed little-endian float32 elements and carries no
header, so `vector_dims` is the byte count divided by four. The hex literal above is the
three-element vector `[1.0, 2.0, 3.0]`. `octet_length` also accepts a string, where it counts UTF-8
bytes rather than characters.

The three distance functions take two vectors of equal size and return a `float64`. `l2_distance`
and `cosine_distance` put the nearest row first with `asc`, which is the default. `inner_product`
runs the other way: the largest value is the most similar, so it needs `desc`. An ascending
`inner_product` returns the least similar rows, and it reports no error.

A `bytes(N)` column declares a maximum length, never an exact one. A `check` over `vector_dims` is
what pins the dimension:

```sql
create table docs (
  id        object_id primary key,
  embedding bytes(3072) not null,
  constraint embedding_is_768d check (vector_dims(embedding) = 768)
);
```

The shell has no bind parameters, so a query vector must be typed as a hex literal. A 768-element
vector is about 6 KB of text. For that size, use a client that binds the vector as a parameter.

## Script Files

Use `source` to execute a file containing one or more SQL statements:

```sql
source ./seed.sql
```

Example `seed.sql`:

```sql
create table users (
  id object_id primary key,
  name string not null,
  active bool default true
);

insert into users (id, name, active)
values (gen_id(), 'Ada Lovelace', true);

select * from users;
```

The file is streamed rather than read into memory, so a dump larger than RAM sources fine and the
first statement runs immediately instead of after the whole file has been parsed.

Statements are split on semicolons, ignoring any that fall inside `'strings'`, `"strings"`,
`` `quoted identifiers` ``, `-- line comments`, `# line comments`, or `/* block comments */`.
Doubled quotes (`'it''s'`) and backslash escapes (`'it\'s'`) are understood, and comments are
stripped before a statement is sent to the server.

Execution stops at the first statement that fails, reporting the file and the line the statement
started on. Pass `--force` to carry on instead and print a summary at the end:

```sql
source ./seed.sql --force
```

An open transaction stops the file either way: the server has already aborted it, so every
remaining statement would fail too.

A file may contain `use` statements to switch databases as it goes; a `use` inside an open
transaction is refused and stops the file, since the rest of it would otherwise run against the
wrong database.

## Output

Queries are printed as Spectre.Console tables:

```text
+----+------+
| id | name |
+----+------+
| 1  | Ada  |
+----+------+
1 rows in set (00:00:00.0123456)
```

DDL and non-query statements print affected row counts:

```text
Query OK, 1 rows affected (00:00:00.0123456)
```

Errors are printed with the exception type and message.

### Vertical Output (`\G`)

Terminate a statement with `\G` instead of `;` to print each row vertically, one column per
line. This is useful for wide rows or rows with long values.

```sql
select * from users\G
```

```text
*************************** 1. row ***************************
  id: 1
name: Ada
2 rows in set (00:00:00.0123456)
```

`\G` works anywhere `;` does, including inside `source` script files and in multi-statement
batches (`select 1; select 2\G`).

## Development

Build the CLI:

```shell
dotnet build CamusDb.SqlSh/CamusDb.SqlSh.csproj
```

Run the line editor tests:

```shell
dotnet test Radline/RadLine.Tests/RadLine.Tests.csproj
```

Create the NuGet package:

```shell
dotnet pack CamusDb.SqlSh/CamusDb.SqlSh.csproj -c Release
```

The package is written to:

```text
CamusDb.SqlSh/nupkg/
```

Run from source:

```shell
dotnet run --project CamusDb.SqlSh/CamusDb.SqlSh.csproj -- northwind
```

Run from source with a connection string:

```shell
dotnet run --project CamusDb.SqlSh/CamusDb.SqlSh.csproj -- -c "Endpoint=http://localhost:5095;Database=northwind"
```

## Troubleshooting

The shell connects to `test` when running `camus-cli northwind`.

Make sure you are running a version that includes database argument support:

```shell
camus-cli -v
```

Then clear caches and update if needed:

```shell
dotnet nuget locals all --clear
dotnet tool update --global CamusDB.SqlSh --add-source ./CamusDb.SqlSh/nupkg
```

Connection string validation fails.

Use a connection string with both required fields:

```text
Endpoint=http://localhost:5095;Database=northwind
```

The shell will not exit.

If a transaction is active, run:

```sql
commit;
```

or:

```sql
rollback;
```

Then run:

```sql
exit
```

### Terminal Detection

The rich editor (colors, multiline, Tab completion) is only enabled when the terminal
reports that it supports ANSI, is interactive, and writes to a real terminal. These
capabilities are detected from the environment — chiefly the `TERM` variable — so some
capable terminals whose `TERM` value is not on Spectre.Console's recognized list (for
example Rio, which sets `TERM=rio`) fall back to a plain prompt with none of those features.

Print the detected capabilities to see why:

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

If the terminal really does support ANSI, force the rich editor:

```shell
camus-cli --force-rich
# or, persistently:
export CAMUS_FORCE_RICH=1
```

Alternatively, set a `TERM` value that is recognized (this also helps other terminal
applications):

```shell
TERM=xterm-256color camus-cli
```

## License

`camus-cli` is released under the MIT License.
