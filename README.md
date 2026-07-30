# CamusDB SQL Shell

`camus-cli` is the command-line SQL shell for [CamusDB](https://github.com/camusdb/camusdb). It connects to one CamusDB node through the .NET native protocol driver and provides an interactive SQL prompt with history, multiline editing, syntax coloring, Tab autocompletion, transactions, and script execution, plus a non-interactive mode for running SQL (`-e`/`--execute`) or a whole `.sql` file (`-f`/`--file`) and exiting.

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
$ camus-cli northwind -u app -p app-secret
$ camus-cli northwind -u app                 # prompts: Password:
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
| `-p`, `--password` | That user's password. When `-u` is given without it, the shell prompts (without echoing). |
| `--token` | Use a bearer token obtained elsewhere instead of logging in with a password. |
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
exactly as with `source` inside the shell. Execution stops at the first statement that
fails: the error is printed with the offending statement, the remaining statements are left
unrun, and the process exits with status `1`.

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

## History

Executed statements are stored in a JSON history file under the system temporary directory:

```text
camusdb.history.json
```

History is loaded when the shell starts and saved when the shell exits normally or receives `Ctrl+C`. Repeating the same command consecutively stores it only once. Statements that inline a password (`CREATE USER … IDENTIFIED BY '…'`, `ALTER USER …`) are kept out of the file — they stay recallable with `Up` for the rest of the session only.

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
```

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
drop table users;
```

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

## Syntax Coloring

The interactive editor colors SQL keywords, shell commands, constants, numbers, quoted strings, and supported function names.

Colored SQL keywords include:

```text
select update from where order by asc desc describe database table set create if exists default
primary key index indexes constraint limit insert into values delete alter rename column drop
null not string int64 float64 object_id oid bool boolean is on in or and between like ilike add
show use tables view views columns group join inner offset unique having explain analyze begin
start transaction commit rollback as distinct cast integer double
```

Colored shell commands:

```text
clear source use exit quit
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
current_timestamp now current_date date_add date_diff date_part date_trunc unix_timestamp from_unixtime
abs ceil ceiling floor sqrt pow power mod sign random round
length lower upper trim ltrim rtrim substring replace contains starts_with ends_with concat
json_valid json_type json_extract json_value json_contains json_array_length
to_string to_int64 to_float64 to_bool to_id str_id
```

## Autocompletion

Press `Tab` to autocomplete the word under the cursor; press it again to cycle through
matches, and `Ctrl+Tab` to cycle backwards.

Completion is context-aware. When the word being typed follows a keyword that expects a
table name — `from`, `into`, `update`, `join`, `table`, `desc`, or `describe` — the shell
suggests the **table names** of the current database. In any other position it suggests the
SQL keywords, functions, and shell commands.

```sql
select * from us⇥      -- completes to a table such as "users"
insert into ⇥          -- cycles through all table names
sel⇥                   -- completes to "select"
```

Table names are loaded from `show tables` and refreshed automatically on startup, after a
`use <database>` switch, and after a `create table` or `drop table` statement.

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

The shell splits statements on semicolons outside quoted strings.

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
