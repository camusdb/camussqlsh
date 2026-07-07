# CamusDB SQL Shell

`camus-cli` is the command-line SQL shell for [CamusDB](https://github.com/camusdb/camusdb). It connects to one CamusDB node through the .NET native protocol driver and provides an interactive SQL prompt with history, multiline editing, syntax coloring, Tab autocompletion, transactions, and script execution.

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

## Command Line Options

```text
camus-cli [database] [options]
```

| Option | Description |
| --- | --- |
| `[database]` | Optional database name. Defaults to `test` when no connection string is provided. |
| `-c`, `--connection-source` | Full CamusDB connection string. Must include `Endpoint` and `Database`. |
| `--force-rich` | Force the rich line editor (colors, multiline, Tab completion) even when the terminal's `TERM` value is not recognized. See [Terminal Detection](#terminal-detection). |
| `--diagnose-terminal` | Print the detected terminal capabilities and exit. Useful for diagnosing why the rich editor is disabled. |
| `-h`, `--help` | Show help. |
| `-v`, `--version` | Show version. |

Environment variables:

| Variable | Description |
| --- | --- |
| `CAMUS_FORCE_RICH` | Set to `1`, `true`, or `yes` to force the rich line editor (same as `--force-rich`). |

Examples:

```shell
$ camus-cli
$ camus-cli northwind
$ camus-cli -c "Endpoint=http://localhost:5095;Database=northwind"
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

History is loaded when the shell starts and saved when the shell exits normally or receives `Ctrl+C`. Repeating the same command consecutively stores it only once.

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
