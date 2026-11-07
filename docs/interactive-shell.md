# Interactive Shell

On an ANSI terminal, `camus-cli` starts a rich line editor. The prompt is `camus>`. A statement
that continues on more lines shows `   ->`.

## Shell commands

The shell runs these commands itself. All other text goes to the server as SQL.

| Command | Action |
| --- | --- |
| `use <database>` | Make `<database>` the current database. |
| `source <file> [--force]` | Run a file of statements. See [Scripting](scripting.md#script-files). |
| `show prepared [sql]`, `\prepared` | Show the statements that the driver keeps prepared. |
| `\plan [flow\|table]` | Set how `EXPLAIN` shows a plan. See [Query plans](output.md#query-plans-explain). |
| `\export csv\|json\|yaml [to <file>] <query>` | Write the rows of a query to a file. See [Exporting](output.md#exporting-a-result-set). |
| `backup …` | Take and manage [backups](backups.md). |
| `clear` | Clear the screen. |
| `help`, `\h`, `\?`, `?`, `/help` | List the shell commands. |
| `exit`, `quit`, `\q`, `/quit`, `/exit` | Leave the shell. |

A `/` prefix is the same as `\` for `\plan` and `\export`.

## Switching databases

```sql
use northwind;
use `order details`;
```

`use` opens the connection again with the new database. Quote a name that is a keyword or that
has spaces, with backticks or double quotes. `use` works at the prompt, in script files, with `-e`
and with `-f`. The shell refuses `use` in an open transaction.

## Transactions

The shell tracks the transaction that `begin` (or `start transaction`) opens:

- Only one transaction can be open at a time.
- The exit commands do not work while a transaction is open. Run `commit` or `rollback` first.
- If `commit`, `rollback` or a statement in the transaction fails, the shell forgets the
  transaction.

See [Transactions](https://camusdb.github.io/docs/sql-transactions/) for the server side.

## Prepared statements

After the driver sees the same SQL two times, it prepares that statement on the server. The result
is the same as inline. `show prepared` shows the state of the last statement, or of the statement
that you give:

```text
camus> show prepared
Prepared statements: 1 (MaxAutoPrepare=128, AutoPrepareMinUsages=2)
  prepared     select id from robots where year = 1984
(the statement you ran last)
```

A typed statement is usually `inline`, because its values make each run different text. Two
connection string keys control the feature:

- `MaxAutoPrepare=` is the number of statements that stay prepared. `0` turns the feature off.
- `AutoPrepareMinUsages=` is the number of runs before the driver prepares a statement.

See [Prepared Statements](https://camusdb.github.io/docs/prepared-statements/).

## Multiline input

`Shift+Enter` starts a new line. A pasted `Enter` is a new line, not a run. The shell also
continues a statement that has an open quote, an open parenthesis or a trailing comma.

Semicolons separate statements. A semicolon in a quoted string does not.

## Keyboard shortcuts

| Key | Action |
| --- | --- |
| `Enter` | Run the current statement. |
| `Shift+Enter` | Start a new line. |
| `Up` / `Down` | Go to the previous or next line. On the first or last line, go through history. |
| `Ctrl+Left` / `Ctrl+Right` | Move one word. |
| `Home` / `End` | Go to the start or end of the line. |
| `PageUp` / `PageDown` | Go to the first or last line. |
| `Tab` / `Ctrl+Tab` | Go to the next or previous completion. |

## Syntax coloring

The editor colors keywords, functions, shell commands, constants and strings, and comments. The
word lists follow the server grammar. A colored word that the server does not reserve is still a
valid table or column name.

## Autocompletion

`Tab` completes the word at the cursor. The suggestions depend on the word before it:

| After | Suggestions |
| --- | --- |
| `from`, `into`, `update`, `join`, `table`, `view`, `desc`, `describe`, `truncate`, `references` | Tables and views of the current database. |
| `show statistics for`, `create trigger <name> on`, `drop trigger <name> on` | Tables. |
| `create index <name> on`, `drop index <name> on` (also with `unique`, `inverted`, `if [not] exists`) | Tables. |
| `show ranges from index` | `table@index`. |
| `drop index [if exists]` | `table.index`. |
| `alter table <table> drop index [if exists]` | The indexes of that table. |
| `alter`, `drop`, `show create`, `comment on` + `sequence` | Sequences. |
| `set cluster setting`, `reset cluster setting` | Configuration keys. |
| `call`, as the first word | Procedures of the loaded C# extensions. |
| `external name` | Trigger types of the loaded C# extensions, in quotes. |
| Any other word | Keywords, functions and shell commands. |

```sql
select * from us⇥              -- "users"
show ranges from index users@⇥ -- "users@by_email", "users@~pk", …
drop index users.⇥             -- "users.by_email", …
```

The shell loads table, view and sequence names at start, after `use`, and after a statement that
changes the schema. It loads the indexes of a table at the first `Tab` that needs them. If that
load is slow, that `Tab` completes nothing, and the next one uses the loaded names.

The names of C# extensions (functions, procedures, trigger types) load one time, at start, with
`SHOW EXTENSIONS`. This command needs a superuser. For other users, these names are not colored or
suggested.

## History

The shell saves the statements that you run:

| Platform | Path |
| --- | --- |
| Any, when `XDG_STATE_HOME` is set | `$XDG_STATE_HOME/camusdb/history.json` |
| Linux and macOS | `~/.config/camusdb/history.json` |
| Windows | `%LOCALAPPDATA%\camusdb\history.json` |

- Only your user can read the file. It keeps the last 2,000 statements.
- The shell saves it at exit and on `Ctrl+C`.
- Two identical statements in sequence are kept one time.
- A statement with a password (`create user … identified by '…'`, `alter user …`) is not saved.
  `Up` recalls it until the session ends.

`--no-history` turns the file off.
