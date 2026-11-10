# Interactive Shell

On an ANSI terminal, `camus-cli` starts a rich line editor. The prompt is `camus>`. A statement
that continues on more lines shows `   ->`.

## Shell commands

The shell runs these commands itself. All other text goes to the server as SQL.

| Command | Action |
| --- | --- |
| `use <database>` | Make `<database>` the current database. |
| `source <file> [--force]` | Run a file of statements. See [Scripting](scripting.md#script-files). |
| `\d [name]`, `\d+ [name]` | List the relations, or describe one. See [Describing objects](#describing-objects). |
| `\dt`, `\dv`, `\dm`, `\ds`, `\di [pattern]` | List the tables, views, materialized views, sequences or indexes. |
| `\df [pattern]`, `\df+ [pattern]` | List the inline functions. See [Inline functions and triggers](#inline-functions-and-triggers). |
| `\l` | List the databases. This is the same as `show databases`. |
| `show prepared [sql]`, `\prepared` | Show the statements that the driver keeps prepared. |
| `\plan [flow\|table]` | Set how `EXPLAIN` shows a plan. See [Query plans](output.md#query-plans-explain). |
| `\export csv\|json\|yaml [to <file>] <query>` | Write the rows of a query to a file. See [Exporting](output.md#exporting-a-result-set). |
| `backup …` | Take and manage [backups](backups.md). |
| `clear` | Clear the screen. |
| `help`, `\h`, `\?`, `?`, `/help` | List the shell commands. |
| `exit`, `quit`, `\q`, `/quit`, `/exit` | Leave the shell. |

A `/` prefix is the same as `\` for `\d`, `\df`, `\l`, `\plan` and `\export`.

## Switching databases

```sql
use northwind;
use `order details`;
```

`use` opens the connection again with the new database. Quote a name that is a keyword or that
has spaces, with backticks or double quotes. `use` works at the prompt, in script files, with `-e`
and with `-f`. The shell refuses `use` in an open transaction.

## Describing objects

The `\d` commands work like the psql commands of the same names. The shell builds each answer from
`SHOW` statements.

| Command | Result |
| --- | --- |
| `\d` | A list of the tables, views, materialized views and sequences. |
| `\d <name>` | A description of the object. |
| `\d+ <name>` | The same description, with the comments and the view definition. |
| `\dt`, `\dv`, `\dm`, `\ds`, `\di` | A list of the tables, views, materialized views, sequences or indexes. |

The letters combine: `\dtv` lists the tables and the views. A name or a pattern can have the `*`
and `?` wildcards. The match is not case-sensitive. Put a name in backticks to match it with no
wildcards. A pattern after `\di` matches the index names.

```text
camus> \d robots
Table "robots"
┌──────────┬─────────┬──────────┬─────────┐
│ Column   │ Type    │ Nullable │ Default │
├──────────┼─────────┼──────────┼─────────┤
│ id       │ OID     │ not null │         │
│ name     │ STRING  │ not null │         │
│ year     │ INT64   │          │ 2024    │
│ owner_id │ OID     │          │         │
│ price    │ FLOAT64 │          │         │
└──────────┴─────────┴──────────┴─────────┘
Indexes:
    "~pk" PRIMARY KEY (id)
    "~fk_robots_owner_id_fkey" (owner_id)
    "robots_name_idx" UNIQUE (name)
    "robots_year_idx" (year) INCLUDE (price)
Foreign-key constraints:
    "robots_owner_id_fkey" FOREIGN KEY (owner_id) REFERENCES owners (id)
```

The description of each kind of object has these parts:

- A table: the columns, the indexes, the check constraints, the foreign keys and the triggers.
  `\d+` adds the column comments, the table comment, the `WITH` options and the `CREATE TRIGGER`
  statement of each inline trigger.
- A view: the columns. `\d+` adds the view definition.
- A materialized view: the columns, the indexes, and the status of its data. `\d+` adds the view
  definition.
- A sequence: its settings and the column that owns it. `\d+` adds the comment.

The `\d` commands need a current database. They work at the prompt, in script files and with `-e`.
They always write tables, also with `--format json` or `--format yaml`. For rows that a program
reads, use the `SHOW` statements. See
[Inspecting the database](https://camusdb.github.io/docs/sql-inspection/).

`\d` does not show the foreign keys of other tables that point to a table.

## Inline functions and triggers

An inline function is a SQL scalar function with a C# body in the statement. An inline trigger is
a row trigger with a C# body in the statement. The server compiles each body. See
[Inline functions](https://github.com/camusdb/camusdb/blob/main/docs/csharp-extensions.md#inline-functions)
and [Triggers in C#](https://github.com/camusdb/camusdb/blob/main/docs/csharp-extensions.md#triggers-in-c)
for the rules of the body. Type the statements as you type other SQL:

```sql
CREATE FUNCTION slugify(s STRING) RETURNS STRING LANGUAGE csharp AS $$
    // A quote, a semicolon or -- in the body is C#, not SQL.
    return s.Trim().ToLowerInvariant().Replace(' ', '-');
$$;

CREATE TRIGGER slug ON posts BEFORE INSERT, UPDATE LANGUAGE csharp AS $$
    if (NEW!.title is { } title)
        NEW.slug = slugify(title);
$$;
```

The shell reads the text between `$$` and `$$` as raw text:

- A semicolon, a quote or a comment in the body does not end the statement.
- `Enter` in an open body adds a line. The statement runs after the closing `$$` and the `;`.
- The shell does not change curly quotes in the body.
- A body that holds `$$` takes a quote with a tag, as in `$fn$ ... $fn$`. Only the same tag ends it.

This is also true for script files and `-e`.

`\d <table>` lists the triggers of a table, of the two forms. `\d+ <table>` adds the body of
each inline trigger. For rows that a program reads, use `SHOW TRIGGERS [ON <table>]` and `SHOW CREATE TRIGGER <name> ON <table>`.

```text
camus> \d posts
…
Triggers:
    "slug" BEFORE INSERT, UPDATE LANGUAGE csharp
    "audit" AFTER INSERT, UPDATE, DELETE EXTERNAL NAME 'MyCompany.Ext.AuditTrigger'
```

`\df` lists the inline functions of the current database. A pattern can have the `*` and `?`
wildcards. `\df+` adds the generation, the compile state on this node, and the `CREATE FUNCTION`
statement of each function. It also shows the code of a dropped or replaced function that a
running statement still holds, with the state `unloading`. For rows that a program reads, use
`SHOW FUNCTIONS` and `SHOW CREATE FUNCTION`.

```text
camus> \df
List of functions
┌─────────┬───────────┬─────────┬──────────────┬──────────┬───────┐
│ Name    │ Arguments │ Returns │ Null input   │ Language │ Owner │
├─────────┼───────────┼─────────┼──────────────┼──────────┼───────┤
│ slugify │ s STRING  │ STRING  │ returns null │ csharp   │ ada   │
└─────────┴───────────┴─────────┴──────────────┴──────────┴───────┘
```

When a body does not compile (`CADB0838`) or uses a type or a member that the server refuses
(`CADB0839`), the server gives a line and a column in the body. The shell shows a caret under that
position in the statement, for each of the first three problems. This applies to
`CREATE FUNCTION` and to `CREATE TRIGGER … LANGUAGE csharp`:

```text
CamusException (CADB0838): Function 'broken' does not compile: (2,12): CS0103: The name 'z' does
not exist in the current context

      return z.Trim();
             ^ CS0103: The name 'z' does not exist in the current context
```

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
continues a statement that has an open quote, an open `$$` function body, an open parenthesis or a
trailing comma.

Semicolons separate statements. A semicolon in a quoted string or in a `$$` function body does not.

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
word lists follow the server grammar. The inline functions of the current database are colored as
functions. A `$$` function body has the color of a string, also when it continues on more lines. A
string or a `/* */` comment that continues on more lines also keeps its color. A colored word that the server does not reserve is still a
valid table or column name.

## Autocompletion

`Tab` completes the word at the cursor. The suggestions depend on the word before it:

| After | Suggestions |
| --- | --- |
| `from`, `into`, `update`, `join`, `table`, `view`, `desc`, `describe`, `truncate`, `references` | Tables and views of the current database. |
| `\d`, `\d+`, as the first word | Tables, views and sequences. |
| `show statistics for`, `create trigger <name> on`, `show triggers on` | Tables. |
| `drop trigger`, `show create trigger` | Triggers of the current database. |
| `drop trigger <name> on`, `show create trigger <name> on` | The tables that have a trigger with that name. |
| `create index <name> on`, `drop index <name> on` (also with `unique`, `inverted`, `if [not] exists`) | Tables. |
| `show ranges from index` | `table@index`. |
| `drop index [if exists]` | `table.index`. |
| `alter table <table> drop index [if exists]` | The indexes of that table. |
| `alter`, `drop`, `show create`, `comment on` + `sequence` | Sequences. |
| `set cluster setting`, `reset cluster setting` | Configuration keys. |
| `drop function [if exists]`, `alter function`, `show create function`, `create or replace function` | Inline functions of the current database. |
| `\df`, `\df+`, as the first word | Inline functions of the current database. |
| `language` | `csharp`. |
| `call`, as the first word | Procedures of the loaded C# extensions. |
| `external name` | Trigger types of the loaded C# extensions, in quotes. |
| Any other word | Keywords, functions (also the inline functions) and shell commands. |

```sql
select * from us⇥              -- "users"
show ranges from index users@⇥ -- "users@by_email", "users@~pk", …
drop index users.⇥             -- "users.by_email", …
```

The shell loads table, view, sequence, inline function and trigger names at start, after `use`,
and after a statement that changes the schema. A dropped function stays colored until the shell
starts again. It loads the indexes of a table at the first `Tab` that needs them. If that
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
