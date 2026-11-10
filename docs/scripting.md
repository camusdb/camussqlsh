# Scripting

## Running SQL with `-e`

```shell
camus-cli northwind -e "select * from users"
camus-cli demo -e "insert into users (id, name) values (1, 'Ada'); select * from users"
camus-cli demo -e "select * from users\G" > users.txt
```

The output is the same as at the prompt, without the banner. A failed statement makes the process
exit with status `1`. For machine-readable rows, see [JSON and YAML](output.md#json-and-yaml).

## Running a .sql file

```shell
camus-cli northwind -f schema.sql
cat schema.sql | camus-cli northwind -f -
camus-cli demo -f seed.sql -e "select count(*) from users"   # the file runs first
```

`-f` reads the file as `source` does. At the first failed statement, it prints the error, the
statement and its line, and exits with status `1`.

## Script files

```sql
source ./seed.sql
source ./seed.sql --force
```

The shell reads the file as a stream, so the file size is not important. The shell splits
statements at semicolons. It ignores a semicolon in:

- Single-quoted and double-quoted strings, and backtick identifiers.
- `$$ … $$` and `$tag$ … $tag$` dollar-quoted text, the body of `CREATE FUNCTION` and of
  `CREATE TRIGGER … LANGUAGE csharp`. The text is raw up to the same quote: it can hold quotes,
  semicolons and comments.
- `--` and `#` line comments, and `/* */` block comments.

`#>`, `#>>` and `#-` are `jsonb` operators, not comments. `#--` is a comment. The shell accepts
doubled quotes (`'it''s'`) and backslash escapes (`'it\'s'`). It removes comments before it sends
a statement.

The shell stops at the first failure and shows the file and line. `--force` continues past
failures and shows a summary at the end. A failure in an open transaction always stops the file.
A `use` in an open transaction also stops it.
