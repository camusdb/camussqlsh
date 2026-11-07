# Output

```text
+----+------+
| id | name |
+----+------+
| 1  | Ada  |
+----+------+
1 rows in set (00:00:00.0123456)

Query OK, 1 rows affected (00:00:00.0123456)
```

Each value shows in the form that you can type back into a statement. For example, `bytes` is an
`x'…'` literal, and `numeric` has all its digits.

An error shows the exception type, the error code and the message. If the message gives a line
and a column, the shell shows that line with a `^` under the column. `CADB0516`, `CADB0517` and
`CADB0519` also show a hint. See [Error Codes](https://camusdb.github.io/docs/error-codes/).

## Vertical output (`\G`)

End a statement with `\G` instead of `;` to show one column on each line:

```text
camus> select * from users\G
*************************** 1. row ***************************
  id: 1
name: Ada
1 rows in set (00:00:00.0123456)
```

`\G` also works in a list of statements (`select 1; select 2\G`).

## JSON and YAML

`--format json` or `--format yaml`, with `-e` or `-f`, writes rows for another program:

```shell
camus-cli demo -e "select id, name from users" --format json | jq '.[].name'
```

- Standard output has only the rows. The row counts, `Query OK` lines and errors go to standard
  error. A failed statement still exits with status `1`.
- Each statement that returns rows gives one document: a JSON array of objects, or a YAML sequence
  of mappings. `---` separates YAML documents. No rows gives `[]`. A statement such as
  `create table` gives no document.
- `\G` has no effect. Without `-e` or `-f`, these formats are an error.

| SQL type | JSON | YAML |
| --- | --- | --- |
| `int64`, `float32`, `float64` | number. `NaN`, `Infinity` and `-Infinity` are strings. | number. `.nan`, `.inf` and `-.inf`. |
| `bool` | `true` or `false` | `true` or `false` |
| `null` | `null` | `null` |
| `numeric` | string, so that no digit is lost | string |
| `string`, `uuid`, `date`, `timestamp` | string | string |
| `bytes` | base64 string, not clipped | `!!binary` |
| `array` | array | flow sequence (`[1, 2, 3]`) |
| `jsonb`, `json` | the document itself. A number keeps all its digits. | the document as flow syntax (`{"a": [1, 2]}`) |

## Exporting a result set

`\export` writes the rows of a query to a CSV, JSON or YAML file, not to the screen:

```sql
\export csv select * from titles
\export json to titles.json select id, title from titles where production_year > 2000
/export yaml to 'my titles.yaml' select * from titles
```

- Without `to <file>`, the name is the first table after `FROM` with the date and time, for
  example `titles-20261014-134650.csv`. Without `FROM`, the name starts with `export`. The file
  goes in the current directory.
- At the prompt, the shell asks for the name, with the suggestion filled in. `Ctrl+C` clears the
  line, and a second `Ctrl+C` cancels. In a script, the shell uses the suggestion.
- Quote a name with a space. `~/` is your home directory. An existing file is replaced.
- The query must return rows (`SELECT`, `SHOW`, `EXPLAIN`).
- JSON and YAML have the same shape as [`--format`](#json-and-yaml). CSV has a header record. In
  CSV, a null is an empty field, `bytes` is base64, and an array or a `jsonb` value is its JSON
  text.
- The rows go to a temporary file first. If the query fails, the target file does not change.

## Query plans (`EXPLAIN`)

The shell shows `EXPLAIN` as a flow, from the source at the top to the result at the bottom. Each
operator is a box in the color of its family. An arrow shows the rows that the upper operator
produces:

```text
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
```

- The bar is the part of the plan cost for that operator. `Highest cost` marks the most costly
  operator.
- A dashed box shows the second input of a join. On a narrow terminal, it is a line in the join
  box.
- A set operation (`UNION`, `INTERSECT`, `EXCEPT`) shows a `LEFT OPERAND` box and a
  `RIGHT OPERAND` box with the plan of each operand. Nested set operations nest their boxes.
- With `EXPLAIN (ANALYZE)`, the arrows show measured and estimated rows, and each operator shows
  its storage counters. `Estimate off Nx` marks a difference of 10 times or more.

`\plan table` shows the plan rows as a table. `\plan flow` shows the flow again. `\plan` alone
shows the setting, which stays until exit. `\G`, `--format json|yaml` and `--tui` always show the
plan rows.
