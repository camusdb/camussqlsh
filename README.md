# CamusDB SQL Shell

`camus-cli` is the command-line SQL shell for [CamusDB](https://github.com/camusdb/camusdb):

- An interactive prompt with history, multiline editing, syntax coloring and Tab completion.
- Non-interactive runs of SQL (`-e`) or `.sql` files (`-f`), with table, JSON or YAML output.
- A full-screen mode (`--tui`) with the catalog, the editor and the results on one screen.
- Workloads that create sample data and put load on a server.

For the SQL, data types, error codes and server configuration, see the
[CamusDB documentation](https://camusdb.github.io/docs/intro/).

<img width="809" height="503" alt="cli" src="https://github.com/user-attachments/assets/f50a6b6b-af7f-4212-9246-1bf5cf696f97" />

## Quick start

```shell
dotnet tool install --global CamusDB.SqlSh     # or: dotnet tool update --global CamusDB.SqlSh

camus-cli northwind                            # local server, gRPC then REST
camus-cli northwind -u app                     # with authentication
camus-cli -c "Endpoint=https://db.example.com:5095;Database=northwind"
camus-cli northwind -e "select * from users" --format json
camus-cli northwind -f schema.sql
camus-cli northwind --tui
```

## Documentation

| Document | Contents |
| --- | --- |
| [Connecting](https://github.com/camusdb/camussqlsh/blob/main/docs/connecting.md) | Connection strings, transports, authentication, plaintext endpoints. |
| [Command line](https://github.com/camusdb/camussqlsh/blob/main/docs/command-line.md) | All options and environment variables. |
| [Interactive shell](https://github.com/camusdb/camussqlsh/blob/main/docs/interactive-shell.md) | Shell commands, `use`, transactions, prepared statements, keys, completion, history. |
| [Scripting](https://github.com/camusdb/camussqlsh/blob/main/docs/scripting.md) | `-e`, `-f` and `source`. |
| [Output](https://github.com/camusdb/camussqlsh/blob/main/docs/output.md) | Tables, `\G`, JSON and YAML, `\export`, `EXPLAIN` plans. |
| [Full-screen mode](https://github.com/camusdb/camussqlsh/blob/main/docs/tui.md) | `--tui` panes and keys. |
| [Backups](https://github.com/camusdb/camussqlsh/blob/main/docs/backups.md) | The `backup` commands. |
| [Workloads](https://github.com/camusdb/camussqlsh/blob/main/docs/workloads.md) | `camus-cli workload`. |
| [Troubleshooting](https://github.com/camusdb/camussqlsh/blob/main/docs/troubleshooting.md) | Common problems and terminal detection. |
| [Development](https://github.com/camusdb/camussqlsh/blob/main/docs/development.md) | Build, test and pack. |

## License

MIT.
