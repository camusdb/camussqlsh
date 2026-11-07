# Command Line Options

```text
camus-cli [database] [options]
camus-cli workload <init|run> <bank|northwind|factory|tpcc|tpcb> [options]
```

| Option | Description |
| --- | --- |
| `[database]` | The current database at start. Without it, the session has no current database. |
| `-c`, `--connection-source` | A full connection string. It must have `Endpoint`. See [Connecting](connecting.md). |
| `-e`, `--execute` | Run the given SQL and exit. See [Scripting](scripting.md). |
| `-f`, `--file` | Run a `.sql` file and exit. `-f -` reads standard input. See [Scripting](scripting.md#running-a-sql-file). |
| `--format` | How `-e` and `-f` show rows: `table` (default), `json` or `yaml`. See [Output](output.md#json-and-yaml). |
| `-u`, `--user` | The user to authenticate as. See [Authentication](connecting.md#authentication). |
| `-p`, `--password` | The password of that user. `-p` with no value prompts. |
| `--token` | A bearer token, instead of a user and a password. |
| `--allow-insecure-credentials` | Send credentials to a remote `http://` endpoint. See [Plaintext endpoints](connecting.md#plaintext-endpoints). |
| `--no-history` | Do not load or save the [history](interactive-shell.md#history). |
| `--tui` | Open the [full-screen mode](tui.md). |
| `--force-rich` | Use the rich line editor on a terminal that the shell does not know. See [Terminal detection](troubleshooting.md#terminal-detection). |
| `--diagnose-terminal` | Print the terminal capabilities that the shell found, and exit. |
| `-h`, `--help` | Show help. |
| `-v`, `--version` | Show the version. |

| Variable | Description |
| --- | --- |
| `CAMUS_FORCE_RICH` | `1`, `true` or `yes` is the same as `--force-rich`. |
| `CAMUS_USER` | The default for `-u`. |
| `CAMUS_PASSWORD` | The default for `-p`. Use it in scripts instead of `-p`. |
| `CAMUS_ACCESS_TOKEN` | The default for `--token`. |
