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
| `--read-only` | Refuse each statement that can change the database or the server. See [Read-only mode](#read-only-mode). |
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

## Read-only mode

With `--read-only`, the shell checks each statement before it sends it. It sends only these
statements:

- `SELECT`, a `WITH` query, and a set operation such as `(SELECT …) UNION (SELECT …)`.
- `EXPLAIN`, `SHOW`, `DESC` and `DESCRIBE`.
- `BEGIN`, `START TRANSACTION`, `COMMIT`, `ROLLBACK` and `SET TRANSACTION`.
- The shell commands, such as `use`, `\d` and `\export`. `\export` checks its query.
- `backup list`, `backup chain` and `backup gc preview`.

The shell refuses each other statement, also a statement that it does not know. It also refuses a
query that writes in a way that its first word does not show:

- A `WITH` clause in front of `INSERT`, `UPSERT`, `UPDATE` or `DELETE`.
- A locking read: `FOR UPDATE`, `FOR NO KEY UPDATE`, `FOR SHARE` or `FOR KEY SHARE`. It takes row
  locks. A plain `EXPLAIN` of a locking read takes no lock, so it runs.
- A call of `nextval` or `setval`, which moves a sequence.
- `ANALYZE <table>`, which stores new statistics.

```text
camus> delete from users where id = 1;
Refused by --read-only: DELETE is not a statement that only reads.
The shell did not send the statement. To run writes, start the shell without --read-only.
```

- A refused statement does not go to the server. An open transaction stays open.
- At the prompt, the shell shows the refusal and waits for the next statement.
- With `-e` or `-f`, a refusal stops the script, and the exit code is 1.
- With `source <file> --force`, the shell goes on after a refusal.
- In `--tui`, a refusal stops the run, and the footer shows `READ-ONLY`.
- `camus-cli workload` refuses `--read-only`, because a workload writes rows.

The check runs in the shell only. It stops a mistake, but it is not a security boundary. The
shell does not read the body of a C# function or extension. To make sure that a user cannot
write, give the user a server account without write privileges.
