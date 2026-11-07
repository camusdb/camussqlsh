# Workloads

`camus-cli workload` creates sample data and puts load on a server:

```text
camus-cli workload init <bank|northwind|factory|tpcc|tpcb> [options]
camus-cli workload run  <bank|northwind|factory|tpcc|tpcb> [options]
```

`init` creates the schema and the data. `run` runs the workload for the duration.

| Option | Description |
| --- | --- |
| `-c`, `--connection-source` | A connection string. The default is the same as for the shell. |
| `--database` | The target database. Default `demo`. |
| `--rows N` | The rows that `init` makes: accounts for `bank` and `tpcb`, warehouses for `tpcc`. Default 1000. |
| `--concurrency N` | Parallel workers for `run`, and parallel writers for `init`. Default 64. |
| `--duration N` | The duration of `run`, in seconds. Default 60. |
| `--locking MODE` | `optimistic` (default) or `pessimistic`. |
| `--isolation LEVEL` | `serializable` (default) or `read-committed`. |
| `--no-prepare` | Run each statement inline, not prepared. |
| `-u`, `-p`, `--allow-insecure-credentials` | The same as for the shell. |

```shell
camus-cli workload init bank --database demo --rows 5000
camus-cli workload run tpcc --concurrency 4 --duration 120
```

See [Workload Utility](https://camusdb.github.io/docs/workload-utility/).
