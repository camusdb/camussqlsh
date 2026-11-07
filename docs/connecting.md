# Connecting

```shell
camus-cli                 # local server, no current database
camus-cli northwind       # local server, current database "northwind"
camus-cli -c "Endpoint=http://db.example.com:5095;Database=northwind"
```

With no connection string, the shell tries gRPC on `http://localhost:5096`, then REST on
`http://localhost:5095`. The banner shows the endpoint, the transport and the database:

```text
Connected to http://localhost:5096 over gRPC, database: northwind
```

Without a database name, use `use <database>` to select one.

## Connection string

- `Endpoint` is necessary. It can be a comma-separated list of nodes.
- Without a `Protocol` key, the shell tries gRPC, then REST, on the same endpoint. Set
  `Protocol=grpc` or `Protocol=rest` to use one transport only.
- `-c` accepts every key of the .NET driver. See the
  [.NET driver documentation](https://camusdb.github.io/docs/dotnet-driver/).

## Authentication

```shell
camus-cli northwind -u app                 # prompts: Password:
camus-cli northwind -u app -p              # -p with no value prompts too
CAMUS_PASSWORD=app-secret camus-cli northwind -u app
```

The shell sends the password one time to get a short-lived token. All subsequent statements send
the token. The banner names the user.

Other ways to give credentials:

- `CAMUS_USER` and `CAMUS_PASSWORD`.
- `User=` and `Password=` in `-c`. A flag wins over the same key in `-c`.
- `--token`, or `CAMUS_ACCESS_TOKEN`, for a token that you got elsewhere. The shell does not renew
  it, so the session ends when the token expires.

Do not use `-p <password>` on a shared host. Every process on the machine can read a command line.

For users, grants and server settings, see
[Authentication](https://camusdb.github.io/docs/sql-authentication/).

## Plaintext endpoints

The shell refuses to send credentials to an `http://` endpoint that is not on this machine. It
stops before the password goes on the network:

```text
$ camus-cli northwind -c "Endpoint=http://db.internal:5095" -u app
Connection failed (CADB0519): This connection string configures credentials against the Endpoint
'http://db.internal:5095', which is neither https nor loopback.
```

Use `https://` for a remote server. If TLS ends in front of the server on a safe network, add
`--allow-insecure-credentials`. The shell writes a warning to stderr and connects. Nothing checks
that the network is safe.
