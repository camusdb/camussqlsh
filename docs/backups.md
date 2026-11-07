# Backups

The `backup` commands use the backup API of the server:

```text
backup full                            take a full backup
backup incremental <parent-backup-id>  add an incremental backup to a chain
backup coordinated                     take a backup of the full cluster at one point
backup list                            list the backups of the node
backup chain <leaf-backup-id>          find and check a restore chain
backup gc preview                      show what retention would delete
backup gc                              run retention now
```

`backup` or `backup help` shows this list.

```text
camus> backup full
    Backup Id: 971a0a88-3d36-42c6-b36b-8d1e773f40c4
         Type: Full
Created (UTC): 2026-08-10 03:45:47
       Parent: (none)
   Partitions: 4
Backup OK (00:00:03.117)
```

- The commands work at the prompt only, not in `--tui`. They do not need a current database.
- The API uses HTTP only. The shell uses `http://localhost:5095`, or the `Endpoint` of `-c`. With
  `Protocol=grpc`, set `BackupEndpoint=` to the HTTP endpoint.
- `backup coordinated` must go to the coordinator. If `Endpoint` has more than one node, set
  `BackupEndpoint=` to the coordinator.
- `BackupTimeout=` sets the timeout in seconds. The default is 300.
- With authentication on, connect as a superuser.

The shell cannot restore. See [Backup and Restore](https://camusdb.github.io/docs/backup-and-restore/).
