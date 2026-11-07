# Troubleshooting

## The shell does not exit

A transaction is open. Run `commit` or `rollback` first.

## The connection string is not valid

`Endpoint` must be a full URL, for example `Endpoint=http://localhost:5095;Database=northwind`.

## An old version runs

```shell
camus-cli -v
dotnet nuget locals all --clear
dotnet tool update --global CamusDB.SqlSh
```

## Terminal detection

The rich editor needs an interactive ANSI terminal. The shell finds this mainly from `TERM`. Some
ANSI terminals have a `TERM` value that Spectre.Console does not know, for example Rio
(`TERM=rio`). On these terminals, the shell uses a plain prompt.

```text
$ camus-cli --diagnose-terminal
Rich editor disabled (falling back to plain prompt). Terminal capabilities:
  IsTerminal  : True
  Ansi        : False
  Interactive : False
  TERM        : rio
  NO_COLOR    : (unset)
  ForceRich   : False
```

To use the rich editor on such a terminal, do one of these:

- Add `--force-rich`.
- Set `CAMUS_FORCE_RICH=1` for all sessions.
- Run with a known `TERM`, for example `TERM=xterm-256color camus-cli`. This also helps other
  terminal programs.
