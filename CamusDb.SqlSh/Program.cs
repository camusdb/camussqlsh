
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDb.SqlSh.Tui;
using CamusDB.Client;
using CommandLine;
using RadLine;
using Spectre.Console;
using System.Reflection;

string? informationalVersion = Assembly.GetExecutingAssembly()
    .GetCustomAttribute<AssemblyInformationalVersionAttribute>()
    ?.InformationalVersion;

string version = informationalVersion?.Split('+')[0]
    ?? Assembly.GetExecutingAssembly().GetName().Version?.ToString(3)
    ?? "unknown";

// --format json|yaml promises a stdout that a parser can read, so the banner cannot go there. The
// option parser runs later, so the raw arguments are read here.
if (!CommandLineArgs.RequestsStructuredOutput(args))
    Console.WriteLine($"CamusDB SQL Shell {version}\n");

int workloadIdx = Array.FindIndex(args, a => string.Equals(a, "workload", StringComparison.OrdinalIgnoreCase));
if (workloadIdx >= 0)
{
    // Collect flags that appeared before "workload" and append them after
    string[] beforeWorkload = args[..workloadIdx];
    string[] afterWorkload = args[(workloadIdx + 1)..];
    await WorkloadCommand.RunAsync([.. afterWorkload, .. beforeWorkload]);
    return;
}

if (args.Length > 0 && (args[0] == "--help" || args[0] == "-h"))
{
    HelpText.PrintUsage();
    return;
}

// Consumed here so CommandLineParser doesn't reject them as unknown options.
bool tuiMode = CommandLineArgs.ConsumeFlag(ref args, "--tui");
bool debugKeys = CommandLineArgs.ConsumeFlag(ref args, "--debug-keys");
bool diagnoseTerminal = CommandLineArgs.ConsumeFlag(ref args, "--diagnose-terminal");
bool forceRich = CommandLineArgs.ConsumeFlag(ref args, "--force-rich")
    || CommandLineArgs.IsTruthy(Environment.GetEnvironmentVariable("CAMUS_FORCE_RICH"));

// A bare -p means "ask me for the password", the way mysql(1) spells it. CommandLineParser binds
// -p to whatever token follows it, so `-p -u alice` would otherwise take "-u" as the password and
// leave the shell with no user at all. Consume that spelling here and prompt later instead.
bool promptForPassword = CommandLineArgs.ConsumePasswordPrompt(ref args);

// Some capable terminals (e.g. Rio) advertise a TERM value that Spectre.Console's ANSI
// detector doesn't recognize, so it disables the rich editor even though the terminal
// handles ANSI fine. Forcing the capabilities lets the whole app render richly.
if (forceRich)
{
    AnsiConsole.Console = AnsiConsole.Create(new AnsiConsoleSettings
    {
        Ansi = AnsiSupport.Yes,
        Interactive = InteractionSupport.Yes,
        ColorSystem = ColorSystemSupport.Detect,
    });
}

ParserResult<Options> optsResult = Parser.Default.ParseArguments<Options>(CommandLineArgs.NormalizeBuiltInOptions(args));

Options? opts = optsResult.Value;
if (opts is null)
    return;

// A password in argv is readable by every local user (ps, /proc/<pid>/cmdline) and lands in the
// invoking shell's own history. The option still works — removing it outright would break
// scripts — but each use gets told the safer spellings, on stderr so -e/-f stdout stays clean.
if (!string.IsNullOrEmpty(opts.Password))
{
    Console.Error.WriteLine(
        "Warning: -p on the command line exposes the password to other local users and to your shell history. " +
        "Prefer running with -u alone (the shell prompts without echoing) or the CAMUS_PASSWORD environment variable.");
}

// -e and -f both run SQL and exit; the shell never prompts in that case, so it also keeps stdout
// clean of the connection banner. "-f -" reads the script from standard input, so a heredoc or a
// pipe works the same as a file on disk.
bool readScriptFromStdin = opts.File == "-";
bool nonInteractive = !string.IsNullOrWhiteSpace(opts.Execute) || !string.IsNullOrWhiteSpace(opts.File);

if (!StructuredResultWriter.TryParse(opts.Format, out OutputFormat outputFormat))
{
    Console.Error.WriteLine($"Unknown --format '{opts.Format}'. Use table, json or yaml.");
    Environment.ExitCode = 1;
    return;
}

// JSON and YAML are for a program that reads stdout, so only -e and -f print them. Every other line
// the shell writes (the "rows in set" footer, "Query OK", an error) goes to stderr through
// AnsiConsole, and stdout carries the documents only.
if (outputFormat != OutputFormat.Table)
{
    if (!nonInteractive)
    {
        Console.Error.WriteLine("--format json and --format yaml need -e or -f.");
        Environment.ExitCode = 1;
        return;
    }

    AnsiConsole.Console = AnsiConsole.Create(new AnsiConsoleSettings
    {
        Out = new AnsiConsoleOutput(Console.Error),
    });
}

// Checked before connecting: a typo in the path shouldn't cost a round trip or look like a
// server problem.
if (!string.IsNullOrWhiteSpace(opts.File) && !readScriptFromStdin && !File.Exists(opts.File))
{
    AnsiConsole.MarkupLine("[red]File not found: {0}[/]", Markup.Escape(opts.File));
    Environment.ExitCode = 1;
    return;
}

if (diagnoseTerminal)
{
    TerminalDiagnostics.WriteCapabilities(forceRich);
    return;
}

if (debugKeys)
{
    TerminalDiagnostics.EchoKeys();
    return;
}

StatementHistory? history = opts.NoHistory ? null : await StatementHistory.TryLoadAsync();

(CamusConnection Connection, string ConnectionString)? connected = await ConnectionSetup.ConnectAsync(opts, promptForPassword);
if (connected is null)
{
    Environment.ExitCode = 1;
    return;
}

(CamusConnection connection, string connectionString) = connected.Value;

// Tell the user where and how they connected, unless they're scripting with -e/-f (keep stdout clean).
if (!nonInteractive)
    ConnectionSetup.WriteBanner(connectionString);

// Set only for --format json|yaml, which is refused above outside -e and -f.
ResultWriter resultWriter = new(outputFormat == OutputFormat.Table
    ? null
    : new StructuredResultWriter(outputFormat, Console.OpenStandardOutput()));

if (nonInteractive)
{
    await ScriptRunner.RunAsync(new ShellSession(connection, connectionString, editor: null, history, resultWriter), opts);
    return;
}

ShellEditor? editor = LineEditor.IsSupported(AnsiConsole.Console) ? ShellEditor.Create(history) : null;

ShellSession session = new(connection, connectionString, editor, history, resultWriter);

session.RegisterCancelKeyHandler();

if (editor is not null)
    await editor.LoadServerVocabularyAsync(connection, connectionString);

// Full-screen mode takes over the terminal, so it runs instead of the REPL, never beside it.
if (tuiMode)
{
    if (editor is null)
    {
        AnsiConsole.MarkupLine("[red]--tui needs an ANSI terminal.[/] Try [cyan]--force-rich[/] or set [cyan]TERM=xterm-256color[/].");
        Environment.ExitCode = 1;
        return;
    }

    await CamusTui.RunAsync(
        connection,
        connectionString,
        ValueFormatter.Format,
        editor.Highlighter,
        editor.Completion,
        UserPaths.QueryScratchPath);

    return;
}

await new InteractiveShell(session).RunAsync();
