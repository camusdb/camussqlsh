
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using Spectre.Console;
using static SqlKind;

// How the shell finds its credentials, which connection strings it tries, and what it tells the user
// about the connection it opened.
internal static class ConnectionSetup
{
    // Default listener ports for a local server: REST on 5095, gRPC on 5096 (both enabled by default).
    private const int DefaultRestPort = 5095;
    private const int DefaultGrpcPort = 5096;

    // Opens the first connection that works. Returns null after it reports the failure.
    internal static async Task<(CamusConnection Connection, string ConnectionString)?> ConnectAsync(Options opts, bool promptForPassword)
    {
        // Credentials come from the flags, then the environment. A user given without a password is
        // prompted for one (silently skipped when stdin is piped, so scripts aren't left hanging on a
        // prompt nobody can answer). The password is exchanged for a short-lived token by the driver on
        // the first statement, so it never travels with the SQL.
        string? authUser = opts.User ?? Environment.GetEnvironmentVariable("CAMUS_USER");
        string? authPassword = opts.Password ?? Environment.GetEnvironmentVariable("CAMUS_PASSWORD");
        string? authToken = opts.AccessToken ?? Environment.GetEnvironmentVariable("CAMUS_ACCESS_TOKEN");

        // A bare -p asks for the prompt outright, so it wins over CAMUS_PASSWORD. Without it, a user
        // given with no password at all is prompted too.
        if (promptForPassword)
            authPassword = PromptPassword() ?? authPassword;
        else if (!string.IsNullOrEmpty(authUser) && string.IsNullOrEmpty(authPassword) && string.IsNullOrEmpty(authToken))
            authPassword = PromptPassword();

        // A password with nobody to authenticate as is a typo, and the server would refuse it with a
        // message that names neither flag.
        if (promptForPassword && string.IsNullOrEmpty(authUser))
            Console.Error.WriteLine("Warning: -p was given without -u, so the shell connects unauthenticated. Add -u <user>.");

        CamusConnection connection;
        string activeConnectionString;

        try
        {
            // Inside the try: the driver validates the connection string as it is parsed — a duplicate key,
            // or credentials against a plaintext endpoint — so a rejection is reported here with a
            // diagnosable message, not a raw stack trace.
            List<string> connectionAttempts = BuildConnectionAttempts(opts, authUser, authPassword, authToken);
            (connection, activeConnectionString) = await ConnectionHelper.OpenFirstAsync(connectionAttempts);
        }
        catch (Exception ex)
        {
            ErrorReporter.WriteConnectionError(ex);
            return null;
        }

        // Only reached when the connection string waives the driver's own refusal, which nothing here can
        // second-guess: the shell names what the waiver allows and connects. stderr again, so scripted runs
        // keep their stdout parseable.
        if (SendsCredentialsCleartext(activeConnectionString))
        {
            Console.Error.WriteLine(
                "Warning: credentials are being sent over plaintext HTTP to a non-localhost endpoint, " +
                "because AllowInsecureCredentials is set. Use an https:// endpoint unless the link itself protects them.");
        }

        return (connection, activeConnectionString);
    }

    // Tells the user where and how they connected.
    internal static void WriteBanner(string connectionString)
    {
        string server = GetConnValue(connectionString, "Endpoint") ?? "unknown server";
        string database = GetConnValue(connectionString, "Database") is { Length: > 0 } db
            ? $"[white]{Markup.Escape(db)}[/]"
            : "[grey58](none)[/]";

        // Never echo the credential itself — only who the shell is acting as.
        string identity = GetConnValue(connectionString, "User") is { Length: > 0 } who
            ? $"[grey58], user:[/] [white]{Markup.Escape(who)}[/]"
            : HasKey(connectionString, "AccessToken")
                ? "[grey58], authenticated with a token[/]"
                : "";

        AnsiConsole.MarkupLine(
            $"[grey58]Connected to[/] [white]{Markup.Escape(server)}[/] [grey58]over[/] " +
            $"[white]{DescribeTransport(connectionString)}[/][grey58], database:[/] {database}{identity}\n");
    }

    // Builds the ordered list of connection strings to try. gRPC is preferred and REST is the
    // fallback, so a server that only speaks one of them (or has gRPC disabled) still connects.
    // When the caller pins Protocol= explicitly, that choice is honored with no fallback.
    private static List<string> BuildConnectionAttempts(Options opts, string? user, string? password, string? token)
    {
        bool allowInsecure = opts.AllowInsecureCredentials;

        if (!string.IsNullOrEmpty(opts.ConnectionSource))
        {
            string cs = WithCredentials(EnsureDatabase(opts.ConnectionSource), user, password, token, allowInsecure);

            // Respect an explicit Protocol= — the user has chosen the transport deliberately.
            if (HasKey(cs, "Protocol"))
                return [cs];

            // No protocol given: try gRPC against their endpoint, then REST against the same endpoint.
            // The gRPC attempt carries the same endpoint as its BackupEndpoint, because that is exactly
            // where REST is being tried next — the backup admin API speaks HTTP only.
            return [WithBackupEndpoint(WithProtocol(cs, "grpc"), GetConnValue(cs, "Endpoint")), WithProtocol(cs, "rest")];
        }

        string db = opts.Database ?? "";

        // No connection string at all: use the well-known local ports for each transport. The gRPC
        // attempt names the well-known HTTP port for backups, which is on a different port than gRPC.
        return
        [
            WithCredentials(
                WithBackupEndpoint($"Endpoint=http://localhost:{DefaultGrpcPort};Database={db};Protocol=grpc", $"http://localhost:{DefaultRestPort}"),
                user, password, token, allowInsecure),
            WithCredentials($"Endpoint=http://localhost:{DefaultRestPort};Database={db};Protocol=rest", user, password, token, allowInsecure),
        ];
    }

    // Adds the authentication keys the driver understands. Credentials passed on the command line (or
    // in the environment) win over the same key inside -c, since they were given more deliberately;
    // anything not supplied is left untouched, so a -c that already carries them still works.
    //
    // --allow-insecure-credentials adds the key that waives the driver's own refusal to carry
    // credentials to a plaintext endpoint that is not loopback. A -c that already sets the key keeps
    // its value, because both spellings say the same thing.
    private static string WithCredentials(string connectionString, string? user, string? password, string? token, bool allowInsecure = false)
    {
        if (!string.IsNullOrEmpty(user))
            connectionString = WithKey(connectionString, "User", user);

        if (!string.IsNullOrEmpty(password))
            connectionString = WithKey(connectionString, "Password", password);

        if (!string.IsNullOrEmpty(token))
            connectionString = WithKey(connectionString, "AccessToken", token);

        if (allowInsecure && !HasKey(connectionString, "AllowInsecureCredentials"))
            connectionString = WithKey(connectionString, "AllowInsecureCredentials", "true");

        return connectionString;
    }

    // Points `backup …` at an HTTP endpoint on a gRPC connection: the backup admin API is REST-only, so
    // without this every backup command on the shell's default (gRPC) connection would fail asking for the
    // key. An endpoint the user set themselves is left alone, and so is an `Endpoint=` pool's tail — a
    // backup goes to one node, and for a coordinated one that node has to be the coordinator, so pinning
    // it deliberately is the caller's job.
    private static string WithBackupEndpoint(string connectionString, string? endpoint)
    {
        if (HasKey(connectionString, "BackupEndpoint") || string.IsNullOrWhiteSpace(endpoint))
            return connectionString;

        string first = endpoint.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .FirstOrDefault() ?? endpoint;

        return WithKey(connectionString, "BackupEndpoint", first);
    }

    // Reads a password without echoing it. Returns null when there is no terminal to prompt on, so a
    // piped/scripted invocation fails on the server's authentication error rather than blocking here.
    private static string? PromptPassword()
    {
        if (Console.IsInputRedirected)
            return null;

        string entered = AnsiConsole.Prompt(new TextPrompt<string>("Password:").Secret().AllowEmpty());
        return string.IsNullOrEmpty(entered) ? null : entered;
    }

    // Human-readable transport name for the resolved connection string (Protocol= defaults to REST).
    private static string DescribeTransport(string connectionString)
    {
        return string.Equals(GetConnValue(connectionString, "Protocol"), "grpc", StringComparison.OrdinalIgnoreCase)
            ? "gRPC"
            : "REST";
    }
}
