
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CommandLine;

public sealed class Options
{
    [Value(0, Required = false, MetaName = "database", HelpText = "Set the database name")]
    public string? Database { get; set; }

    [Option('c', "connection-source", Required = false, HelpText = "Set the connection string")]
    public string? ConnectionSource { get; set; }

    [Option('e', "execute", Required = false, HelpText = "Execute a SQL statement (or ;-separated statements) and exit")]
    public string? Execute { get; set; }

    [Option('f', "file", Required = false, HelpText = "Execute the statements in a .sql file and exit (- reads standard input)")]
    public string? File { get; set; }

    [Option('u', "user", Required = false, HelpText = "User to authenticate as")]
    public string? User { get; set; }

    [Option('p', "password", Required = false, HelpText = "That user's password (prefer the prompt or CAMUS_PASSWORD: on the command line it is visible to other users)")]
    public string? Password { get; set; }

    [Option("token", Required = false, HelpText = "Use a bearer token obtained elsewhere instead of logging in")]
    public string? AccessToken { get; set; }

    [Option("format", Required = false, HelpText = "How -e and -f print rows: table (default), json or yaml")]
    public string? Format { get; set; }

    [Option("no-history", Required = false, HelpText = "Do not load or save the statement history")]
    public bool NoHistory { get; set; }

    [Option("allow-insecure-credentials", Required = false, HelpText = "Send credentials to a plaintext http:// endpoint that is not on this machine")]
    public bool AllowInsecureCredentials { get; set; }
}
