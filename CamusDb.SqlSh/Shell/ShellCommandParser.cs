
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

// Recognizes the shell's own commands that only the prompt takes.
internal static class ShellCommandParser
{
    // Recognizes the words that leave the shell: `exit` and `quit`, plus the psql-style `\q` and the
    // slash-style `/quit` and `/exit` that users bring over from other clients.
    internal static bool IsExitCommand(string sql)
    {
        string[] commands = ["exit", "quit", "\\q", "/quit", "/exit"];
        foreach (string command in commands)
        {
            if (string.Equals(sql, command, StringComparison.InvariantCultureIgnoreCase))
                return true;
        }

        return false;
    }

    // Recognizes the words that list the shell's commands: `help`, plus the psql-style `\h` and `\?`,
    // `?` and the slash-style `/help`. A trailing `;` is allowed, since users type one out of habit.
    internal static bool IsHelpCommand(string sql)
    {
        string trimmed = sql.TrimEnd(';').TrimEnd();

        string[] commands = ["help", "\\h", "\\?", "?", "/help"];
        foreach (string command in commands)
        {
            if (string.Equals(trimmed, command, StringComparison.InvariantCultureIgnoreCase))
                return true;
        }

        return false;
    }

    // Recognizes the shell's own `show prepared` / `\prepared` command, optionally followed by the
    // statement to ask about instead of the last one executed.
    internal static bool IsShowPrepared(string sql, out string? statement)
    {
        statement = null;
        string trimmed = sql.Trim().TrimEnd(';').TrimEnd();

        string[] prefixes = ["show prepared", "\\prepared"];
        foreach (string prefix in prefixes)
        {
            if (!trimmed.StartsWith(prefix, StringComparison.InvariantCultureIgnoreCase))
                continue;

            string rest = trimmed[prefix.Length..];

            // "show preparedfoo" is not this command; a bare prefix or one followed by SQL is.
            if (rest.Length > 0 && !char.IsWhiteSpace(rest[0]))
                continue;

            rest = rest.Trim();
            statement = rest.Length > 0 ? rest : null;
            return true;
        }

        return false;
    }
}
