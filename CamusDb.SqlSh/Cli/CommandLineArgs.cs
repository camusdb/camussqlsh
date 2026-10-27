
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

// Reads the arguments that the shell takes before CommandLineParser sees them.
internal static class CommandLineArgs
{
    // True when argv holds a bare -p / --password: one that ends the arguments, or that is followed by
    // another option instead of a value. The token is removed so the parser cannot bind that following
    // option to it. `-p SECRET`, `-pSECRET` and `--password=SECRET` are values, and are left alone.
    internal static bool ConsumePasswordPrompt(ref string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            if (args[i] != "-p" && args[i] != "--password")
                continue;

            if (i + 1 < args.Length && !LooksLikeOption(args[i + 1]))
                return false;

            args = [.. args[..i], .. args[(i + 1)..]];
            return true;
        }

        return false;
    }

    // True for a token the parser reads as an option name. A single "-" is a value: -f - reads the
    // script from standard input.
    private static bool LooksLikeOption(string arg)
    {
        return arg.Length > 1 && arg[0] == '-';
    }

    internal static bool ConsumeFlag(ref string[] args, string flag)
    {
        bool present = Array.Exists(args, a => string.Equals(a, flag, StringComparison.OrdinalIgnoreCase));
        if (present)
            args = [.. args.Where(a => !string.Equals(a, flag, StringComparison.OrdinalIgnoreCase))];

        return present;
    }

    // True when argv asks for --format json or --format yaml, in either the "--format x" or the
    // "--format=x" spelling. Only the banner needs this before the option parser runs.
    internal static bool RequestsStructuredOutput(string[] args)
    {
        for (int i = 0; i < args.Length; i++)
        {
            string? value = args[i] == "--format" && i + 1 < args.Length
                ? args[i + 1]
                : args[i].StartsWith("--format=", StringComparison.Ordinal) ? args[i]["--format=".Length..] : null;

            if (StructuredResultWriter.TryParse(value, out OutputFormat format) && format != OutputFormat.Table)
                return true;
        }

        return false;
    }

    internal static bool IsTruthy(string? value)
    {
        return !string.IsNullOrEmpty(value)
            && (value == "1"
                || string.Equals(value, "true", StringComparison.OrdinalIgnoreCase)
                || string.Equals(value, "yes", StringComparison.OrdinalIgnoreCase));
    }

    internal static IEnumerable<string> NormalizeBuiltInOptions(IEnumerable<string> args)
    {
        foreach (string arg in args)
        {
            yield return arg switch
            {
                "-h" => "--help",
                "-v" => "--version",
                _ => arg
            };
        }
    }
}
