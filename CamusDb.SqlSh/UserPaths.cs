/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

/// <summary>
/// Locations of the per-user files the shell keeps (statement history, the TUI's query scratch
/// file). Everything lives under the user's own profile — never the shared temporary directory,
/// where a fixed, predictable name lets any other local user pre-create or symlink the file and
/// then read whatever the shell writes or clobber an arbitrary file with it.
/// </summary>
internal static class UserPaths
{
    /// <summary>
    /// Directory holding the shell's per-user files. Honors $XDG_STATE_HOME (the Linux convention
    /// for state meant to survive a session) when set; elsewhere the per-user application-data
    /// directory (~/.config on Unix, %LOCALAPPDATA% on Windows).
    /// </summary>
    internal static string StateDirectory()
    {
        if (!OperatingSystem.IsWindows())
        {
            string? xdgState = Environment.GetEnvironmentVariable("XDG_STATE_HOME");
            if (!string.IsNullOrWhiteSpace(xdgState) && Path.IsPathRooted(xdgState))
                return Path.Combine(xdgState, "camusdb");
        }

        string appData = OperatingSystem.IsWindows()
            ? Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData)
            : Environment.GetFolderPath(Environment.SpecialFolder.ApplicationData);

        return Path.Combine(appData, "camusdb");
    }

    internal static string HistoryPath => Path.Combine(StateDirectory(), "history.json");

    // A fixed name is fine inside a directory only this user can reach, and it is what lets the
    // TUI restore the previous session's query buffer.
    internal static string QueryScratchPath => Path.Combine(StateDirectory(), "query.sql");

    /// <summary>
    /// Creates the state directory with owner-only permissions (0700) on Unix, so the files
    /// inside are unreadable to other local users no matter what mode they themselves get.
    /// </summary>
    internal static void EnsureStateDirectory()
    {
        string dir = StateDirectory();
        Directory.CreateDirectory(dir);

        if (!OperatingSystem.IsWindows())
            File.SetUnixFileMode(dir, UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute);
    }
}
