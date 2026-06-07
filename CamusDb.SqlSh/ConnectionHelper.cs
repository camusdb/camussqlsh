
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

internal static class ConnectionHelper
{
    internal static void Validate(string connectionString)
    {
        Dictionary<string, string> values = connectionString
            .Split(';', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(part => part.Split('=', 2, StringSplitOptions.TrimEntries))
            .Where(parts => parts.Length == 2)
            .GroupBy(parts => parts[0], StringComparer.InvariantCultureIgnoreCase)
            .ToDictionary(group => group.Key, group => group.Last()[1], StringComparer.InvariantCultureIgnoreCase);

        if (!values.TryGetValue("Endpoint", out string? endpoint) ||
            string.IsNullOrWhiteSpace(endpoint) ||
            !Uri.TryCreate(endpoint, UriKind.Absolute, out _))
        {
            throw new ArgumentException("Connection string must include a valid Endpoint. Example: Endpoint=http://localhost:5095;Database=mydb");
        }

        if (!values.TryGetValue("Database", out string? database) ||
            string.IsNullOrWhiteSpace(database))
        {
            throw new ArgumentException("Connection string must include Database. Example: Endpoint=http://localhost:5095;Database=mydb");
        }
    }

    internal static async Task<CamusConnection> OpenAsync(string connectionString)
    {
        Validate(connectionString);

        SessionPoolOptions options = new()
        {
            MinimumPooledSessions = 1,
            MaximumActiveSessions = 20,
        };

        SessionPoolManager manager = SessionPoolManager.Create(options);

        CamusConnectionStringBuilder builder = new(connectionString)
        {
            SessionPoolManager = manager
        };

        CamusConnection connection = new(builder);
        await connection.OpenAsync();

        CamusPingCommand ping = connection.CreatePingCommand();
        await ping.ExecuteNonQueryAsync();

        return connection;
    }
}
