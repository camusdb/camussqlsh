
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
        // Read through the same quote-aware parser the driver uses, so a quoted value carrying a
        // ';' — a password, or a RoutingNodes map — is one value here too.
        string? endpoint = SqlKind.GetConnValue(connectionString, "Endpoint");

        // Endpoint accepts a comma-separated pool of nodes, which the driver round-robins over, so
        // each member is validated on its own. The whole value is not a URI.
        string[] endpoints = endpoint is not null
            ? endpoint.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            : [];

        if (endpoints.Length == 0 || endpoints.Any(e => !Uri.TryCreate(e, UriKind.Absolute, out _)))
        {
            throw new ArgumentException("Connection string must include a valid Endpoint. Example: Endpoint=http://localhost:5095");
        }
    }

    /// <summary>
    /// Copies the reader's current row into a dictionary keyed by column name.
    ///
    /// <para>Each cell is taken as the driver holds it, so every one of the twelve
    /// <see cref="ColumnType"/> members arrives with its own backing field. A per-type rebuild
    /// through the reader's typed accessors cannot do that: it has no branch for
    /// <see cref="ColumnType.Bytes"/>, <see cref="ColumnType.Date"/>,
    /// <see cref="ColumnType.DateTime"/>, <see cref="ColumnType.Float32"/> or
    /// <see cref="ColumnType.Array"/>, so each of those reached the display as the text
    /// <c>GetString</c> makes of it — <c>System.Byte[]</c> for a bytes column.</para>
    /// </summary>
    internal static Dictionary<string, ColumnValue> ReadCurrentRow(CamusDataReader reader)
    {
        Dictionary<string, ColumnValue> row = new(reader.FieldCount);

        for (int i = 0; i < reader.FieldCount; i++)
            row[reader.GetName(i)] = reader.GetColumnValue(i);

        return row;
    }

    internal static async Task<CamusConnection> OpenAsync(string connectionString)
    {
        Validate(connectionString);

        // CamusDB has no server-side sessions to pool; the gRPC stream pool is tuned with the
        // ChannelPoolSize= connection-string key, which callers can set on their own string.
        CamusConnectionStringBuilder builder = new(connectionString);

        CamusConnection connection = new(builder);
        await connection.OpenAsync();

        CamusPingCommand ping = connection.CreatePingCommand();
        await ping.ExecuteNonQueryAsync();

        return connection;
    }

    /// <summary>
    /// Opens the first connection string in <paramref name="attempts"/> that connects, returning
    /// the live connection together with the connection string that won. Used to prefer gRPC and
    /// silently fall back to REST when the gRPC endpoint can't be reached; the last attempt's
    /// exception is rethrown if every attempt fails.
    /// </summary>
    internal static async Task<(CamusConnection Connection, string ConnectionString)> OpenFirstAsync(
        IReadOnlyList<string> attempts)
    {
        Exception? lastError = null;

        for (int i = 0; i < attempts.Count; i++)
        {
            string attempt = attempts[i];
            try
            {
                CamusConnection connection = await OpenAsync(attempt);
                return (connection, attempt);
            }
            catch (Exception ex) when (i < attempts.Count - 1)
            {
                // Not the last attempt: remember the error and fall through to the next transport.
                lastError = ex;
            }
        }

        // Only reached when the list was empty; the loop rethrows the final attempt's error otherwise.
        throw lastError ?? new InvalidOperationException("No connection attempts were provided.");
    }
}
