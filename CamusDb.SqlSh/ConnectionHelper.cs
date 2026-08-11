
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
            throw new ArgumentException("Connection string must include a valid Endpoint. Example: Endpoint=http://localhost:5095");
        }
    }

    internal static Dictionary<string, ColumnValue> ReadCurrentRow(CamusDataReader reader)
    {
        Dictionary<string, ColumnValue> row = new(reader.FieldCount);
        for (int i = 0; i < reader.FieldCount; i++)
        {
            ColumnValue cv = reader.IsDBNull(i)
                ? new ColumnValue { Type = ColumnType.Null }
                : reader.GetDataTypeName(i) switch
                {
                    "Id" => new ColumnValue { Type = ColumnType.Id, StrValue = reader.GetString(i) },
                    "Integer64" => new ColumnValue { Type = ColumnType.Integer64, LongValue = reader.GetInt64(i) },
                    "Bool" => new ColumnValue { Type = ColumnType.Bool, BoolValue = reader.GetBoolean(i) },
                    "Float64" => new ColumnValue { Type = ColumnType.Float64, FloatValue = (float)reader.GetDouble(i) },
                    "Uuid" => new ColumnValue { Type = ColumnType.Uuid, UuidValue = reader.GetGuid(i).ToString() },
                    _ => new ColumnValue { Type = ColumnType.String, StrValue = reader.GetString(i) }
                };
            row[reader.GetName(i)] = cv;
        }
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
