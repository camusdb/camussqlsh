
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
