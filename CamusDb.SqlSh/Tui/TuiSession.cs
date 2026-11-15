/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using static SqlKind;

namespace CamusDb.SqlSh.Tui;

/// <summary>
/// The connection that every tab uses: the connection string, the current database and the open
/// transaction. A <c>BEGIN</c> in one tab is open in every tab.
/// </summary>
internal sealed class TuiSession
{
    private CamusTransaction? _transaction;

    public TuiSession(CamusConnection connection, string connectionString)
    {
        Connection = connection;
        ConnectionString = connectionString;
        Database = GetConnValue(connectionString, "Database") is { Length: > 0 } db ? db : null;
    }

    // `use` replaces the connection, so none of these are readonly.
    public CamusConnection Connection { get; private set; }

    public string ConnectionString { get; private set; }

    /// <summary>Null when the session has no current database.</summary>
    public string? Database { get; private set; }

    public CamusTransaction? Transaction
    {
        get => Volatile.Read(ref _transaction);
        set => Volatile.Write(ref _transaction, value);
    }

    /// <summary>Takes the open transaction away from the session, for the rollback at exit.</summary>
    public CamusTransaction? TakeTransaction() => Interlocked.Exchange(ref _transaction, null);

    public bool IsCurrent(string database) => string.Equals(database, Database, StringComparison.OrdinalIgnoreCase);

    public bool HasDatabase => SqlKind.HasDatabase(ConnectionString);

    /// <summary>A connection to the node with no database, for the statements that need none.</summary>
    public Task<CamusConnection> OpenEndpointAsync() =>
        ConnectionHelper.OpenAsync(GetEndpointConnectionString(ConnectionString));

    /// <summary>A connection to another database, for the catalog. The caller closes it.</summary>
    public Task<CamusConnection> OpenDatabaseAsync(string database) =>
        ConnectionHelper.OpenAsync(SwapDatabase(ConnectionString, database));

    /// <summary>
    /// Points the session at another database. There is no server-side session to tell, so the
    /// connection string is the only thing that changes.
    /// </summary>
    public async Task SwitchDatabaseAsync(string database)
    {
        ConnectionString = SwapDatabase(ConnectionString, database);
        Connection = await ConnectionHelper.OpenAsync(ConnectionString);
        Database = database;
    }
}
