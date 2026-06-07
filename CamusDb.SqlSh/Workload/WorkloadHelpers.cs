
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

internal static class WorkloadHelpers
{
    internal static async Task DDL(CamusConnection conn, string sql)
    {
        using CamusCommand cmd = conn.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        await cmd.ExecuteDDLAsync();
    }

    internal static async Task Exec(CamusConnection conn, string sql, CamusTransaction? tx = null)
    {
        using CamusCommand cmd = conn.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = tx;
        await cmd.ExecuteNonQueryAsync();
    }

    internal static string Esc(string s) => s.Replace("'", "''");
}
