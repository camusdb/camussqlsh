
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

namespace CamusDb.SqlSh.Tests;

// SHOW [CLUSTER] TRANSACTIONS reads the open transactions of the host, and KILL TRANSACTION rolls one back.
// Neither reads a database, so the shell sends both on the endpoint connection, outside its own open
// transaction. TRANSACTIONS and KILL are plain identifiers to the parser, so a table can have either name.
public sealed class TestTransactionActivityStatements
{
    // The list runs on the endpoint connection and returns rows.
    [TestCase("SHOW TRANSACTIONS")]
    [TestCase("show  transactions")]
    [TestCase("SHOW CLUSTER TRANSACTIONS")]
    public void TestShowTransactionsNeedsNoDatabase(string sql)
    {
        Assert.That(SqlKind.IsSystemLevelQuery(sql), Is.True);
        Assert.That(SqlKind.IsQueryable(sql), Is.True);
    }

    // The server dispatches a kill before it opens a database, as it does CANCEL QUERY.
    [TestCase("KILL TRANSACTION 'b71a90e314-t1764000000000.3'")]
    [TestCase("kill  transaction 'x'")]
    public void TestKillTransactionIsServerLevel(string sql)
    {
        Assert.That(SqlKind.IsKillTransaction(sql), Is.True);
        Assert.That(SqlKind.IsServerLevelDDL(sql), Is.True);
        Assert.That(SqlKind.IsQueryable(sql), Is.False);
    }

    // A column or a table named after the words is an ordinary name.
    [Test]
    public void TestTheWordsStayNames()
    {
        Assert.That(SqlKind.IsKillTransaction("SELECT kill FROM transactions"), Is.False);
        Assert.That(SqlKind.IsSystemLevelQuery("SELECT * FROM transactions"), Is.False);
    }
}
