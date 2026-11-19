
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using CamusDb.SqlSh.Tui;
using NUnit.Framework;

namespace CamusDb.SqlSh.Tests;

// DDL in the database goes in the open transaction, so the client and the server can refuse a statement
// that a ROLLBACK cannot undo. TRUNCATE is one: the client refuses it with CADB0538 before it sends it.
// The tests need no server. A deferred BEGIN does not contact the server, and the refusal comes before
// the client connects. Sent outside the transaction, TRUNCATE goes to the server and fails to connect.
public sealed class TestDdlInTransaction
{
    // Nothing listens on port 1, so each statement that the client sends fails to connect.
    private const string ConnectionString = "Endpoint=http://127.0.0.1:1;Database=shop";

    private const string StatementNotAllowedInTransaction = "CADB0538";

    private static CamusConnection DeferredConnection() => new(new CamusConnectionStringBuilder(ConnectionString))
    {
        DefaultTransactionOptions = new CamusTransactionOptions { DeferBegin = true },
    };

    private static ShellSession NewShellSession() =>
        new(DeferredConnection(), ConnectionString, editor: null, history: null, new ResultWriter(null));

    [TestCase("TRUNCATE TABLE orders")]
    [TestCase("truncate orders")]
    public async Task TestShellRefusesTruncateInTransaction(string sql)
    {
        ShellSession session = NewShellSession();

        await session.ExecuteStatement("BEGIN", vertical: false);
        Assert.That(session.Transaction, Is.Not.Null);

        CamusException? ex = Assert.ThrowsAsync<CamusException>(() => session.ExecuteStatement(sql, vertical: false));
        Assert.That(ex!.Code, Is.EqualTo(StatementNotAllowedInTransaction));
    }

    // -e runs its text through ExecuteSql: BEGIN, then TRUNCATE fails, and ROLLBACK does not run.
    [Test]
    public void TestExecuteRefusesTruncateInTransaction()
    {
        ShellSession session = NewShellSession();

        CamusException? ex = Assert.ThrowsAsync<CamusException>(
            () => session.ExecuteSql("BEGIN; TRUNCATE TABLE orders; ROLLBACK"));

        Assert.That(ex!.Code, Is.EqualTo(StatementNotAllowedInTransaction));
        Assert.That(session.Transaction, Is.Not.Null);
    }

    // -f runs the statements of the file one at a time through ExecuteStatement.
    [Test]
    public async Task TestScriptRefusesTruncateInTransaction()
    {
        ShellSession session = NewShellSession();
        List<SqlStatement> statements = [];

        await foreach (SqlStatement statement in SqlScript.ReadStatements(new StringReader("BEGIN;\nTRUNCATE TABLE orders;\nROLLBACK;\n")))
            statements.Add(statement);

        await session.ExecuteStatement(statements[0].Sql, statements[0].Vertical);

        CamusException? ex = Assert.ThrowsAsync<CamusException>(
            () => session.ExecuteStatement(statements[1].Sql, statements[1].Vertical));

        Assert.That(ex!.Code, Is.EqualTo(StatementNotAllowedInTransaction));
    }

    [Test]
    public async Task TestTuiRefusesTruncateInTransaction()
    {
        TuiSession session = new(DeferredConnection(), ConnectionString);
        TuiContext context = new(_ => "");
        StatementRunner runner = new(session, context, new CatalogPane(session, context, completion: null), completion: null);

        await runner.RunStatementAsync(new ResultView(context), "BEGIN", new RunLog(), CancellationToken.None);
        Assert.That(session.Transaction, Is.Not.Null);

        CamusException? ex = Assert.ThrowsAsync<CamusException>(
            () => runner.RunStatementAsync(new ResultView(context), "TRUNCATE TABLE orders", new RunLog(), CancellationToken.None));

        Assert.That(ex!.Code, Is.EqualTo(StatementNotAllowedInTransaction));
    }

    // The control case: without a transaction, the shell sends TRUNCATE to the server. So the refusal
    // above comes from the transaction.
    [Test]
    public void TestTruncateWithoutTransactionGoesToServer()
    {
        ShellSession session = NewShellSession();

        Exception? ex = Assert.CatchAsync(() => session.ExecuteStatement("TRUNCATE TABLE orders", vertical: false));

        Assert.That((ex as CamusException)?.Code, Is.Not.EqualTo(StatementNotAllowedInTransaction));
    }

    // After a failure the shell drops the transaction, also when the rollback cannot reach the server.
    [Test]
    public async Task TestDiscardDropsTransactionWhenRollbackFails()
    {
        ShellSession session = NewShellSession();

        await session.ExecuteStatement("BEGIN", vertical: false);

        Assert.That(await session.DiscardTransactionAsync(), Is.True);
        Assert.That(session.Transaction, Is.Null);
        Assert.That(await session.DiscardTransactionAsync(), Is.False);
    }
}
