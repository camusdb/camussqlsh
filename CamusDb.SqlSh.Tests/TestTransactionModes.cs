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

// The shell reads the modes of BEGIN and START TRANSACTION, because it opens the transaction through the
// client and does not send the BEGIN text. Before, it did not read them, so BEGIN READ ONLY opened a
// read-write transaction. The tests need no server: a deferred BEGIN does not contact the server.
public sealed class TestTransactionModes
{
    private const string ConnectionString = "Endpoint=http://127.0.0.1:1;Database=shop";

    private static CamusConnection DeferredConnection() => new(new CamusConnectionStringBuilder(ConnectionString))
    {
        DefaultTransactionOptions = new CamusTransactionOptions { DeferBegin = true },
    };

    private static ShellSession NewShellSession() =>
        new(DeferredConnection(), ConnectionString, editor: null, history: null, new ResultWriter(null));

    [TestCase("BEGIN", null, null)]
    [TestCase("begin;", null, null)]
    [TestCase("BEGIN TRANSACTION", null, null)]
    [TestCase("BEGIN WORK", null, null)]
    [TestCase("START TRANSACTION", null, null)]
    [TestCase("BEGIN READ ONLY", CamusTransactionMode.ReadOnly, null)]
    [TestCase("begin read write", CamusTransactionMode.ReadWrite, null)]
    [TestCase("START TRANSACTION READ ONLY", CamusTransactionMode.ReadOnly, null)]
    [TestCase("BEGIN ISOLATION LEVEL SERIALIZABLE", null, CamusIsolationLevel.Serializable)]
    [TestCase("BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED", null, CamusIsolationLevel.ReadCommitted)]
    [TestCase("BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY", CamusTransactionMode.ReadOnly, CamusIsolationLevel.Serializable)]
    [TestCase("BEGIN READ ONLY ISOLATION LEVEL READ COMMITTED", CamusTransactionMode.ReadOnly, CamusIsolationLevel.ReadCommitted)]
    [TestCase("begin -- a comment\nread only;", CamusTransactionMode.ReadOnly, null)]
    [TestCase("BEGIN /* snapshot */ READ ONLY", CamusTransactionMode.ReadOnly, null)]
    [TestCase("BEGIN READ ONLY\\G", CamusTransactionMode.ReadOnly, null)]
    public void TestParseModes(string sql, CamusTransactionMode? mode, CamusIsolationLevel? isolation)
    {
        CamusTransactionOptions options = BeginStatement.Parse(sql);

        Assert.That(options.Mode, Is.EqualTo(mode));
        Assert.That(options.IsolationLevel, Is.EqualTo(isolation));
    }

    [TestCase("START")]
    [TestCase("BEGIN READ")]
    [TestCase("BEGIN READ ONLY READ WRITE")]
    [TestCase("BEGIN ISOLATION LEVEL REPEATABLE READ")]
    [TestCase("BEGIN ISOLATION LEVEL SERIALIZABLE ISOLATION LEVEL READ COMMITTED")]
    [TestCase("BEGIN DEFERRABLE")]
    [TestCase("BEGIN , READ ONLY")]
    [TestCase("BEGIN READ ONLY,")]
    [TestCase("BEGIN READ ONLY,, READ WRITE")]
    [TestCase("BEGIN TRANSACTION TRANSACTION")]
    public void TestParseRefusesUnknownModes(string sql)
    {
        Assert.Throws<TransactionModeException>(() => BeginStatement.Parse(sql));
    }

    [TestCase("begin", true)]
    [TestCase("  BEGIN READ ONLY", true)]
    [TestCase("start transaction", true)]
    [TestCase("startup_log", false)]
    [TestCase("beginning", false)]
    public void TestIsBeginTx(string sql, bool expected)
    {
        Assert.That(SqlKind.IsBeginTx(sql), Is.EqualTo(expected));
    }

    [Test]
    public async Task TestShellOpensReadOnlyTransaction()
    {
        ShellSession session = NewShellSession();

        await session.ExecuteStatement("BEGIN READ ONLY", vertical: false);

        Assert.That(session.Transaction, Is.Not.Null);
        Assert.That(session.Transaction!.Options.Mode, Is.EqualTo(CamusTransactionMode.ReadOnly));
    }

    [Test]
    public async Task TestShellKeepsDefaultsForPlainBegin()
    {
        ShellSession session = NewShellSession();

        await session.ExecuteStatement("BEGIN", vertical: false);

        Assert.That(session.Transaction!.Options.Mode, Is.Null);
        Assert.That(session.Transaction!.Options.IsolationLevel, Is.Null);
        Assert.That(session.Transaction!.Options.DeferBegin, Is.True);
    }

    // The shell opens no transaction for a mode it cannot read, so a script stops before its writes.
    [Test]
    public void TestShellRefusesUnknownMode()
    {
        ShellSession session = NewShellSession();

        Assert.ThrowsAsync<TransactionModeException>(
            () => session.ExecuteStatement("BEGIN ISOLATION LEVEL REPEATABLE READ", vertical: false));

        Assert.That(session.Transaction, Is.Null);
    }

    [Test]
    public async Task TestTuiOpensReadOnlyTransaction()
    {
        TuiSession session = new(DeferredConnection(), ConnectionString);
        TuiContext context = new(_ => "");
        StatementRunner runner = new(session, context, new CatalogPane(session, context, completion: null), completion: null);
        RunLog log = new();

        await runner.RunStatementAsync(new ResultView(context), "START TRANSACTION READ ONLY, ISOLATION LEVEL SERIALIZABLE", log, CancellationToken.None);

        Assert.That(session.Transaction!.Options.Mode, Is.EqualTo(CamusTransactionMode.ReadOnly));
        Assert.That(session.Transaction!.Options.IsolationLevel, Is.EqualTo(CamusIsolationLevel.Serializable));
        Assert.That(log.Entries.Single().Text, Does.StartWith("transaction started, READ ONLY, ISOLATION LEVEL SERIALIZABLE"));
    }

    [Test]
    public void TestDescribeModes()
    {
        Assert.That(BeginStatement.DescribeModes(new CamusTransactionOptions()), Is.Null);
        Assert.That(
            BeginStatement.DescribeModes(new CamusTransactionOptions { Mode = CamusTransactionMode.ReadOnly, IsolationLevel = CamusIsolationLevel.ReadCommitted }),
            Is.EqualTo("READ ONLY, ISOLATION LEVEL READ COMMITTED"));
    }
}
