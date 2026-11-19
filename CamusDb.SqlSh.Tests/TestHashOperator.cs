
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

namespace CamusDb.SqlSh.Tests;

// The server reads '#' as XOR, and as the first character of the jsonb operators #>, #>> and #-.
// The shell must send such a statement as typed. A '#' opens a comment only before the first token of
// a statement, where the server cannot run the text anyway.
public sealed class TestHashOperator
{
    private static readonly string[] OperatorStatements =
    [
        "SELECT 5 # -1 AS a",
        "SELECT 5 #3 AS a",
        "SELECT 5 # 3 AS a",
        "SELECT 6 # 3",
        "DELETE FROM t WHERE flags # 4 = 0 AND id = 5",
        "SELECT doc #> '{a,b}', doc #>> '{a}', doc #- '{a}' FROM t",
        "SELECT a\n  # b\nFROM t",
    ];

    // What the shell does with -e: fold the smart quotes, then split the statements.
    private static List<SqlStatement> SplitAsShell(string input)
    {
        SqlScanner scanner = new();
        string folded = SqlKind.NormalizeSmartQuotes(input);

        return scanner.Feed(folded).Concat(scanner.Flush()).ToList();
    }

    // What the shell does with -f and source: fold and feed one line at a time.
    private static async Task<List<SqlStatement>> SplitAsScript(string script)
    {
        List<SqlStatement> statements = [];

        await foreach (SqlStatement statement in SqlScript.ReadStatements(new StringReader(script)))
            statements.Add(statement);

        return statements;
    }

    [TestCaseSource(nameof(OperatorStatements))]
    public void TestOperatorReachesServerAsTyped(string sql)
    {
        List<SqlStatement> statements = SplitAsShell(sql + ";");

        Assert.That(statements, Has.Count.EqualTo(1));
        Assert.That(statements[0].Sql, Is.EqualTo(sql));
    }

    [TestCaseSource(nameof(OperatorStatements))]
    public void TestOperatorReachesServerAsTypedWithoutTerminator(string sql)
    {
        List<SqlStatement> statements = SplitAsShell(sql);

        Assert.That(statements, Has.Count.EqualTo(1));
        Assert.That(statements[0].Sql, Is.EqualTo(sql));
    }

    [TestCaseSource(nameof(OperatorStatements))]
    public async Task TestOperatorReachesServerAsTypedFromScript(string sql)
    {
        List<SqlStatement> statements = await SplitAsScript(sql + ";\n");

        Assert.That(statements, Has.Count.EqualTo(1));
        Assert.That(statements[0].Sql, Is.EqualTo(sql));
    }

    // The prompt waits for the parenthesis after a '#', so it does not run "SELECT 5".
    [Test]
    public void TestPromptReadsPastOperator()
    {
        Assert.That(SqlScript.IsIncomplete("SELECT 5 # (3"), Is.True);
        Assert.That(SqlScript.IsIncomplete("SELECT 5\n  # (3"), Is.True);
        Assert.That(SqlScript.IsIncomplete("SELECT 5 # 3"), Is.False);
        Assert.That(SqlScript.IsIncomplete("# (note"), Is.False);
    }

    [Test]
    public async Task TestCommentLinesBetweenStatements()
    {
        const string script = "# header\n#-----\nSELECT 1;\n  # a note\nSELECT 2; # after the terminator\nSELECT 3\\G # after \\G\n";

        List<SqlStatement> statements = await SplitAsScript(script);

        Assert.That(statements.Select(s => s.Sql), Is.EqualTo(new[] { "SELECT 1", "SELECT 2", "SELECT 3" }));
        Assert.That(statements.Select(s => s.Line), Is.EqualTo(new[] { 3, 5, 6 }));
        Assert.That(statements[2].Vertical, Is.True);
    }

    // The TUI runs the statement at the cursor by its offsets, so they must include the text after '#'.
    [Test]
    public void TestOffsetsIncludeOperator()
    {
        const string text = "SELECT 5 # -1 AS a;\nSELECT 5\n  #3 AS a;";

        List<SqlStatement> statements = SplitAsShell(text);

        Assert.That(statements, Has.Count.EqualTo(2));
        Assert.That(text[statements[0].Start..(statements[0].End + 1)], Is.EqualTo("SELECT 5 # -1 AS a;"));
        Assert.That(text[statements[1].Start..(statements[1].End + 1)], Is.EqualTo("SELECT 5\n  #3 AS a;"));
    }

    [Test]
    public void TestCommentBeforeStatementOnSameLine()
    {
        List<SqlStatement> statements = SplitAsShell("SELECT 1; # note\nSELECT 2 # 3;");

        Assert.That(statements.Select(s => s.Sql), Is.EqualTo(new[] { "SELECT 1", "SELECT 2 # 3" }));
    }

    [Test]
    public void TestSmartQuotesFoldAfterOperator()
    {
        Assert.That(SqlKind.NormalizeSmartQuotes("SELECT 5 # 3, ‘bob’"), Is.EqualTo("SELECT 5 # 3, 'bob'"));
    }

    [Test]
    public void TestSmartQuotesStayInLeadingComment()
    {
        Assert.That(SqlKind.NormalizeSmartQuotes("# it’s a note"), Is.EqualTo("# it’s a note"));
        Assert.That(SqlKind.NormalizeSmartQuotes("SELECT 1; # it’s a note"), Is.EqualTo("SELECT 1; # it’s a note"));
    }

    [Test]
    public void TestSmartQuoteFolderCarriesStatementAcrossLines()
    {
        SqlKind.SmartQuoteFolder folder = new();

        Assert.That(folder.Fold("SELECT 5"), Is.EqualTo("SELECT 5"));
        Assert.That(folder.Fold("  # 3, ‘bob’"), Is.EqualTo("  # 3, 'bob'"));
    }

    [Test]
    public void TestLexerColorsOperatorAsCode()
    {
        SqlLexState state = default;

        List<SqlToken> tokens = SqlLineLexer.Tokenize("SELECT 5 # -1 AS a", ref state);

        Assert.That(tokens.Select(t => t.Kind), Has.No.Member(SqlTokenKind.Comment));
        Assert.That(state.InStatement, Is.True);
    }

    [Test]
    public void TestLexerCarriesStatementAcrossLines()
    {
        SqlLexState state = default;

        SqlLineLexer.Tokenize("SELECT 'a", ref state);
        SqlLineLexer.Tokenize("b' /* x", ref state);
        SqlLineLexer.Tokenize("*/", ref state);

        List<SqlToken> tokens = SqlLineLexer.Tokenize("  # 1", ref state);

        Assert.That(tokens.Select(t => t.Kind), Has.No.Member(SqlTokenKind.Comment));
    }

    [TestCase("# note")]
    [TestCase("  # note")]
    [TestCase("SELECT 1; # note")]
    [TestCase("SELECT 1\\G # note")]
    public void TestLexerColorsLeadingHashAsComment(string line)
    {
        SqlLexState state = default;

        List<SqlToken> tokens = SqlLineLexer.Tokenize(line, ref state);

        Assert.That(tokens[^1].Kind, Is.EqualTo(SqlTokenKind.Comment));
        Assert.That(line[tokens[^1].Start..], Is.EqualTo("# note"));
    }

    // The read-only check reads the same tokens as the lexer, so a word after the '#' counts.
    [TestCase("SELECT flags # 4 FROM t FOR UPDATE")]
    [TestCase("SELECT 1 # 2, nextval('s')")]
    [TestCase("WITH c AS (SELECT 1 # 2) DELETE FROM t")]
    public void TestReadOnlyCheckSeesPastOperator(string sql)
    {
        Assert.That(ReadOnlyGuard.Refusal(sql), Is.Not.Null);
    }

    [Test]
    public void TestReadOnlyCheckAllowsOperatorInRead()
    {
        Assert.That(ReadOnlyGuard.Refusal("SELECT 5 # -1 AS a"), Is.Null);
    }
}
