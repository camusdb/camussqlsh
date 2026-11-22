/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using NUnit.Framework;

namespace CamusDb.SqlSh.Tests;

// SHOW PARTITIONS (server 0e09fa99) and the SHOW INDEXES row of an expression index (server 52a00f7b).
public sealed class TestServerFeatureSupport
{
    // SHOW PARTITIONS opens no database, so it goes on the endpoint connection and needs no `use`.
    [TestCase("SHOW PARTITIONS", true)]
    [TestCase("show  partitions;", true)]
    [TestCase("show partitionsx", false)]
    [TestCase("select * from partitions", false)]
    public void TestShowPartitionsIsSystemLevel(string sql, bool expected)
    {
        Assert.That(SqlKind.IsSystemLevelQuery(sql), Is.EqualTo(expected));
    }

    // A plain index keeps the join of Columns and Directions, with no quotes.
    [TestCase("year", "DESC", "`year` DESC", "year DESC")]
    [TestCase("a,b", "ASC,DESC", "`a`, `b` DESC", "a, b DESC")]
    [TestCase("tags", "", "`tags`", "tags")]
    [TestCase("a,b", "ASC,DESC", null, "a, b DESC")]
    public void TestPlainIndexKeyList(string columns, string directions, string? key, string expected)
    {
        Assert.That(DescribeCommand.IndexKeyList(columns, directions, key), Is.EqualTo(expected));
    }

    // An expression part can hold commas, so the shell shows Key. Split on commas, the DESC of the
    // first case went after a part of the expression.
    [TestCase(
        "(coalesce(`a`, `b`)),c",
        "DESC,ASC",
        "(coalesce(`a`, `b`)) DESC, `c`")]
    [TestCase(
        "score,(jsonb_object_field_text(`doc`, 'email'))",
        "DESC,ASC",
        "`score` DESC, (jsonb_object_field_text(`doc`, 'email'))")]
    [TestCase(
        "(lower(`email`))",
        "DESC",
        "(lower(`email`)) DESC")]
    public void TestExpressionIndexKeyList(string columns, string directions, string key)
    {
        Assert.That(DescribeCommand.IndexKeyList(columns, directions, key), Is.EqualTo(key));
    }
}
