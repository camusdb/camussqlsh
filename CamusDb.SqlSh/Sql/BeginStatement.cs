/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

// Reads the transaction modes of BEGIN and START TRANSACTION. The shell does not send the BEGIN text to
// the server: it calls BeginTransactionAsync, so the modes must go in CamusTransactionOptions. The server
// grammar takes only BEGIN and START TRANSACTION, with no modes.
//
// The syntax is the PostgreSQL one:
//
//     BEGIN [TRANSACTION | WORK] [mode [, ...]]
//     START TRANSACTION [mode [, ...]]
//
//     mode: ISOLATION LEVEL { SERIALIZABLE | READ COMMITTED }
//         | READ ONLY
//         | READ WRITE
//
// The comma between two modes is optional, as in PostgreSQL. A mode that CamusDB does not have, such as
// REPEATABLE READ or DEFERRABLE, gives an error. Before, the shell did not read the modes, so BEGIN READ
// ONLY opened a read-write transaction and the server did not refuse its writes.
internal static class BeginStatement
{
    // The options for the modes of the statement. A mode that the statement does not name stays null,
    // so the connection default applies. Throws TransactionModeException for a mode it cannot read.
    internal static CamusTransactionOptions Parse(string sql)
    {
        List<string> words = Words(sql);
        int i = 0;

        if (Is(words, i, "begin"))
        {
            i++;

            if (Is(words, i, "transaction") || Is(words, i, "work"))
                i++;
        }
        else if (Is(words, i, "start") && Is(words, i + 1, "transaction"))
        {
            i += 2;
        }
        else
        {
            throw new TransactionModeException("Expected BEGIN or START TRANSACTION.");
        }

        CamusIsolationLevel? isolation = null;
        CamusTransactionMode? mode = null;

        bool firstMode = true;

        while (i < words.Count)
        {
            // One comma can come between two modes.
            if (!firstMode && words[i] == ",")
            {
                i++;

                if (i == words.Count)
                    throw new TransactionModeException("Expected a transaction mode after the comma.");
            }

            firstMode = false;

            if (Is(words, i, "isolation") && Is(words, i + 1, "level"))
            {
                if (isolation is not null)
                    throw new TransactionModeException("The isolation level is given more than one time.");

                i += 2;

                if (Is(words, i, "serializable"))
                {
                    isolation = CamusIsolationLevel.Serializable;
                    i++;
                }
                else if (Is(words, i, "read") && Is(words, i + 1, "committed"))
                {
                    isolation = CamusIsolationLevel.ReadCommitted;
                    i += 2;
                }
                else
                {
                    throw new TransactionModeException(
                        $"CamusDB has the isolation levels SERIALIZABLE and READ COMMITTED, not {Describe(words, i, 2)}.");
                }
            }
            else if (Is(words, i, "read") && (Is(words, i + 1, "only") || Is(words, i + 1, "write")))
            {
                if (mode is not null)
                    throw new TransactionModeException("The access mode is given more than one time.");

                mode = Is(words, i + 1, "only") ? CamusTransactionMode.ReadOnly : CamusTransactionMode.ReadWrite;
                i += 2;
            }
            else
            {
                throw new TransactionModeException(
                    $"Unknown transaction mode {Describe(words, i, 3)}. The modes are ISOLATION LEVEL {{SERIALIZABLE | READ COMMITTED}}, READ ONLY and READ WRITE.");
            }
        }

        return new CamusTransactionOptions { IsolationLevel = isolation, Mode = mode };
    }

    // A short text for the messages, for example "READ ONLY, ISOLATION LEVEL READ COMMITTED".
    internal static string? DescribeModes(CamusTransactionOptions options)
    {
        List<string> parts = [];

        if (options.Mode is { } mode)
            parts.Add(mode == CamusTransactionMode.ReadOnly ? "READ ONLY" : "READ WRITE");

        if (options.IsolationLevel is { } isolation)
            parts.Add(isolation == CamusIsolationLevel.Serializable ? "ISOLATION LEVEL SERIALIZABLE" : "ISOLATION LEVEL READ COMMITTED");

        return parts.Count == 0 ? null : string.Join(", ", parts);
    }

    private static bool Is(List<string> words, int i, string word) =>
        i < words.Count && string.Equals(words[i], word, StringComparison.OrdinalIgnoreCase);

    // Up to `count` words from position i, for a message. "the end of the statement" when there are none.
    private static string Describe(List<string> words, int i, int count)
    {
        if (i >= words.Count)
            return "the end of the statement";

        string text = string.Join(' ', words.Skip(i).Take(count).TakeWhile(w => w != ","));

        return "'" + (text.Length == 0 ? words[i] : text).ToUpperInvariant() + "'";
    }

    // The words and the commas of the statement, without comments, white space and the final ';' or \G.
    // Each other character is kept as its own item, so an unknown mode gives an error.
    private static List<string> Words(string sql)
    {
        List<string> words = [];
        SqlLexState state = default;

        foreach (string rawLine in sql.Split('\n'))
        {
            string line = rawLine.TrimEnd('\r');

            foreach (SqlToken token in SqlLineLexer.Tokenize(line, ref state))
            {
                if (token.Kind == SqlTokenKind.Comment)
                    continue;

                string text = line.Substring(token.Start, token.Length);

                if (string.IsNullOrWhiteSpace(text) || text is ";" or "\\G")
                    continue;

                words.Add(text);
            }
        }

        return words;
    }
}

// A BEGIN or START TRANSACTION with a mode that the shell cannot read. The shell did not open a
// transaction.
internal sealed class TransactionModeException(string message) : Exception(message);
