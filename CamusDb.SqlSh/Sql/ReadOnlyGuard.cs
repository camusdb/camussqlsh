
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

// Refuses a statement that can change the database or the server, when the shell runs with
// --read-only.
//
// The check runs in the shell, before the statement goes to the server, and the server does not know
// about it. It stops a mistake. It is not a security boundary: a user who must not write needs a server
// account without write privileges.
//
// The check is an allowlist. A statement runs only when its first word starts a statement that reads:
// SELECT, a WITH query, EXPLAIN, SHOW, DESCRIBE, the transaction statements and SET TRANSACTION. The
// check refuses each other statement, also one that a newer server adds. It also refuses a query that
// writes in a way that its first word does not show:
// - a WITH clause in front of INSERT, UPSERT, UPDATE or DELETE;
// - a locking read (FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR KEY SHARE), which takes row locks.
//   PostgreSQL refuses it in a read-only transaction too. A plain EXPLAIN takes no lock, so it can show
//   the plan of a locking read;
// - a call of nextval or setval, which moves a sequence. Plain EXPLAIN can run a subquery while it
//   plans, so the check applies to each form of EXPLAIN.
//
// The words come from the tokens of the editor's lexer, so a word in a string, in a comment or in a
// `quoted identifier` does not count. The check does not read the body of a C# function or extension.
internal static class ReadOnlyGuard
{
    private readonly record struct Lexeme(string Text, SqlTokenKind Kind)
    {
        public bool IsWord(string word) =>
            Kind == SqlTokenKind.Word && string.Equals(Text, word, StringComparison.OrdinalIgnoreCase);
    }

    // Throws ReadOnlyRefusedException when the statement cannot run under --read-only.
    internal static void Check(string sql)
    {
        if (Refusal(sql) is { } reason)
            throw new ReadOnlyRefusedException(reason);
    }

    // The reason that the statement cannot run under --read-only, or null when it only reads.
    internal static string? Refusal(string sql)
    {
        List<Lexeme> tokens = CodeTokens(sql);

        // A set operation can start with the parenthesis of its first operand: (SELECT …) UNION (SELECT …).
        int first = tokens.FindIndex(t => t.Text != "(");

        // Only comments and white space: the server gets nothing that it can run.
        if (first < 0)
            return null;

        Lexeme head = tokens[first];

        if (head.Kind != SqlTokenKind.Word)
            return "The read-only check does not know this statement.";

        string verb = head.Text.ToLowerInvariant();

        return verb switch
        {
            "select" or "with" => QueryRefusal(tokens, first, checkLocks: true),
            "explain" => QueryRefusal(tokens, first, checkLocks: ExplainRuns(tokens, first)),
            "show" or "desc" or "describe" or "use" => null,
            "begin" or "start" or "commit" or "rollback" => null,
            "set" when first + 1 < tokens.Count && tokens[first + 1].IsWord("transaction") => null,
            "analyze" => "ANALYZE stores new statistics for the table.",
            _ => $"{verb.ToUpperInvariant()} is not a statement that only reads.",
        };
    }

    // The reason that `backup <arguments>` cannot run under --read-only, or null. A backup does not change
    // the data, but it writes to the backup folder of the server, and gc deletes from it. The usage, list,
    // chain and gc preview only read.
    internal static string? BackupRefusal(string arguments)
    {
        string[] parts = arguments.Split(' ', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        if (parts.Length == 0)
            return null;

        string subcommand = parts[0].ToLowerInvariant();

        if (subcommand is "help" or "list" or "chain")
            return null;

        if (subcommand == "gc" && parts.Length == 2 && parts[1].ToLowerInvariant() is "preview" or "--dry-run")
            return null;

        return subcommand == "gc"
            ? "backup gc deletes backups from the server. Use backup gc preview to see what it would delete."
            : $"backup {subcommand} writes a backup to the server.";
    }

    // Looks through a SELECT, a WITH query or an EXPLAIN for a write that the first word does not show.
    private static string? QueryRefusal(List<Lexeme> tokens, int first, bool checkLocks)
    {
        for (int i = first; i < tokens.Count; i++)
        {
            Lexeme token = tokens[i];

            if (token.Kind != SqlTokenKind.Word)
                continue;

            switch (token.Text.ToLowerInvariant())
            {
                // FOR UPDATE and FOR NO KEY UPDATE lock rows. Any other UPDATE is the write after a WITH clause.
                case "update" when IsLockClause(tokens, i):
                case "share" when IsLockClause(tokens, i):
                    if (checkLocks)
                        return $"A locking read (FOR … {token.Text.ToUpperInvariant()}) takes row locks.";
                    break;

                case "insert" or "upsert" or "update" or "delete":
                    return $"{token.Text.ToUpperInvariant()} writes rows.";

                case "nextval" or "setval" when i + 1 < tokens.Count && tokens[i + 1].Text == "(":
                    return $"{token.Text.ToLowerInvariant()}() moves a sequence.";
            }
        }

        return null;
    }

    // UPDATE or SHARE right after FOR or KEY: FOR UPDATE, FOR NO KEY UPDATE, FOR SHARE, FOR KEY SHARE.
    private static bool IsLockClause(List<Lexeme> tokens, int i) =>
        i > 0 && (tokens[i - 1].IsWord("for") || tokens[i - 1].IsWord("key"));

    // True for EXPLAIN (ANALYZE), which runs its statement. The options come before the first SELECT or WITH.
    private static bool ExplainRuns(List<Lexeme> tokens, int first)
    {
        for (int i = first + 1; i < tokens.Count; i++)
        {
            if (tokens[i].IsWord("select") || tokens[i].IsWord("with"))
                return false;

            if (tokens[i].IsWord("analyze"))
                return true;
        }

        return false;
    }

    // The tokens of the statement without comments and white space. The lexer reads one line at a time,
    // and its state carries a string or a comment over to the next line.
    private static List<Lexeme> CodeTokens(string sql)
    {
        List<Lexeme> tokens = [];
        SqlLexState state = default;

        foreach (string rawLine in sql.Split('\n'))
        {
            string line = rawLine.TrimEnd('\r');

            foreach (SqlToken token in SqlLineLexer.Tokenize(line, ref state))
            {
                if (token.Kind == SqlTokenKind.Comment)
                    continue;

                string text = line.Substring(token.Start, token.Length);

                if (token.Kind == SqlTokenKind.Plain && string.IsNullOrWhiteSpace(text))
                    continue;

                tokens.Add(new Lexeme(text, token.Kind));
            }
        }

        return tokens;
    }
}

// A statement that the shell does not send, because it runs with --read-only. The server did not see the
// statement, so an open transaction is still open.
internal sealed class ReadOnlyRefusedException(string reason)
    : Exception($"Refused by --read-only: {reason}")
{
    public string Reason { get; } = reason;
}
