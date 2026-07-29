
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;

internal static class WorkloadHelpers
{
    /// <summary>A single bound parameter: the <c>@name</c> placeholder, its column type, and its value.</summary>
    internal readonly record struct Param(string Name, ColumnType Type, object? Value)
    {
        /// <summary>Lets a plain <c>("@name", type, value)</c> tuple stand in for a <see cref="Param"/>.</summary>
        public static implicit operator Param((string Name, ColumnType Type, object? Value) tuple)
            => new(tuple.Name, tuple.Type, tuple.Value);
    }

    internal static Param P(string name, ColumnType type, object? value) => new(name, type, value);

    internal static async Task DDL(CamusConnection conn, string sql)
    {
        using CamusCommand cmd = conn.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        await cmd.ExecuteDDLAsync();
    }

    internal static async Task Exec(CamusConnection conn, string sql, CamusTransaction? tx = null, CancellationToken ct = default)
    {
        using CamusCommand cmd = conn.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = tx;
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs a non-query with bound parameters. Values never travel inside the SQL text.</summary>
    internal static async Task ExecWithParams(
        CamusConnection conn, string sql, Param[] parameters, CamusTransaction? tx = null, CancellationToken ct = default)
    {
        using CamusCommand cmd = conn.CreateCamusCommand(sql);
        cmd.CommandTimeout = 60;
        cmd.Transaction = tx;
        foreach (Param p in parameters)
            cmd.Parameters.Add(p.Name, p.Type, p.Value);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    /// <summary>Runs a query with bound parameters and drains the reader, returning the row count.</summary>
    internal static async Task<int> QueryWithParams(
        CamusConnection conn, string sql, Param[] parameters, CancellationToken ct = default)
    {
        using CamusCommand cmd = conn.CreateSelectCommand(sql);
        cmd.CommandTimeout = 30;
        foreach (Param p in parameters)
            cmd.Parameters.Add(p.Name, p.Type, p.Value);

        CamusDataReader reader = await cmd.ExecuteReaderAsync(ct);
        int rows = 0;
        while (await reader.ReadAsync(ct))
            rows++;
        return rows;
    }

    /// <summary>
    /// Runs <paramref name="body"/> over <paramref name="source"/> with at most <paramref name="degree"/>
    /// operations in flight. Init paths use this so seeding isn't a strictly serial round-trip-per-row
    /// walk: the transport multiplexes the concurrent commands over its pooled streams.
    /// </summary>
    internal static Task ForEachAsync<T>(IEnumerable<T> source, int degree, Func<T, Task> body)
    {
        ParallelOptions options = new() { MaxDegreeOfParallelism = Math.Max(1, degree) };
        return Parallel.ForEachAsync(source, options, async (item, _) => await body(item));
    }

    /// <summary>Splits <paramref name="source"/> into fixed-size chunks so each chunk can be one transaction.</summary>
    internal static List<List<T>> Chunk<T>(IEnumerable<T> source, int size)
    {
        List<List<T>> chunks = [];
        List<T> current = new(size);

        foreach (T item in source)
        {
            current.Add(item);
            if (current.Count < size)
                continue;

            chunks.Add(current);
            current = new List<T>(size);
        }

        if (current.Count > 0)
            chunks.Add(current);

        return chunks;
    }

    /// <summary>Server codes meaning "this row is already there" — see CamusDBErrorCodes.</summary>
    private const string DuplicatePrimaryKeyCode = "CADB0402";
    private const string DuplicateUniqueKeyCode = "CADB0300";

    private const int DefaultSeedAttempts = 4;

    /// <summary>
    /// Seed-path wrapper around <see cref="RunBatch"/> that survives a lost commit reply.
    /// <para>
    /// A commit that times out client-side (<see cref="OperationCanceledException"/> out of the gRPC
    /// batcher) leaves the outcome <b>unknown</b>: the request reached the wire, so the server may well
    /// have committed it. Replaying such a transaction from <c>BEGIN</c> is only safe when the work is
    /// idempotent — which is exactly the case for the init paths, whose batches are INSERTs with
    /// deterministic primary keys and disjoint key ranges. If the lost commit did land, the replay comes
    /// back with a duplicate-key error, and because a batch commits all-or-nothing that error <i>is</i>
    /// the confirmation that the batch is already durable.
    /// </para>
    /// <para>
    /// Do not use this for the <c>run</c> workloads: their statements are read-modify-write
    /// (<c>balance = balance - @amount</c>) and a replay would double-apply.
    /// </para>
    /// </summary>
    internal static async Task RunSeedBatch(
        CamusConnection conn,
        CamusTransactionOptions txOptions,
        IEnumerable<(string Sql, Param[] Parameters)> statements,
        int maxAttempts = DefaultSeedAttempts)
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                await RunBatch(conn, txOptions, statements);
                return;
            }
            catch (CamusException ex) when (attempt > 0 && IsDuplicateKey(ex))
            {
                // Only reachable after a replay: an earlier attempt's commit was durable after all, we
                // just never saw the reply. Treat the batch as seeded.
                return;
            }
            catch (OperationCanceledException) when (attempt < maxAttempts - 1)
            {
                // Client-side commit/statement timeout. Back off before replaying; the jitter keeps a
                // fleet of parallel writers from retrying in lockstep.
                await Task.Delay(SeedRetryDelay(attempt));
            }
        }
    }

    private static bool IsDuplicateKey(CamusException ex)
        => ex.Code is DuplicatePrimaryKeyCode or DuplicateUniqueKeyCode;

    // 100 ms x 2^attempt, plus up to 100 ms of jitter.
    private static TimeSpan SeedRetryDelay(int attempt)
        => TimeSpan.FromMilliseconds(100d * (1 << Math.Min(attempt, 4)) + Random.Shared.Next(100));

    /// <summary>
    /// Executes a chunk of parameterized statements inside one transaction, using the workload's
    /// concurrency options (optimistic + serializable by default).
    /// </summary>
    internal static async Task RunBatch(
        CamusConnection conn,
        CamusTransactionOptions txOptions,
        IEnumerable<(string Sql, Param[] Parameters)> statements)
    {
        CamusTransaction tx = await conn.BeginTransactionAsync(txOptions);
        try
        {
            foreach ((string sql, Param[] parameters) in statements)
                await ExecWithParams(conn, sql, parameters, tx);

            await tx.CommitAsync();
        }
        catch
        {
            try { await tx.RollbackAsync(); } catch { }
            throw;
        }
    }
}
