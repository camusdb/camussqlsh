/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;

// Watches the keyboard while one statement runs at the prompt. A lone ESC or a Ctrl+C cancels Token.
// Each other key goes to `held`, in the order it arrived, for the line editor to read next.
//
// The prompt sets Console.TreatControlCAsInput, so Ctrl+C arrives as a key and not as a signal. Before
// the watch, no code read the keys while a statement ran. The Ctrl+C stayed in the input, and the line
// editor read it at the next prompt: on an empty prompt, that ended the shell.
internal sealed class CancelWatch : IAsyncDisposable
{
    // How often the watch looks for a key. A cancel needs no faster answer than this.
    private const int PollMilliseconds = 10;

    // A terminal writes an escape sequence in one burst, but the burst can arrive in two reads. A
    // short wait tells a sequence that is still on its way from a lone ESC (the same wait as in
    // TerminalInput of the TUI).
    private const int SequenceWaitMilliseconds = 25;

    private readonly IKeyboard keyboard;

    private readonly ConcurrentQueue<ConsoleKeyInfo> held;

    private readonly CancellationTokenSource cancel = new();

    private readonly Task loop;

    private volatile bool stop;

    internal CancelWatch(IKeyboard keyboard, ConcurrentQueue<ConsoleKeyInfo> held)
    {
        this.keyboard = keyboard;
        this.held = held;

        // The loop sleeps between polls, so it gets its own thread and not one of the pool.
        loop = Task.Factory.StartNew(Run, CancellationToken.None, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    public CancellationToken Token => cancel.Token;

    public bool Cancelled => cancel.IsCancellationRequested;

    // The key that cancelled the statement: "ESC" or "Ctrl+C". Null until a cancel.
    public string? CancelKey { get; private set; }

    public async ValueTask DisposeAsync()
    {
        stop = true;

        // The line editor must not read keys while the loop still reads them.
        await loop.ConfigureAwait(false);

        cancel.Dispose();
    }

    internal static bool IsControlC(ConsoleKeyInfo key) =>
        key.KeyChar == '\u0003' || (key.Key == ConsoleKey.C && (key.Modifiers & ConsoleModifiers.Control) != 0);

    private void Run()
    {
        while (!stop)
        {
            if (!keyboard.KeyAvailable)
            {
                Thread.Sleep(PollMilliseconds);
                continue;
            }

            ConsoleKeyInfo key = keyboard.ReadKey();

            if (IsControlC(key))
            {
                Cancel("Ctrl+C");
                continue;
            }

            // An ESC with more input behind it starts a sequence, such as Alt+key. The editor gets it
            // and the rest of the sequence as it gets them without the watch.
            if (key.Key == ConsoleKey.Escape && !MoreInput())
            {
                Cancel("ESC");
                continue;
            }

            held.Enqueue(key);
        }
    }

    private void Cancel(string key)
    {
        // A second press while the statement stops changes nothing.
        if (cancel.IsCancellationRequested)
            return;

        CancelKey = key;

        try
        {
            cancel.Cancel();
        }
        catch (AggregateException)
        {
            // A callback that the client registered on the token failed. The token is cancelled all
            // the same, and the statement reports its own error. The loop must go on, or the keys
            // stop.
        }
    }

    private bool MoreInput()
    {
        for (int i = 0; i < SequenceWaitMilliseconds; i++)
        {
            if (keyboard.KeyAvailable)
                return true;

            Thread.Sleep(1);
        }

        return keyboard.KeyAvailable;
    }
}

// A statement that ESC or Ctrl+C cancelled at the prompt. `AutocommitWrite` is true for a statement
// that can write outside a transaction: the server commits such a write by itself, so a cancel that
// arrives late does not undo it.
internal sealed class StatementCancelledException(string key, TimeSpan elapsed, bool autocommitWrite, Exception inner)
    : Exception($"Cancelled with {key}", inner)
{
    public string Key { get; } = key;

    public TimeSpan Elapsed { get; } = elapsed;

    public bool AutocommitWrite { get; } = autocommitWrite;
}
