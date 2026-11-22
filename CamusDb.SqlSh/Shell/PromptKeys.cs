/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using RadLine;
using System.Collections.Concurrent;

// The keys of the prompt. The line editor reads its keys through this class, so that ESC and Ctrl+C
// can cancel a statement while it runs.
//
// While a statement runs, a CancelWatch reads each key that arrives. A lone ESC or a Ctrl+C cancels the
// statement. The watch holds each other key, and the line editor reads the held keys first at the next
// prompt. Thus text typed ahead, or the rest of a paste, is not lost.
internal sealed class PromptKeys : IInputSource
{
    private readonly IKeyboard keyboard;

    private readonly ConcurrentQueue<ConsoleKeyInfo> held = new();

    public PromptKeys() : this(new ConsoleKeyboard())
    {
    }

    internal PromptKeys(IKeyboard keyboard)
    {
        this.keyboard = keyboard;
    }

    public bool ByPassProcessing => false;

    public bool IsKeyAvailable() => !held.IsEmpty || keyboard.KeyAvailable;

    public ConsoleKeyInfo ReadKey() => held.TryDequeue(out ConsoleKeyInfo key) ? key : keyboard.ReadKey();

    // Starts to watch the keys for one statement. Dispose the watch before the line editor reads again,
    // so that only one reader takes keys from the terminal.
    public CancelWatch Watch() => new(keyboard, held);
}

// The terminal's keyboard, behind an interface so that a test can type keys.
internal interface IKeyboard
{
    bool KeyAvailable { get; }

    ConsoleKeyInfo ReadKey();
}

internal sealed class ConsoleKeyboard : IKeyboard
{
    public bool KeyAvailable => Console.KeyAvailable;

    public ConsoleKeyInfo ReadKey() => Console.ReadKey(intercept: true);
}
