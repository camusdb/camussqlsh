/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using System.Collections.Concurrent;
using NUnit.Framework;

namespace CamusDb.SqlSh.Tests;

// While a statement runs at the prompt, a CancelWatch reads the keys. A lone ESC or a Ctrl+C cancels
// the statement. Each other key stays for the line editor, in the order it arrived.
public sealed class TestCancelKeys
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(5);

    private static readonly ConsoleKeyInfo Escape = new('\u001b', ConsoleKey.Escape, false, false, false);

    // With TreatControlCAsInput, .NET reports Ctrl+C as the key C with Control, and the char 0x03.
    private static readonly ConsoleKeyInfo ControlC = new('\u0003', ConsoleKey.C, false, false, true);

    private static ConsoleKeyInfo Char(char c) => new(c, ConsoleKey.NoName, false, false, false);

    [Test]
    public async Task TestLoneEscapeCancels()
    {
        FakeKeyboard keyboard = new(Escape);
        PromptKeys keys = new(keyboard);

        CancelWatch watch = keys.Watch();

        Assert.That(watch.Token.WaitHandle.WaitOne(Timeout), Is.True);
        Assert.That(watch.CancelKey, Is.EqualTo("ESC"));

        await watch.DisposeAsync();

        Assert.That(keys.IsKeyAvailable(), Is.False);
    }

    [Test]
    public async Task TestControlCCancels()
    {
        FakeKeyboard keyboard = new(ControlC);
        PromptKeys keys = new(keyboard);

        CancelWatch watch = keys.Watch();

        Assert.That(watch.Token.WaitHandle.WaitOne(Timeout), Is.True);
        Assert.That(watch.CancelKey, Is.EqualTo("Ctrl+C"));

        await watch.DisposeAsync();

        // The Ctrl+C does not reach the next prompt, where it ends the shell on an empty line.
        Assert.That(keys.IsKeyAvailable(), Is.False);
    }

    // An ESC with more input behind it starts a sequence, such as Alt+b. It is not a cancel.
    [Test]
    public async Task TestEscapeSequenceIsHeld()
    {
        FakeKeyboard keyboard = new(Escape, Char('b'));
        PromptKeys keys = new(keyboard);

        CancelWatch watch = keys.Watch();
        await keyboard.WaitUntilEmpty();
        await watch.DisposeAsync();

        Assert.That(watch.Cancelled, Is.False);
        Assert.That(ReadAll(keys), Is.EqualTo(new[] { Escape, Char('b') }));
    }

    // Text typed ahead, or the rest of a paste, reaches the line editor in the order it arrived.
    [Test]
    public async Task TestOtherKeysAreHeldInOrder()
    {
        FakeKeyboard keyboard = new(Char('s'), Char('e'), Char('l'));
        PromptKeys keys = new(keyboard);

        CancelWatch watch = keys.Watch();
        await keyboard.WaitUntilEmpty();
        await watch.DisposeAsync();

        Assert.That(watch.Cancelled, Is.False);

        // A key typed after the watch stops comes after the held keys.
        keyboard.Type(Char('e'));

        Assert.That(ReadAll(keys), Is.EqualTo(new[] { Char('s'), Char('e'), Char('l'), Char('e') }));
    }

    // The keys around a cancel stay for the editor. Only the cancel key goes.
    [Test]
    public async Task TestKeysAroundCancelAreHeld()
    {
        FakeKeyboard keyboard = new(Char('a'), ControlC, Char('b'));
        PromptKeys keys = new(keyboard);

        CancelWatch watch = keys.Watch();
        await keyboard.WaitUntilEmpty();
        await watch.DisposeAsync();

        Assert.That(watch.Cancelled, Is.True);
        Assert.That(ReadAll(keys), Is.EqualTo(new[] { Char('a'), Char('b') }));
    }

    // A second press while the statement stops keeps the first key as the cause.
    [Test]
    public async Task TestSecondCancelKeyKeepsTheFirst()
    {
        FakeKeyboard keyboard = new(ControlC, Escape);
        PromptKeys keys = new(keyboard);

        CancelWatch watch = keys.Watch();
        await keyboard.WaitUntilEmpty();
        await watch.DisposeAsync();

        Assert.That(watch.CancelKey, Is.EqualTo("Ctrl+C"));
        Assert.That(keys.IsKeyAvailable(), Is.False);
    }

    // After the watch stops, it reads no more keys: the line editor is the only reader again.
    [Test]
    public async Task TestDisposedWatchReadsNoKeys()
    {
        FakeKeyboard keyboard = new();
        PromptKeys keys = new(keyboard);

        CancelWatch watch = keys.Watch();
        await watch.DisposeAsync();

        keyboard.Type(ControlC);
        await Task.Delay(100);

        Assert.That(watch.Cancelled, Is.False);
        Assert.That(keyboard.Count, Is.EqualTo(1));
    }

    private static List<ConsoleKeyInfo> ReadAll(PromptKeys keys)
    {
        List<ConsoleKeyInfo> read = [];

        while (keys.IsKeyAvailable())
            read.Add(keys.ReadKey());

        return read;
    }

    private sealed class FakeKeyboard(params ConsoleKeyInfo[] typed) : IKeyboard
    {
        private readonly ConcurrentQueue<ConsoleKeyInfo> keys = new(typed);

        public int Count => keys.Count;

        public bool KeyAvailable => !keys.IsEmpty;

        public ConsoleKeyInfo ReadKey() => keys.TryDequeue(out ConsoleKeyInfo key)
            ? key
            : throw new InvalidOperationException("ReadKey with no key available");

        public void Type(ConsoleKeyInfo key) => keys.Enqueue(key);

        // The watch takes a key off the queue before it decides what to do with it. The wait after the
        // queue is empty is longer than one poll and one sequence wait, so the last key has its place.
        public async Task WaitUntilEmpty()
        {
            DateTime end = DateTime.UtcNow + Timeout;

            while (!keys.IsEmpty && DateTime.UtcNow < end)
                await Task.Delay(5);

            await Task.Delay(150);
        }
    }
}
