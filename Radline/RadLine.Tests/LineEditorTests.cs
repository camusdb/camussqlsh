using System;
using System.Threading;
using System.Threading.Tasks;
using Shouldly;
using Spectre.Console.Testing;
using Xunit;

namespace RadLine.Tests
{
    public sealed class LineEditorTests
    {
        [Fact]
        public async Task Should_Return_Entered_Text_When_Pressing_Enter()
        {
            // Given
            var editor = new LineEditor(
                new TestConsole(),
                new TestInputSource()
                    .Push("Patrik")
                    .PushEnter());

            // When
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            result.ShouldBe("Patrik");
        }

        [Fact]
        public async Task Should_Add_New_Line_When_Pressing_Shift_And_Enter()
        {
            // Given
            var editor = new LineEditor(
                new TestConsole(),
                new TestInputSource()
                    .Push("Patrik")
                    .PushNewLine()
                    .Push("Svensson")
                    .PushEnter())
            {
                MultiLine = true,
            };

            // When
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            result.ShouldBe($"Patrik{Environment.NewLine}Svensson");
        }

        [Fact]
        public async Task Should_Move_Between_Lines_When_Pressing_Up_And_Down_In_Multiline_Input()
        {
            // Given
            var editor = new LineEditor(
                new TestConsole(),
                new TestInputSource()
                    .Push("first")
                    .PushNewLine()
                    .Push("second")
                    .Push(ConsoleKey.UpArrow)
                    .Push(" line")
                    .Push(ConsoleKey.DownArrow)
                    .Push(" line")
                    .PushEnter())
            {
                MultiLine = true,
            };

            // When
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            result.ShouldBe($"first line{Environment.NewLine}second line");
        }

        [Fact]
        public async Task Should_Move_To_Previous_History_When_Pressing_Up_On_First_Line_In_Multiline_Input()
        {
            // Given
            var editor = new LineEditor(
                new TestConsole(),
                new TestInputSource()
                    .Push("first")
                    .PushNewLine()
                    .Push("second")
                    .Push(ConsoleKey.UpArrow)
                    .Push(ConsoleKey.UpArrow)
                    .PushEnter())
            {
                MultiLine = true,
            };

            editor.History.Add("history item");

            // When
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            result.ShouldBe("history item");
        }

        [Fact]
        public async Task Should_Move_To_Previous_Item_In_History()
        {
            // Given
            var editor = new LineEditor(
                new TestConsole(),
                new TestInputSource()
                    .Push(ConsoleKey.UpArrow)
                    .Push(ConsoleKey.UpArrow)
                    .Push(ConsoleKey.UpArrow)
                    .PushEnter());

            editor.History.Add("Foo");
            editor.History.Add("Bar");
            editor.History.Add("Baz");

            // When
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            result.ShouldBe("Foo");
        }

        [Fact]
        public async Task Should_Move_To_Next_Item_In_History()
        {
            // Given
            var editor = new LineEditor(
                new TestConsole(),
                new TestInputSource()
                    .Push(ConsoleKey.UpArrow)
                    .Push(ConsoleKey.UpArrow)
                    .Push(ConsoleKey.UpArrow)
                    .Push(ConsoleKey.DownArrow)
                    .Push(ConsoleKey.DownArrow)
                    .PushEnter());

            editor.History.Add("Foo");
            editor.History.Add("Bar");
            editor.History.Add("Baz");

            // When
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            result.ShouldBe("Baz");
        }

        [Fact]
        public async Task Should_Add_Entered_Text_To_History()
        {
            // Given
            var input = new TestInputSource();
            var editor = new LineEditor(new TestConsole(), input);
            input.Push("Patrik").PushEnter();
            await editor.ReadLine(CancellationToken.None);

            // When
            input.Push(ConsoleKey.UpArrow).PushEnter();
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            result.ShouldBe("Patrik");
        }

        [Fact]
        public async Task Should_Not_Add_Entered_Text_To_History_If_Its_The_Same_As_The_Last_Entry()
        {
            // Given
            var input = new TestInputSource();
            var editor = new LineEditor(new TestConsole(), input);
            input.Push("Patrik").PushNewLine().Push("Svensson").PushEnter();
            await editor.ReadLine(CancellationToken.None);

            // When
            input.Push("Patrik").PushNewLine().Push("Svensson").PushEnter();
            var result = await editor.ReadLine(CancellationToken.None);

            // Then
            editor.History.Count.ShouldBe(1);
        }
    }
}
