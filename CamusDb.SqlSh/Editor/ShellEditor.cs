
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

using CamusDB.Client;
using RadLine;
using Spectre.Console;
using static SqlKind;

// The rich line editor with its highlighter and its completion. The TUI colors its editor pane with
// the same highlighter, so the three are built and kept together.
internal sealed class ShellEditor
{
    private static readonly string[] Regexes = [
        @"(?<number>\b\d+(\.\d+)?\b)",
        @"(?<singlequote>'(?:\\'|[^'])*')",
        @"(?<escapedquote>`(?:\\`|[^`])*`)",
        "(?<doublequote>\"(?:\\\\\"|[^\"])*\")"
    ];

    private static readonly string[] CommentRegexes = [
        @"(?<linecomment>--.*$)",
        @"(?<blockcomment>/\*[\s\S]*?(\*/|$))"
    ];

    // The color of a function name, kept beside the highlighter so the functions of the loaded C#
    // extensions, which are known only after the connection opens, are colored like the built-ins.
    private static readonly Style FunctionStyle = new(foreground: Color.Lime);

    private ShellEditor(LineEditor editor, WordHighlighter highlighter, SqlCompletion completion)
    {
        Editor = editor;
        Highlighter = highlighter;
        Completion = completion;
    }

    public LineEditor Editor { get; }

    public WordHighlighter Highlighter { get; }

    public SqlCompletion Completion { get; }

    internal static ShellEditor Create(StatementHistory? history)
    {
        WordHighlighter worldHighlighter = new();

        Style keywordStyle = new(foreground: Color.Blue);
        Style commandStyle = new(foreground: Color.LightSkyBlue1);
        Style constantsStyle = new(foreground: Color.LightPink3);
        Style commentStyle = new(foreground: Color.Grey58);

        foreach (string keyword in SqlVocabulary.Keywords)
            worldHighlighter.AddWord(keyword, keywordStyle);

        foreach (string func in SqlVocabulary.Functions)
            worldHighlighter.AddWord(func, FunctionStyle);

        foreach (string command in SqlVocabulary.Commands)
            worldHighlighter.AddWord(command, commandStyle);

        foreach (string command in SqlVocabulary.SlashCommands)
            worldHighlighter.AddWord(command, commandStyle);

        foreach (string constant in SqlVocabulary.Constants)
            worldHighlighter.AddWord(constant, constantsStyle);

        foreach (string commentRegex in CommentRegexes)
            worldHighlighter.AddRegex(commentRegex, commentStyle);

        foreach (string regex in Regexes)
            worldHighlighter.AddRegex(regex, constantsStyle);

        SqlCompletion sqlCompletion = new([
            .. SqlVocabulary.Keywords,
            .. SqlVocabulary.Functions,
            .. SqlVocabulary.Commands,
            .. SqlVocabulary.Constants]);

        LineEditor editor = new()
        {
            MultiLine = true,
            Text = "",
            Prompt = new MyLineNumberPrompt(new Style(foreground: Color.PaleTurquoise1)),
            Completion = sqlCompletion,
            Highlighter = worldHighlighter
        };

        // Enter submits the statement. To insert a new line without submitting, RadLine binds
        // Shift+Enter by default, but most terminals send a plain Enter for that combo and never
        // deliver the Shift modifier. Add alternatives that terminals can actually distinguish:
        //   - Alt/Option+Enter: natural, works in iTerm2 and any terminal with Option-as-Meta.
        //   - Ctrl+O: a real control character (0x0F), so it works in every terminal as a fallback.
        editor.KeyBindings.Add<NewLineCommand>(ConsoleKey.Enter, ConsoleModifiers.Alt);
        editor.KeyBindings.Add<NewLineCommand>(ConsoleKey.O, ConsoleModifiers.Control);

        // A pasted newline arrives as a LineFeed (0x0A), which .NET reports as Enter+Control
        // (real Enter is a CarriageReturn with no modifiers). Binding Enter+Control to a new line
        // keeps multi-line pastes intact instead of silently collapsing them onto one line.
        editor.KeyBindings.Add<NewLineCommand>(ConsoleKey.Enter, ConsoleModifiers.Control);

        if (history is not null)
        {
            foreach (string item in history.Entries)
                editor.History.Add(item);
        }

        return new ShellEditor(editor, worldHighlighter, sqlCompletion);
    }

    // Loads the names that only the server knows: the tables of the current database, the
    // configuration keys and the functions of the loaded C# extensions.
    internal async Task LoadServerVocabularyAsync(CamusConnection connection, string connectionString)
    {
        if (HasDatabase(connectionString))
            await Completion.RefreshTablesAsync(connection);

        // Configuration keys come over the endpoint connection rather than the database one, because the
        // statements they complete need no database either — the keys are offered even in a session that
        // never runs `use`. Best-effort: completion falling back to the static vocabulary is not worth
        // keeping the user from a prompt over.
        try
        {
            CamusConnection settingsConnection = await ConnectionHelper.OpenAsync(GetEndpointConnectionString(connectionString));
            await Completion.RefreshSettingsAsync(settingsConnection);

            // The C# extensions this node loaded. SHOW EXTENSIONS needs a superuser, so for any other
            // user the load fails and the extension names are neither completed nor colored.
            await Completion.RefreshExtensionsAsync(settingsConnection);

            foreach (string function in Completion.ExtensionFunctions)
                Highlighter.AddWord(function, FunctionStyle);
        }
        catch (Exception)
        {
            // Ignored: the shell is already connected, and this cache is a convenience.
        }
    }
}
