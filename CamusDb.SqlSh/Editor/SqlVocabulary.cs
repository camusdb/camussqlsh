
/**
 * This file is part of CamusDB
 *
 * For the full copyright and license information, please view the LICENSE.txt
 * file that was distributed with this source code.
 */

// The words the line editor colors and completes. The keywords and the functions follow the server's
// parser and function registry; the commands are the shell's own.
internal static class SqlVocabulary
{
    internal static readonly string[] Keywords = [
        "select",
        "update",
        "from",
        "where",
        "order",
        "by",
        "asc",
        "desc",
        "describe",
        "database",
        "table",
        "set",
        "create",
        "if",
        "exists",
        "default",
        "primary",
        "key",
        "index",
        "indexes",
        "constraint",
        "check",
        "limit",
        "insert",
        "into",
        "values",
        // INSERT … RETURNING <list>: a reserved word to the parser, so a column or a table called
        // returning needs backticks.
        "returning",
        // INSERT … OVERRIDING SYSTEM VALUE keeps a supplied value for a GENERATED ALWAYS column, and
        // OVERRIDING USER VALUE discards the supplied value of every identity column. OVERRIDING and
        // VALUE are plain identifiers to the parser and stay usable as names. SYSTEM and USER are
        // listed below for other statements.
        "overriding",
        "value",
        // INSERT … ON CONFLICT [(cols) | ON CONSTRAINT <name>] DO NOTHING | DO UPDATE SET c = excluded.c,
        // and UPSERT INTO <table>, the update form with the primary key as the target. All five are
        // plain identifiers to the parser and stay usable as names. ON and CONSTRAINT are listed
        // for other statements.
        "conflict",
        "do",
        "nothing",
        "excluded",
        "upsert",
        "delete",
        "alter",
        "rename",
        "column",
        "drop",
        // TRUNCATE [TABLE] <table>: empties a base table by moving its contents generation. The
        // TABLE noise word is optional, so the verb is what the editor colors.
        "truncate",
        "force",
        "relink",
        "orphan",
        "include",
        // SHOW CREATE TABLE <table> WITHOUT INDEXES prints the table with no CREATE INDEX
        // statements after it. The PRIMARY KEY is always printed, because it is part of the
        // table definition.
        "without",
        "null",
        "not",
        "string",
        "char",
        "varchar",
        "text",
        "int",
        "int64",
        "smallint",
        "float32",
        "float64",
        "real",
        "object_id",
        "oid",
        "bool",
        "boolean",
        "bytes",
        "blob",
        "date",
        "datetime",
        "timestamp",
        "uuid",
        "guid",
        "array",
        // INTERVAL '1 day 2 hours', INTERVAL '3' HOUR, INTERVAL 3 HOUR and CAST(x AS interval). An
        // interval is an expression type only: a column, a stored value and an index key cannot have
        // it. The word is a keyword only before a quote, a digit or a parameter, so a column called
        // interval stays usable. The unit words (year, month, day, hour, minute, second) are listed
        // below as functions.
        "interval",
        // NUMERIC (alias DECIMAL): an exact decimal with precision 38 and scale 9. Both words are
        // reserved to the parser. NUMERIC also starts the typed literal NUMERIC '1.23'.
        "numeric",
        "decimal",
        // JSONB (alias JSON): one JSON document per cell. Neither word is reserved to the parser, so a
        // table or a column can still be called json or jsonb. Both also start a typed literal, as in
        // JSONB '{"a": 1}'.
        "jsonb",
        "json",
        // GIN indexes over a jsonb column: CREATE INDEX <name> ON <table> USING GIN (<col>
        // [jsonb_ops | jsonb_path_ops]), and the CockroachDB forms CREATE INVERTED INDEX and an
        // INVERTED INDEX inside CREATE TABLE. The two operator class names build the same index.
        "using",
        "gin",
        "inverted",
        "jsonb_ops",
        "jsonb_path_ops",
        "is",
        "on",
        "in",
        "or",
        "and",
        "between",
        "like",
        "ilike",
        // x LIKE pattern ESCAPE '!' sets the character that makes the next % or _ a literal one.
        "escape",
        "add",
        "show",
        "use",
        "tables",
        "columns",
        "group",
        "join",
        "inner",
        // [INNER] JOIN, LEFT [OUTER] JOIN, RIGHT [OUTER] JOIN and CROSS JOIN. LEFT, RIGHT, OUTER and
        // CROSS are reserved words to the parser, so a column or a table with one of these names needs
        // backticks. The server refuses FULL JOIN by name; FULL is listed below for MATCH FULL.
        "left",
        "right",
        "outer",
        "cross",
        "offset",
        "unique",
        "having",
        "explain",
        "analyze",
        // EXPLAIN (LOGICAL) and EXPLAIN (PHYSICAL) choose which plan the server prints. Both
        // words are plain identifiers to the parser, so both stay usable as names.
        "logical",
        "physical",
        "begin",
        "start",
        "transaction",
        "commit",
        "rollback",
        "evict",
        "cache",
        // Options of the {cache=<name>, ttl=<value><unit>, strict} table hint. Both words are
        // plain identifiers to the parser and stay usable as names.
        "ttl",
        "strict",
        "comment",
        "as",
        // AS OF SYSTEM TIME is lexed as one token by the server, but the editor highlights
        // token by token, so each word is listed on its own.
        "of",
        "system",
        "time",
        "distinct",
        // Set operations: <select> UNION|INTERSECT|EXCEPT [ALL|DISTINCT] <select>. All three are
        // reserved words to the parser, so a column or a table with one of these names needs backticks.
        "union",
        "intersect",
        "except",
        // Quantified comparisons: <expr> <op> ANY|SOME|ALL (<subquery>). ANY and SOME are plain
        // identifiers to the parser and stay usable as names. ALL is listed below for GRANT.
        "any",
        "some",
        // Window functions: <function>(<args>) OVER ([PARTITION BY …] [ORDER BY …] [ROWS | RANGE
        // <frame>]). OVER is a reserved word to the parser, so a column or a table called over needs
        // backticks. The other words are plain identifiers and stay usable as names. ORDER, BY, RANGE,
        // ROW, BETWEEN and AND are listed for other statements. The server refuses GROUPS frames and
        // EXCLUDE by name, but they are still colored, so a refused statement reads the same as an
        // accepted one.
        "over",
        "partition",
        "rows",
        "unbounded",
        "preceding",
        "following",
        "current",
        "groups",
        "exclude",
        "cast",
        "case",
        "when",
        "then",
        "else",
        "end",
        "integer",
        "double",
        "float",
        "databases",
        "to",
        "branch",
        "branches",
        "ancestors",
        "isolation",
        "level",
        "read",
        "committed",
        "serializable",
        "only",
        "write",
        "locking",
        "optimistic",
        "pessimistic",
        // SET TRANSACTION PRIORITY <value>: how the scheduler ranks this transaction against
        // others. PRIORITY and each value are plain identifiers to the parser, so all six words
        // stay usable as names. They are colored like LOCKING and its two modes above.
        "priority",
        "background",
        "low",
        "normal",
        "high",
        "critical",
        "user",
        "identified",
        "with",
        "grant",
        "grants",
        "revoke",
        "privileges",
        "all",
        "for",
        "sha256_password",
        // FLUSH PRIVILEGES drops this node's cached grants. FLUSH SESSIONS ends every session.
        // SHOW USERS lists the accounts. FLUSH, SESSIONS and USERS are plain identifiers to the
        // parser, so all three stay usable as names. A table named users is common in a schema.
        "flush",
        "sessions",
        "users",
        "engine",
        "stats",
        // SHOW STATEMENT STATS [LIKE '<pattern>'] and SHOW INDEX USAGE [FROM <table>]: the node's
        // per-statement counters and the use of each index. STATEMENT and USAGE are plain identifiers
        // to the parser and stay usable as names.
        "statement",
        "usage",
        "variables",
        // SHOW STATISTICS FOR [TABLE] <table>: the optimizer's advisory table statistics. The server
        // matches STATISTICS as a plain identifier so it stays usable as a name, but the editor
        // colors it like the rest of the SHOW vocabulary.
        "statistics",
        // SHOW RANGES FROM TABLE|INDEX and SHOW RANGE … FOR ROW (…): where a relation's key space is
        // divided, and which span holds one row. Like STATISTICS, all three words are plain
        // identifiers to the parser and stay usable as table and column names; the editor colors
        // them because they read as part of the SHOW vocabulary.
        "ranges",
        "range",
        "row",
        // SHOW SLOW QUERIES [LIKE '<pattern>']: the node's record of statements that ran over the
        // configured threshold. Both words are plain identifiers to the parser and stay usable as
        // names; the editor colors them because they read as part of the SHOW vocabulary.
        "slow",
        "queries",
        // SHOW [CLUSTER] QUERIES, SHOW [CLUSTER] CONNECTIONS and CANCEL QUERY '<query_id>': the
        // statements and client connections a node serves now, and the stop for one running read.
        // None of the words is reserved, so all of them stay usable as table and column names.
        "connections",
        "cancel",
        "query",
        // SET/RESET CLUSTER SETTING and SHOW CLUSTER SETTINGS. Both spellings of the last word are
        // listed because the statements differ: SETTING for the mutations, SETTINGS for the listing.
        "cluster",
        "setting",
        "settings",
        "reset",
        "view",
        "views",
        "materialized",
        "refresh",
        "cascade",
        // DROP VIEW <view> RESTRICT is the default: the drop fails when another view reads it.
        // RESTRICT is a plain identifier to the parser, exactly like CASCADE beside it.
        "restrict",
        "option",
        "local",
        "cascaded",
        "no",
        "data",
        "concurrently",
        "owner",
        // Large-value column storage. ALTER TABLE <t> ALTER [COLUMN] <c> SET STORAGE <strategy>
        // picks the form of future writes, and ALTER TABLE <t> REWRITE STORAGE [INLINE] converts
        // the rows that already exist. A strategy is one of PLAIN, MAIN, EXTERNAL or EXTENDED.
        // Every word here is a plain identifier to the parser and stays usable as a name.
        "storage",
        "rewrite",
        "inline",
        "plain",
        "main",
        "external",
        "extended",
        // Sequences. SEQUENCE and SEQUENCES are reserved words to the parser, like VIEW and VIEWS.
        // Every option word of CREATE/ALTER SEQUENCE, of TRUNCATE … RESTART|CONTINUE IDENTITY, and
        // of an identity column (GENERATED ALWAYS|BY DEFAULT AS IDENTITY, SERIAL, BIGSERIAL) is a
        // plain identifier to the parser and stays usable as a name. START, WITH, BY, CACHE, NO,
        // RENAME and TO are listed above for other statements.
        "sequence",
        "sequences",
        "increment",
        "minvalue",
        "maxvalue",
        "cycle",
        "restart",
        "continue",
        "identity",
        "generated",
        "always",
        "serial",
        "bigserial",
        // Foreign keys: REFERENCES <table> [(cols)], FOREIGN KEY (cols) REFERENCES …, ON DELETE|UPDATE
        // NO ACTION|RESTRICT|CASCADE|SET NULL|SET DEFAULT, MATCH SIMPLE|FULL|PARTIAL, [NOT] DEFERRABLE
        // and INITIALLY IMMEDIATE|DEFERRED. REFERENCES, FOREIGN, DEFERRABLE and INITIALLY are
        // reserved words to the parser, and NO ACTION is one token to it. MATCH, its three kinds,
        // IMMEDIATE and DEFERRED are plain identifiers and stay usable as names. The server refuses
        // CASCADE, SET NULL, SET DEFAULT, MATCH FULL|PARTIAL and DEFERRABLE by name, but they are
        // still colored, so a refused statement reads the same as an accepted one. NO, SET, NULL,
        // DEFAULT, CASCADE and RESTRICT are listed above for other statements.
        "references",
        "foreign",
        "action",
        "match",
        "simple",
        "full",
        "partial",
        "deferrable",
        "initially",
        "immediate",
        "deferred",
        // C# extensions. CREATE TRIGGER <name> ON <table> BEFORE|AFTER <events> EXTERNAL NAME
        // '<Type>', DROP TRIGGER <name> ON <table>, CALL <procedure>(<args>) and SHOW EXTENSIONS.
        // None of these words is reserved to the parser, so all of them stay usable as table and
        // column names. EXTERNAL is listed above for SET STORAGE.
        "trigger",
        // SHOW TRIGGERS [ON <table>] and SHOW CREATE TRIGGER <name> ON <table> list the triggers of
        // both forms. An inline trigger is CREATE TRIGGER … LANGUAGE csharp AS $$ <body> $$, with
        // the words of the inline functions below.
        "triggers",
        "before",
        "after",
        "name",
        "call",
        "extensions",
        // CREATE EXTENSION [IF NOT EXISTS] <name> [VERSION '<version>'], ALTER EXTENSION <name> UPDATE
        // [TO '<version>'] and DROP EXTENSION [IF EXISTS] <name>. EXTENSION and VERSION are plain
        // identifiers to the parser and stay usable as names.
        "extension",
        "version",
        // Inline functions: CREATE [OR REPLACE] FUNCTION <name>(<param> <type>, …) RETURNS <type>
        // [RETURNS NULL ON NULL INPUT | CALLED ON NULL INPUT] LANGUAGE csharp AS $$ <body> $$,
        // DROP FUNCTION [IF EXISTS] <name>, ALTER FUNCTION <name> OWNER TO <user>, SHOW FUNCTIONS
        // and SHOW CREATE FUNCTION <name>. None of these words is reserved to the parser. OR,
        // REPLACE, NULL, ON, AS, OWNER and TO are listed above for other statements.
        "function",
        "functions",
        "returns",
        "called",
        "input",
        "language",
        "csharp",
    ];

    internal static readonly string[] Functions = [
        "count",
        "max",
        "min",
        "avg",
        "sum",
        "gen_id",
        "gen_uuid_v4",
        "gen_uuid_v7",
        "current_timestamp",
        "now",
        // Clock functions. now and transaction_timestamp are aliases of current_timestamp, the start of
        // the transaction. statement_timestamp is the start of the statement. clock_timestamp (alias
        // sysdate) reads the clock again on each call.
        "transaction_timestamp",
        "statement_timestamp",
        "clock_timestamp",
        "sysdate",
        // Session functions: what session the statement runs in. They take no arguments and are
        // rejected as column defaults and CHECK conditions, which replay with no session behind them.
        "current_database",
        "current_user",
        "current_role",
        "is_superuser",
        "current_date",
        "date_add",
        // date_sub(t, n, 'unit') and date_sub(t, INTERVAL n unit) subtract where date_add adds.
        "date_sub",
        "date_diff",
        "date_part",
        "date_trunc",
        "unix_timestamp",
        "unix_timestamp_ms",
        "from_unixtime",
        // EXTRACT(<part> FROM <value>) is the SQL-standard spelling of date_part.
        "extract",
        // Date constructors: make_date(y, m, d), make_timestamp(y, mo, d, h, mi, s), MySQL
        // makedate(year, dayofyear) and last_day(x), the last day of the month of x.
        "make_date",
        "make_timestamp",
        "makedate",
        "last_day",
        // Interval functions of PostgreSQL. age(a, b) and age(a) give an interval, make_interval builds
        // one from parts, the justify functions move 24 hours into a day and 30 days into a month, and
        // date_bin(stride, t, origin) puts t at the start of its bin.
        "age",
        "make_interval",
        "justify_days",
        "justify_hours",
        "justify_interval",
        "date_bin",
        // Date patterns: PostgreSQL to_char and to_timestamp, and MySQL date_format and str_to_date.
        // to_date with two arguments also parses a pattern; it is listed below with the casts.
        "to_char",
        "to_timestamp",
        "date_format",
        "str_to_date",
        // MySQL date-part functions of one argument. Most of these names are also common column
        // names, and the editor colors such a column like a function.
        "year",
        "quarter",
        "month",
        "week",
        "day",
        "dayofmonth",
        "dayofyear",
        "dayofweek",
        "weekday",
        "hour",
        "minute",
        "second",
        "dayname",
        "monthname",
        "abs",
        "ceil",
        "ceiling",
        "floor",
        "sqrt",
        "pow",
        "power",
        // Logarithms and the exponential. log(x) is the base-10 logarithm, and log(b, x) uses base b.
        "ln",
        "log",
        "log10",
        "exp",
        "mod",
        "sign",
        "random",
        // Trigonometric functions, in the PostgreSQL forms. The names that end in d take or return
        // degrees, and radians and degrees convert between the two units. pi() takes no arguments.
        "sin",
        "cos",
        "tan",
        "cot",
        "asin",
        "acos",
        "atan",
        "atan2",
        "sind",
        "cosd",
        "tand",
        "cotd",
        "asind",
        "acosd",
        "atand",
        "atan2d",
        "radians",
        "degrees",
        "pi",
        "round",
        // trunc(x[, n]) cuts a value toward zero at n fraction digits, where round rounds. A NUMERIC
        // stays NUMERIC and is cut exactly.
        "trunc",
        "length",
        "lower",
        "upper",
        "trim",
        "ltrim",
        "rtrim",
        "substring",
        // strpos(haystack, needle). POSITION(needle IN haystack) is the SQL-standard spelling, and the
        // parser turns it into a strpos call.
        "strpos",
        "position",
        "replace",
        // like_escape(x, pattern, escape) is the call that x LIKE pattern ESCAPE escape parses to.
        "like_escape",
        "regexp_like",
        "regexp_match",
        "regexp_matches",
        "regexp_replace",
        "regexp_substr",
        "regexp_instr",
        "regexp_count",
        "regexp_split_to_array",
        "regexp_split_to_table",
        "contains",
        "starts_with",
        "ends_with",
        "concat",
        // The MySQL-style JSON functions. They take text and $.a.b[0] paths, and read a jsonb argument
        // as its text.
        "json_valid",
        "json_type",
        "json_extract",
        "json_value",
        "json_contains",
        "json_array_length",
        // The jsonb functions of PostgreSQL, with the json_* aliases that PostgreSQL has. The operator
        // functions (jsonb_contains for @>, jsonb_exists for ?, …) are what a driver that rewrites ?
        // uses, and a GIN index serves them as it serves the operators.
        "to_jsonb",
        "to_json",
        "jsonb_build_object",
        "json_build_object",
        "jsonb_build_array",
        "json_build_array",
        "jsonb_object",
        "json_object",
        "jsonb_typeof",
        "json_typeof",
        "jsonb_array_length",
        "jsonb_extract_path",
        "json_extract_path",
        "jsonb_extract_path_text",
        "json_extract_path_text",
        "jsonb_object_field",
        "jsonb_object_field_text",
        "jsonb_path_get",
        "jsonb_path_get_text",
        "jsonb_contains",
        "jsonb_contained",
        "jsonb_exists",
        "jsonb_exists_any",
        "jsonb_exists_all",
        "jsonb_delete_path",
        "jsonb_set",
        "jsonb_set_lax",
        "jsonb_insert",
        "jsonb_strip_nulls",
        "json_strip_nulls",
        "jsonb_pretty",
        // The jsonb aggregates. ORDER BY inside the call is not supported.
        "jsonb_agg",
        "json_agg",
        "jsonb_object_agg",
        "json_object_agg",
        // Array functions: cardinality and array_length give the length of an array(T) value, and
        // array_contains tells whether it holds a value.
        "cardinality",
        "array_length",
        "array_contains",
        "coalesce",
        "ifnull",
        "nvl",
        "nullif",
        "to_string",
        "to_int64",
        "to_float64",
        "to_float32",
        "to_bool",
        "to_bytes",
        "to_date",
        "to_datetime",
        "to_id",
        "str_id",
        // Vector measurement and distance functions. A vector is a bytes value that holds packed
        // little-endian float32 elements, so these read a bytes column, not a dedicated type.
        // octet_length also accepts a string, where it counts UTF-8 bytes, not characters.
        "octet_length",
        "bit_length",
        "bit_count",
        "vector_dims",
        "l2_distance",
        "inner_product",
        "cosine_distance",
        // Sequence functions. nextval and setval move a counter; currval and lastval report the
        // last value this transaction drew.
        "nextval",
        "currval",
        "lastval",
        "setval",
        // Bytes functions. encode/decode convert between bytes and hex, base64 or escape text, and the
        // MySQL names hex, unhex, to_base64 and from_base64 do the same for one format. convert_to and
        // convert_from accept UTF-8 only. The get_ and set_ functions take 0-based offsets.
        "encode",
        "decode",
        "hex",
        "unhex",
        "to_base64",
        "from_base64",
        "convert_to",
        "convert_from",
        "get_byte",
        "set_byte",
        "get_bit",
        "set_bit",
        "gen_random_bytes",
        // Hash functions. md5 and the sha functions return lowercase hex text; digest and hmac return
        // bytes; crc32 returns an unsigned int64. None of them is a password hash.
        "md5",
        "sha1",
        "sha256",
        "sha512",
        "digest",
        "hmac",
        "crc32",
        // generate_series(start, stop [, step]) is a table function: it goes in FROM, not in a select
        // list.
        "generate_series",
        // The jsonb table functions, which also go in FROM: jsonb_each(doc) gives key and value rows,
        // and jsonb_array_elements(doc) gives one value row for each element.
        "jsonb_each",
        "json_each",
        "jsonb_each_text",
        "json_each_text",
        "jsonb_object_keys",
        "json_object_keys",
        "jsonb_array_elements",
        "json_array_elements",
        "jsonb_array_elements_text",
        "json_array_elements_text",
        // Window functions. Each one needs an OVER clause. count, sum, avg, min and max also take one,
        // and are listed above as aggregates.
        "row_number",
        "rank",
        "dense_rank",
        "percent_rank",
        "cume_dist",
        "ntile",
        "lag",
        "lead",
        "first_value",
        "last_value",
        "nth_value",
    ];

    internal static readonly string[] Commands = [
        "clear",
        "help",
        "source",
        "use",
        "exit",
        "quit",
        "workload",
        "init",
        "run",
        "backup",
    ];

    // The shell's own commands that start with a '\\' or a '/'. They are colored like the commands
    // above, but completion does not offer them.
    internal static readonly string[] SlashCommands = [
        "\\d",
        "\\dt",
        "\\dv",
        "\\dm",
        "\\ds",
        "\\di",
        "\\df",
        "/df",
        "\\l",
        "/d",
        "\\export",
        "/export",
        "\\plan",
        "/plan",
        "\\prepared",
        "\\h",
        "/help",
        "\\q",
        "/quit",
        "/exit",
    ];

    internal static readonly string[] Constants = [
        "true",
        "false",
    ];
}
