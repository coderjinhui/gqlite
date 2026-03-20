use rustyline::completion::{Completer, Pair};
use rustyline::highlight::Highlighter;
use rustyline::hint::Hinter;
use rustyline::Context;
use rustyline::{Helper, Validator};
use std::borrow::Cow;

const DOT_COMMANDS: &[&str] =
    &[".checkpoint", ".database", ".exit", ".help", ".open", ".quit", ".schema", ".tables"];

const CYPHER_KEYWORDS: &[&str] = &[
    "ADD",
    "ALL",
    "ALTER",
    "AND",
    "AS",
    "ASC",
    "BEGIN",
    "BOOL",
    "BY",
    "CALL",
    "CASE",
    "CAST",
    "COLUMN",
    "COMMIT",
    "COPY",
    "CREATE",
    "DELETE",
    "DELIMITER",
    "DESC",
    "DETACH",
    "DISTINCT",
    "DOUBLE",
    "DROP",
    "ELSE",
    "END",
    "EXISTS",
    "FALSE",
    "FROM",
    "HEADER",
    "IN",
    "INT64",
    "IS",
    "KEY",
    "LIMIT",
    "MATCH",
    "MERGE",
    "NODE",
    "NOT",
    "NULL",
    "ON",
    "OPTIONAL",
    "OR",
    "ORDER",
    "PRIMARY",
    "REL",
    "RENAME",
    "RETURN",
    "ROLLBACK",
    "SERIAL",
    "SET",
    "SKIP",
    "STRING",
    "TABLE",
    "THEN",
    "TO",
    "TRUE",
    "UNION",
    "UNWIND",
    "WHEN",
    "WHERE",
    "WITH",
    "YIELD",
];

#[derive(Helper, Validator)]
pub struct GqliteHelper;

impl Highlighter for GqliteHelper {
    fn highlight_hint<'h>(&self, hint: &'h str) -> Cow<'h, str> {
        // ANSI: 90 = bright black (dark gray)
        Cow::Owned(format!("\x1b[90m{hint}\x1b[0m"))
    }
}

impl GqliteHelper {
    pub fn find_matches(line: &str, pos: usize) -> (usize, Vec<String>) {
        let text = &line[..pos];

        if text.starts_with('.') {
            let lower = text.to_lowercase();
            let matches: Vec<String> = DOT_COMMANDS
                .iter()
                .filter(|cmd| cmd.starts_with(&lower) && cmd.len() > lower.len())
                .map(|cmd| cmd.to_string())
                .collect();
            return (0, matches);
        }

        // Find the start of the current word
        let word_start =
            text.rfind(|c: char| !c.is_alphanumeric() && c != '_').map(|i| i + 1).unwrap_or(0);

        let word = &text[word_start..];
        if word.is_empty() {
            return (word_start, vec![]);
        }
        let upper = word.to_uppercase();

        let matches: Vec<String> = CYPHER_KEYWORDS
            .iter()
            .filter(|kw| kw.starts_with(&upper) && kw.len() > upper.len())
            .map(|kw| kw.to_string())
            .collect();

        (word_start, matches)
    }
}

impl Completer for GqliteHelper {
    type Candidate = Pair;

    fn complete(
        &self,
        line: &str,
        pos: usize,
        _ctx: &Context<'_>,
    ) -> rustyline::Result<(usize, Vec<Pair>)> {
        let (start, matches) = Self::find_matches(line, pos);
        let pairs = matches
            .into_iter()
            .map(|m| Pair { display: m.clone(), replacement: m[pos - start..].to_string() })
            .collect();
        Ok((pos, pairs))
    }
}

impl Hinter for GqliteHelper {
    type Hint = String;

    fn hint(&self, line: &str, pos: usize, _ctx: &Context<'_>) -> Option<String> {
        if pos < line.len() {
            return None;
        }
        let (start, matches) = Self::find_matches(line, pos);
        matches.first().map(|m| m[pos - start..].to_string())
    }
}
