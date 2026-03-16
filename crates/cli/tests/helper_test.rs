use gqlite_cli::helper::GqliteHelper;

// ── dot-command 补全 ────────────────────────────────────────────

#[test]
fn dot_command_prefix_match() {
    let (start, matches) = GqliteHelper::find_matches(".check", 6);
    assert_eq!(start, 0);
    assert_eq!(matches, vec![".checkpoint"]);
}

#[test]
fn dot_command_multiple_matches() {
    // ".e" matches both .exit
    let (start, matches) = GqliteHelper::find_matches(".e", 2);
    assert_eq!(start, 0);
    assert!(matches.contains(&".exit".to_string()));
}

#[test]
fn dot_command_exact_no_match() {
    // Exact match should return empty (no further suggestion)
    let (_, matches) = GqliteHelper::find_matches(".quit", 5);
    assert!(matches.is_empty());
}

#[test]
fn dot_command_case_insensitive() {
    let (_, matches) = GqliteHelper::find_matches(".CHECK", 6);
    assert_eq!(matches, vec![".checkpoint"]);
}

#[test]
fn dot_command_unknown_prefix() {
    let (_, matches) = GqliteHelper::find_matches(".xyz", 4);
    assert!(matches.is_empty());
}

// ── Cypher 关键字补全 ───────────────────────────────────────────

#[test]
fn cypher_keyword_prefix_match() {
    let (start, matches) = GqliteHelper::find_matches("ma", 2);
    assert_eq!(start, 0);
    assert_eq!(matches, vec!["MATCH".to_string()]);
}

#[test]
fn cypher_keyword_case_insensitive() {
    let (_, matches) = GqliteHelper::find_matches("Ma", 2);
    assert_eq!(matches, vec!["MATCH".to_string()]);
}

#[test]
fn cypher_keyword_multiple_matches() {
    // "DE" matches DELETE, DELIMITER, DESC, DETACH
    let (_, matches) = GqliteHelper::find_matches("DE", 2);
    assert!(matches.contains(&"DELETE".to_string()));
    assert!(matches.contains(&"DESC".to_string()));
    assert!(matches.contains(&"DETACH".to_string()));
}

#[test]
fn cypher_keyword_exact_no_match() {
    let (_, matches) = GqliteHelper::find_matches("MATCH", 5);
    assert!(matches.is_empty());
}

#[test]
fn cypher_keyword_mid_line() {
    // "MATCH (n) w" → word_start=10, matches WHERE/WITH
    let line = "MATCH (n) w";
    let (start, matches) = GqliteHelper::find_matches(line, line.len());
    assert_eq!(start, 10);
    assert!(matches.contains(&"WHERE".to_string()));
    assert!(matches.contains(&"WITH".to_string()));
}

#[test]
fn empty_input_no_match() {
    let (_, matches) = GqliteHelper::find_matches("", 0);
    assert!(matches.is_empty());
}

#[test]
fn space_only_no_match() {
    let (_, matches) = GqliteHelper::find_matches("   ", 3);
    assert!(matches.is_empty());
}
