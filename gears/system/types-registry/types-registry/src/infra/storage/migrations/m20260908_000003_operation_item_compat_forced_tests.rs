//! Cross-dialect checks for the `operation_item.compat_forced` migration.

use super::{DOWN_STATEMENTS, MYSQL_UP_STATEMENTS, PG_UP_STATEMENTS, SQLITE_UP_STATEMENTS};

const TABLE: &str = "types_registry__operation_item";
const COLUMN: &str = "compat_forced";

fn lists() -> [(&'static str, &'static [&'static str]); 3] {
    [
        ("postgres", PG_UP_STATEMENTS),
        ("sqlite", SQLITE_UP_STATEMENTS),
        ("mysql", MYSQL_UP_STATEMENTS),
    ]
}

fn only_statement(name: &str, statements: &'static [&'static str]) -> &'static str {
    assert_eq!(
        statements.len(),
        1,
        "{name} adds one column in one statement"
    );
    statements[0]
}

/// Every dialect alters the same table and adds the same column.
#[test]
fn every_backend_adds_the_column_to_the_operation_item_table() {
    for (name, statements) in lists() {
        let sql = only_statement(name, statements);
        assert!(
            sql.contains(&format!("ALTER TABLE {TABLE}")),
            "{name} must alter {TABLE}, got {sql}",
        );
        assert!(
            sql.contains(&format!("ADD COLUMN IF NOT EXISTS {COLUMN} "))
                || sql.contains(&format!("ADD COLUMN {COLUMN} ")),
            "{name} must add {COLUMN}, got {sql}",
        );
    }
}

/// **`NOT NULL DEFAULT false` is the whole upgrade story.** Rows a deployment
/// already holds were all accepted while acceptance refused every effective
/// `force` (ceiling C9), so `false` is the true value for each of them — not a
/// convenient placeholder. Without the default the statement cannot run against a
/// non-empty table at all.
#[test]
fn every_backend_declares_the_column_not_null_and_defaulted_to_false() {
    for (name, statements) in lists() {
        let sql = only_statement(name, statements);
        assert!(
            sql.contains("NOT NULL"),
            "{name} must be NOT NULL, got {sql}"
        );
        let defaulted = sql.contains("DEFAULT false") || sql.contains("DEFAULT 0");
        assert!(
            defaulted,
            "{name} must default the column so existing rows are covered, got {sql}",
        );
    }
}

/// The boolean is lowered the way every other boolean in this schema is: a native
/// type on Postgres, `TINYINT(1)` on MySQL, and an `INTEGER` with a 0/1 `CHECK` on
/// SQLite — without which SQLite would accept a `7` that the other two refuse.
#[test]
fn the_boolean_is_lowered_per_backend_with_sqlite_carrying_the_check() {
    let pg = only_statement("postgres", PG_UP_STATEMENTS);
    assert!(pg.contains("boolean"), "got {pg}");

    let mysql = only_statement("mysql", MYSQL_UP_STATEMENTS);
    assert!(mysql.contains("TINYINT(1)"), "got {mysql}");

    let sqlite = only_statement("sqlite", SQLITE_UP_STATEMENTS);
    assert!(sqlite.contains("INTEGER"), "got {sqlite}");
    assert!(
        sqlite.contains(&format!("CHECK ({COLUMN} IN (0, 1))")),
        "SQLite must constrain the lowered boolean, got {sqlite}",
    );
}

/// SQLite has no `IF NOT EXISTS` on `ADD COLUMN`, so guarding it there would be a
/// syntax error rather than a safety net. Pinned so that copying the Postgres
/// spelling across does not silently break the SQLite path.
#[test]
fn only_postgres_guards_the_add_column() {
    assert!(PG_UP_STATEMENTS[0].contains("ADD COLUMN IF NOT EXISTS"));
    assert!(!SQLITE_UP_STATEMENTS[0].contains("IF NOT EXISTS"));
    assert!(!MYSQL_UP_STATEMENTS[0].contains("IF NOT EXISTS"));
}

/// Down removes the column and nothing else — the table predates this migration.
#[test]
fn down_drops_the_column_and_never_the_table() {
    assert_eq!(
        DOWN_STATEMENTS,
        [format!("ALTER TABLE {TABLE} DROP COLUMN {COLUMN}")]
    );
    assert!(
        !DOWN_STATEMENTS.iter().any(|s| s.contains("DROP TABLE")),
        "the table is the initial migration's to drop",
    );
}

/// The column must never be named `force`: it is a MySQL reserved word, and
/// `ADD COLUMN force` is error 1064 there — which is how this migration was first
/// written and how the MySQL container suite caught it.
#[test]
fn no_backend_names_the_column_with_a_reserved_word() {
    for (name, statements) in lists() {
        let sql = only_statement(name, statements);
        assert!(
            !sql.contains(" force "),
            "{name} must not name the column `force`, which MySQL reserves; got {sql}",
        );
    }
}
