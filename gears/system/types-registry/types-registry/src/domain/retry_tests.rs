//! Tests for the redelivery classifier.
//!
//! These pin the *direction* of the policy rather than a list of engine codes:
//! the module deliberately has no table to assert against, and the property
//! worth guarding is which side an unrecognized failure lands on.

use super::{database_failure_may_clear, scoped_failure_may_clear};
use toolkit_db::DbError;
use toolkit_db::secure::ScopeError;

/// The whole point of the allowlist: a failure that reached the engine or the
/// wire is retried, because the cheap way to be wrong is to spend deliveries
/// rather than to dead-letter work a reread would have admitted.
#[test]
fn engine_and_transport_failures_are_retried() {
    assert!(database_failure_may_clear(&DbError::Sea(
        sea_orm::DbErr::ConnectionAcquire(sea_orm::ConnAcquireErr::Timeout)
    )));
    assert!(database_failure_may_clear(&DbError::Io(
        std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset",)
    )));
}

/// Regression guard. `DbError::Lock` wraps the same `sqlx::Error` and
/// `io::Error` values the arms beside it carry, and a catch-all arm once made
/// every one of them a dead letter.
#[test]
fn an_advisory_lock_failure_is_retried_like_the_errors_it_wraps() {
    let io = toolkit_db::advisory_locks::DbLockError::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "broken pipe",
    ));

    assert!(
        database_failure_may_clear(&DbError::Lock(io)),
        "a lock failure carrying a transport error must not be permanent",
    );
}

/// A redelivery reads the same configuration, so these produce the same
/// failure and spending the budget on them buys nothing.
#[test]
fn configuration_and_programming_failures_are_permanent() {
    assert!(!database_failure_may_clear(&DbError::InvalidConfig(
        "invalid configuration".into()
    )));
    assert!(!database_failure_may_clear(&DbError::UnknownDsn(
        "no-such-dsn".into()
    )));
    assert!(!database_failure_may_clear(&DbError::FeatureDisabled("pg")));
    assert!(!database_failure_may_clear(&DbError::ConnRequestedInsideTx));
}

/// Scope compilation is deterministic: the same scope refuses the same way on
/// every delivery. `Denied` in particular must never spend the budget.
#[test]
fn scope_decisions_are_permanent_but_a_scoped_database_failure_is_not() {
    assert!(!scoped_failure_may_clear(&ScopeError::Denied(
        "not allowed"
    )));
    assert!(!scoped_failure_may_clear(&ScopeError::Invalid(
        "invalid scope"
    )));
    assert!(!scoped_failure_may_clear(&ScopeError::GraphSyntax(
        "no projected columns".into()
    )));
    assert!(!scoped_failure_may_clear(&ScopeError::TenantNotInScope {
        tenant_id: uuid::Uuid::nil(),
    }));

    assert!(scoped_failure_may_clear(&ScopeError::Db(
        sea_orm::DbErr::ConnectionAcquire(sea_orm::ConnAcquireErr::Timeout)
    )));
}

/// `DBProvider` preserves a scoped failure inside `Other`, so the wrapper must
/// not turn a scope denial into eight deliveries.
#[test]
fn a_scope_denial_wrapped_by_the_provider_stays_permanent() {
    let wrapped = DbError::Other(anyhow::Error::new(ScopeError::Denied("not allowed")));

    assert!(!database_failure_may_clear(&wrapped));
}

/// An opaque `Other` is the case the default exists for: nothing here can tell
/// whether it clears, and the delivery budget bounds the cost of assuming it
/// might.
#[test]
fn an_opaque_wrapped_failure_takes_the_retry_side() {
    assert!(database_failure_may_clear(&DbError::Other(
        anyhow::anyhow!("connection reset")
    )));
}

/// The behaviour this classifier deliberately changed. Invalid SQL against a
/// healthy engine is permanent in fact, and it is still reported as a dead
/// letter — but only after the budget is spent, because no classifier can tell
/// `no such table` apart from a schema that a concurrent migration is still
/// creating without restating the engine's own vocabulary.
#[test]
fn a_query_failure_is_retried_and_left_to_the_delivery_budget() {
    let missing_table = ScopeError::Db(sea_orm::DbErr::Query(sea_orm::RuntimeErr::Internal(
        "no such table: operation".into(),
    )));

    assert!(
        scoped_failure_may_clear(&missing_table),
        "a query failure is bounded by worker.max_delivery_attempts, not by this classifier",
    );
}
