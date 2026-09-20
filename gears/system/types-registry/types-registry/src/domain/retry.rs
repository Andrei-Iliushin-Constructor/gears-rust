//! Whether redelivering an admission can reach a different answer than this
//! pass did.
//!
//! The outbox leased handler asks this once per failed admission to choose
//! between `MessageResult::Retry` and dead-lettering the operation (T21). It is
//! a judgement about *redelivery*, not about transactions: lock contention is
//! already absorbed on the commit path by `Db::transaction_with_retry`, which
//! carries its own attempt budget and jittered backoff (see
//! [`admission::worker`](super::admission::worker)). A contention error that
//! still arrives here has survived that, and the only question left is whether
//! to spend another delivery on it.
//!
//! # Why this names the permanent failures, not the temporary ones
//!
//! The two ways to be wrong do not cost the same.
//!
//! Calling a temporary failure permanent dead-letters work a redelivery would
//! have admitted: the operation leaves the queue on a network blip and an
//! operator has to notice the dead-letter row. Calling a permanent failure
//! temporary costs `worker.max_delivery_attempts` deliveries of a message whose
//! entire payload is one operation `UUID`, and then dead-letters it anyway.
//!
//! So the default belongs on the retry side, and only failures that *cannot*
//! read differently next time are named. That delivery budget is what makes it
//! safe: it already exists to stop a persistently failing message from holding
//! its partition (SPEC §8.1, T21), so nothing here decides whether a message
//! eventually leaves the queue — only how many deliveries it takes.
//!
//! # Why no backend, and no table of engine codes
//!
//! Enumerating `SQLSTATE` codes per engine here would restate, in a second
//! place, the classification `toolkit_db::contention` already owns for the
//! transaction layer. It would also be a hand-written table of `sqlx` types and
//! code strings that keeps compiling while it silently stops matching across a
//! driver upgrade — the failure mode
//! `libs/toolkit-db/tests/error_classification.rs` exists to catch, and which
//! synthetic unit tests cannot. Nothing above needs the distinction: every
//! engine and transport failure falls on the same side of this question, so the
//! engine that produced it is not an input.

use toolkit_db::DbError;
use toolkit_db::advisory_locks::DbLockError;
use toolkit_db::secure::ScopeError;

/// Whether a scoped storage failure can read differently on redelivery.
///
/// Only [`ScopeError::Db`] ever reached the engine. Every other variant states
/// something about the scope this pass compiled or the SQL/PGQ its builder
/// rendered, and a redelivery compiles the same scope from the same
/// configuration.
///
/// Unlike [`database_failure_may_clear`] this cannot be exhaustive — `ScopeError` is
/// `#[non_exhaustive]`, so a new variant cannot be made a compile error here
/// and naming the others alongside a wildcard is a `match_same_arms` lint. The
/// wildcard therefore answers `false`, which is the opposite of this module's
/// default, and deliberately: every variant this type carries apart from `Db`
/// is a deterministic refusal — `Invalid`, `Denied`, `TenantNotInScope`,
/// `UnresolvedScopeProperty`, `GraphSyntax` — so a variant added later most
/// likely is one too. A new one that is *not* belongs on the other side, and
/// that is a decision for whoever adds it.
#[must_use]
pub fn scoped_failure_may_clear(error: &ScopeError) -> bool {
    matches!(error, ScopeError::Db(_))
}

/// Whether a database failure can read differently on redelivery.
///
/// The match is exhaustive on purpose: a variant added to [`DbError`] must be
/// classified here rather than inheriting a default, which is how
/// [`DbError::Lock`] — wrapping the same `sqlx` and `io` failures the arms
/// beside it carry — came to be treated as permanent under an earlier
/// catch-all.
#[must_use]
pub fn database_failure_may_clear(error: &DbError) -> bool {
    match error {
        // Reached the engine or the wire. The next delivery may find a healthy
        // connection or a drained pool.
        DbError::Sqlx(_) | DbError::Sea(_) | DbError::Io(_) => true,

        // Not every lock failure is contention; see [`lock_failure_may_clear`].
        DbError::Lock(error) => lock_failure_may_clear(error),

        // Resolved from configuration, or a programming mistake in this
        // process. A redelivery reads the same configuration and makes the same
        // call, so it produces the same failure.
        DbError::UnknownDsn(_)
        | DbError::FeatureDisabled(_)
        | DbError::InvalidConfig(_)
        | DbError::ConfigConflict(_)
        | DbError::InvalidSqlitePragma { .. }
        | DbError::UnknownSqlitePragma(_)
        | DbError::InvalidParameter(_)
        | DbError::SqlitePragma(_)
        | DbError::EnvVar { .. }
        | DbError::UrlParse(_)
        | DbError::ConnRequestedInsideTx => false,

        // `DBProvider` preserves a scoped failure in this wrapper, and a scope
        // denial must not spend the delivery budget. Anything else under
        // `Other` is opaque, which is exactly the case the retry side is the
        // default for.
        DbError::Other(error) => error
            .downcast_ref::<ScopeError>()
            .is_none_or(scoped_failure_may_clear),
    }
}

/// Whether an advisory-lock failure can read differently on redelivery.
///
/// Stated as a *negative* match, so the two named variants are the whole
/// permanent set and everything else — including a variant added later — keeps
/// this module's retry default:
///
/// - `AlreadyHeld` is contention. Another session holds the key and will
///   release it; that is what an advisory lock is for, and the commit path's
///   own `transaction_with_retry` budget having run out says nothing about the
///   next delivery.
/// - `Io` and `UnexpectedDatabaseResult` are the transport and the engine
///   answering oddly, which is the same case the arms beside `Lock` cover.
/// - `Database` is a `sqlx::Error` wherever `toolkit-db` was built with a
///   server-side lock backend, and is unnameable here when it was not — which
///   is why this is a negative match rather than an exhaustive one. Whether a
///   dependency's `pg`/`mysql` features are on is not something this gear can
///   express in a `cfg`.
///
/// The two permanent ones both describe *this* process rather than the
/// database. `InvalidConfig` is the lock configuration a redelivery compiles
/// again from the same settings; `NotHeld` is a release of a lock this process
/// never took, which is a bug here and which redelivering reproduces rather
/// than clears. Retrying either spends `worker.max_delivery_attempts`
/// deliveries, dead-letters the operation anyway, and hides the
/// misconfiguration behind the repeats.
fn lock_failure_may_clear(error: &DbLockError) -> bool {
    !matches!(
        error,
        DbLockError::InvalidConfig { .. } | DbLockError::NotHeld
    )
}

#[cfg(test)]
#[path = "retry_tests.rs"]
mod retry_tests;
