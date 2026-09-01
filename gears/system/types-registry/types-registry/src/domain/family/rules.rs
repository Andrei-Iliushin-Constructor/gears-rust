//! The three non-stored family rules: kind, minor shape, minor contiguity.
//!
//! All three are asked of a **new member** only. A content revision adds nobody to
//! the family, so it is not gated here — which is also why closing a region cannot
//! freeze the entities already in it (T11, SPEC §8.1 step 3).
//!
//! # Each rule is one exact lookup, never a scan
//!
//! `database.sql` fixes which single identifier decides each question, and that is
//! what makes the rules keyed rather than family-wide:
//!
//! ```text
//! shape, minor-bearing candidate vM.n~   -> refuse while vM~ exists
//! shape, major-only candidate vM~        -> refuse while vM.0~ exists
//! contiguity, candidate vM.n~ with n > 0 -> refuse unless vM.(n-1)~ exists
//! ```
//!
//! Both are scoped to one **major**, as the compatibility chain is, so a family may
//! hold a major-only `v1~` beside a minor-bearing `v2.0~`. The kind rule is the one
//! that is family-wide, because a family key deliberately maps a type and an
//! Instance spelling onto one row and one of them has to lose.
//!
//! # Tombstones count, and one primitive is why
//!
//! A `DELETED` predecessor still satisfies contiguity: its definition remains the
//! compatibility baseline until purge, so skipping it would let an ordinary deletion
//! move the baseline. A `DELETED` member likewise still decides shape. Both fall out
//! of `EntityStore::find_by_gts_id` returning tombstones — one primitive, both rules,
//! and no way for the two to disagree about what a tombstone means.
//!
//! # What is *not* here
//!
//! The predecessor is **not** a dependency edge (T13) and **not** part of the
//! revision vector (T15). Such an edge would forbid deleting `v1.0~` while `v1.1~`
//! exists, which ADR-0008 permits and ADR-0004 relies on. With no edge between two
//! minors, the family row is an admission's and a purge's only serialization point
//! for these rules — see `version_family_repo`'s note on what that row can and
//! cannot lock today.

use gts::GtsId;
use toolkit_db::DbTx;
use toolkit_db::secure::{AccessScope, ScopeError};
use toolkit_macros::domain_model;

use super::key::{FamilyKey, sibling_id};
use crate::domain::enums::EntityKind;
use crate::domain::ports::{Stores, VersionFamilyRow};

/// Why a family refuses a new member.
///
/// A value rather than an error, like [`PolicyRefusal`](crate::domain::policy::PolicyRefusal):
/// the commit path turns it into the item's outcome, and nothing here is a fault.
/// One variant per rule, so a caller can count refusals by rule without parsing
/// prose.
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FamilyRefusal {
    /// A family holds one kind. Deliberately **not** `already_exists`: the
    /// identifier is free, and saying otherwise sends a caller looking for a
    /// conflicting entity that does not exist.
    KindConflict {
        gts_id: String,
        family_key: FamilyKey,
        candidate: EntityKind,
        existing: EntityKind,
    },
    /// Minor shape: within one major, either every member carries a minor or none
    /// does. `conflicting` is the member that already decided it.
    MinorShape { gts_id: String, conflicting: String },
    /// Minor contiguity: the minors of a major are contiguous and open at `M.0`.
    MissingPredecessor { gts_id: String, predecessor: String },
}

impl FamilyRefusal {
    /// The stable machine reason recorded on the operation item.
    #[must_use]
    pub const fn reason(&self) -> &'static str {
        match self {
            Self::KindConflict { .. } => "family_kind_conflict",
            Self::MinorShape { .. } => "family_shape_conflict",
            Self::MissingPredecessor { .. } => "missing_predecessor",
        }
    }
}

impl std::fmt::Display for FamilyRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KindConflict {
                gts_id,
                family_key,
                candidate,
                existing,
            } => write!(
                f,
                "'{gts_id}' is a {candidate}, but version family '{family_key}' already holds \
                 {existing} members; a family holds one kind"
            ),
            Self::MinorShape {
                gts_id,
                conflicting,
            } => write!(
                f,
                "'{gts_id}' cannot join the major that '{conflicting}' already spells the other \
                 way; within one major either every member carries a minor or none does"
            ),
            Self::MissingPredecessor {
                gts_id,
                predecessor,
            } => write!(
                f,
                "'{gts_id}' requires its predecessor '{predecessor}'; the minors of a major are \
                 contiguous and open at M.0"
            ),
        }
    }
}

/// The exact lookups the shape and contiguity rules need, chosen by the
/// candidate's **own** version.
///
/// Three variants because there are exactly three shapes a last segment can have,
/// and each asks a different pair of questions. Two `Option<String>` fields beside
/// a discriminant would have made "a first minor with a predecessor" representable.
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum VersionProbe {
    /// A major-only candidate `vM~`: refused while `vM.0~` exists.
    MajorOnly { blocker: String },
    /// The first minor `vM.0~`: refused while `vM~` exists. It opens the major, so
    /// it has no predecessor.
    FirstMinor { blocker: String },
    /// `vM.n~` with `n > 0`: refused while `vM~` exists, and refused *unless*
    /// `vM.(n-1)~` exists.
    LaterMinor {
        blocker: String,
        predecessor: String,
    },
}

/// Which identifiers decide this candidate's shape and contiguity. Pure.
///
/// `None` for a last segment with no major — a UUID tail, which acceptance refuses
/// (SPEC §8.1 step 4), so the write path never reaches it. Returning `None` rather
/// than panicking keeps the function total on an input it cannot see.
#[must_use]
pub fn version_probe(id: &GtsId) -> Option<VersionProbe> {
    let last = id.segments().last()?;
    let major = last.ver_major_opt()?;
    Some(match last.ver_minor() {
        None => VersionProbe::MajorOnly {
            blocker: sibling_id(id, major, Some(0)),
        },
        Some(0) => VersionProbe::FirstMinor {
            blocker: sibling_id(id, major, None),
        },
        Some(minor) => VersionProbe::LaterMinor {
            blocker: sibling_id(id, major, None),
            // `minor > 0` here, so this cannot underflow; `saturating_sub` says so
            // to the reader as well as to the compiler.
            predecessor: sibling_id(id, major, Some(minor.saturating_sub(1))),
        },
    })
}

/// Ask the family whether it admits this new member.
///
/// Runs inside the admission commit transaction, after the family row is taken or
/// created: every question is about committed state, and the answer must not be
/// older than the transaction that acts on it.
///
/// The family row is passed whole rather than as `(key, id)`: the kind rule needs
/// the id and its refusal names the key, and two parameters that must come from one
/// row are two chances to pass a mismatched pair.
///
/// `family_is_new` skips the kind rule for a family this admission is founding —
/// there is no existing member to conflict with, and asking would be one read that
/// can only answer `None`.
///
/// # Errors
/// Propagates the scoped reads. A refusal is `Ok(Some(..))`: it is an outcome, not
/// a fault.
pub async fn admits_new_member(
    stores: &dyn Stores,
    tx: &DbTx<'_>,
    scope: &AccessScope,
    id: &GtsId,
    family: &VersionFamilyRow,
    candidate_kind: EntityKind,
    family_is_new: bool,
) -> Result<Option<FamilyRefusal>, ScopeError> {
    if !family_is_new
        && let Some(existing) = stores.kind_in_family(tx, scope, family.id).await?
        && existing != candidate_kind
    {
        return Ok(Some(FamilyRefusal::KindConflict {
            gts_id: id.id().to_owned(),
            family_key: family.family_key.clone(),
            candidate: candidate_kind,
            existing,
        }));
    }

    // A candidate whose version this function cannot read is one acceptance should
    // have refused; there is nothing to check and nothing to invent.
    let Some(probe) = version_probe(id) else {
        return Ok(None);
    };

    let (blocker, predecessor) = match &probe {
        VersionProbe::MajorOnly { blocker } | VersionProbe::FirstMinor { blocker } => {
            (blocker, None)
        }
        VersionProbe::LaterMinor {
            blocker,
            predecessor,
        } => (blocker, Some(predecessor)),
    };

    if stores.find_by_gts_id(tx, scope, blocker).await?.is_some() {
        return Ok(Some(FamilyRefusal::MinorShape {
            gts_id: id.id().to_owned(),
            conflicting: blocker.clone(),
        }));
    }

    if let Some(predecessor) = predecessor
        && stores
            .find_by_gts_id(tx, scope, predecessor)
            .await?
            .is_none()
    {
        return Ok(Some(FamilyRefusal::MissingPredecessor {
            gts_id: id.id().to_owned(),
            predecessor: predecessor.clone(),
        }));
    }

    Ok(None)
}

#[cfg(test)]
#[path = "rules_tests.rs"]
mod rules_tests;
