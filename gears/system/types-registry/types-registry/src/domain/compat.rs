//! Compatibility against one baseline (ADR-0003, SPEC §8.1 step 3).
//!
//! Types Registry enforces `BACKWARD` and compares a candidate against **one**
//! definition, never against history: set inclusion is transitive, so a chain
//! every edge of which was established carries the guarantee at its endpoints
//! without any earlier revision being re-examined (ADR-0003 §*The comparison
//! baseline*).
//!
//! # Which definition that is, is a property of the identifier
//!
//! ```text
//! not a Type Schema         -> no comparison; an Instance is validated, not compared
//! major 0                   -> no comparison (ADR-0015: no mode is enforced there)
//! vM~,   creation           -> no comparison; nothing precedes a first admission
//! vM~,   revision           -> its own current revision      intra-entity, never waivable
//! vM.0~                     -> no comparison; it opens the major
//! vM.n~, n > 0, creation    -> the current definition of vM.(n-1)~   cross-minor, waivable
//! ```
//!
//! **The asymmetry is the whole design.** Within one identifier a consumer is
//! *carried onto* the new revision by a floating `$ref`, so that edge underwrites a
//! mechanism and nothing may waive it. Across a minor boundary a consumer *chooses*
//! to move, so ADR-0004 lets one candidate waive that edge with `force`. Both edges
//! are the same `Valid(baseline) ⊆ Valid(candidate)` — a minor boundary changes
//! which definition is the baseline and nothing else.
//!
//! # Why the baseline is named rather than searched for
//!
//! Contiguity (ADR-0004) puts the predecessor's identifier *in* the candidate's:
//! `vM.n~` always compares against `vM.(n-1)~`. Selecting "the highest admitted
//! minor below the candidate" instead would let a concurrent admission move the
//! baseline between selection and commit. A `DELETED` predecessor still decides it:
//! deletion does not unaccept the instances its definition accepted.

use gts::{CompatibilityVerdict, GtsId, GtsIdSegment, GtsStore, SchemaComparison, StoreError};
use serde_json::Value;
use toolkit_macros::domain_model;

use crate::domain::admission::{AdmissionFailureReason, Precondition};
use crate::domain::family::{VersionProbe, version_probe};

/// Why a candidate is compared against nothing, and is admissible anyway.
///
/// Distinct from a refusal: each variant is a case where no baseline **exists**,
/// so there is no verdict to fail closed on. `principle-fail-closed` governs an
/// *undecidable* comparison, not an absent one.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Exemption {
    /// An Instance is validated against a Type Schema revision, never compared
    /// against a previous value.
    NotATypeSchema,
    /// ADR-0015: major 0 is an unstable profile that enforces no mode, so it
    /// carries no whole-history guarantee to protect.
    MajorZero,
    /// A first admission of a major-only identifier: nothing precedes it.
    FirstAdmission,
    /// `vM.0~` opens its major, so it has no preceding minor (ADR-0004).
    FirstMinor,
}

impl Exemption {
    /// A stable token naming why no comparison was owed.
    #[must_use]
    pub const fn label(self) -> &'static str {
        match self {
            Self::NotATypeSchema => "exempt_not_a_type_schema",
            Self::MajorZero => "exempt_major_zero",
            Self::FirstAdmission => "exempt_first_admission",
            Self::FirstMinor => "exempt_first_minor",
        }
    }
}

/// The definition one candidate is checked against.
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Baseline {
    /// No comparison is owed. Carries which case it was, so a span can record it.
    Exempt(Exemption),
    /// The candidate's own current revision — intra-entity, never waivable.
    CurrentRevision,
    /// The current definition of the preceding minor — cross-minor, waivable by
    /// `force` where the deployment permits it.
    PrecedingMinor { gts_id: String },
    /// The last segment names no readable major, so no baseline can be selected.
    ///
    /// Acceptance refuses such an identifier (SPEC §8.1 step 4), so this is
    /// unreachable from the write path. It exists so the *absence* of a version
    /// refuses the candidate rather than admitting it with the check skipped.
    Unreadable,
}

impl Baseline {
    /// Whether `force` has this check to waive: the cross-minor edge, and only it.
    #[must_use]
    pub const fn waivable(&self) -> bool {
        matches!(self, Self::PrecedingMinor { .. })
    }

    /// A stable token naming which selection this is, for the unit span.
    ///
    /// Each exemption keeps its own token: on a span "no baseline" is not an answer
    /// an operator can act on, while "no baseline because the major is 0" is. It
    /// carries **no identifier** — which preceding minor it was is a separate span
    /// field, because an identifier is unbounded and a token is not.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Exempt(exemption) => exemption.label(),
            Self::CurrentRevision => "current_revision",
            Self::PrecedingMinor { .. } => "preceding_minor",
            Self::Unreadable => "unreadable_version",
        }
    }
}

/// Which definition this candidate is compared against. Pure: no database.
///
/// The precondition is what separates a creation from a revision, and it is the
/// accepted one rather than a fresh existence probe: the comparison must rest on
/// the same decision the caller's optimistic precondition rests on.
///
/// A **revision of a minor-bearing** Type Schema selects [`Baseline::CurrentRevision`]
/// — the strict, non-waivable edge. ADR-0004 makes such a revision permanently
/// inadmissible and acceptance refuses it first, so the arm is unreachable; it
/// answers with the stricter of the two rather than with the waivable one, so that
/// a future path reaching it cannot arrive at a weaker check by accident.
#[must_use]
pub fn select_baseline(id: &GtsId, precondition: Precondition) -> Baseline {
    if !id.is_type() {
        return Baseline::Exempt(Exemption::NotATypeSchema);
    }
    // Asked before the minor arithmetic: quarantine is a property of the major, so
    // `v0.1~` is exempt rather than compared against `v0.0~`.
    match id.segments().last().and_then(GtsIdSegment::ver_major_opt) {
        None => return Baseline::Unreadable,
        Some(0) => return Baseline::Exempt(Exemption::MajorZero),
        Some(_) => {}
    }
    // The predecessor is read from the contiguity rule's own probe, never derived
    // again here: two spellings of one identifier is how a baseline and a rule come
    // to disagree about which definition precedes a candidate.
    let Some(probe) = version_probe(id) else {
        return Baseline::Unreadable;
    };
    match (probe, precondition) {
        // Nothing precedes a first admission, and `vM.0~` opens its major.
        (VersionProbe::MajorOnly { .. }, Precondition::MustNotExist) => {
            Baseline::Exempt(Exemption::FirstAdmission)
        }
        (VersionProbe::FirstMinor { .. }, Precondition::MustNotExist) => {
            Baseline::Exempt(Exemption::FirstMinor)
        }
        (VersionProbe::LaterMinor { predecessor, .. }, Precondition::MustNotExist) => {
            Baseline::PrecedingMinor {
                gts_id: predecessor,
            }
        }
        // Every revision compares against its own current definition. For a
        // minor-bearing identifier that arm is unreachable (see above); it answers
        // with the non-waivable edge so reaching it cannot weaken the check.
        (_, Precondition::Version(_)) => Baseline::CurrentRevision,
    }
}

/// Compare a candidate against its baseline through the **only** entry point P0
/// uses.
///
/// `GtsStore::compare_documents` resolves both sides against the store before
/// classifying anything, which SPEC §7 prerequisite 6 requires: a level that is
/// closed only through a `$ref` to its base is misclassified when the authored
/// documents are compared. `GtsStore::is_minor_compatible` compares unresolved
/// content and is never called from this gear.
///
/// **`baseline` is the old side.** Adding an optional property to a closed level is
/// backward compatible in one direction and incompatible in the other, so a
/// transposed call yields a wrong verdict rather than a vague one. The parameter
/// order is fixed here so that exactly one call site can get it wrong, and a test
/// pins it.
///
/// # Errors
/// [`StoreError`] when either side has a reference this store cannot resolve.
/// Failing is the point: an unresolvable reference must not be reported as a
/// verdict (`principle-fail-closed`).
pub fn backward_comparison(
    store: &GtsStore,
    baseline: &Value,
    candidate: &Value,
) -> Result<SchemaComparison, StoreError> {
    store.compare_documents(baseline, candidate)
}

/// What a verdict means for admission: `None` admits, `Some` refuses under that
/// reason.
///
/// `forced` is an **accepted** waiver — the deployment enabled
/// `allow_compatibility_force` and the candidate's baseline is the cross-minor one
/// (ADR-0004). Establishing both is the caller's job; by the time a verdict is being
/// read, `force` waives the check whichever way it came out, because what ADR-0004
/// permits waiving is the check and not one of its outcomes.
#[must_use]
pub const fn refusal(
    verdict: CompatibilityVerdict,
    forced: bool,
) -> Option<AdmissionFailureReason> {
    match verdict {
        CompatibilityVerdict::Compatible => None,
        _ if forced => None,
        // Never collapsed into one code: ADR-0003 makes the undecided verdict
        // distinct from the incompatible one, and SPEC §16.12 makes that
        // distinction observable.
        CompatibilityVerdict::Incompatible => {
            Some(AdmissionFailureReason::IncompatibleWithBaseline)
        }
        CompatibilityVerdict::Unknown => Some(AdmissionFailureReason::CompatibilityUndecidable),
    }
}

#[cfg(test)]
#[path = "compat_tests.rs"]
mod compat_tests;
