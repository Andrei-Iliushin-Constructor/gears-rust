//! The admission reason vocabulary, asserted rather than grepped (P16 rule 3).
//!
//! # What is compile-enforced, and what this file adds
//!
//! Production code already makes part of it compile-enforced: `metric_label` and
//! `from_wire` are exhaustive matches, so a new variant cannot compile without a
//! stored code and a metric label. What that does *not* catch is a code that
//! collides with another, a code that round-trips to the wrong variant, or a label
//! an operator cannot read.
//!
//! Those properties are asserted below over [`known`] — and **[`known`] is checked
//! against the module's own source**, because nothing else can. Rust has no
//! enumeration over an enum's variants without a derive, and the one available here
//! (`strum::EnumIter`, re-exported by `sea_orm`) would put a storage dependency in
//! a pure domain module. An exhaustive `match` in a test is not a substitute: it
//! forces a new variant to be *named*, but a variant named in every match and
//! omitted from the list is invisible to every assertion that only ever reads the
//! list. That gap was real here until [`the_listed_vocabulary_matches_the_enum`]
//! closed it by parsing the enum block out of `include_str!`.

#![allow(clippy::expect_used, clippy::unwrap_used)]

use super::AdmissionFailureReason as Reason;

/// Every known variant. `Unknown` is deliberately absent: it is the escape hatch
/// for a code this build does not know, not a member of the vocabulary.
fn known() -> Vec<Reason> {
    vec![
        Reason::ActivationWriteSetExceeded,
        Reason::AlreadyExists,
        Reason::BaselineUnresolvable,
        Reason::CompatibilityUndecidable,
        Reason::DependentInvalid,
        Reason::EntityDeleted,
        Reason::FamilyKindConflict,
        Reason::FamilyShapeConflict,
        Reason::IncompatibleWithBaseline,
        Reason::InvalidDocument,
        Reason::InvalidIdentifier,
        Reason::InvalidSchema,
        Reason::InvalidValue,
        Reason::MissingPredecessor,
        Reason::PreconditionFailed,
        Reason::ResolutionClosureExceeded,
        Reason::ResolvedDocumentTooLarge,
        Reason::RevalidationExhausted,
        Reason::UnparsablePayload,
        Reason::UnreadableVersion,
        Reason::UnrecognizedPayload,
    ]
}

/// The count [`known`] must have. Bumped deliberately, which is the point: a
/// variant added without a thought about the dashboards reading it fails here.
const KNOWN_VARIANTS: usize = 21;

/// The enum's variant names, read out of this module's own source.
///
/// Ugly, and the only thing that actually works: see the module header. The parse
/// is deliberately narrow — the `pub enum` line, then indented identifiers up to the
/// closing brace — and every step fails loudly rather than silently returning a
/// short list, because a parser that quietly finds nothing would turn this guard
/// into a test that always passes.
fn variant_names_in_source() -> Vec<String> {
    const SOURCE: &str = include_str!("reasons.rs");

    let (_, after) = SOURCE
        .split_once("pub enum AdmissionFailureReason {")
        .expect("the enum declaration must be found; has it been renamed?");
    let (body, _) = after
        .split_once("\n}")
        .expect("the enum body must be terminated by a closing brace at column 0");

    let names: Vec<String> = body
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty() && !line.starts_with("//") && !line.starts_with("#["))
        .map(|line| {
            line.trim_end_matches(',')
                .split(['(', ' '])
                .next()
                .unwrap_or_default()
                .to_owned()
        })
        .filter(|name| name.starts_with(|c: char| c.is_ascii_uppercase()))
        .collect();

    assert!(
        names.len() > 5,
        "the parse found only {names:?}; the enum's shape changed and this guard \
         would otherwise pass by finding nothing",
    );
    names
}

/// **The completeness guard.** Every variant the enum declares is in [`known`], and
/// every entry of [`known`] is a variant — so a reason cannot enter the vocabulary
/// without entering the assertions below, and a removed one cannot linger.
#[test]
fn the_listed_vocabulary_matches_the_enum() {
    let mut declared = variant_names_in_source();
    declared.retain(|name| name != "Unknown");
    declared.sort_unstable();

    let mut listed: Vec<String> = known().iter().map(|r| variant_name(r).to_owned()).collect();
    listed.sort_unstable();

    assert_eq!(
        listed, declared,
        "`known()` and the enum disagree: a variant was added or removed without \
         updating the list this file asserts over",
    );
    assert_eq!(
        declared.len(),
        KNOWN_VARIANTS,
        "the vocabulary changed size: update KNOWN_VARIANTS deliberately",
    );
}

/// One arm per variant: adding a variant to the enum stops this file compiling
/// until it is named here too. On its own that only forces the *name* to exist,
/// which is why [`the_listed_vocabulary_matches_the_enum`] exists.
fn variant_name(reason: &Reason) -> &'static str {
    {
        match reason {
            Reason::ActivationWriteSetExceeded => "ActivationWriteSetExceeded",
            Reason::AlreadyExists => "AlreadyExists",
            Reason::BaselineUnresolvable => "BaselineUnresolvable",
            Reason::CompatibilityUndecidable => "CompatibilityUndecidable",
            Reason::DependentInvalid => "DependentInvalid",
            Reason::EntityDeleted => "EntityDeleted",
            Reason::FamilyKindConflict => "FamilyKindConflict",
            Reason::FamilyShapeConflict => "FamilyShapeConflict",
            Reason::IncompatibleWithBaseline => "IncompatibleWithBaseline",
            Reason::InvalidDocument => "InvalidDocument",
            Reason::InvalidIdentifier => "InvalidIdentifier",
            Reason::InvalidSchema => "InvalidSchema",
            Reason::InvalidValue => "InvalidValue",
            Reason::MissingPredecessor => "MissingPredecessor",
            Reason::PreconditionFailed => "PreconditionFailed",
            Reason::ResolutionClosureExceeded => "ResolutionClosureExceeded",
            Reason::ResolvedDocumentTooLarge => "ResolvedDocumentTooLarge",
            Reason::RevalidationExhausted => "RevalidationExhausted",
            Reason::UnparsablePayload => "UnparsablePayload",
            Reason::UnreadableVersion => "UnreadableVersion",
            Reason::UnrecognizedPayload => "UnrecognizedPayload",
            Reason::Unknown(_) => "Unknown",
        }
    }
}

/// No entry appears twice, or one of them would be silently untested.
#[test]
fn no_variant_is_listed_twice() {
    let mut names: Vec<&str> = known().iter().map(variant_name).collect();
    let total = names.len();
    names.sort_unstable();
    names.dedup();
    assert_eq!(names.len(), total, "duplicate entry in `known()`");
}

/// `Unknown` is the escape hatch for a code this build cannot read, not a member of
/// the vocabulary, so it must never be listed as one.
#[test]
fn the_unknown_escape_hatch_is_not_part_of_the_vocabulary() {
    assert!(
        known().iter().all(|r| variant_name(r) != "Unknown"),
        "Unknown is listed as a vocabulary member",
    );
}

/// Every stored code restores its own variant. A code that round-trips to the
/// *wrong* variant would relabel a refusal in the metrics without any read failing.
#[test]
fn every_code_round_trips_to_the_variant_that_wrote_it() {
    for reason in known() {
        let code = reason.as_str().to_owned();
        assert_eq!(
            Reason::from_wire(&code),
            reason,
            "'{code}' did not restore the variant it came from",
        );
    }
}

/// The stored code and the metric label are the same string for a known reason —
/// which is what lets an operator move between a stored `error_payload` and a
/// dashboard without a translation table.
#[test]
fn a_known_reasons_stored_code_is_its_metric_label() {
    for reason in known() {
        assert_eq!(reason.as_str(), reason.metric_label(), "{reason:?}");
    }
}

/// No two reasons share a code. Two refusals under one code are one number an
/// operator cannot act on, which is the failure P16 exists to prevent.
#[test]
fn no_two_reasons_share_a_code() {
    let mut codes: Vec<&str> = known().iter().map(Reason::metric_label).collect();
    let total = codes.len();
    codes.sort_unstable();
    codes.dedup();
    assert_eq!(codes.len(), total, "duplicate code in the vocabulary");
}

/// Every code is a readable snake-case token, not a `Debug` rendering that would
/// change shape the day someone renames a variant.
#[test]
fn every_code_is_stable_snake_case() {
    for reason in known() {
        let code = reason.metric_label();
        assert!(!code.is_empty(), "{reason:?} has an empty code");
        assert!(
            code.chars().all(|c| c.is_ascii_lowercase() || c == '_'),
            "'{code}' is not snake-case, so it did not come from an explicit mapping",
        );
        assert!(
            !code.starts_with('_') && !code.ends_with('_') && !code.contains("__"),
            "'{code}' is malformed",
        );
    }
}

/// An unfamiliar code is **preserved** as a stored code and **bounded** as a metric
/// label. Both halves matter: a rolling deployment reads rows another version wrote,
/// and a metric whose label set is whatever those rows contain has unbounded
/// cardinality.
#[test]
fn an_unfamiliar_code_is_preserved_in_storage_and_bounded_in_metrics() {
    let restored = Reason::from_wire("something_a_later_version_wrote");
    assert_eq!(
        restored,
        Reason::Unknown("something_a_later_version_wrote".to_owned()),
    );
    assert_eq!(
        restored.as_str(),
        "something_a_later_version_wrote",
        "the row's own code survives the round trip verbatim",
    );
    assert_eq!(
        restored.metric_label(),
        "other",
        "and shares one bounded series rather than minting one of its own",
    );
}

/// `other` is reserved for the unknown bucket: no known reason may claim it, or a
/// real refusal would land in the bucket meant for codes this build cannot read.
#[test]
fn no_known_reason_claims_the_unknown_bucket() {
    assert!(
        known().iter().all(|r| r.metric_label() != "other"),
        "a known reason is hiding in the `other` series",
    );
}

/// This task's own additions are in the vocabulary, each under its own code. The
/// two compatibility refusals in particular must never collapse into one
/// (SPEC 16.12).
#[test]
fn t17s_compatibility_reasons_are_three_distinct_codes() {
    let codes = [
        Reason::IncompatibleWithBaseline.metric_label(),
        Reason::CompatibilityUndecidable.metric_label(),
        Reason::BaselineUnresolvable.metric_label(),
    ];
    let mut unique = codes.to_vec();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(unique.len(), 3, "{codes:?}");
    for code in codes {
        assert_eq!(Reason::from_wire(code).metric_label(), code);
    }
}
