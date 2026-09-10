//! Admission tracing spans.

use tracing::{Span, field};
use uuid::Uuid;

use crate::domain::enums::OperationKind;

/// The label an operation's kind carries.
const fn kind_label(kind: OperationKind) -> &'static str {
    match kind {
        OperationKind::Registration => "registration",
        OperationKind::Deletion => "deletion",
    }
}

/// The span covering one admission pass over one operation.
#[must_use]
pub fn operation_span(operation_id: Uuid) -> Span {
    tracing::info_span!(
        "types_registry.admission.operation",
        %operation_id,
        kind = field::Empty,
        dry_run = field::Empty,
    )
}

/// Fill in the two fields [`operation_span`] left empty.
pub fn record_operation_facts(span: &Span, kind: OperationKind, dry_run: bool) {
    span.record("kind", kind_label(kind));
    span.record("dry_run", dry_run);
}

/// The span covering one admission unit — one candidate, one operation item.
///
/// The two GTS versions are recorded at creation because they are constants of the
/// running binary: a verdict means whatever the checker that produced it meant, and
/// a checker upgrade can change the verdict for an unchanged pair of schemas
/// (ADR-0003). The compatibility fields are left empty for
/// [`record_compat_facts`] — they are what the evaluation *learns*.
#[must_use]
pub fn unit_span(
    operation_id: Uuid,
    gts_id: &str,
    kind: OperationKind,
    dry_run: bool,
    operation_item_id: i64,
) -> Span {
    tracing::info_span!(
        "types_registry.admission.unit",
        %operation_id,
        gts_id,
        kind = kind_label(kind),
        dry_run,
        operation_item_id,
        gts_spec_version = gts::GTS_SPECIFICATION_VERSION,
        gts_impl_version = gts::GTS_IMPLEMENTATION_VERSION,
        baseline = field::Empty,
        baseline_gts_id = field::Empty,
        baseline_revision = field::Empty,
        compat_verdict = field::Empty,
    )
}

/// What the compatibility check learned about one candidate.
///
/// **Identifiers are span fields, never metric labels** (SPEC §8.6): a baseline
/// identifier is unbounded, so a per-event field is the only place it can go. The
/// metric carries the bounded half — the verdict and the waiver — and this carries
/// the half that says *which definition* produced it.
#[derive(Clone, Copy, Debug)]
pub struct CompatFacts<'a> {
    /// Which selection this was, as a stable token: `current_revision`,
    /// `preceding_minor`, or one of the `exempt_*` cases.
    pub baseline: &'static str,
    /// The baseline's identifier, absent where no comparison was owed.
    pub gts_id: Option<&'a str>,
    /// The baseline's revision number, absent for the same reason.
    pub revision: Option<i32>,
    /// The verdict, absent where no comparison ran. An absent verdict beside a
    /// present `baseline` token is exactly how an exemption reads.
    pub verdict: Option<&'static str>,
}

/// Fill in the compatibility fields [`unit_span`] left empty.
///
/// Called for a refused candidate as well as an admitted one: a refusal is the case
/// where knowing the baseline matters most.
pub fn record_compat_facts(span: &Span, facts: CompatFacts<'_>) {
    span.record("baseline", facts.baseline);
    if let Some(gts_id) = facts.gts_id {
        span.record("baseline_gts_id", gts_id);
    }
    if let Some(revision) = facts.revision {
        span.record("baseline_revision", revision);
    }
    if let Some(verdict) = facts.verdict {
        span.record("compat_verdict", verdict);
    }
}

#[cfg(test)]
#[cfg_attr(coverage_nightly, coverage(off))]
#[path = "observability_tests.rs"]
mod observability_tests;
