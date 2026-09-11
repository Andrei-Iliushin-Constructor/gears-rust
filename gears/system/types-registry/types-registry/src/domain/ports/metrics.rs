//! Output port for admission metrics.

use std::time::Duration;

use gts::CompatibilityVerdict;
use toolkit_macros::domain_model;

use crate::domain::admission::vector::VectorDrift;
use crate::domain::enums::{OperationItemStatus, OperationKind};

/// Which half of SPEC §8.1 refused a submission.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RefusalStage {
    /// Steps 1–8: a refusal before the request became a durable operation.
    Acceptance,
    /// Step 3 onwards, per candidate: a refusal recorded on an operation item.
    Admission,
}

impl RefusalStage {
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Acceptance => "acceptance",
            Self::Admission => "admission",
        }
    }
}

/// A candidate outcome — the only statuses a terminalized candidate can carry.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TerminalStatus {
    Succeeded,
    Unchanged,
    Failed,
}

impl TerminalStatus {
    /// Stable snake-case label value, independent of `Debug`.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Succeeded => "succeeded",
            Self::Unchanged => "unchanged",
            Self::Failed => "failed",
        }
    }
}

impl TryFrom<OperationItemStatus> for TerminalStatus {
    type Error = OperationItemStatus;

    fn try_from(status: OperationItemStatus) -> Result<Self, Self::Error> {
        match status {
            OperationItemStatus::Succeeded => Ok(Self::Succeeded),
            OperationItemStatus::Unchanged => Ok(Self::Unchanged),
            OperationItemStatus::Failed => Ok(Self::Failed),
            // Preserve the non-terminal status in the error.
            non_terminal => Err(non_terminal),
        }
    }
}

/// The two labels every per-candidate series carries (T20, `plan.md` P16 rule 2).
///
/// One struct rather than two bare arguments, because both are being added to
/// *existing* call sites: `(false, kind)` and `(kind, false)` would both compile
/// at most of them, and a transposed pair is a mislabelled series that nothing
/// fails on. Named fields make the transposition unrepresentable.
///
/// There is no `Default`, deliberately. Defaulting to `registration` / `false`
/// is exactly how a dry run ends up counted beside passes that wrote.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct PassLabels {
    pub kind: OperationKind,
    /// `true` for a rollback-only pass, which writes nothing.
    pub dry_run: bool,
}

impl PassLabels {
    #[must_use]
    pub const fn new(kind: OperationKind, dry_run: bool) -> Self {
        Self { kind, dry_run }
    }

    /// Stable snake-case label value for the operation kind.
    #[must_use]
    pub const fn kind_label(self) -> &'static str {
        match self.kind {
            OperationKind::Registration => "registration",
            OperationKind::Deletion => "deletion",
        }
    }
}

/// The admission path's instrument set.
pub trait AdmissionMetrics: std::fmt::Debug + Send + Sync {
    /// Count initial unchanged probes by hit or miss.
    fn unchanged_probe(&self, hit: bool);

    /// Count candidates terminalized by this pass, by status, mode and kind.
    fn candidate_terminalized(&self, status: TerminalStatus, labels: PassLabels);

    /// `types_registry_refusals_total{stage,reason,kind,dry_run}` — one increment
    /// per refusal.
    fn refused(&self, stage: RefusalStage, reason: &'static str, labels: PassLabels);

    /// Count each computed verdict in
    /// `types_registry_compat_verdicts_total{verdict,forced,dry_run}`.
    /// Includes compatible verdicts, which do not increment [`Self::refused`].
    /// Exempt candidates emit nothing; their baseline is recorded on the unit span.
    /// `forced` records the effective ADR-0004 waiver.
    ///
    /// Takes the whole [`PassLabels`] and renders only `dry_run` from it: a verdict
    /// is computed for a registration and never for a deletion, so a `kind` label
    /// here would be one constant series — noise rather than signal.
    fn compat_verdict(&self, verdict: CompatibilityVerdict, forced: bool, labels: PassLabels);

    /// Count revalidation retries by drift.
    fn revalidation_retried(&self, drift: &VectorDrift);

    /// Record dependents rewritten by one revision, including zero.
    ///
    /// **A dry run records nothing**, and that is decided here rather than left to
    /// the caller: this histogram answers how close a deployment runs to
    /// `limits.activation_write_set`, and a rollback-only pass rewrote no
    /// dependents, so its hypothetical set is not a data point about that pressure.
    /// The case that matters stays visible — exceeding the bound is a refusal, and
    /// refusals carry `dry_run`.
    fn observe_activation_write_set(&self, refreshed: usize, labels: PassLabels);

    /// `types_registry_operation_duration_seconds` — one admission pass, wall-clock.
    fn observe_operation_duration(&self, elapsed: Duration);
}

/// Instruments that count nothing, for a caller with no meter to inject.
#[domain_model]
#[derive(Debug, Default)]
pub struct NoopMetrics;

impl AdmissionMetrics for NoopMetrics {
    fn unchanged_probe(&self, _hit: bool) {}

    fn candidate_terminalized(&self, _status: TerminalStatus, _labels: PassLabels) {}

    fn refused(&self, _stage: RefusalStage, _reason: &'static str, _labels: PassLabels) {}

    fn compat_verdict(&self, _verdict: CompatibilityVerdict, _forced: bool, _labels: PassLabels) {}

    fn revalidation_retried(&self, _drift: &VectorDrift) {}

    fn observe_activation_write_set(&self, _refreshed: usize, _labels: PassLabels) {}

    fn observe_operation_duration(&self, _elapsed: Duration) {}
}
