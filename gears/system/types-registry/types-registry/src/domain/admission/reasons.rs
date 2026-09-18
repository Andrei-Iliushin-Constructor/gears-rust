//! Stable admission failure codes used in stored outcomes and metrics.

use toolkit_macros::domain_model;

/// A candidate refusal, or a code preserved from another service version.
#[domain_model]
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum AdmissionFailureReason {
    ActivationWriteSetExceeded,
    /// A permanent system failure or exhausted delivery budget stopped admission,
    /// so the candidate was never decided. Distinct from every other reason here, which
    /// states something about the candidate: this one states that admission
    /// stopped trying.
    AdmissionAbandoned,
    AlreadyExists,
    /// The baseline's own references no longer resolve, so no comparison could be
    /// performed. Distinct from an undecidable one: the check never ran.
    BaselineUnresolvable,
    /// A selected in-batch dependency — an authored `$ref`, the derivation base
    /// or an Instance's conforming type — did not reach a successful outcome.
    BlockedByDependency,
    /// The preceding minor of a minor-bearing candidate was submitted in the same
    /// batch and failed, so the implicit `vM.(n-1)~ -> vM.n~` edge never closed.
    BlockedByPredecessor,
    /// `compare_documents` returned `Unknown`, distinct from an incompatible verdict.
    CompatibilityUndecidable,
    /// A required base, conforming type or schema reference is absent.
    DependencyNotFound,
    DependentInvalid,
    /// The declared dialect differs from the major's pinned dialect (ADR-0014).
    DialectChanged,
    EntityDeleted,
    FamilyKindConflict,
    /// Deleting this entity would strand a live direct registered dependant.
    /// The refusal reports how many; never which ones.
    HasRegisteredDependents,
    FamilyShapeConflict,
    /// `Valid(baseline) ⊆ Valid(candidate)` does not hold (ADR-0003).
    IncompatibleWithBaseline,
    /// ADR-0015: a registered Instance cannot conform to a major-0 Type Schema.
    InstanceOfMajorZero,
    InvalidDocument,
    InvalidIdentifier,
    InvalidSchema,
    InvalidValue,
    MissingPredecessor,
    /// The deletion target is not `ACTIVE`. Distinct from
    /// [`Self::EntityDeleted`], which says the entity a *revision* wanted is
    /// gone: this one says the deletion has nothing left to do, and a second
    /// attempt must never read as "retry with a newer version".
    NotActive,
    PreconditionFailed,
    ResolutionClosureExceeded,
    ResolvedDocumentTooLarge,
    RevalidationExhausted,
    /// ADR-0015 quarantine: a stable candidate's immediate derivation base names
    /// a major-0 entity.
    StableDerivesFromMajorZero,
    /// ADR-0015: a stable candidate `$ref`s a major-0 entity.
    StableRefsMajorZero,
    UnparsablePayload,
    UnreadableVersion,
    UnrecognizedPayload,
    /// Preserve an unrecognized stored code without adding a metric label.
    Unknown(String),
}

impl AdmissionFailureReason {
    /// Restore a typed reason while retaining codes from other service versions.
    #[must_use]
    pub fn from_wire(code: &str) -> Self {
        match code {
            "activation_write_set_exceeded" => Self::ActivationWriteSetExceeded,
            "admission_abandoned" => Self::AdmissionAbandoned,
            "already_exists" => Self::AlreadyExists,
            "baseline_unresolvable" => Self::BaselineUnresolvable,
            "blocked_by_dependency" => Self::BlockedByDependency,
            "blocked_by_predecessor" => Self::BlockedByPredecessor,
            "compatibility_undecidable" => Self::CompatibilityUndecidable,
            "dependency_not_found" => Self::DependencyNotFound,
            "dependent_invalid" => Self::DependentInvalid,
            "dialect_changed" => Self::DialectChanged,
            "entity_deleted" => Self::EntityDeleted,
            "family_kind_conflict" => Self::FamilyKindConflict,
            "has_registered_dependents" => Self::HasRegisteredDependents,
            "family_shape_conflict" => Self::FamilyShapeConflict,
            "incompatible_with_baseline" => Self::IncompatibleWithBaseline,
            "instance_of_major_zero" => Self::InstanceOfMajorZero,
            "invalid_document" => Self::InvalidDocument,
            "invalid_identifier" => Self::InvalidIdentifier,
            "invalid_schema" => Self::InvalidSchema,
            "invalid_value" => Self::InvalidValue,
            "missing_predecessor" => Self::MissingPredecessor,
            "not_active" => Self::NotActive,
            "precondition_failed" => Self::PreconditionFailed,
            "resolution_closure_exceeded" => Self::ResolutionClosureExceeded,
            "resolved_document_too_large" => Self::ResolvedDocumentTooLarge,
            "revalidation_exhausted" => Self::RevalidationExhausted,
            "stable_derives_from_major_zero" => Self::StableDerivesFromMajorZero,
            "stable_refs_major_zero" => Self::StableRefsMajorZero,
            "unparsable_payload" => Self::UnparsablePayload,
            "unreadable_version" => Self::UnreadableVersion,
            "unrecognized_payload" => Self::UnrecognizedPayload,
            unknown => Self::Unknown(unknown.to_owned()),
        }
    }

    /// The stable code persisted in error payloads and returned to clients.
    #[must_use]
    pub fn as_str(&self) -> &str {
        match self {
            Self::Unknown(code) => code,
            known => known.metric_label(),
        }
    }

    /// A bounded metric label: unknown codes share the single `other` series.
    #[must_use]
    pub const fn metric_label(&self) -> &'static str {
        match self {
            Self::ActivationWriteSetExceeded => "activation_write_set_exceeded",
            Self::AdmissionAbandoned => "admission_abandoned",
            Self::AlreadyExists => "already_exists",
            Self::BaselineUnresolvable => "baseline_unresolvable",
            Self::BlockedByDependency => "blocked_by_dependency",
            Self::BlockedByPredecessor => "blocked_by_predecessor",
            Self::CompatibilityUndecidable => "compatibility_undecidable",
            Self::DependencyNotFound => "dependency_not_found",
            Self::DependentInvalid => "dependent_invalid",
            Self::DialectChanged => "dialect_changed",
            Self::EntityDeleted => "entity_deleted",
            Self::FamilyKindConflict => "family_kind_conflict",
            Self::HasRegisteredDependents => "has_registered_dependents",
            Self::FamilyShapeConflict => "family_shape_conflict",
            Self::IncompatibleWithBaseline => "incompatible_with_baseline",
            Self::InstanceOfMajorZero => "instance_of_major_zero",
            Self::InvalidDocument => "invalid_document",
            Self::InvalidIdentifier => "invalid_identifier",
            Self::InvalidSchema => "invalid_schema",
            Self::InvalidValue => "invalid_value",
            Self::MissingPredecessor => "missing_predecessor",
            Self::NotActive => "not_active",
            Self::PreconditionFailed => "precondition_failed",
            Self::ResolutionClosureExceeded => "resolution_closure_exceeded",
            Self::ResolvedDocumentTooLarge => "resolved_document_too_large",
            Self::RevalidationExhausted => "revalidation_exhausted",
            Self::StableDerivesFromMajorZero => "stable_derives_from_major_zero",
            Self::StableRefsMajorZero => "stable_refs_major_zero",
            Self::UnparsablePayload => "unparsable_payload",
            Self::UnreadableVersion => "unreadable_version",
            Self::UnrecognizedPayload => "unrecognized_payload",
            Self::Unknown(_) => "other",
        }
    }
}

impl std::fmt::Display for AdmissionFailureReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Why delivery gave up on a message, as the `error_code` a client reads back.
///
/// A second axis to [`AdmissionFailureReason`], not a part of it: that one says
/// something about a *candidate*, this one about a *delivery*. The one place they
/// meet is [`AdmissionFailureReason::AdmissionAbandoned`], which is the reason
/// stored on the items of an operation delivery abandoned for one of the codes
/// below.
///
/// This is wire vocabulary, not a log field: it travels in the dead-letter
/// `reason` and in the item's `error_payload`, which REST hands back on
/// `GET /operations/{id}`. Those two must agree, which is why it is an enum —
/// the same reason [`AdmissionFailureReason::AdmissionAbandoned`] stopped being
/// a literal in `infra::outbox`.
///
/// `abandonment_write_failed` / `abandonment_write_timeout` are deliberately
/// absent: they name a *log* field on the terminalization attempt, are never
/// returned to a caller, and putting them here would suggest they are.
#[domain_model]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DeliveryFailure {
    /// Admission ran and failed on its own terms; the code is
    /// `WorkerError::code()`'s, already exhaustive over a closed enum.
    ///
    /// Kept as a payload rather than flattened into variants here so the two
    /// vocabularies stay one each: duplicating `WorkerError`'s codes would give
    /// this enum a second, drifting copy of them.
    Admission(&'static str),
    /// The envelope declared a `payload_type` this queue does not handle.
    UnexpectedPayloadType,
    /// The message body is not an operation `UUID`. No redelivery changes bytes.
    InvalidOperationPayload,
    /// A `ServiceError` that is not a `WorkerError`, so it carries no code of its
    /// own. Broad on purpose — the cause goes to the operator log, never here.
    ServiceFailure,
    /// The delivery budget was spent and the operation row no longer exists.
    ///
    /// Shares its string with `WorkerError::OperationNotFound`, and that is
    /// intended: both say the operation row is gone, and a client cannot act on
    /// which layer noticed.
    OperationNotFound,
    /// The delivery budget was spent and the operation is still not terminal.
    DeliveryBudgetExhausted,
    /// Admission was cut off at its own deadline, leaving the reserve that the
    /// abandonment write needs. Distinct from [`Self::DeliveryBudgetExhausted`]:
    /// that is the delivery *after* the last one admission was allowed to run.
    AdmissionDeadlineExceeded,
}

impl DeliveryFailure {
    /// The stable `error_code` string. Exhaustive, so a variant added later is a
    /// compile error rather than a silently missing code.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Admission(code) => code,
            Self::UnexpectedPayloadType => "unexpected_payload_type",
            Self::InvalidOperationPayload => "invalid_operation_payload",
            Self::ServiceFailure => "admission_service_failure",
            Self::OperationNotFound => "operation_not_found",
            Self::DeliveryBudgetExhausted => "delivery_budget_exhausted",
            Self::AdmissionDeadlineExceeded => "admission_deadline_exceeded",
        }
    }
}

impl std::fmt::Display for DeliveryFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
#[path = "reasons_tests.rs"]
mod reasons_tests;
