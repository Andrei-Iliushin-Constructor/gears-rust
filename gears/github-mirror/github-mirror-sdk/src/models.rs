//! Public models for the github-mirror gear.
//!
//! Transport-agnostic data structures defining the contract between the
//! github-mirror gear and its consumers. All models carry `#[domain_model]`
//! so infrastructure types cannot leak into them.

use toolkit_macros::domain_model;

/// Runtime identity of the mirror gear.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MirrorStatus {
    pub gear: String,
    pub version: String,
    pub api_base_url: String,
}

/// A mirrored GitHub repository (minimal read-slice shape).
///
/// Field set intentionally starts small — it mirrors what the first
/// read-slice (`GET /github-mirror/v1/repos`) serves from the local store
/// and grows as further entity fields are ported.
#[domain_model]
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Repository {
    /// GitHub's numeric repository id.
    pub id: i64,
    pub owner: String,
    pub name: String,
    pub full_name: String,
    pub private: bool,
    pub description: Option<String>,
}
