//! Configuration for the license resolver gear.

use serde::Deserialize;

/// Gear configuration.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct LicenseResolverConfig {
    /// Vendor selector used to pick a backend plugin implementation.
    ///
    /// The gear queries types-registry for plugin instances matching this
    /// vendor and selects the one with the lowest priority.
    pub vendor: String,
}

impl Default for LicenseResolverConfig {
    fn default() -> Self {
        Self {
            vendor: "constructorfabric".to_owned(),
        }
    }
}
