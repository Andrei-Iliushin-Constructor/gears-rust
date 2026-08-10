use std::path::PathBuf;

use serde::{Deserialize, Serialize};

/// Configuration for the `file_parser` gear
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileParserConfig {
    #[serde(default = "default_max_file_size_mb")]
    pub max_file_size_mb: u64,

    /// Base directory for local file parsing (**required at runtime**). Only
    /// files under this directory (after symlink resolution / canonicalization)
    /// are allowed.  The gear will fail to start if this field is missing or
    /// the path cannot be resolved.
    pub allowed_local_base_dir: PathBuf,
}

fn default_max_file_size_mb() -> u64 {
    100
}
