use serde::Deserialize;

use crate::domain::scope::{CollectionMode, ScopeConfig};
use crate::infra::github::compression::Compression;
use toolkit_utils::var_expand::ExpandVarsError;

#[derive(Debug, Clone, Deserialize)]
pub struct GithubMirrorConfig {
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
    /// Temporary shortcut until credstore integration (gears-rust#4534):
    /// GitHub token used by sync. Unauthenticated requests work for public
    /// repositories at a much lower rate limit.
    ///
    /// Supports `${VAR}` and `${VAR:-default}` so the token can live in the
    /// environment instead of a checked-in config file; call
    /// [`GithubMirrorConfig::resolved_token`] rather than reading the field.
    #[serde(default)]
    pub github_token: Option<String>,
    /// What a sync collects when the request does not say (PRD §5.4).
    ///
    /// The shipped default differs from [`ScopeConfig::default`] in one
    /// place: timeline collection is on. The reference implementation leaves
    /// it off as a high-volume, low-signal feed, but the mirror has always
    /// collected it and turning it off here would silently shrink what an
    /// existing deployment stores.
    #[serde(default = "default_scope")]
    pub scope: ScopeConfig,
    /// How cached response bodies are stored: `none`, `gzip` or `zstd`
    /// (PRD §5.6). GitHub JSON gzips to roughly a fifth of its size, so the
    /// default is on.
    #[serde(default = "default_compression")]
    pub cache_compression: String,
    /// How many repositories the gear syncs at the same time
    /// (PRD §6.1 "parallel synchronization of multiple repositories").
    ///
    /// Zero is read as one: a queue with no worker would accept syncs and
    /// never run them.
    #[serde(default = "default_max_concurrent_syncs")]
    pub max_concurrent_syncs: usize,
    /// Ceiling on GitHub requests in flight across every running sync.
    ///
    /// GitHub's secondary rate limit triggers on concurrency rather than
    /// volume, and the PRD's threshold is "zero bans with parallelism <= 8"
    /// (PRD §6.1), so the default sits at that bound. Without this ceiling
    /// each extra concurrent repository would multiply the request rate.
    #[serde(default = "default_max_concurrent_requests")]
    pub max_concurrent_requests: usize,
}

/// Repositories synced at once when the config says nothing: enough to keep
/// the queue moving, low enough that one tenant's backlog is not the whole
/// gear's work.
fn default_max_concurrent_syncs() -> usize {
    4
}

/// GitHub requests in flight at once when the config says nothing.
fn default_max_concurrent_requests() -> usize {
    8
}

fn default_compression() -> String {
    "gzip".to_owned()
}

/// The gear's shipped scope: the type's default, with timeline turned on to
/// preserve the behaviour the mirror has had since #4532.
fn default_scope() -> ScopeConfig {
    let mut scope = ScopeConfig::default();
    scope.collection.timeline = CollectionMode::Open;
    scope
}

impl GithubMirrorConfig {
    /// The configured compression mode.
    ///
    /// # Errors
    /// `Validation` when the config names a mode that does not exist, so a
    /// typo fails at startup rather than at the first cache write.
    pub fn resolved_compression(&self) -> Result<Compression, crate::domain::error::DomainError> {
        Compression::parse(&self.cache_compression)
    }

    /// The token with any `${VAR}` reference expanded from the environment.
    ///
    /// # Errors
    /// Returns the expansion error when the config names a variable that is
    /// not set and gives no default, so a typo fails loudly at startup
    /// instead of silently syncing unauthenticated.
    pub fn resolved_token(&self) -> Result<Option<String>, ExpandVarsError> {
        self.github_token
            .as_deref()
            .map(toolkit_utils::var_expand::expand_env_vars)
            .transpose()
            .map(|token| token.filter(|t| !t.is_empty()))
    }

    /// The configured GitHub API base URL, validated.
    ///
    /// Everything the gear fetches is built on this value, and it is echoed
    /// by the health endpoint, so a malformed or non-HTTP value should stop
    /// the gear at startup instead of producing garbage requests later.
    ///
    /// # Errors
    /// `Validation` when the value does not parse as a URL or its scheme is
    /// not `http`/`https`.
    pub fn resolved_api_base_url(&self) -> Result<String, crate::domain::error::DomainError> {
        let parsed = url::Url::parse(&self.api_base_url).map_err(|e| {
            crate::domain::error::DomainError::Validation {
                field: "api_base_url".to_owned(),
                message: format!("`{}` is not a valid URL: {e}", self.api_base_url),
            }
        })?;
        if !matches!(parsed.scheme(), "http" | "https") {
            return Err(crate::domain::error::DomainError::Validation {
                field: "api_base_url".to_owned(),
                message: format!(
                    "`{}` must use http or https, not `{}`",
                    self.api_base_url,
                    parsed.scheme()
                ),
            });
        }
        Ok(self.api_base_url.clone())
    }
}

impl Default for GithubMirrorConfig {
    fn default() -> Self {
        Self {
            api_base_url: default_api_base_url(),
            github_token: None,
            scope: default_scope(),
            cache_compression: default_compression(),
            max_concurrent_syncs: default_max_concurrent_syncs(),
            max_concurrent_requests: default_max_concurrent_requests(),
        }
    }
}

fn default_api_base_url() -> String {
    "https://api.github.com".to_owned()
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
mod tests {
    use super::*;

    #[test]
    fn default_points_at_public_github_api() {
        let cfg = GithubMirrorConfig::default();
        assert_eq!(cfg.api_base_url, "https://api.github.com");
    }

    #[test]
    fn deserializes_with_missing_field_using_default() {
        let cfg: GithubMirrorConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(cfg.api_base_url, "https://api.github.com");
    }

    #[test]
    fn deserializes_explicit_base_url() {
        let cfg: GithubMirrorConfig =
            serde_json::from_str(r#"{"api_base_url":"https://ghe.local/api/v3"}"#).unwrap();
        assert_eq!(cfg.api_base_url, "https://ghe.local/api/v3");
    }

    #[test]
    fn a_literal_token_is_returned_as_is() {
        let cfg: GithubMirrorConfig =
            serde_json::from_str(r#"{"github_token":"ghp_literal"}"#).expect("config must parse");
        assert_eq!(
            cfg.resolved_token().unwrap().as_deref(),
            Some("ghp_literal")
        );
    }

    #[test]
    fn no_token_stays_none() {
        let cfg = GithubMirrorConfig::default();
        assert_eq!(cfg.resolved_token().unwrap(), None);
    }

    #[test]
    fn a_variable_reference_falls_back_to_its_default() {
        let cfg: GithubMirrorConfig = serde_json::from_str(
            r#"{"github_token":"${GH_MIRROR_UNSET_TOKEN:-ghp_from_default}"}"#,
        )
        .expect("config must parse");
        assert_eq!(
            cfg.resolved_token().unwrap().as_deref(),
            Some("ghp_from_default"),
            "the config must read the variable, not the literal text"
        );
    }

    #[test]
    fn an_unset_variable_with_an_empty_default_means_no_token() {
        let cfg: GithubMirrorConfig =
            serde_json::from_str(r#"{"github_token":"${GH_MIRROR_UNSET_TOKEN:-}"}"#)
                .expect("config must parse");
        assert_eq!(
            cfg.resolved_token().unwrap(),
            None,
            "an empty expansion must sync unauthenticated, not send an empty header"
        );
    }

    #[test]
    fn a_valid_base_url_passes_validation() {
        let cfg = GithubMirrorConfig::default();
        assert_eq!(
            cfg.resolved_api_base_url().unwrap(),
            "https://api.github.com"
        );
    }

    #[test]
    fn a_base_url_that_is_not_a_url_fails_validation() {
        let cfg: GithubMirrorConfig =
            serde_json::from_str(r#"{"api_base_url":"not a url at all"}"#)
                .expect("config must parse");
        let err = cfg.resolved_api_base_url().unwrap_err();
        assert!(
            matches!(err, crate::domain::error::DomainError::Validation { ref field, .. } if field == "api_base_url"),
            "expected a Validation error on api_base_url, got {err:?}"
        );
    }

    #[test]
    fn a_non_http_base_url_scheme_fails_validation() {
        let cfg: GithubMirrorConfig =
            serde_json::from_str(r#"{"api_base_url":"ftp://api.github.com"}"#)
                .expect("config must parse");
        let err = cfg.resolved_api_base_url().unwrap_err();
        assert!(
            matches!(err, crate::domain::error::DomainError::Validation { ref message, .. } if message.contains("http")),
            "expected the error to name the allowed schemes, got {err:?}"
        );
    }

    #[test]
    fn a_wrongly_typed_config_value_fails_to_parse() {
        assert!(
            serde_json::from_str::<GithubMirrorConfig>(r#"{"api_base_url":42}"#).is_err(),
            "a non-string api_base_url must be rejected at parse time"
        );
    }

    #[test]
    fn an_unset_variable_without_a_default_fails_loudly() {
        let cfg: GithubMirrorConfig =
            serde_json::from_str(r#"{"github_token":"${GH_MIRROR_MISSING_TOKEN}"}"#)
                .expect("config must parse");
        assert!(
            cfg.resolved_token().is_err(),
            "a typo in the variable name must not silently drop the token"
        );
    }
}
