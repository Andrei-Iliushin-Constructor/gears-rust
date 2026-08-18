use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct GithubMirrorConfig {
    #[serde(default = "default_api_base_url")]
    pub api_base_url: String,
}

impl Default for GithubMirrorConfig {
    fn default() -> Self {
        Self {
            api_base_url: default_api_base_url(),
        }
    }
}

fn default_api_base_url() -> String {
    "https://api.github.com".to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_points_at_public_github_api() {
        let cfg = GithubMirrorConfig::default();
        assert_eq!(cfg.api_base_url, "https://api.github.com");
    }

    #[test]
    fn deserializes_with_missing_field_using_default() {
        let cfg: GithubMirrorConfig = serde_json::from_str("{}").unwrap_or_default();
        assert_eq!(cfg.api_base_url, "https://api.github.com");
    }

    #[test]
    fn deserializes_explicit_base_url() {
        let cfg: GithubMirrorConfig =
            serde_json::from_str(r#"{"api_base_url":"https://ghe.local/api/v3"}"#)
                .unwrap_or_default();
        assert_eq!(cfg.api_base_url, "https://ghe.local/api/v3");
    }
}
