use toolkit_macros::domain_model;

pub const GEAR_NAME: &str = "github-mirror";

#[domain_model]
#[derive(Debug, Clone)]
pub struct ServiceConfig {
    pub api_base_url: String,
}

#[domain_model]
#[derive(Debug, Clone)]
pub struct MirrorStatus {
    pub gear: String,
    pub version: String,
    pub api_base_url: String,
}

#[domain_model]
pub struct Service {
    config: ServiceConfig,
}

impl Service {
    #[must_use]
    pub fn new(config: ServiceConfig) -> Self {
        Self { config }
    }

    #[must_use]
    pub fn status(&self) -> MirrorStatus {
        MirrorStatus {
            gear: GEAR_NAME.to_owned(),
            version: env!("CARGO_PKG_VERSION").to_owned(),
            api_base_url: self.config.api_base_url.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn status_reports_gear_name_and_configured_base_url() {
        let service = Service::new(ServiceConfig {
            api_base_url: "https://api.github.com".to_owned(),
        });

        let status = service.status();

        assert_eq!(status.gear, GEAR_NAME);
        assert_eq!(status.api_base_url, "https://api.github.com");
        assert!(!status.version.is_empty());
    }
}
