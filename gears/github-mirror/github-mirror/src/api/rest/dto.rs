use crate::domain::service::MirrorStatus;

#[derive(Debug)]
#[toolkit_macros::api_dto(response)]
pub struct GithubMirrorHealthDto {
    pub gear: String,
    pub version: String,
    pub api_base_url: String,
}

impl From<MirrorStatus> for GithubMirrorHealthDto {
    fn from(status: MirrorStatus) -> Self {
        Self {
            gear: status.gear,
            version: status.version,
            api_base_url: status.api_base_url,
        }
    }
}


