use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct OidcToken {
    pub access_token: String,
    pub expires_in: u64,
}
