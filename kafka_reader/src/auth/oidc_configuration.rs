use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct OidcConfiguration {
    pub token_endpoint: String,
}
