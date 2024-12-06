use crate::auth::bearer_token_provider::BearerTokenProvider;
use crate::auth::oidc_token::OidcToken;
use anyhow::bail;

pub struct EmptyBearerTokenProvider;

impl BearerTokenProvider for EmptyBearerTokenProvider {
    fn get_token(&self) -> Result<OidcToken, anyhow::Error> {
        bail!("Token won't be computed for NoTokenBearerTokenProvider")
    }
}
