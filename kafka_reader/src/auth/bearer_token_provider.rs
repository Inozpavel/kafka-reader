use crate::auth::oidc_token::OidcToken;

pub trait BearerTokenProvider: Send + Sync {
    fn get_token(&self) -> Result<OidcToken, anyhow::Error>;
}
