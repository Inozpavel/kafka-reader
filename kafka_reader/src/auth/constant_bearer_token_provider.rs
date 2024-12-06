use crate::auth::bearer_token_provider::BearerTokenProvider;
use crate::auth::oidc_token::OidcToken;

pub struct ConstantBearerTokenProvider(pub String);

impl BearerTokenProvider for ConstantBearerTokenProvider {
    fn get_token(&self) -> Result<OidcToken, anyhow::Error> {
        Ok(OidcToken {
            access_token: self.0.clone(),
            expires_in: u64::MAX,
        })
    }
}
