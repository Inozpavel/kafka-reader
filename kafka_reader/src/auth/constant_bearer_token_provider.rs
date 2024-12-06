use crate::auth::bearer_token_provider::BearerTokenProvider;
use crate::auth::oidc_token::OidcToken;

pub struct ConstantBearerTokenProvider {
    token: String,
}

impl ConstantBearerTokenProvider {
    pub fn new(token: String) -> Self {
        Self { token }
    }
}

impl BearerTokenProvider for ConstantBearerTokenProvider {
    fn get_token(&self) -> Result<OidcToken, anyhow::Error> {
        Ok(OidcToken {
            access_token: self.token.clone(),
            expires_in: u64::MAX,
        })
    }
}
