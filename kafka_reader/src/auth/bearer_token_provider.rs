use crate::auth::oidc_token::OidcToken;

pub trait BearerTokenProvider: Send + Sync {
    fn get_token(&self) -> Result<OidcToken, anyhow::Error>;
}

impl<T: BearerTokenProvider + ?Sized> BearerTokenProvider for Box<T> {
    fn get_token(&self) -> Result<OidcToken, anyhow::Error> {
        (**self).get_token()
    }
}
