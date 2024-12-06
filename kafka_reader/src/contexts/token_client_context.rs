use crate::auth::bearer_token_provider::BearerTokenProvider;
use rdkafka::client::OAuthToken;
use rdkafka::consumer::ConsumerContext;
use rdkafka::ClientContext;
use std::error::Error;

pub struct MainClientContext<T> {
    token_provider: T,
}

impl<T> MainClientContext<T> {
    pub fn new(token_provider: T) -> Self {
        Self { token_provider }
    }
}

impl<T: BearerTokenProvider> ClientContext for MainClientContext<T> {
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> Result<OAuthToken, Box<dyn Error>> {
        let token = self.token_provider.get_token()?;

        Ok(OAuthToken {
            token: token.access_token,
            principal_name: "".to_string(),
            lifetime_ms: token.expires_in as i64,
        })
    }
}

impl<T: BearerTokenProvider> ConsumerContext for MainClientContext<T> {}
