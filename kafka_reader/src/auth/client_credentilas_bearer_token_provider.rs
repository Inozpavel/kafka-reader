use crate::auth::bearer_token_provider::BearerTokenProvider;
use crate::auth::oidc_configuration::OidcConfiguration;
use crate::auth::oidc_token::OidcToken;
use anyhow::{bail, Context};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tokio_util::sync::CancellationToken;
use tracing::error;

pub struct ClientCredentialsBearerTokenProvider {
    token_refresh_data: Arc<TokenRefreshData>,
    token: Option<watch::Receiver<Option<OidcToken>>>,
    token_refresh_cancellation_token: CancellationToken,
}

pub struct TokenRefreshData {
    oidc_url: String,
    client_id: String,
    client_secret: String,
}

impl Drop for ClientCredentialsBearerTokenProvider {
    fn drop(&mut self) {
        self.token_refresh_cancellation_token.cancel()
    }
}
impl ClientCredentialsBearerTokenProvider {
    pub fn new(token_refresh_data: TokenRefreshData) -> Self {
        Self {
            token_refresh_data: Arc::new(token_refresh_data),
            token: None,
            token_refresh_cancellation_token: CancellationToken::new(),
        }
    }

    pub fn start_refresh(&mut self) {
        self.token_refresh_cancellation_token.cancel();
        self.token_refresh_cancellation_token = CancellationToken::new();
        let refresh_data = self.token_refresh_data.clone();

        let (transmitter, receiver) = watch::channel(None);

        let token = self.token_refresh_cancellation_token.clone();
        tokio::task::spawn(async move {
            while !token.is_cancelled() {
                let new_token = match Self::refresh_token(&refresh_data).await {
                    Ok(token) => token,
                    Err(e) => {
                        error!("Bearer token refresh error\n{e:?}");
                        tokio::time::sleep(Duration::from_millis(1000)).await;
                        continue;
                    }
                };

                let next_refresh_sleep_time = (new_token.expires_in / 3).max(1000);

                let _ = transmitter.send(Some(new_token));

                tokio::time::sleep(Duration::from_millis(next_refresh_sleep_time)).await
            }
        });
        self.token = Some(receiver);
    }

    pub fn new_with_started_token_refresh(token_refresh_data: TokenRefreshData) -> Self {
        let mut result = Self::new(token_refresh_data);
        result.start_refresh();
        result
    }

    pub async fn refresh_token(
        token_refresh_data: &TokenRefreshData,
    ) -> Result<OidcToken, anyhow::Error> {
        let client = reqwest::Client::new();
        let oidc_configuration = client
            .get(&token_refresh_data.oidc_url)
            .send()
            .await
            .context("While sending request for oidc configuration")?
            .error_for_status()
            .context("While checking oidc configuration response status")?
            .json::<OidcConfiguration>()
            .await
            .context("While parsing oidc configuration")?;

        let form_data = [
            ("client_id", &*token_refresh_data.client_id),
            ("client_secret", &*token_refresh_data.client_secret),
            ("grant_type", "client_credentials"),
        ];

        let token_response = client
            .put(&oidc_configuration.token_endpoint)
            .form(&form_data)
            .send()
            .await
            .context("While sending request for new bearer token")?
            .error_for_status()
            .context("While checking token response status")?
            .json::<OidcToken>()
            .await
            .context("While parsing token response")?;

        Ok(token_response)
    }
}

impl BearerTokenProvider for ClientCredentialsBearerTokenProvider {
    fn get_token(&self) -> Result<OidcToken, anyhow::Error> {
        let Some(token) = &self.token else {
            bail!("Token provider update wasn't called");
        };

        let token = match &*token.borrow() {
            None => tokio::runtime::Handle::current()
                .block_on(Self::refresh_token(&self.token_refresh_data))
                .context("While refreshing token")?,
            Some(token_value) => token_value.clone(),
        };

        Ok(token)
    }
}
