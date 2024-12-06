use crate::auth::bearer_token_provider::BearerTokenProvider;
use crate::auth::client_credentials_bearer_token_provider::{
    ClientCredentialsBearerTokenProvider, TokenRefreshData,
};
use crate::auth::constant_bearer_token_provider::ConstantBearerTokenProvider;
use crate::auth::empty_bearer_token_provider::EmptyBearerTokenProvider;
use crate::connection_settings::ConnectionSettings;
use crate::consumer::{SaslMechanism, SecurityProtocol};
use crate::contexts::token_client_context::MainClientContext;

pub fn build_main_client_context(
    connection_settings: &ConnectionSettings,
) -> MainClientContext<Box<dyn BearerTokenProvider>> {
    let context: MainClientContext<Box<dyn BearerTokenProvider>> =
        match &connection_settings.security_protocol {
            SecurityProtocol::Plaintext | SecurityProtocol::Ssl => {
                MainClientContext::new(Box::new(EmptyBearerTokenProvider))
            }
            SecurityProtocol::SaslPlaintext(mechanism) => {
                let provider = sasl_mechanism_to_token_provider(mechanism);
                MainClientContext::new(provider)
            }
            SecurityProtocol::SaslSsl(mechanism) => {
                let provider = sasl_mechanism_to_token_provider(mechanism);
                MainClientContext::new(provider)
            }
        };

    context
}

fn sasl_mechanism_to_token_provider(
    sasl_mechanism: &SaslMechanism,
) -> Box<dyn BearerTokenProvider> {
    match sasl_mechanism {
        SaslMechanism::ConstantToken(token) => {
            Box::new(ConstantBearerTokenProvider::new(token.clone()))
        }
        SaslMechanism::BearerClientCredentials(credentials) => Box::new(
            ClientCredentialsBearerTokenProvider::new_with_started_token_refresh(
                TokenRefreshData {
                    client_secret: credentials.client_secret.clone(),
                    client_id: credentials.client_id.clone(),
                    oidc_url: credentials.open_id_configuration_url.clone(),
                },
            ),
        ),
    }
}
