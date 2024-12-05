use std::fmt::{Display, Formatter};

#[derive(Debug)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
    SaslPlaintext(SaslMechanism),
    SaslSsl(SaslMechanism),
}

#[derive(Debug)]
pub enum SaslMechanism {
    ConstantToken(String),
    BearerClientCredentials(BearerClientCredentials),
}

#[derive(Debug)]
pub struct BearerClientCredentials {
    pub open_id_configuration_url: String,
    pub client_id: String,
    pub client_secret: String,
}

impl Display for SecurityProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityProtocol::Plaintext => write!(f, "plaintext"),
            SecurityProtocol::Ssl => write!(f, "ssl"),
            SecurityProtocol::SaslPlaintext(_) => write!(f, "sasl_plaintext"),
            SecurityProtocol::SaslSsl(_) => write!(f, "sasl_ssl"),
        }
    }
}
