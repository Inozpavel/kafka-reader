use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone)]
pub enum SecurityProtocol {
    Plaintext,
    Ssl,
}

impl Display for SecurityProtocol {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            SecurityProtocol::Plaintext => write!(f, "plaintext"),
            SecurityProtocol::Ssl => write!(f, "ssl"),
        }
    }
}
