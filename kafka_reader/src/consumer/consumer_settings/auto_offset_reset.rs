use std::fmt::{Display, Formatter};

#[derive(Debug, Copy, Clone)]
pub enum AutoOffsetReset {
    Earliest,
    Latest,
}

impl Display for AutoOffsetReset {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            AutoOffsetReset::Earliest => write!(f, "earliest"),
            AutoOffsetReset::Latest => write!(f, "latest"),
        }
    }
}
