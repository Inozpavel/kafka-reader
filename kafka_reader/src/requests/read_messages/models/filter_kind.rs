use regex::Regex;

#[derive(Debug)]
pub enum FilterKind {
    String(String),
    Regex(Regex),
}
