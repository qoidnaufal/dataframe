use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::ErrorKind),
    HeaderNotFound(String),
    Other(String),
    ValToString,
    ValToFloat64,
    ValToInt64,
    ValToUsize
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Self::Io(kind) => kind.to_string(),
            Self::HeaderNotFound(h) => format!("Header {h} doesn't exist"),
            Self::Other(s) => s.to_string(),
            Self::ValToString
            | Self::ValToFloat64
            | Self::ValToInt64
            | Self::ValToUsize => "Incompatible type conversion".to_string()
        };

        f.write_str(text.as_str())
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value.kind())
    }
}

impl std::error::Error for Error {}
