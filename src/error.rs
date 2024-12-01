use std::io;

#[derive(Debug)]
pub enum Error {
    Io(io::ErrorKind),
    HeaderNotFound(String),
    Other(String)
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let text = match self {
            Error::Io(kind) => kind.to_string(),
            Error::HeaderNotFound(h) => format!("Header {h} doesn't exist"),
            Error::Other(s) => s.to_string(),
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
