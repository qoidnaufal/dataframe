use std::{
    collections::HashMap,
    io::{self, BufReader, Read},
    path::Path,
};

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

#[derive(Debug)]
pub struct DataFrame {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl DataFrame {
    pub fn read_csv<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = std::fs::File::open(&path)?;
        let mut buf = BufReader::new(file);

        let mut s = String::new();
        buf.read_to_string(&mut s)?;

        Ok(Self::read_str(s))
    }

    pub fn read_str(input: String) -> Self {
        let data = input
            .lines()
            .map(|line| line.split(",").map(ToString::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let headers = data[0].clone();
        let rows = data[1..].to_vec();

        Self { headers, rows }
    }

    pub fn col(&self, header: &str) -> Option<Vec<&str>> {
        let querry = self.headers.iter().position(|h| *h == header);
        if let Some(idx) = querry {
            Some(self.rows.iter().map(|row| row[idx].as_str()).collect::<Vec<_>>())
        } else {
            None
        }
    }

    pub fn row(&self, idx: usize) -> Option<HashMap<&str, &str>> {
        let len = self.rows.len();
        if idx > len {
            return None;
        }

        let res = self
            .headers
            .iter()
            .zip(&self.rows[idx])
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect::<HashMap<_, _>>();

        Some(res)
    }

    pub fn get_headers(&self) -> Vec<&str> {
        self.headers.iter().map(|header| header.as_str()).collect::<Vec<_>>()
    }

    pub fn loc<F: FnMut(&mut String)>(&mut self, header: &str, mut f: F) -> Result<(), Error> {
        let pos = self.headers.iter().position(|h| h == header);
        if let Some(idx) = pos {
            self.rows.iter_mut().for_each(|row| {
                row.get_mut(idx).map(|s| f(s));
            });
            Ok(())
        } else {
            return Err(Error::HeaderNotFound(header.to_string()));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read() -> DataFrame {
        let csv = "name,nationality,xg
Lionel Messi,Argentine,66.66
C. Ronaldo,Portugal,0.69
Darwin Nunez,Uruguay,69.69
M. Balotello,Italia,8.88
";
        DataFrame::read_str(csv.to_string())
    }

    #[test]
    fn col() {
        let df = read();
        let headers = df.get_headers();

        headers.iter().for_each(|header| {
            let col = df.col(*header);
            assert!(col.is_some());
        });
    }

    #[test]
    fn loc() -> Result<(), Error> {
        let mut df = read();
        let headers = df.get_headers();
        let header = headers.first().map(ToString::to_string).unwrap();

        df.loc(header.as_str(), |column| {
            *column += " modify";
        })?;

        df.col(header.as_str()).map(|col| {
            col.iter().for_each(|val| {
                assert!(val.contains("modify"));
            });
        });

        Ok(())
    }
}
