use std::{
    collections::HashMap,
    io::{BufReader, Read},
    path::Path,
};

mod error;

pub use error::Error;

#[derive(Debug)]
pub enum Val {
    String(String),
    Int64(i64),
    Float64(f64),
}

impl From<&str> for Val {
    fn from(value: &str) -> Self {
        value.parse::<i64>()
            .map_or_else(|_| {
                value.parse::<f64>().map_or_else(|_| Val::String(value.to_string()),
                    Val::Float64
                )
            },
            |num| Val::Int64(num))
    }
}

#[derive(Debug)]
pub struct DataFrame {
    headers: Vec<String>,
    rows: Vec<Vec<Val>>,
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
        let rows = data[1..].iter().map(|row| {
            row.iter().map(|c| {
                Val::from(c.as_str())
            }).collect::<Vec<_>>()
        }).collect::<Vec<_>>();

        Self { headers, rows }
    }

    pub fn col(&self, header: &str) -> Option<Vec<&Val>> {
        self.headers.iter().position(|h| *h == header).map(|idx| {
            self.rows.iter().map(|row| &row[idx]).collect::<Vec<_>>()
        })
    }

    pub fn row(&self, idx: usize) -> Option<HashMap<&str, &Val>> {
        let len = self.rows.len();
        if idx >= len {
            return None;
        }

        let res = self
            .headers
            .iter()
            .zip(&self.rows[idx])
            .map(|(k, v)| (k.as_str(), v))
            .collect::<HashMap<_, _>>();

        Some(res)
    }

    pub fn get_headers(&self) -> Vec<&str> {
        self.headers.iter().map(|header| header.as_str()).collect::<Vec<_>>()
    }

    pub fn loc<F: FnMut(&mut Val)>(&mut self, header: &str, mut f: F) -> Result<(), Error> {
        self.headers.iter().position(|h| h == header).map(|idx| {
            self.rows.iter_mut().for_each(|row| {
                if let Some(s) = row.get_mut(idx) { f(s) };
            });
            
        }).ok_or(Error::HeaderNotFound(header.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn df() -> DataFrame {
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
        let df = df();
        let headers = df.get_headers();

        headers.iter().for_each(|header| {
            let col = df.col(*header);
            assert!(col.is_some());
        });
    }

    #[test]
    fn row() {
        let df = df();
        let row_3 = df.row(3);
        let row_4 = df.row(4);
        assert!(row_3.is_some());
        assert!(row_4.is_none())
    }

    #[test]
    fn loc() -> Result<(), Error> {
        let mut df = df();
        let headers = df
            .get_headers()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        headers.iter().try_for_each(|header| {
            df.loc(header, |val| {
                match val {
                    Val::String(s) => *s += " modify",
                    Val::Int64(num) => *num -= 6969,
                    Val::Float64(num) => *num += 6.9,
                }
            })?;

            Ok::<(), Error>(())
        })?;

        headers.iter().for_each(|header| {
            df.col(header).map(|col| {
                col.iter().for_each(|val| {
                    match val {
                        Val::String(s) => assert!(s.contains("modify")),
                        Val::Int64(num) => assert!(num.is_negative()),
                        Val::Float64(num) => assert!(num.signum() == 1.0),
                    }
                });
            });
        });

        Ok(())
    }
}
