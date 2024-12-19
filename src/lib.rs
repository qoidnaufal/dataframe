use std::{
    collections::HashMap,
    hash::Hash,
    io::{BufReader, Read},
    path::Path
};

mod error;

pub use error::Error;

#[derive(Debug, Clone)]
pub enum Val {
    String(String),
    Usize(usize),
    Int64(i64),
    Float64(f64),
}

impl std::fmt::Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::String(s) => write!(f, "{}", s),
            Val::Usize(u) => write!(f, "{}", u),
            Val::Int64(i) => write!(f, "{}", i),
            Val::Float64(f64) => write!(f, "{}", f64),
        }
    }
}

impl Hash for Val {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self {
            Val::String(s) => s.hash(state),
            Val::Usize(u) => u.hash(state),
            _ => panic!("f64 & i64 are not for dataframe index")
        }
    }
}

impl PartialEq for Val {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Val::String(s1), Val::String(s2)) => s1.eq(s2),
            (Val::Int64(i1), Val::Int64(i2)) => i1.eq(i2),
            (Val::Float64(f1), Val::Float64(f2)) => f1.eq(f2),
            (Val::Usize(u1), Val::Usize(u2)) => u1.eq(u2),
            _ => false
        }
    }
}

impl Eq for Val {}

impl PartialOrd for Val {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (Val::String(s1), Val::String(s2)) => Some(s1.cmp(s2)),
            (Val::Int64(i1), Val::Int64(i2)) => Some(i1.cmp(i2)),
            (Val::Float64(f1), Val::Float64(f2)) => Some(f1.total_cmp(f2)),
            (Val::Usize(u1), Val::Usize(u2)) => Some(u1.cmp(u2)),
            _ => None
        }
    }
}

impl From<&str> for Val {
    fn from(value: &str) -> Self {
        value.parse::<i64>()
            .map_or_else(|_| {
                value.parse::<f64>()
                    .map_or_else(|_| Val::String(value.to_string()),
                    Val::Float64
                )
            },
            Val::Int64
        )
    }
}

impl From<String> for Val {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&String> for Val {
    fn from(value: &String) -> Self {
        Self::String(value.clone())
    }
}

impl From<i64> for Val {
    fn from(value: i64) -> Self {
        Self::Int64(value)
    }
}

impl From<usize> for Val {
    fn from(value: usize) -> Self {
        Self::Usize(value)
    }
}

impl From<f64> for Val {
    fn from(value: f64) -> Self {
        Self::Float64(value)
    }
}

impl TryFrom<&Val> for String {
    type Error = Error;
    fn try_from(value: &Val) -> Result<Self, Self::Error> {
        match value {
            Val::String(s) => Ok(s.to_owned()),
            _ => Err(Error::ValToString)
        }
    }
}

impl TryFrom<&Val> for usize {
    type Error = Error;
    fn try_from(value: &Val) -> Result<Self, Self::Error> {
        match value {
            Val::Usize(n) => Ok(*n),
            _ => Err(Error::ValToUsize)
        }
    }
}

impl TryFrom<&Val> for i64 {
    type Error = Error;
    fn try_from(value: &Val) -> Result<Self, Self::Error> {
        match value {
            Val::Int64(n) => Ok(*n),
            _ => Err(Error::ValToInt64)
        }
    }
}

impl TryFrom<&Val> for f64 {
    type Error = Error;
    fn try_from(value: &Val) -> Result<Self, Self::Error> {
        match value {
            Val::Float64(n) => Ok(*n),
            _ => Err(Error::ValToFloat64)
        }
    }
}

impl Val {
    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float64(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Val::Int64(_))
    }

    pub fn is_usize(&self) -> bool {
        matches!(self, Val::Usize(_))
    }

    pub fn is_str(&self) -> bool {
        matches!(self, Val::String(_))
    }
}

pub struct DataFrame {
    headers: Vec<String>,
    data: Vec<Val>,
    width: usize,
    height: usize
}

impl std::fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.headers.iter().filter_map(|header| {
            let Some(col) = self.col(header) else { return None };
            let len = col.iter().map(|val| val.to_string().len()).max();
            len
        }).collect::<Vec<_>>();
        len.iter().enumerate().try_for_each(|(i, spacing)| {
            let spacing = spacing + 4;
            if i == len.len() - 1 {
                write!(f, "+{:->spacing$}+", "")
            } else {
                write!(f, "+{:->spacing$}", "")
            }
        })?;
        self.headers.iter().zip(&len).enumerate().try_for_each(|(i, (header, len))| {
            let spacing = (len - header.len()) + 5;
            if i % self.width == 0 {
                write!(f, "\n| {}{:>spacing$}", header, "| ")
            } else {
                write!(f, "{}{:>spacing$}", header, "| ")
            }
        })?;
        writeln!(f, "")?;
        len.iter().enumerate().try_for_each(|(i, spacing)| {
            let spacing = spacing + 4;
            if i == len.len() - 1 {
                write!(f, "+{:->spacing$}+", "")
            } else {
                write!(f, "+{:->spacing$}", "")
            }
        })?;
        self.data.iter().enumerate().try_for_each(|(i, d)| {
            let spacing = (len[i % self.width] - d.to_string().len()) + 5;
            if i % self.width == 0 {
                write!(f, "\n| {}{:>spacing$}", d, "| ")
            } else {
                write!(f, "{}{:>spacing$}", d, "| ")
            }
        })?;
        len.iter().enumerate().try_for_each(|(i, spacing)| {
            let spacing = spacing + 4;
            if i == 0 {
                write!(f, "\n+{:->spacing$}+", "")
            } else {
                write!(f, "{:->spacing$}+", "")
            }
        })
    }
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
        let mut width = 0;
        let mut height = 0;
        let raw = input
            .lines()
            .flat_map(|line| {
                height += 1;
                let l = line.split(",").map(ToString::to_string).collect::<Vec<_>>();
                width = l.len();
                l
            })
            .collect::<Vec<_>>();
        let headers = raw[0..width].to_vec();
        height -= 1;
        let data = raw[width..].iter().map(|d| Val::from(d.as_str())).collect();

        Self { headers, data, width, height }
    }

    pub fn col(&self, header: &str) -> Option<Vec<&Val>> {
        let Some(header) = self.headers.iter().position(|h| h == header) else { return None };
        Some(self.data.iter().enumerate().filter_map(|(i, d)| {
            if i % self.width == header {
                Some(d)
            } else { None }
        }).collect())
    }

    pub fn row(&self, idx: usize) -> Option<HashMap<&str, &Val>> {
        if idx >= self.height {
            return None;
        }
        Some(
            self
                .headers
                .iter()
                .zip(&self.data[idx * self.width..self.width * (1 + idx)])
                .map(|(h, v)| (h.as_str(), v))
                .collect()
        )

    }

    pub fn headers(&self) -> &Vec<String> {
        &self.headers
    }

    pub fn loc<F: FnMut(&mut Val)>(&mut self, header: &str, mut f: F) -> Result<(), Error> {
        let Some(header) = self.headers.iter().position(|h| h == header) else { return Err(Error::HeaderNotFound(header.to_string())) };
        self.data.iter_mut().enumerate().try_for_each(|(idx, d)| {
            if idx % self.width == header {
                f(d)
            }

            Ok::<(), Error>(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn df() -> DataFrame {
        let csv = "name,nationality,xg,goals
Lionel Messi,Argentine,66.66,66
C. Ronaldo,Portugal,-0.69,3
Darwin Nunez,Uruguay,69.69,6969
M. Balotelli,Italy,8.88,888
";
        DataFrame::read_str(csv.to_string())
    }

    #[test]
    fn col() {
        let df = df();
        let headers = df.headers();

        headers.iter().for_each(|header| {
            let col = df.col(header);
            assert!(col.is_some());
        });
    }

    #[test]
    fn row() {
        let df = df();
        let headers = df.headers();
        let row_3 = df.row(3usize);
        let row_4 = df.row(4usize);

        let pos_of_xg = headers.iter().position(|h| h == "xg");
        assert!(pos_of_xg.is_some());

        assert!(row_3.is_some_and(|n| {
                n.get("xg").is_some_and(|val| {
                    val.is_float() && !val.is_int()
                })
            })
        );
        assert!(row_4.is_none());

    }

    #[test]
    fn modify_col_dtype() -> Result<(), Error> {
        let mut df = df();

        df.loc("goals", |val| {
            if let Val::Int64(num) = val {
                *val = Val::Usize(*num as usize);
            }
        })?;

        df.col("goals").inspect(|values| {
            values.iter().for_each(|v| {
                assert!(v.is_usize());
            });
        });

        Ok(())
    }

    #[test]
    fn loc() -> Result<(), Error> {
        let mut df = df();
        let headers = df
            .headers()
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>();

        headers.iter().try_for_each(|header| {
            df.loc(header, |val| {
                match val {
                    Val::String(s) => *s += " modify",
                    Val::Int64(num) => *num *= -1,
                    Val::Float64(num) => *num += 6.9,
                    _ => ()
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
                        _ => ()
                    }
                });
            });
        });

        Ok(())
    }
}
