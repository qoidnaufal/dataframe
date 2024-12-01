use std::{
    collections::{HashMap, HashSet}, hash::Hash, io::{BufReader, Read}, path::Path
};

mod error;

pub use error::Error;

#[derive(Debug, Clone)]
pub enum Val {
    String(String),
    Int64(i64),
    Float64(f64),
    Usize(usize),
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

impl Val {
    pub fn is_float(&self) -> bool {
        matches!(self, Self::Float64(_))
    }

    pub fn is_int(&self) -> bool {
        matches!(self, Val::Int64(_))
    }

    pub fn is_str(&self) -> bool {
        matches!(self, Val::String(_))
    }
}

#[derive(Debug)]
pub struct Row {
    index: Val,
    values: HashMap<String, Val>
}

impl Row {
    fn get_val(&self, header: &str) -> Option<&Val> {
        self.values.get(header)
    }

    fn get_mut_val(&mut self, header: &str) -> Option<&mut Val> {
        self.values.get_mut(header)
    }

    fn values(&self) -> HashMap<&str, &Val> {
        self.values.iter().map(|(k, v)| (k.as_str(), v)).collect()
    }
}

#[derive(Debug)]
pub struct DataFrame {
    headers: Vec<String>,
    index: HashSet<Val>,
    data: Vec<Row>,
}

impl DataFrame {
    pub fn read_csv<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = std::fs::File::open(&path)?;
        let mut buf = BufReader::new(file);

        let mut s = String::new();
        buf.read_to_string(&mut s)?;

        Ok(Self::read_str(s))
    }

    // TODO: ensure each column has the same Val
    pub fn read_str(input: String) -> Self {
        let raw = input
            .lines()
            .map(|line| line.split(",").map(ToString::to_string).collect::<Vec<_>>())
            .collect::<Vec<_>>();
        let headers = raw[0].clone();
        let mut df_index = HashSet::new();
        let data = raw[1..].iter().enumerate().map(|(row_idx, row)| {
            let index = Val::Usize(row_idx);
            if !df_index.contains(&index) {
                df_index.insert(index.clone());
            }
            let values = row.iter().enumerate().map(|(val_idx, val)| {
                let header = headers[val_idx].clone();
                (header, Val::from(val.as_str()))
            }).collect::<HashMap<_, _>>();

            Row { index, values }

        }).collect::<Vec<_>>();

        Self { headers, index: df_index, data }
    }

    pub fn col(&self, header: &str) -> Option<Vec<&Val>> {
        self.data.iter().map(|row| {
            row.get_val(header)
        }).collect::<Option<Vec<_>>>()
    }

    pub fn row<I: Into<Val>>(&self, idx: I) -> Option<HashMap<&str, &Val>> {
        match idx.into() {
            Val::String(ref s) => {
                if !self.index.contains(&Val::from(s)) {
                    None
                } else {
                    let res = self.data
                        .iter()
                        .filter(|row| row.index == Val::from(s))
                        .flat_map(|row| row.values())
                        .collect::<HashMap<_, _>>();
                    Some(res)
                }
            }
            Val::Usize(u) => {
                let len = self.data.len();
                if u >= len {
                    None
                } else {
                    let res = self.data
                        .iter()
                        .filter(|row| row.index == Val::Usize(u))
                        .flat_map(|row| row.values())
                        .collect::<HashMap<_, _>>();
                    Some(res)
                }
            },
            _ => panic!("f64 & i64 can't be used as index")
        }

    }

    pub fn headers(&self) -> Vec<&str> {
        self.headers.iter().map(|header| header.as_str()).collect::<Vec<_>>()
    }

    pub fn loc<F: FnMut(&mut Val)>(&mut self, header: &str, mut f: F) -> Result<(), Error> {
        self.data.iter_mut().try_for_each(|row| {
            if let Some(val) = row.get_mut_val(header) {
                f(val);
                Ok(())
            } else {
                Err(Error::HeaderNotFound(header.to_string()))
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn df() -> DataFrame {
        let csv = "name,nationality,xg,goals
Lionel Messi,Argentine,66.66,66
C. Ronaldo,Portugal,0.69,3
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
            let col = df.col(*header);
            assert!(col.is_some());
        });
    }

    #[test]
    fn row() {
        let df = df();
        let row_3 = df.row(3usize);
        let row_4 = df.row(4usize);

        assert!(row_3.is_some_and(|n| {
                n.get("xg").is_some_and(|val| {
                    val.is_float() && !val.is_int()
                })
            })
        );
        assert!(row_4.is_none());

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
