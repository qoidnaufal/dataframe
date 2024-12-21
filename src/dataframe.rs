use std::{
    collections::HashMap,
    io::{BufReader, Read},
    path::Path
};

use crate::{Val, Error};

#[derive(Clone, Default)]
pub struct DataFrame {
    headers: Vec<String>,
    data: Vec<Val>,
    width: usize,
    height: usize
}

// FIXME: make it better, i find it kinda messy
impl std::fmt::Debug for DataFrame {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let lens = self.headers.iter().filter_map(|header| {
            let Some(col) = self.col(header) else { return None };
            let len = col.iter().map(|val| val.to_string().len()).max();
            len
        }).collect::<Vec<_>>();
        printh_borders(&lens, f)?;
        self.headers.iter().zip(&lens).enumerate().try_for_each(|(i, (header, len))| {
            let spacing = len + 5 - header.len();
            if i % self.width == 0 {
                write!(f, "\n| {}{:>spacing$}", header, "| ")
            } else {
                write!(f, "{}{:>spacing$}", header, "| ")
            }
        })?;
        writeln!(f, "")?;
        printh_borders(&lens, f)?;
        self.data.iter().enumerate().try_for_each(|(i, d)| {
            let spacing = lens[i % self.width] + 5 - d.to_string().len();
            if i % self.width == 0 {
                write!(f, "\n| {}{:>spacing$}", d, "| ")
            } else if i == self.data.len() - 1 {
                write!(f, "{}{:>spacing$}\n", d, "| ")
            } else {
                write!(f, "{}{:>spacing$}", d, "| ")
            }
        })?;
        printh_borders(&lens, f)
    }
}

fn printh_borders(lens: &Vec<usize>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    lens.iter().enumerate().try_for_each(|(i, spacing)| {
        let spacing = spacing + 4;
        if i == lens.len() - 1 {
            write!(f, "+{:->spacing$}+", "")
        } else {
            write!(f, "+{:->spacing$}", "")
        }
    })
}

impl DataFrame {
    pub fn new(headers: Vec<String>, data: Vec<Val>, width: usize, height: usize) -> Self {
        Self {
            headers,
            data,
            width,
            height,
        }
    }

    pub fn read_csv<P: AsRef<Path>>(path: P) -> Result<Self, Error> {
        let file = std::fs::File::open(&path)?;
        let mut buf = BufReader::new(file);

        let mut s = String::new();
        buf.read_to_string(&mut s)?;

        Self::read_str(s)
    }

    pub fn read_str(input: String) -> Result<Self, Error> {
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
        let data = raw[width..].iter().map(|d| {
            let val: Val = d.parse()?;
            Ok::<Val, Error>(val)
        }).collect::<Result<Vec<Val>, Error>>()?;

        Ok(Self { headers, data, width, height })
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
        DataFrame::read_str(csv.to_string()).unwrap()
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
