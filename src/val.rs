use std::str::FromStr;
use crate::Error;

#[derive(Debug, Clone)]
pub enum Val {
    String(String),

    Isize(isize),
    Usize(usize),

    Int128(i128),
    UInt128(u128),

    Int64(i64),
    Uint64(u64),

    Int32(i32),
    Uint32(u32),

    Int16(i16),
    Uint16(u16),

    Int8(i8),
    Uint8(u8),

    Float64(f64),
    Float32(f32),
}

impl Default for Val {
    fn default() -> Self {
        Self::String(Default::default())
    }
}

impl std::fmt::Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::String(val) => write!(f, "{:?}", val),
            Val::Isize(val) => write!(f, "{}", val),
            Val::Usize(val) => write!(f, "{}", val),
            Val::Int128(val) => write!(f, "{}", val),
            Val::UInt128(val) => write!(f, "{}", val),
            Val::Int64(val) => write!(f, "{}", val),
            Val::Uint64(val) => write!(f, "{}", val),
            Val::Int32(val) => write!(f, "{}", val),
            Val::Uint32(val) => write!(f, "{}", val),
            Val::Int16(val) => write!(f, "{}", val),
            Val::Uint16(val) => write!(f, "{}", val),
            Val::Int8(val) => write!(f, "{}", val),
            Val::Uint8(val) => write!(f, "{}", val),
            Val::Float64(val) => write!(f, "{}", val),
            Val::Float32(val) => write!(f, "{}", val),
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

// FIXME: use derive macro to turn a struct into dataframe
impl FromStr for Val {
    type Err = Error;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let val = value.parse::<i64>()
            .map_or_else(|_| {
                value
                    .parse::<f64>()
                    .map_or_else(|_| Val::String(value.to_string()),
                    Val::Float64
                )
            },
            Val::Int64
        );
        Ok(val)
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
