use crate::provider::{RawBytes, ValueType};

/// Owned SQLite value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl Value {
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Integer(_) => ValueType::Integer,
            Value::Float(_) => ValueType::Float,
            Value::Text(_) => ValueType::Text,
            Value::Blob(_) => ValueType::Blob,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(v) => Some(v.as_str()),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&[u8]> {
        match self {
            Value::Blob(v) => Some(v.as_slice()),
            _ => None,
        }
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Integer(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Text(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Value::Text(value.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Value::Blob(value)
    }
}

/// Borrowed SQLite value view.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ValueRef<'a> {
    Null,
    Integer(i64),
    Float(f64),
    Text(&'a str),
    Blob(&'a [u8]),
}

impl<'a> ValueRef<'a> {
    pub fn value_type(&self) -> ValueType {
        match self {
            ValueRef::Null => ValueType::Null,
            ValueRef::Integer(_) => ValueType::Integer,
            ValueRef::Float(_) => ValueType::Float,
            ValueRef::Text(_) => ValueType::Text,
            ValueRef::Blob(_) => ValueType::Blob,
        }
    }

    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ValueRef::Integer(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ValueRef::Float(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_str(&self) -> Option<&'a str> {
        match self {
            ValueRef::Text(v) => Some(*v),
            _ => None,
        }
    }

    pub fn as_blob(&self) -> Option<&'a [u8]> {
        match self {
            ValueRef::Blob(v) => Some(*v),
            _ => None,
        }
    }

    pub fn to_owned(&self) -> Value {
        match self {
            ValueRef::Null => Value::Null,
            ValueRef::Integer(v) => Value::Integer(*v),
            ValueRef::Float(v) => Value::Float(*v),
            ValueRef::Text(v) => Value::Text((*v).to_owned()),
            ValueRef::Blob(v) => Value::Blob((*v).to_vec()),
        }
    }

    /// # Safety
    /// Caller must ensure `raw` points to valid UTF-8 for the duration of `'a`.
    pub unsafe fn from_raw_text(raw: RawBytes) -> ValueRef<'a> {
        ValueRef::Text(unsafe { raw.as_str_unchecked() })
    }

    /// # Safety
    /// Caller must ensure `raw` points to valid bytes for the duration of `'a`.
    pub unsafe fn from_raw_blob(raw: RawBytes) -> ValueRef<'a> {
        ValueRef::Blob(unsafe { raw.as_slice() })
    }
}

#[cfg(test)]
mod tests {
    use super::{Value, ValueRef};

    #[test]
    fn value_ref_to_owned() {
        let value = ValueRef::Text("hello");
        assert_eq!(value.to_owned(), Value::Text("hello".to_owned()));
    }
}
