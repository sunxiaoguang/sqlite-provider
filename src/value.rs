use crate::provider::{RawBytes, ValueType};

/// Owned SQLite value.
#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    /// SQL NULL.
    Null,
    /// 64-bit integer.
    Integer(i64),
    /// 64-bit floating point.
    Float(f64),
    /// UTF-8 text.
    Text(String),
    /// Arbitrary bytes.
    Blob(Vec<u8>),
}

impl Value {
    /// Return the SQLite value type tag for this owned value.
    pub fn value_type(&self) -> ValueType {
        match self {
            Value::Null => ValueType::Null,
            Value::Integer(_) => ValueType::Integer,
            Value::Float(_) => ValueType::Float,
            Value::Text(_) => ValueType::Text,
            Value::Blob(_) => ValueType::Blob,
        }
    }

    /// Borrow the integer payload when this is `Value::Integer`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Value::Integer(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrow the float payload when this is `Value::Float`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Value::Float(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrow the text payload when this is `Value::Text`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::Text(v) => Some(v.as_str()),
            _ => None,
        }
    }

    /// Borrow the blob payload when this is `Value::Blob`.
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
    /// SQL NULL.
    Null,
    /// Borrowed 64-bit integer.
    Integer(i64),
    /// Borrowed 64-bit floating point.
    Float(f64),
    /// Borrowed UTF-8 text.
    Text(&'a str),
    /// Borrowed bytes.
    Blob(&'a [u8]),
}

impl<'a> ValueRef<'a> {
    /// Return the SQLite value type tag for this borrowed value.
    pub fn value_type(&self) -> ValueType {
        match self {
            ValueRef::Null => ValueType::Null,
            ValueRef::Integer(_) => ValueType::Integer,
            ValueRef::Float(_) => ValueType::Float,
            ValueRef::Text(_) => ValueType::Text,
            ValueRef::Blob(_) => ValueType::Blob,
        }
    }

    /// Borrow the integer payload when this is `ValueRef::Integer`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            ValueRef::Integer(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrow the float payload when this is `ValueRef::Float`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            ValueRef::Float(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrow the text payload when this is `ValueRef::Text`.
    pub fn as_str(&self) -> Option<&'a str> {
        match self {
            ValueRef::Text(v) => Some(*v),
            _ => None,
        }
    }

    /// Borrow the blob payload when this is `ValueRef::Blob`.
    pub fn as_blob(&self) -> Option<&'a [u8]> {
        match self {
            ValueRef::Blob(v) => Some(*v),
            _ => None,
        }
    }

    /// Copy this borrowed value into an owned [`Value`].
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
    /// Caller must ensure `raw` points to valid bytes for the duration of `'a`.
    ///
    /// If `raw` is valid UTF-8 this returns `ValueRef::Text`; otherwise it
    /// falls back to `ValueRef::Blob` to avoid unchecked UTF-8 assumptions.
    pub unsafe fn from_raw_text(raw: RawBytes) -> ValueRef<'a> {
        match unsafe { raw.as_str() } {
            Some(text) => ValueRef::Text(text),
            None => ValueRef::Blob(unsafe { raw.as_slice() }),
        }
    }

    /// # Safety
    /// Caller must ensure `raw` points to valid bytes for the duration of `'a`.
    pub unsafe fn from_raw_blob(raw: RawBytes) -> ValueRef<'a> {
        ValueRef::Blob(unsafe { raw.as_slice() })
    }
}

#[cfg(test)]
mod tests {
    use crate::provider::RawBytes;

    use super::{Value, ValueRef};

    #[test]
    fn value_ref_to_owned() {
        let value = ValueRef::Text("hello");
        assert_eq!(value.to_owned(), Value::Text("hello".to_owned()));
    }

    #[test]
    fn value_ref_from_raw_text_falls_back_to_blob_when_invalid_utf8() {
        let bytes = [0xff_u8, 0xfe_u8];
        let raw = RawBytes {
            ptr: bytes.as_ptr(),
            len: bytes.len(),
        };
        let value = unsafe { ValueRef::from_raw_text(raw) };
        assert_eq!(value, ValueRef::Blob(&bytes));
    }
}
