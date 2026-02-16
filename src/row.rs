use crate::provider::{RawBytes, Sqlite3Api, ValueType};
use crate::statement::Statement;
use crate::value::{Value, ValueRef};

/// Row view for the current step.
pub struct Row<'s, 'c, 'p, P: Sqlite3Api> {
    stmt: &'s Statement<'c, 'p, P>,
}

impl<'s, 'c, 'p, P: Sqlite3Api> Row<'s, 'c, 'p, P> {
    pub(crate) fn new(stmt: &'s Statement<'c, 'p, P>) -> Self {
        Self { stmt }
    }

    /// Number of columns.
    pub fn column_count(&self) -> i32 {
        unsafe { self.stmt.conn.api.column_count(self.stmt.stmt) }
    }

    /// Column type for the current row.
    pub fn column_type(&self, col: i32) -> ValueType {
        unsafe { self.stmt.conn.api.column_type(self.stmt.stmt, col) }
    }

    /// Column integer value.
    pub fn column_int64(&self, col: i32) -> i64 {
        unsafe { self.stmt.conn.api.column_int64(self.stmt.stmt, col) }
    }

    /// Column floating value.
    pub fn column_double(&self, col: i32) -> f64 {
        unsafe { self.stmt.conn.api.column_double(self.stmt.stmt, col) }
    }

    /// Raw column text bytes (SQLite-owned).
    pub fn column_text_raw(&self, col: i32) -> RawBytes {
        unsafe { self.stmt.conn.api.column_text(self.stmt.stmt, col) }
    }

    /// Raw column blob bytes (SQLite-owned).
    pub fn column_blob_raw(&self, col: i32) -> RawBytes {
        unsafe { self.stmt.conn.api.column_blob(self.stmt.stmt, col) }
    }

    /// Column text as UTF-8 if valid.
    pub fn column_text(&self, col: i32) -> Option<&str> {
        unsafe { self.column_text_raw(col).as_str() }
    }

    /// Column blob bytes.
    pub fn column_blob(&self, col: i32) -> &[u8] {
        unsafe { self.column_blob_raw(col).as_slice() }
    }

    /// Column value as a borrowed view.
    pub fn column_value_ref(&self, col: i32) -> ValueRef<'_> {
        match self.column_type(col) {
            ValueType::Null => ValueRef::Null,
            ValueType::Integer => ValueRef::Integer(self.column_int64(col)),
            ValueType::Float => ValueRef::Float(self.column_double(col)),
            ValueType::Text => unsafe { ValueRef::from_raw_text(self.column_text_raw(col)) },
            ValueType::Blob => unsafe { ValueRef::from_raw_blob(self.column_blob_raw(col)) },
        }
    }

    /// Column value as an owned `Value`.
    pub fn column_value(&self, col: i32) -> Value {
        self.column_value_ref(col).to_owned()
    }
}
