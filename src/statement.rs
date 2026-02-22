use core::ptr::NonNull;

use crate::Connection;
use crate::error::Result;
use crate::provider::{Sqlite3Api, StepResult, ValueType};
use crate::row::Row;
use crate::value::Value;

/// Prepared statement wrapper.
pub struct Statement<'c, 'p, P: Sqlite3Api> {
    pub(crate) conn: &'c Connection<'p, P>,
    pub(crate) stmt: NonNull<P::Stmt>,
}

impl<'c, 'p, P: Sqlite3Api> Statement<'c, 'p, P> {
    pub(crate) fn new(conn: &'c Connection<'p, P>, stmt: NonNull<P::Stmt>) -> Self {
        Self { conn, stmt }
    }

    /// Reset the statement.
    pub fn reset(&mut self) -> Result<()> {
        unsafe { self.conn.api.reset(self.stmt) }
    }

    /// Step the statement; returns `Some(Row)` while rows are available.
    pub fn step(&mut self) -> Result<Option<Row<'_, 'c, 'p, P>>> {
        match unsafe { self.conn.api.step(self.stmt)? } {
            StepResult::Row => Ok(Some(Row::new(self))),
            StepResult::Done => Ok(None),
        }
    }

    /// Bind NULL at parameter `idx`.
    pub fn bind_null(&mut self, idx: i32) -> Result<()> {
        unsafe { self.conn.api.bind_null(self.stmt, idx) }
    }

    /// Bind integer at parameter `idx`.
    pub fn bind_int64(&mut self, idx: i32, value: i64) -> Result<()> {
        unsafe { self.conn.api.bind_int64(self.stmt, idx, value) }
    }

    /// Bind double at parameter `idx`.
    pub fn bind_double(&mut self, idx: i32, value: f64) -> Result<()> {
        unsafe { self.conn.api.bind_double(self.stmt, idx, value) }
    }

    /// Bind text at parameter `idx`.
    pub fn bind_text(&mut self, idx: i32, value: &str) -> Result<()> {
        unsafe { self.conn.api.bind_text(self.stmt, idx, value) }
    }

    /// Bind blob at parameter `idx`.
    pub fn bind_blob(&mut self, idx: i32, value: &[u8]) -> Result<()> {
        unsafe { self.conn.api.bind_blob(self.stmt, idx, value) }
    }

    /// Bind an owned value at parameter `idx`.
    pub fn bind_value(&mut self, idx: i32, value: &Value) -> Result<()> {
        match value {
            Value::Null => self.bind_null(idx),
            Value::Integer(v) => self.bind_int64(idx, *v),
            Value::Float(v) => self.bind_double(idx, *v),
            Value::Text(v) => self.bind_text(idx, v),
            Value::Blob(v) => self.bind_blob(idx, v),
        }
    }

    /// Number of columns in the result set.
    pub fn column_count(&self) -> i32 {
        unsafe { self.conn.api.column_count(self.stmt) }
    }

    /// Column type for the current row.
    pub fn column_type(&self, col: i32) -> ValueType {
        unsafe { self.conn.api.column_type(self.stmt, col) }
    }

    /// Expose the raw statement handle.
    pub fn raw_handle(&self) -> NonNull<P::Stmt> {
        self.stmt
    }
}

impl<'c, 'p, P: Sqlite3Api> Drop for Statement<'c, 'p, P> {
    fn drop(&mut self) {
        let _ = unsafe { self.conn.api.finalize(self.stmt) };
    }
}
