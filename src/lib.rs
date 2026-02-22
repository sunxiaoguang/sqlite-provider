//! Backend-agnostic abstraction over the SQLite3 C API.

mod connection;
mod error;
mod function;
mod provider;
mod row;
mod statement;
mod value;
mod vtab;

pub use crate::connection::{
    AuthorizerAction, AuthorizerEvent, AuthorizerResult, Backup, Blob, CallbackHandle, Connection,
    SerializedDb, TraceEvent, TraceMask, authorizer,
};
pub use crate::error::{Error, ErrorCode, Result};
pub use crate::function::Context;
pub use crate::provider::{
    ApiVersion, ColumnMetadata, FeatureSet, FunctionFlags, OpenFlags, OpenOptions, OwnedBytes,
    RawBytes, Sqlite3Api, Sqlite3Backup, Sqlite3BlobIo, Sqlite3Hooks, Sqlite3Keying,
    Sqlite3Metadata, Sqlite3Serialize, Sqlite3Wal, StepResult, ValueType,
};
pub use crate::row::Row;
pub use crate::statement::Statement;
pub use crate::value::{Value, ValueRef};
pub use crate::vtab::{BestIndexInfo, Cursor, VTab, VTabCursor, VirtualTable};

pub use crate::provider::sqlite3_module;
