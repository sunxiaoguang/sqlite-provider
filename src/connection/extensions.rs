use core::marker::PhantomData;
use core::ptr::NonNull;

use crate::error::Result;
use crate::provider::{
    ColumnMetadata, OwnedBytes, RawBytes, Sqlite3Backup, Sqlite3BlobIo, Sqlite3Metadata,
    Sqlite3Serialize, Sqlite3Wal,
};
use crate::statement::Statement;

use super::core::Connection;

/// RAII wrapper around an online backup handle.
///
/// The handle borrows both source and destination connections used to create
/// it, preventing use after either connection is closed.
pub struct Backup<'c, 'p, P: Sqlite3Backup> {
    api: &'p P,
    handle: NonNull<P::Backup>,
    _conn: PhantomData<&'c Connection<'p, P>>,
}

impl<'c, 'p, P: Sqlite3Backup> Backup<'c, 'p, P> {
    /// Copy up to `pages` pages.
    pub fn step(&self, pages: i32) -> Result<()> {
        unsafe { self.api.backup_step(self.handle, pages) }
    }

    /// Remaining pages in the backup.
    pub fn remaining(&self) -> i32 {
        unsafe { self.api.backup_remaining(self.handle) }
    }

    /// Total page count in the backup.
    pub fn pagecount(&self) -> i32 {
        unsafe { self.api.backup_pagecount(self.handle) }
    }
}

impl<'c, 'p, P: Sqlite3Backup> Drop for Backup<'c, 'p, P> {
    fn drop(&mut self) {
        let _ = unsafe { self.api.backup_finish(self.handle) };
    }
}

impl<'p, P: Sqlite3Backup> Connection<'p, P> {
    /// Start a backup from this database to `dest`.
    pub fn backup_to<'c>(
        &'c self,
        dest: &'c Connection<'p, P>,
        name: &str,
    ) -> Result<Backup<'c, 'p, P>> {
        let handle = unsafe { self.api.backup_init(dest.db, name, self.db, "main")? };
        Ok(Backup {
            api: self.api,
            handle,
            _conn: PhantomData,
        })
    }
}

/// RAII wrapper around an incremental blob handle.
///
/// The handle borrows the originating connection so blob operations cannot
/// outlive the database handle.
pub struct Blob<'c, 'p, P: Sqlite3BlobIo> {
    api: &'p P,
    handle: NonNull<P::Blob>,
    _conn: PhantomData<&'c Connection<'p, P>>,
}

impl<'c, 'p, P: Sqlite3BlobIo> Blob<'c, 'p, P> {
    /// Read from the blob into `buf` at `offset`.
    pub fn read(&self, buf: &mut [u8], offset: i32) -> Result<()> {
        unsafe { self.api.blob_read(self.handle, buf, offset) }
    }

    /// Write `buf` into the blob at `offset`.
    pub fn write(&self, buf: &[u8], offset: i32) -> Result<()> {
        unsafe { self.api.blob_write(self.handle, buf, offset) }
    }

    /// Blob size in bytes.
    pub fn len(&self) -> i32 {
        unsafe { self.api.blob_bytes(self.handle) }
    }

    /// Whether the blob has zero length.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'c, 'p, P: Sqlite3BlobIo> Drop for Blob<'c, 'p, P> {
    fn drop(&mut self) {
        let _ = unsafe { self.api.blob_close(self.handle) };
    }
}

impl<'p, P: Sqlite3BlobIo> Connection<'p, P> {
    /// Open an incremental blob handle.
    pub fn open_blob<'c>(
        &'c self,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<Blob<'c, 'p, P>> {
        let handle = unsafe {
            self.api
                .blob_open(self.db, db_name, table, column, rowid, flags)?
        };
        Ok(Blob {
            api: self.api,
            handle,
            _conn: PhantomData,
        })
    }
}

/// Serialized database buffer owned by the backend.
pub struct SerializedDb<'p, P: Sqlite3Serialize> {
    api: &'p P,
    bytes: OwnedBytes,
}

impl<'p, P: Sqlite3Serialize> SerializedDb<'p, P> {
    /// View the serialized bytes.
    pub fn as_slice(&self) -> &[u8] {
        unsafe { core::slice::from_raw_parts(self.bytes.ptr.as_ptr(), self.bytes.len) }
    }

    /// Copy the bytes into a Vec and free the backend buffer.
    pub fn into_vec(self) -> Vec<u8> {
        let me = core::mem::ManuallyDrop::new(self);
        let vec = me.as_slice().to_vec();
        unsafe { Sqlite3Serialize::free(me.api, me.bytes) };
        vec
    }
}

impl<'p, P: Sqlite3Serialize> Drop for SerializedDb<'p, P> {
    fn drop(&mut self) {
        unsafe { Sqlite3Serialize::free(self.api, self.bytes) };
    }
}

impl<'p, P: Sqlite3Serialize> Connection<'p, P> {
    /// Serialize the database into an owned buffer.
    pub fn serialize(&self, schema: Option<&str>, flags: u32) -> Result<SerializedDb<'p, P>> {
        let bytes = unsafe { self.api.serialize(self.db, schema, flags)? };
        Ok(SerializedDb {
            api: self.api,
            bytes,
        })
    }

    /// Deserialize bytes into the database.
    pub fn deserialize(&self, schema: Option<&str>, data: &[u8], flags: u32) -> Result<()> {
        unsafe { self.api.deserialize(self.db, schema, data, flags) }
    }
}

impl<'p, P: Sqlite3Wal> Connection<'p, P> {
    /// Run a WAL checkpoint.
    pub fn wal_checkpoint(&self, db_name: Option<&str>) -> Result<()> {
        unsafe { self.api.wal_checkpoint(self.db, db_name) }
    }

    /// Run a WAL checkpoint with a mode, returning (log, checkpointed) page counts.
    pub fn wal_checkpoint_v2(&self, db_name: Option<&str>, mode: i32) -> Result<(i32, i32)> {
        unsafe { self.api.wal_checkpoint_v2(self.db, db_name, mode) }
    }

    /// Return libsql WAL frame count if supported.
    pub fn wal_frame_count(&self) -> Result<Option<u32>> {
        unsafe { self.api.wal_frame_count(self.db) }
    }
}

impl<'p, P: Sqlite3Metadata> Connection<'p, P> {
    /// Fetch column metadata for a table.
    pub fn table_column_metadata(
        &self,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata> {
        unsafe {
            self.api
                .table_column_metadata(self.db, db_name, table, column)
        }
    }
}

impl<'p, P: Sqlite3Metadata> Statement<'_, 'p, P> {
    /// Raw declared column type.
    pub fn column_decltype_raw(&self, col: i32) -> Option<RawBytes> {
        unsafe { self.conn.api.column_decltype(self.stmt, col) }
    }

    /// Raw column name.
    pub fn column_name_raw(&self, col: i32) -> Option<RawBytes> {
        unsafe { self.conn.api.column_name(self.stmt, col) }
    }

    /// Raw column table name.
    pub fn column_table_name_raw(&self, col: i32) -> Option<RawBytes> {
        unsafe { self.conn.api.column_table_name(self.stmt, col) }
    }
}
