use super::*;

#[allow(unsafe_op_in_unsafe_fn)]
unsafe impl Sqlite3Metadata for LibSqlite3 {
    unsafe fn table_column_metadata(
        &self,
        db: NonNull<Self::Db>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata> {
        let table = CString::new(table)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "table contains NUL"))?;
        let column = CString::new(column)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "column contains NUL"))?;
        let db_name = match db_name {
            Some(name) => Some(
                CString::new(name)
                    .map_err(|_| Error::with_message(ErrorCode::Misuse, "db name contains NUL"))?,
            ),
            None => None,
        };
        let func = match self.fns.table_column_metadata {
            Some(func) => func,
            None => {
                return Err(Error::feature_unavailable(
                    "table_column_metadata not available",
                ));
            }
        };
        let mut data_type = null();
        let mut coll_seq = null();
        let mut not_null = 0;
        let mut primary_key = 0;
        let mut autoinc = 0;
        let rc = func(
            db.as_ptr(),
            db_name.as_ref().map(|s| s.as_ptr()).unwrap_or(null()),
            table.as_ptr(),
            column.as_ptr(),
            &mut data_type,
            &mut coll_seq,
            &mut not_null,
            &mut primary_key,
            &mut autoinc,
        );
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, Some(db)));
        }
        Ok(ColumnMetadata {
            data_type: raw_cstr(data_type).map(raw_bytes_from_cstr),
            coll_seq: raw_cstr(coll_seq).map(raw_bytes_from_cstr),
            not_null: not_null != 0,
            primary_key: primary_key != 0,
            autoinc: autoinc != 0,
        })
    }

    unsafe fn column_decltype(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes> {
        let ptr = (self.fns.column_decltype)(stmt.as_ptr(), col);
        raw_cstr(ptr).map(raw_bytes_from_cstr)
    }

    unsafe fn column_name(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes> {
        let ptr = (self.fns.column_name)(stmt.as_ptr(), col);
        raw_cstr(ptr).map(raw_bytes_from_cstr)
    }

    unsafe fn column_table_name(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes> {
        let func = self.fns.column_table_name?;
        let ptr = func(stmt.as_ptr(), col);
        raw_cstr(ptr).map(raw_bytes_from_cstr)
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe impl Sqlite3Hooks for LibSqlite3 {
    unsafe fn trace_v2(
        &self,
        db: NonNull<Self::Db>,
        mask: u32,
        callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
        context: *mut c_void,
    ) -> Result<()> {
        let trace = match self.fns.trace_v2 {
            Some(trace) => trace,
            None => return Err(Error::feature_unavailable("trace_v2 not available")),
        };
        let rc = trace(db.as_ptr(), mask, callback, context);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn progress_handler(
        &self,
        db: NonNull<Self::Db>,
        n: i32,
        callback: Option<extern "C" fn(*mut c_void) -> i32>,
        context: *mut c_void,
    ) -> Result<()> {
        let progress = match self.fns.progress_handler {
            Some(progress) => progress,
            None => return Err(Error::feature_unavailable("progress_handler not available")),
        };
        progress(db.as_ptr(), n, callback, context);
        Ok(())
    }

    unsafe fn busy_timeout(&self, db: NonNull<Self::Db>, ms: i32) -> Result<()> {
        let busy_timeout = match self.fns.busy_timeout {
            Some(busy_timeout) => busy_timeout,
            None => return Err(Error::feature_unavailable("busy_timeout not available")),
        };
        let rc = busy_timeout(db.as_ptr(), ms);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn set_authorizer(
        &self,
        db: NonNull<Self::Db>,
        callback: Option<
            extern "C" fn(
                *mut c_void,
                i32,
                *const c_char,
                *const c_char,
                *const c_char,
                *const c_char,
            ) -> i32,
        >,
        context: *mut c_void,
    ) -> Result<()> {
        let set_authorizer = match self.fns.set_authorizer {
            Some(set_authorizer) => set_authorizer,
            None => return Err(Error::feature_unavailable("set_authorizer not available")),
        };
        let rc = set_authorizer(db.as_ptr(), callback, context);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe impl Sqlite3Backup for LibSqlite3 {
    type Backup = sqlite3_backup;

    unsafe fn backup_init(
        &self,
        dest_db: NonNull<Self::Db>,
        dest_name: &str,
        source_db: NonNull<Self::Db>,
        source_name: &str,
    ) -> Result<NonNull<Self::Backup>> {
        let backup_init = match self.fns.backup_init {
            Some(backup_init) => backup_init,
            None => return Err(Error::feature_unavailable("backup_init not available")),
        };
        let dest_name = CString::new(dest_name)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "dest name contains NUL"))?;
        let source_name = CString::new(source_name)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "source name contains NUL"))?;
        let backup = backup_init(
            dest_db.as_ptr(),
            dest_name.as_ptr(),
            source_db.as_ptr(),
            source_name.as_ptr(),
        );
        if let Some(backup) = NonNull::new(backup) {
            Ok(backup)
        } else {
            let rc = (self.fns.errcode)(dest_db.as_ptr());
            Err(self.error_from_rc(rc, Some(dest_db)))
        }
    }

    unsafe fn backup_step(&self, backup: NonNull<Self::Backup>, pages: i32) -> Result<()> {
        let backup_step = match self.fns.backup_step {
            Some(backup_step) => backup_step,
            None => return Err(Error::feature_unavailable("backup_step not available")),
        };
        let rc = backup_step(backup.as_ptr(), pages);
        if rc == SQLITE_OK || rc == SQLITE_DONE {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn backup_remaining(&self, backup: NonNull<Self::Backup>) -> i32 {
        self.fns
            .backup_remaining
            .map(|f| f(backup.as_ptr()))
            .unwrap_or(0)
    }

    unsafe fn backup_pagecount(&self, backup: NonNull<Self::Backup>) -> i32 {
        self.fns
            .backup_pagecount
            .map(|f| f(backup.as_ptr()))
            .unwrap_or(0)
    }

    unsafe fn backup_finish(&self, backup: NonNull<Self::Backup>) -> Result<()> {
        let backup_finish = match self.fns.backup_finish {
            Some(backup_finish) => backup_finish,
            None => return Err(Error::feature_unavailable("backup_finish not available")),
        };
        let rc = backup_finish(backup.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe impl Sqlite3BlobIo for LibSqlite3 {
    type Blob = sqlite3_blob;

    unsafe fn blob_open(
        &self,
        db: NonNull<Self::Db>,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<NonNull<Self::Blob>> {
        let blob_open = match self.fns.blob_open {
            Some(blob_open) => blob_open,
            None => return Err(Error::feature_unavailable("blob_open not available")),
        };
        let db_name = CString::new(db_name)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "db name contains NUL"))?;
        let table = CString::new(table)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "table contains NUL"))?;
        let column = CString::new(column)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "column contains NUL"))?;
        let mut blob = null_mut();
        let rc = blob_open(
            db.as_ptr(),
            db_name.as_ptr(),
            table.as_ptr(),
            column.as_ptr(),
            rowid,
            flags as c_int,
            &mut blob,
        );
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, Some(db)));
        }
        NonNull::new(blob).ok_or_else(|| {
            Error::with_message(
                ErrorCode::Error,
                "sqlite3_blob_open returned success with null blob handle",
            )
        })
    }

    unsafe fn blob_read(
        &self,
        blob: NonNull<Self::Blob>,
        data: &mut [u8],
        offset: i32,
    ) -> Result<()> {
        if data.len() > i32::MAX as usize {
            return Err(Error::with_message(
                ErrorCode::TooBig,
                "blob read buffer too large",
            ));
        }
        let blob_read = match self.fns.blob_read {
            Some(blob_read) => blob_read,
            None => return Err(Error::feature_unavailable("blob_read not available")),
        };
        let ptr = if data.is_empty() {
            null_mut()
        } else {
            data.as_mut_ptr() as *mut c_void
        };
        let rc = blob_read(blob.as_ptr(), ptr, clamp_len(data.len()), offset);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn blob_write(&self, blob: NonNull<Self::Blob>, data: &[u8], offset: i32) -> Result<()> {
        if data.len() > i32::MAX as usize {
            return Err(Error::with_message(
                ErrorCode::TooBig,
                "blob write payload too large",
            ));
        }
        let blob_write = match self.fns.blob_write {
            Some(blob_write) => blob_write,
            None => return Err(Error::feature_unavailable("blob_write not available")),
        };
        let ptr = if data.is_empty() {
            null()
        } else {
            data.as_ptr() as *const c_void
        };
        let rc = blob_write(blob.as_ptr(), ptr, clamp_len(data.len()), offset);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn blob_bytes(&self, blob: NonNull<Self::Blob>) -> i32 {
        self.fns.blob_bytes.map(|f| f(blob.as_ptr())).unwrap_or(0)
    }

    unsafe fn blob_close(&self, blob: NonNull<Self::Blob>) -> Result<()> {
        let blob_close = match self.fns.blob_close {
            Some(blob_close) => blob_close,
            None => return Err(Error::feature_unavailable("blob_close not available")),
        };
        let rc = blob_close(blob.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe impl Sqlite3Serialize for LibSqlite3 {
    unsafe fn serialize(
        &self,
        db: NonNull<Self::Db>,
        schema: Option<&str>,
        flags: u32,
    ) -> Result<OwnedBytes> {
        let serialize = match self.fns.serialize {
            Some(serialize) => serialize,
            None => return Err(Error::feature_unavailable("serialize not available")),
        };
        let schema = match schema {
            Some(schema) => Some(
                CString::new(schema)
                    .map_err(|_| Error::with_message(ErrorCode::Misuse, "schema contains NUL"))?,
            ),
            None => None,
        };
        let mut len: i64 = 0;
        let ptr = serialize(
            db.as_ptr(),
            schema.as_ref().map(|s| s.as_ptr()).unwrap_or(null()),
            &mut len,
            flags,
        );
        if ptr.is_null() {
            if len == 0 {
                let empty_ptr = (self.fns.malloc)(1);
                let empty_ptr = NonNull::new(empty_ptr as *mut u8)
                    .ok_or_else(|| Error::new(ErrorCode::NoMem))?;
                return Ok(OwnedBytes {
                    ptr: empty_ptr,
                    len: 0,
                });
            }
            let rc = (self.fns.errcode)(db.as_ptr());
            return Err(self.error_from_rc(rc, Some(db)));
        }
        let len = usize::try_from(len).map_err(|_| {
            unsafe { (self.fns.free)(ptr as *mut c_void) };
            Error::with_message(ErrorCode::TooBig, "serialized payload length out of range")
        })?;
        Ok(OwnedBytes {
            ptr: NonNull::new_unchecked(ptr),
            len,
        })
    }

    unsafe fn deserialize(
        &self,
        db: NonNull<Self::Db>,
        schema: Option<&str>,
        data: &[u8],
        flags: u32,
    ) -> Result<()> {
        let deserialize = match self.fns.deserialize {
            Some(deserialize) => deserialize,
            None => return Err(Error::feature_unavailable("deserialize not available")),
        };
        if data.len() > i32::MAX as usize {
            return Err(Error::with_message(
                ErrorCode::TooBig,
                "deserialize payload too large",
            ));
        }
        let schema = match schema {
            Some(schema) => Some(
                CString::new(schema)
                    .map_err(|_| Error::with_message(ErrorCode::Misuse, "schema contains NUL"))?,
            ),
            None => None,
        };
        let mut owned = null_mut::<c_uchar>();
        if !data.is_empty() {
            owned = (self.fns.malloc)(data.len() as c_int) as *mut c_uchar;
            if owned.is_null() {
                return Err(Error::new(ErrorCode::NoMem));
            }
            std::ptr::copy_nonoverlapping(data.as_ptr(), owned, data.len());
        }
        let mut deserialize_flags = flags;
        if !owned.is_null() {
            deserialize_flags |= SQLITE_DESERIALIZE_FREEONCLOSE;
        }
        let len = i64::try_from(data.len())
            .map_err(|_| Error::with_message(ErrorCode::TooBig, "deserialize payload too large"))?;
        let rc = deserialize(
            db.as_ptr(),
            schema.as_ref().map(|s| s.as_ptr()).unwrap_or(null()),
            owned,
            len,
            len,
            deserialize_flags,
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            if !owned.is_null() {
                (self.fns.free)(owned as *mut c_void);
            }
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn free(&self, bytes: OwnedBytes) {
        (self.fns.free)(bytes.ptr.as_ptr() as *mut c_void);
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe impl Sqlite3Wal for LibSqlite3 {
    unsafe fn wal_checkpoint(&self, db: NonNull<Self::Db>, db_name: Option<&str>) -> Result<()> {
        let wal_checkpoint = match self.fns.wal_checkpoint {
            Some(wal_checkpoint) => wal_checkpoint,
            None => return Err(Error::feature_unavailable("wal_checkpoint not available")),
        };
        let db_name = match db_name {
            Some(db_name) => Some(
                CString::new(db_name)
                    .map_err(|_| Error::with_message(ErrorCode::Misuse, "db name contains NUL"))?,
            ),
            None => None,
        };
        let rc = wal_checkpoint(
            db.as_ptr(),
            db_name.as_ref().map(|s| s.as_ptr()).unwrap_or(null()),
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn wal_checkpoint_v2(
        &self,
        db: NonNull<Self::Db>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)> {
        let wal_checkpoint_v2 = match self.fns.wal_checkpoint_v2 {
            Some(wal_checkpoint_v2) => wal_checkpoint_v2,
            None => {
                return Err(Error::feature_unavailable(
                    "wal_checkpoint_v2 not available",
                ));
            }
        };
        let db_name = match db_name {
            Some(db_name) => Some(
                CString::new(db_name)
                    .map_err(|_| Error::with_message(ErrorCode::Misuse, "db name contains NUL"))?,
            ),
            None => None,
        };
        let mut log = 0;
        let mut checkpointed = 0;
        let rc = wal_checkpoint_v2(
            db.as_ptr(),
            db_name.as_ref().map(|s| s.as_ptr()).unwrap_or(null()),
            mode,
            &mut log,
            &mut checkpointed,
        );
        if rc == SQLITE_OK {
            Ok((log, checkpointed))
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn wal_frame_count(&self, db: NonNull<Self::Db>) -> Result<Option<u32>> {
        let wal_frame_count = match self.fns.libsql_wal_frame_count {
            Some(wal_frame_count) => wal_frame_count,
            None => return Ok(None),
        };
        let mut count = 0_u32;
        let rc = wal_frame_count(db.as_ptr(), &mut count);
        if rc == SQLITE_OK {
            Ok(Some(count))
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }
}
