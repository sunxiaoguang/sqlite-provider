//! SQLite3 C ABI shim over a registered `sqlite-provider` backend.

use libc::{c_char, c_void};
use sqlite_provider::{
    ApiVersion, ColumnMetadata, Error, FeatureSet, FunctionFlags, OpenFlags, OpenOptions, RawBytes,
    Result, Sqlite3Api, Sqlite3Backup, Sqlite3BlobIo, Sqlite3Hooks, Sqlite3Metadata,
    Sqlite3Serialize, Sqlite3Wal, StepResult, ValueType,
};
#[cfg(feature = "default-backend")]
use sqlite_provider_sqlite3::LibSqlite3;
use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut, NonNull};
use std::sync::{Mutex, OnceLock};

const SQLITE_OK: i32 = 0;
const SQLITE_ERROR: i32 = 1;
const SQLITE_ABORT: i32 = 4;
const SQLITE_BUSY: i32 = 5;
const SQLITE_NOMEM: i32 = 7;
const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_NOTFOUND: i32 = 12;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_MISUSE: i32 = 21;
const SQLITE_ROW: i32 = 100;
const SQLITE_DONE: i32 = 101;

#[allow(dead_code)]
const SQLITE_CHECKPOINT_PASSIVE: i32 = 0;
#[allow(dead_code)]
const SQLITE_CHECKPOINT_FULL: i32 = 1;
#[allow(dead_code)]
const SQLITE_CHECKPOINT_RESTART: i32 = 2;
#[allow(dead_code)]
const SQLITE_CHECKPOINT_TRUNCATE: i32 = 3;

const SQLITE_INTEGER: i32 = 1;
const SQLITE_FLOAT: i32 = 2;
const SQLITE_TEXT: i32 = 3;
const SQLITE_BLOB: i32 = 4;
const SQLITE_NULL: i32 = 5;

#[allow(dead_code)]
const SQLITE_STATIC: isize = 0;
#[allow(dead_code)]
const SQLITE_TRANSIENT: isize = -1;

fn err_code(err: &Error) -> i32 {
    err.code.code().unwrap_or(SQLITE_ERROR)
}

fn clamp_len(len: usize) -> i32 {
    if len > i32::MAX as usize {
        i32::MAX
    } else {
        len as i32
    }
}

unsafe fn cstr_to_str<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    CStr::from_ptr(ptr).to_str().ok()
}

unsafe fn bytes_from_ptr<'a>(ptr: *const c_void, len: i32) -> Option<&'a [u8]> {
    if len <= 0 {
        return Some(&[]);
    }
    if ptr.is_null() {
        return None;
    }
    Some(std::slice::from_raw_parts(ptr as *const u8, len as usize))
}

unsafe fn bytes_from_ptr_mut<'a>(ptr: *mut c_void, len: i32) -> Option<&'a mut [u8]> {
    if len <= 0 {
        return Some(&mut []);
    }
    if ptr.is_null() {
        return None;
    }
    Some(std::slice::from_raw_parts_mut(ptr as *mut u8, len as usize))
}

unsafe fn text_from_ptr<'a>(ptr: *const c_char, len: i32) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    if len < 0 {
        return CStr::from_ptr(ptr).to_str().ok();
    }
    let bytes = std::slice::from_raw_parts(ptr as *const u8, len as usize);
    std::str::from_utf8(bytes).ok()
}

fn value_type_to_code(value_type: ValueType) -> i32 {
    match value_type {
        ValueType::Integer => SQLITE_INTEGER,
        ValueType::Float => SQLITE_FLOAT,
        ValueType::Text => SQLITE_TEXT,
        ValueType::Blob => SQLITE_BLOB,
        ValueType::Null => SQLITE_NULL,
    }
}

fn api_version_number(version: ApiVersion) -> i32 {
    (version.major as i32) * 1_000_000 + (version.minor as i32) * 1_000 + (version.patch as i32)
}

fn set_err_string(err: *mut *mut c_char, msg: &str) {
    if err.is_null() {
        return;
    }
    let bytes = msg.as_bytes();
    let out = sqlite3_malloc64((bytes.len() + 1) as i32) as *mut c_char;
    if out.is_null() {
        unsafe { *err = null_mut() };
        return;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out as *mut u8, bytes.len());
        *out.add(bytes.len()) = 0;
        *err = out;
    }
}

/// Object-safe core SPI using erased handles.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Api`.
pub unsafe trait AbiCore: Send + Sync + 'static {
    fn api_version(&self) -> ApiVersion;
    fn feature_set(&self) -> FeatureSet;
    fn backend_name(&self) -> &'static str;
    fn backend_version(&self) -> Option<ApiVersion>;

    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<c_void>>;
    unsafe fn close(&self, db: NonNull<c_void>) -> Result<()>;

    unsafe fn prepare_v2(&self, db: NonNull<c_void>, sql: &str) -> Result<NonNull<c_void>>;
    unsafe fn prepare_v3(
        &self,
        db: NonNull<c_void>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<c_void>>;

    unsafe fn step(&self, stmt: NonNull<c_void>) -> Result<StepResult>;
    unsafe fn reset(&self, stmt: NonNull<c_void>) -> Result<()>;
    unsafe fn finalize(&self, stmt: NonNull<c_void>) -> Result<()>;

    unsafe fn bind_null(&self, stmt: NonNull<c_void>, idx: i32) -> Result<()>;
    unsafe fn bind_int64(&self, stmt: NonNull<c_void>, idx: i32, v: i64) -> Result<()>;
    unsafe fn bind_double(&self, stmt: NonNull<c_void>, idx: i32, v: f64) -> Result<()>;
    unsafe fn bind_text(&self, stmt: NonNull<c_void>, idx: i32, v: &str) -> Result<()>;
    unsafe fn bind_blob(&self, stmt: NonNull<c_void>, idx: i32, v: &[u8]) -> Result<()>;

    unsafe fn column_count(&self, stmt: NonNull<c_void>) -> i32;
    unsafe fn column_type(&self, stmt: NonNull<c_void>, col: i32) -> ValueType;
    unsafe fn column_int64(&self, stmt: NonNull<c_void>, col: i32) -> i64;
    unsafe fn column_double(&self, stmt: NonNull<c_void>, col: i32) -> f64;
    unsafe fn column_text(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes;
    unsafe fn column_blob(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes;

    unsafe fn errcode(&self, db: NonNull<c_void>) -> i32;
    unsafe fn errmsg(&self, db: NonNull<c_void>) -> *const c_char;
    unsafe fn extended_errcode(&self, db: NonNull<c_void>) -> Option<i32>;

    unsafe fn create_function_v2(
        &self,
        db: NonNull<c_void>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_func: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        x_step: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        x_final: Option<extern "C" fn(*mut c_void)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()>;

    unsafe fn create_window_function(
        &self,
        db: NonNull<c_void>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_step: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        x_final: Option<extern "C" fn(*mut c_void)>,
        x_value: Option<extern "C" fn(*mut c_void)>,
        x_inverse: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()>;

    unsafe fn aggregate_context(&self, ctx: NonNull<c_void>, bytes: usize) -> *mut c_void;

    unsafe fn result_null(&self, ctx: NonNull<c_void>);
    unsafe fn result_int64(&self, ctx: NonNull<c_void>, v: i64);
    unsafe fn result_double(&self, ctx: NonNull<c_void>, v: f64);
    unsafe fn result_text(&self, ctx: NonNull<c_void>, v: &str);
    unsafe fn result_blob(&self, ctx: NonNull<c_void>, v: &[u8]);
    unsafe fn result_error(&self, ctx: NonNull<c_void>, msg: &str);
    unsafe fn user_data(&self, ctx: NonNull<c_void>) -> *mut c_void;

    unsafe fn value_type(&self, v: NonNull<c_void>) -> ValueType;
    unsafe fn value_int64(&self, v: NonNull<c_void>) -> i64;
    unsafe fn value_double(&self, v: NonNull<c_void>) -> f64;
    unsafe fn value_text(&self, v: NonNull<c_void>) -> RawBytes;
    unsafe fn value_blob(&self, v: NonNull<c_void>) -> RawBytes;
}

unsafe impl<P: Sqlite3Api> AbiCore for P {
    fn api_version(&self) -> ApiVersion {
        Sqlite3Api::api_version(self)
    }

    fn feature_set(&self) -> FeatureSet {
        Sqlite3Api::feature_set(self)
    }

    fn backend_name(&self) -> &'static str {
        Sqlite3Api::backend_name(self)
    }

    fn backend_version(&self) -> Option<ApiVersion> {
        Sqlite3Api::backend_version(self)
    }

    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<c_void>> {
        let db = Sqlite3Api::open(self, filename, options)?;
        Ok(NonNull::new_unchecked(db.as_ptr() as *mut c_void))
    }

    unsafe fn close(&self, db: NonNull<c_void>) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Api::close(self, db)
    }

    unsafe fn prepare_v2(&self, db: NonNull<c_void>, sql: &str) -> Result<NonNull<c_void>> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        let stmt = Sqlite3Api::prepare_v2(self, db, sql)?;
        Ok(NonNull::new_unchecked(stmt.as_ptr() as *mut c_void))
    }

    unsafe fn prepare_v3(
        &self,
        db: NonNull<c_void>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<c_void>> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        let stmt = Sqlite3Api::prepare_v3(self, db, sql, flags)?;
        Ok(NonNull::new_unchecked(stmt.as_ptr() as *mut c_void))
    }

    unsafe fn step(&self, stmt: NonNull<c_void>) -> Result<StepResult> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::step(self, stmt)
    }

    unsafe fn reset(&self, stmt: NonNull<c_void>) -> Result<()> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::reset(self, stmt)
    }

    unsafe fn finalize(&self, stmt: NonNull<c_void>) -> Result<()> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::finalize(self, stmt)
    }

    unsafe fn bind_null(&self, stmt: NonNull<c_void>, idx: i32) -> Result<()> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::bind_null(self, stmt, idx)
    }

    unsafe fn bind_int64(&self, stmt: NonNull<c_void>, idx: i32, v: i64) -> Result<()> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::bind_int64(self, stmt, idx, v)
    }

    unsafe fn bind_double(&self, stmt: NonNull<c_void>, idx: i32, v: f64) -> Result<()> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::bind_double(self, stmt, idx, v)
    }

    unsafe fn bind_text(&self, stmt: NonNull<c_void>, idx: i32, v: &str) -> Result<()> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::bind_text(self, stmt, idx, v)
    }

    unsafe fn bind_blob(&self, stmt: NonNull<c_void>, idx: i32, v: &[u8]) -> Result<()> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::bind_blob(self, stmt, idx, v)
    }

    unsafe fn column_count(&self, stmt: NonNull<c_void>) -> i32 {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::column_count(self, stmt)
    }

    unsafe fn column_type(&self, stmt: NonNull<c_void>, col: i32) -> ValueType {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::column_type(self, stmt, col)
    }

    unsafe fn column_int64(&self, stmt: NonNull<c_void>, col: i32) -> i64 {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::column_int64(self, stmt, col)
    }

    unsafe fn column_double(&self, stmt: NonNull<c_void>, col: i32) -> f64 {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::column_double(self, stmt, col)
    }

    unsafe fn column_text(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::column_text(self, stmt, col)
    }

    unsafe fn column_blob(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Api::column_blob(self, stmt, col)
    }

    unsafe fn errcode(&self, db: NonNull<c_void>) -> i32 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Api::errcode(self, db)
    }

    unsafe fn errmsg(&self, db: NonNull<c_void>) -> *const c_char {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Api::errmsg(self, db)
    }

    unsafe fn extended_errcode(&self, db: NonNull<c_void>) -> Option<i32> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Api::extended_errcode(self, db)
    }

    unsafe fn create_function_v2(
        &self,
        db: NonNull<c_void>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_func: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        x_step: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        x_final: Option<extern "C" fn(*mut c_void)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        let x_func = x_func.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f));
        let x_step = x_step.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f));
        let x_final = x_final.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context)>(f));
        Sqlite3Api::create_function_v2(
            self,
            db,
            name,
            n_args,
            flags,
            x_func,
            x_step,
            x_final,
            user_data,
            drop_user_data,
        )
    }

    unsafe fn create_window_function(
        &self,
        db: NonNull<c_void>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_step: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        x_final: Option<extern "C" fn(*mut c_void)>,
        x_value: Option<extern "C" fn(*mut c_void)>,
        x_inverse: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        let x_step = x_step.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f));
        let x_final = x_final.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context)>(f));
        let x_value = x_value.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context)>(f));
        let x_inverse = x_inverse.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f));
        Sqlite3Api::create_window_function(
            self,
            db,
            name,
            n_args,
            flags,
            x_step,
            x_final,
            x_value,
            x_inverse,
            user_data,
            drop_user_data,
        )
    }

    unsafe fn aggregate_context(&self, ctx: NonNull<c_void>, bytes: usize) -> *mut c_void {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3Api::aggregate_context(self, ctx, bytes)
    }

    unsafe fn result_null(&self, ctx: NonNull<c_void>) {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3Api::result_null(self, ctx)
    }

    unsafe fn result_int64(&self, ctx: NonNull<c_void>, v: i64) {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3Api::result_int64(self, ctx, v)
    }

    unsafe fn result_double(&self, ctx: NonNull<c_void>, v: f64) {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3Api::result_double(self, ctx, v)
    }

    unsafe fn result_text(&self, ctx: NonNull<c_void>, v: &str) {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3Api::result_text(self, ctx, v)
    }

    unsafe fn result_blob(&self, ctx: NonNull<c_void>, v: &[u8]) {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3Api::result_blob(self, ctx, v)
    }

    unsafe fn result_error(&self, ctx: NonNull<c_void>, msg: &str) {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3Api::result_error(self, ctx, msg)
    }

    unsafe fn user_data(&self, ctx: NonNull<c_void>) -> *mut c_void {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        P::user_data(ctx)
    }

    unsafe fn value_type(&self, v: NonNull<c_void>) -> ValueType {
        let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
        Sqlite3Api::value_type(self, v)
    }

    unsafe fn value_int64(&self, v: NonNull<c_void>) -> i64 {
        let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
        Sqlite3Api::value_int64(self, v)
    }

    unsafe fn value_double(&self, v: NonNull<c_void>) -> f64 {
        let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
        Sqlite3Api::value_double(self, v)
    }

    unsafe fn value_text(&self, v: NonNull<c_void>) -> RawBytes {
        let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
        Sqlite3Api::value_text(self, v)
    }

    unsafe fn value_blob(&self, v: NonNull<c_void>) -> RawBytes {
        let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
        Sqlite3Api::value_blob(self, v)
    }
}

/// Optional hook SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Hooks`.
pub unsafe trait AbiHooks: Send + Sync + 'static {
    unsafe fn trace_v2(
        &self,
        db: NonNull<c_void>,
        mask: u32,
        callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
        context: *mut c_void,
    ) -> Result<()>;
    unsafe fn progress_handler(
        &self,
        db: NonNull<c_void>,
        n: i32,
        callback: Option<extern "C" fn() -> i32>,
        context: *mut c_void,
    ) -> Result<()>;
    unsafe fn busy_timeout(&self, db: NonNull<c_void>, ms: i32) -> Result<()>;
    unsafe fn set_authorizer(
        &self,
        db: NonNull<c_void>,
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
    ) -> Result<()>;
}

unsafe impl<P: Sqlite3Hooks> AbiHooks for P {
    unsafe fn trace_v2(
        &self,
        db: NonNull<c_void>,
        mask: u32,
        callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
        context: *mut c_void,
    ) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Hooks::trace_v2(self, db, mask, callback, context)
    }

    unsafe fn progress_handler(
        &self,
        db: NonNull<c_void>,
        n: i32,
        callback: Option<extern "C" fn() -> i32>,
        context: *mut c_void,
    ) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Hooks::progress_handler(self, db, n, callback, context)
    }

    unsafe fn busy_timeout(&self, db: NonNull<c_void>, ms: i32) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Hooks::busy_timeout(self, db, ms)
    }

    unsafe fn set_authorizer(
        &self,
        db: NonNull<c_void>,
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
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Hooks::set_authorizer(self, db, callback, context)
    }
}

/// Optional backup SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Backup`.
pub unsafe trait AbiBackup: Send + Sync + 'static {
    unsafe fn backup_init(
        &self,
        dest_db: NonNull<c_void>,
        dest_name: &str,
        source_db: NonNull<c_void>,
        source_name: &str,
    ) -> Result<NonNull<c_void>>;
    unsafe fn backup_step(&self, backup: NonNull<c_void>, pages: i32) -> Result<()>;
    unsafe fn backup_remaining(&self, backup: NonNull<c_void>) -> i32;
    unsafe fn backup_pagecount(&self, backup: NonNull<c_void>) -> i32;
    unsafe fn backup_finish(&self, backup: NonNull<c_void>) -> Result<()>;
}

unsafe impl<P: Sqlite3Backup> AbiBackup for P {
    unsafe fn backup_init(
        &self,
        dest_db: NonNull<c_void>,
        dest_name: &str,
        source_db: NonNull<c_void>,
        source_name: &str,
    ) -> Result<NonNull<c_void>> {
        let dest_db = NonNull::new_unchecked(dest_db.as_ptr() as *mut P::Db);
        let source_db = NonNull::new_unchecked(source_db.as_ptr() as *mut P::Db);
        let backup = Sqlite3Backup::backup_init(self, dest_db, dest_name, source_db, source_name)?;
        Ok(NonNull::new_unchecked(backup.as_ptr() as *mut c_void))
    }

    unsafe fn backup_step(&self, backup: NonNull<c_void>, pages: i32) -> Result<()> {
        let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
        Sqlite3Backup::backup_step(self, backup, pages)
    }

    unsafe fn backup_remaining(&self, backup: NonNull<c_void>) -> i32 {
        let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
        Sqlite3Backup::backup_remaining(self, backup)
    }

    unsafe fn backup_pagecount(&self, backup: NonNull<c_void>) -> i32 {
        let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
        Sqlite3Backup::backup_pagecount(self, backup)
    }

    unsafe fn backup_finish(&self, backup: NonNull<c_void>) -> Result<()> {
        let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
        Sqlite3Backup::backup_finish(self, backup)
    }
}

/// Optional blob SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3BlobIo`.
pub unsafe trait AbiBlobIo: Send + Sync + 'static {
    unsafe fn blob_open(
        &self,
        db: NonNull<c_void>,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<NonNull<c_void>>;
    unsafe fn blob_read(&self, blob: NonNull<c_void>, data: &mut [u8], offset: i32) -> Result<()>;
    unsafe fn blob_write(&self, blob: NonNull<c_void>, data: &[u8], offset: i32) -> Result<()>;
    unsafe fn blob_bytes(&self, blob: NonNull<c_void>) -> i32;
    unsafe fn blob_close(&self, blob: NonNull<c_void>) -> Result<()>;
}

unsafe impl<P: Sqlite3BlobIo> AbiBlobIo for P {
    unsafe fn blob_open(
        &self,
        db: NonNull<c_void>,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<NonNull<c_void>> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        let blob = Sqlite3BlobIo::blob_open(self, db, db_name, table, column, rowid, flags)?;
        Ok(NonNull::new_unchecked(blob.as_ptr() as *mut c_void))
    }

    unsafe fn blob_read(&self, blob: NonNull<c_void>, data: &mut [u8], offset: i32) -> Result<()> {
        let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
        Sqlite3BlobIo::blob_read(self, blob, data, offset)
    }

    unsafe fn blob_write(&self, blob: NonNull<c_void>, data: &[u8], offset: i32) -> Result<()> {
        let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
        Sqlite3BlobIo::blob_write(self, blob, data, offset)
    }

    unsafe fn blob_bytes(&self, blob: NonNull<c_void>) -> i32 {
        let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
        Sqlite3BlobIo::blob_bytes(self, blob)
    }

    unsafe fn blob_close(&self, blob: NonNull<c_void>) -> Result<()> {
        let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
        Sqlite3BlobIo::blob_close(self, blob)
    }
}

/// Optional serialize SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Serialize`.
pub unsafe trait AbiSerialize: Send + Sync + 'static {
    unsafe fn serialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        flags: u32,
    ) -> Result<sqlite_provider::OwnedBytes>;
    unsafe fn deserialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        data: &[u8],
        flags: u32,
    ) -> Result<()>;
    unsafe fn free(&self, bytes: sqlite_provider::OwnedBytes);
}

unsafe impl<P: Sqlite3Serialize> AbiSerialize for P {
    unsafe fn serialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        flags: u32,
    ) -> Result<sqlite_provider::OwnedBytes> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Serialize::serialize(self, db, schema, flags)
    }

    unsafe fn deserialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        data: &[u8],
        flags: u32,
    ) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Serialize::deserialize(self, db, schema, data, flags)
    }

    unsafe fn free(&self, bytes: sqlite_provider::OwnedBytes) {
        Sqlite3Serialize::free(self, bytes)
    }
}

/// Optional WAL SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Wal`.
pub unsafe trait AbiWal: Send + Sync + 'static {
    unsafe fn wal_checkpoint(&self, db: NonNull<c_void>, db_name: Option<&str>) -> Result<()>;
    unsafe fn wal_checkpoint_v2(
        &self,
        db: NonNull<c_void>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)>;
    unsafe fn wal_frame_count(&self, db: NonNull<c_void>) -> Result<Option<u32>>;
}

unsafe impl<P: Sqlite3Wal> AbiWal for P {
    unsafe fn wal_checkpoint(&self, db: NonNull<c_void>, db_name: Option<&str>) -> Result<()> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Wal::wal_checkpoint(self, db, db_name)
    }

    unsafe fn wal_checkpoint_v2(
        &self,
        db: NonNull<c_void>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Wal::wal_checkpoint_v2(self, db, db_name, mode)
    }

    unsafe fn wal_frame_count(&self, db: NonNull<c_void>) -> Result<Option<u32>> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Wal::wal_frame_count(self, db)
    }
}

/// Optional metadata SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Metadata`.
pub unsafe trait AbiMetadata: Send + Sync + 'static {
    unsafe fn table_column_metadata(
        &self,
        db: NonNull<c_void>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata>;
    unsafe fn column_decltype(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes>;
    unsafe fn column_name(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes>;
    unsafe fn column_table_name(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes>;
}

unsafe impl<P: Sqlite3Metadata> AbiMetadata for P {
    unsafe fn table_column_metadata(
        &self,
        db: NonNull<c_void>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3Metadata::table_column_metadata(self, db, db_name, table, column)
    }

    unsafe fn column_decltype(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Metadata::column_decltype(self, stmt, col)
    }

    unsafe fn column_name(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Metadata::column_name(self, stmt, col)
    }

    unsafe fn column_table_name(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3Metadata::column_table_name(self, stmt, col)
    }
}

/// Extra optional SPI needed by the ABI layer for non-core SQLite helpers.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Api`.
pub unsafe trait Sqlite3AbiExtras: Sqlite3Api {
    unsafe fn db_filename(&self, db: NonNull<Self::Db>, name: Option<&str>) -> Option<RawBytes>;
    unsafe fn get_autocommit(&self, db: NonNull<Self::Db>) -> i32;
    unsafe fn total_changes(&self, db: NonNull<Self::Db>) -> i32;
    unsafe fn changes(&self, db: NonNull<Self::Db>) -> i32;
    unsafe fn changes64(&self, db: NonNull<Self::Db>) -> i64;
    unsafe fn last_insert_rowid(&self, db: NonNull<Self::Db>) -> i64;
    unsafe fn interrupt(&self, db: NonNull<Self::Db>);
    unsafe fn db_config(&self, db: NonNull<Self::Db>, op: i32) -> i32;
    unsafe fn limit(&self, db: NonNull<Self::Db>, id: i32, new_value: i32) -> i32;
    unsafe fn stmt_readonly(&self, stmt: NonNull<Self::Stmt>) -> i32;
    unsafe fn stmt_busy(&self, stmt: NonNull<Self::Stmt>) -> i32;
    unsafe fn bind_parameter_count(&self, stmt: NonNull<Self::Stmt>) -> i32;
    unsafe fn bind_parameter_name(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Option<RawBytes>;
    unsafe fn bind_parameter_index(&self, stmt: NonNull<Self::Stmt>, name: &str) -> i32;
    unsafe fn context_db_handle(&self, ctx: NonNull<Self::Context>) -> Option<NonNull<Self::Db>>;
}

/// Object-safe wrapper for `Sqlite3AbiExtras`.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3AbiExtras`.
pub unsafe trait AbiExtras: Send + Sync + 'static {
    unsafe fn db_filename(&self, db: NonNull<c_void>, name: Option<&str>) -> Option<RawBytes>;
    unsafe fn get_autocommit(&self, db: NonNull<c_void>) -> i32;
    unsafe fn total_changes(&self, db: NonNull<c_void>) -> i32;
    unsafe fn changes(&self, db: NonNull<c_void>) -> i32;
    unsafe fn changes64(&self, db: NonNull<c_void>) -> i64;
    unsafe fn last_insert_rowid(&self, db: NonNull<c_void>) -> i64;
    unsafe fn interrupt(&self, db: NonNull<c_void>);
    unsafe fn db_config(&self, db: NonNull<c_void>, op: i32) -> i32;
    unsafe fn limit(&self, db: NonNull<c_void>, id: i32, new_value: i32) -> i32;
    unsafe fn stmt_readonly(&self, stmt: NonNull<c_void>) -> i32;
    unsafe fn stmt_busy(&self, stmt: NonNull<c_void>) -> i32;
    unsafe fn bind_parameter_count(&self, stmt: NonNull<c_void>) -> i32;
    unsafe fn bind_parameter_name(&self, stmt: NonNull<c_void>, idx: i32) -> Option<RawBytes>;
    unsafe fn bind_parameter_index(&self, stmt: NonNull<c_void>, name: &str) -> i32;
    unsafe fn context_db_handle(&self, ctx: NonNull<c_void>) -> Option<NonNull<c_void>>;
}

unsafe impl<P: Sqlite3AbiExtras> AbiExtras for P {
    unsafe fn db_filename(&self, db: NonNull<c_void>, name: Option<&str>) -> Option<RawBytes> {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::db_filename(self, db, name)
    }

    unsafe fn get_autocommit(&self, db: NonNull<c_void>) -> i32 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::get_autocommit(self, db)
    }

    unsafe fn total_changes(&self, db: NonNull<c_void>) -> i32 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::total_changes(self, db)
    }

    unsafe fn changes(&self, db: NonNull<c_void>) -> i32 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::changes(self, db)
    }

    unsafe fn changes64(&self, db: NonNull<c_void>) -> i64 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::changes64(self, db)
    }

    unsafe fn last_insert_rowid(&self, db: NonNull<c_void>) -> i64 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::last_insert_rowid(self, db)
    }

    unsafe fn interrupt(&self, db: NonNull<c_void>) {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::interrupt(self, db)
    }

    unsafe fn db_config(&self, db: NonNull<c_void>, op: i32) -> i32 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::db_config(self, db, op)
    }

    unsafe fn limit(&self, db: NonNull<c_void>, id: i32, new_value: i32) -> i32 {
        let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
        Sqlite3AbiExtras::limit(self, db, id, new_value)
    }

    unsafe fn stmt_readonly(&self, stmt: NonNull<c_void>) -> i32 {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3AbiExtras::stmt_readonly(self, stmt)
    }

    unsafe fn stmt_busy(&self, stmt: NonNull<c_void>) -> i32 {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3AbiExtras::stmt_busy(self, stmt)
    }

    unsafe fn bind_parameter_count(&self, stmt: NonNull<c_void>) -> i32 {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3AbiExtras::bind_parameter_count(self, stmt)
    }

    unsafe fn bind_parameter_name(&self, stmt: NonNull<c_void>, idx: i32) -> Option<RawBytes> {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3AbiExtras::bind_parameter_name(self, stmt, idx)
    }

    unsafe fn bind_parameter_index(&self, stmt: NonNull<c_void>, name: &str) -> i32 {
        let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
        Sqlite3AbiExtras::bind_parameter_index(self, stmt, name)
    }

    unsafe fn context_db_handle(&self, ctx: NonNull<c_void>) -> Option<NonNull<c_void>> {
        let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
        Sqlite3AbiExtras::context_db_handle(self, ctx)
            .map(|db| NonNull::new_unchecked(db.as_ptr() as *mut c_void))
    }
}

/// Provider registration state for the ABI shim.
#[derive(Clone, Copy)]
pub struct ProviderState {
    core: &'static dyn AbiCore,
    hooks: Option<&'static dyn AbiHooks>,
    backup: Option<&'static dyn AbiBackup>,
    blob: Option<&'static dyn AbiBlobIo>,
    serialize: Option<&'static dyn AbiSerialize>,
    wal: Option<&'static dyn AbiWal>,
    metadata: Option<&'static dyn AbiMetadata>,
    extras: Option<&'static dyn AbiExtras>,
    libversion: &'static CStr,
    libversion_number: i32,
}

impl ProviderState {
    pub fn new(core: &'static dyn AbiCore) -> Self {
        let version = core.api_version();
        let number = api_version_number(version);
        let text = format!("{}.{}.{}", version.major, version.minor, version.patch);
        let cstr = CString::new(text).unwrap_or_else(|_| CString::new("0.0.0").unwrap());
        let libversion = Box::leak(cstr.into_boxed_c_str());
        Self {
            core,
            hooks: None,
            backup: None,
            blob: None,
            serialize: None,
            wal: None,
            metadata: None,
            extras: None,
            libversion,
            libversion_number: number,
        }
    }

    pub fn with_hooks(mut self, hooks: &'static dyn AbiHooks) -> Self {
        self.hooks = Some(hooks);
        self
    }

    pub fn with_backup(mut self, backup: &'static dyn AbiBackup) -> Self {
        self.backup = Some(backup);
        self
    }

    pub fn with_blob(mut self, blob: &'static dyn AbiBlobIo) -> Self {
        self.blob = Some(blob);
        self
    }

    pub fn with_serialize(mut self, serialize: &'static dyn AbiSerialize) -> Self {
        self.serialize = Some(serialize);
        self
    }

    pub fn with_wal(mut self, wal: &'static dyn AbiWal) -> Self {
        self.wal = Some(wal);
        self
    }

    pub fn with_metadata(mut self, metadata: &'static dyn AbiMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    pub fn with_extras(mut self, extras: &'static dyn AbiExtras) -> Self {
        self.extras = Some(extras);
        self
    }
}

#[derive(Debug)]
pub enum RegisterError {
    AlreadySet,
}

static PROVIDER: OnceLock<ProviderState> = OnceLock::new();
#[cfg(feature = "default-backend")]
static DEFAULT_BOOTSTRAP_RC: OnceLock<i32> = OnceLock::new();

/// Register the provider state for the ABI shim.
///
/// The provider must outlive the entire process.
pub fn register_provider(state: ProviderState) -> std::result::Result<(), RegisterError> {
    PROVIDER.set(state).map_err(|_| RegisterError::AlreadySet)
}

fn provider() -> Option<&'static ProviderState> {
    PROVIDER.get()
}

fn ensure_default_provider() -> i32 {
    if provider().is_some() {
        return SQLITE_OK;
    }

    #[cfg(feature = "default-backend")]
    {
        let rc = *DEFAULT_BOOTSTRAP_RC.get_or_init(|| {
            let api = match LibSqlite3::load() {
                Some(api) => api,
                None => return SQLITE_CANTOPEN,
            };
            let state = ProviderState::new(api).with_metadata(api);
            match register_provider(state) {
                Ok(()) | Err(RegisterError::AlreadySet) => SQLITE_OK,
            }
        });
        if rc == SQLITE_OK || provider().is_some() {
            return SQLITE_OK;
        }
        return rc;
    }

    #[cfg(not(feature = "default-backend"))]
    {
        SQLITE_MISUSE
    }
}

#[repr(C)]
pub struct sqlite3 {
    db: NonNull<c_void>,
    stmts: *mut sqlite3_stmt,
    filename: Option<CString>,
}

#[repr(C)]
pub struct sqlite3_stmt {
    stmt: NonNull<c_void>,
    db: *mut sqlite3,
    next: *mut sqlite3_stmt,
    last_step: StepResult,
    sql: Option<CString>,
}

unsafe fn add_stmt(db: &mut sqlite3, stmt: *mut sqlite3_stmt) {
    (*stmt).next = db.stmts;
    db.stmts = stmt;
}

unsafe fn remove_stmt(db: &mut sqlite3, stmt: *mut sqlite3_stmt) {
    let mut cur = db.stmts;
    let mut prev: *mut sqlite3_stmt = null_mut();
    while !cur.is_null() {
        if cur == stmt {
            if prev.is_null() {
                db.stmts = (*cur).next;
            } else {
                (*prev).next = (*cur).next;
            }
            break;
        }
        prev = cur;
        cur = (*cur).next;
    }
}

struct SerializedEntry {
    provider: &'static dyn AbiSerialize,
    len: usize,
}

static SERIALIZED: OnceLock<Mutex<std::collections::HashMap<usize, SerializedEntry>>> =
    OnceLock::new();

fn serialized_map() -> &'static Mutex<std::collections::HashMap<usize, SerializedEntry>> {
    SERIALIZED.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

#[no_mangle]
pub extern "C" fn sqlite3_initialize() -> i32 {
    ensure_default_provider()
}

#[no_mangle]
pub extern "C" fn sqlite3_shutdown() -> i32 {
    SQLITE_OK
}

/// Initialize the default backend provider.
///
/// Returns `SQLITE_OK` if a provider is already registered or if the default
/// `libsqlite3` backend initializes successfully.
#[no_mangle]
pub extern "C" fn sqlite3_provider_init_default() -> i32 {
    ensure_default_provider()
}

#[no_mangle]
pub extern "C" fn sqlite3_open(filename: *const c_char, db_out: *mut *mut sqlite3) -> i32 {
    let flags = OpenFlags::READ_WRITE | OpenFlags::CREATE;
    sqlite3_open_v2(filename, db_out, flags.bits() as i32, null())
}

#[no_mangle]
pub extern "C" fn sqlite3_open_v2(
    filename: *const c_char,
    db_out: *mut *mut sqlite3,
    flags: i32,
    vfs: *const c_char,
) -> i32 {
    let init_rc = ensure_default_provider();
    if init_rc != SQLITE_OK {
        return init_rc;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db_out.is_null() {
        return SQLITE_MISUSE;
    }
    let filename = unsafe { cstr_to_str(filename) };
    let filename = match filename {
        Some(name) => name,
        None => return SQLITE_MISUSE,
    };
    let vfs = unsafe { cstr_to_str(vfs) };
    let options = OpenOptions { flags: OpenFlags::from_bits(flags as u32), vfs };
    let db = match unsafe { state.core.open(filename, options) } {
        Ok(db) => db,
        Err(err) => return err_code(&err),
    };
    let filename = CString::new(filename).ok();
    let handle = Box::new(sqlite3 { db, stmts: null_mut(), filename });
    unsafe { *db_out = Box::into_raw(handle) };
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_close(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db_ref = unsafe { &mut *db };
    if !db_ref.stmts.is_null() {
        return SQLITE_BUSY;
    }
    let rc = match unsafe { state.core.close(db_ref.db) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    };
    if rc == SQLITE_OK {
        unsafe { drop(Box::from_raw(db)) };
    }
    rc
}

#[no_mangle]
pub extern "C" fn sqlite3_close_v2(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db_ref = unsafe { &mut *db };
    let mut cur = db_ref.stmts;
    while !cur.is_null() {
        unsafe {
            let next = (*cur).next;
            let _ = state.core.finalize((*cur).stmt);
            drop(Box::from_raw(cur));
            cur = next;
        }
    }
    db_ref.stmts = null_mut();
    let rc = match unsafe { state.core.close(db_ref.db) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    };
    if rc == SQLITE_OK {
        unsafe { drop(Box::from_raw(db)) };
    }
    rc
}

#[no_mangle]
pub extern "C" fn sqlite3_db_filename(db: *mut sqlite3, db_name: *const c_char) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if db.is_null() {
        return null();
    }
    let db_ref = unsafe { &*db };
    let db_name = unsafe { cstr_to_str(db_name) };
    if let Some(extras) = state.extras {
        if let Some(raw) = unsafe { extras.db_filename(db_ref.db, db_name) } {
            return raw.ptr as *const c_char;
        }
    }
    db_ref
        .filename
        .as_ref()
        .map(|name| name.as_ptr())
        .unwrap_or(null())
}

#[no_mangle]
pub extern "C" fn sqlite3_trace_v2(
    db: *mut sqlite3,
    mask: u32,
    callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
    context: *mut c_void,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { hooks.trace_v2(db.db, mask, callback, context) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_progress_handler(
    db: *mut sqlite3,
    n: i32,
    callback: Option<extern "C" fn() -> i32>,
    context: *mut c_void,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { hooks.progress_handler(db.db, n, callback, context) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_busy_timeout(db: *mut sqlite3, ms: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { hooks.busy_timeout(db.db, ms) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_set_authorizer(
    db: *mut sqlite3,
    callback: Option<extern "C" fn() -> i32>,
    context: *mut c_void,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    let callback = callback.map(|f| unsafe {
        std::mem::transmute::<
            extern "C" fn() -> i32,
            extern "C" fn(
                *mut c_void,
                i32,
                *const c_char,
                *const c_char,
                *const c_char,
                *const c_char,
            ) -> i32,
        >(f)
    });
    match unsafe { hooks.set_authorizer(db.db, callback, context) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_context_db_handle(context: *mut c_void) -> *mut sqlite3 {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let extras = match state.extras {
        Some(extras) => extras,
        None => return null_mut(),
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return null_mut(),
    };
    match unsafe { extras.context_db_handle(ctx) } {
        Some(db) => db.as_ptr() as *mut sqlite3,
        None => null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_prepare_v2(
    db: *mut sqlite3,
    sql: *const c_char,
    len: i32,
    out_stmt: *mut *mut sqlite3_stmt,
    tail: *mut *const c_char,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || out_stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let sql_bytes = unsafe {
        if sql.is_null() {
            return SQLITE_MISUSE;
        }
        if len < 0 {
            CStr::from_ptr(sql).to_bytes()
        } else {
            std::slice::from_raw_parts(sql as *const u8, len as usize)
        }
    };
    let sql_str = match std::str::from_utf8(sql_bytes) {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let db_ref = unsafe { &mut *db };
    let stmt = match unsafe { state.core.prepare_v2(db_ref.db, sql_str) } {
        Ok(stmt) => stmt,
        Err(err) => return err_code(&err),
    };
    let sql_cstr = CString::new(sql_str).ok();
    let stmt_handle = Box::new(sqlite3_stmt {
        stmt,
        db,
        next: null_mut(),
        last_step: StepResult::Done,
        sql: sql_cstr,
    });
    let stmt_ptr = Box::into_raw(stmt_handle);
    unsafe { add_stmt(db_ref, stmt_ptr) };
    unsafe {
        *out_stmt = stmt_ptr;
        if !tail.is_null() {
            *tail = null();
        }
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = unsafe { &mut *stmt };
    let rc = match unsafe { state.core.finalize(stmt_ref.stmt) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    };
    if !stmt_ref.db.is_null() {
        unsafe { remove_stmt(&mut *stmt_ref.db, stmt) };
    }
    unsafe { drop(Box::from_raw(stmt)) };
    rc
}

#[no_mangle]
pub extern "C" fn sqlite3_step(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = unsafe { &mut *stmt };
    match unsafe { state.core.step(stmt_ref.stmt) } {
        Ok(StepResult::Row) => {
            stmt_ref.last_step = StepResult::Row;
            SQLITE_ROW
        }
        Ok(StepResult::Done) => {
            stmt_ref.last_step = StepResult::Done;
            SQLITE_DONE
        }
        Err(err) => {
            stmt_ref.last_step = StepResult::Done;
            err_code(&err)
        }
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = unsafe { &mut *stmt };
    stmt_ref.last_step = StepResult::Done;
    match unsafe { state.core.reset(stmt_ref.stmt) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_exec(
    db: *mut sqlite3,
    sql: *const c_char,
    callback: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_char, *mut *mut c_char) -> i32>,
    context: *mut c_void,
    err_out: *mut *mut c_char,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || sql.is_null() {
        return SQLITE_MISUSE;
    }
    if !err_out.is_null() {
        unsafe { *err_out = null_mut() };
    }
    let sql_str = unsafe { CStr::from_ptr(sql) };
    let sql_str = match sql_str.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let db_ref = unsafe { &mut *db };
    let stmt = match unsafe { state.core.prepare_v2(db_ref.db, sql_str) } {
        Ok(stmt) => stmt,
        Err(err) => {
            if let Some(msg) = err.message.as_deref() {
                set_err_string(err_out, msg);
            }
            return err_code(&err);
        }
    };
    let col_count = unsafe { state.core.column_count(stmt) };
    let mut name_bufs: Vec<Vec<u8>> = Vec::new();
    let mut name_ptrs: Vec<*mut c_char> = vec![null_mut(); col_count as usize];
    if col_count > 0 && callback.is_some() {
        if let Some(metadata) = state.metadata {
            for i in 0..col_count {
                let name = unsafe { metadata.column_name(stmt, i) };
                let bytes = name
                    .and_then(|raw| unsafe { raw.as_str() }.map(|s| s.as_bytes().to_vec()))
                    .unwrap_or_default();
                let mut buf = bytes;
                buf.push(0);
                name_ptrs[i as usize] = buf.as_mut_ptr() as *mut c_char;
                name_bufs.push(buf);
            }
        }
    }
    let rc;
    loop {
        match unsafe { state.core.step(stmt) } {
            Ok(StepResult::Row) => {
                if let Some(cb) = callback {
                    let mut value_bufs: Vec<Vec<u8>> = Vec::with_capacity(col_count as usize);
                    let mut value_ptrs: Vec<*mut c_char> = Vec::with_capacity(col_count as usize);
                    for i in 0..col_count {
                        let raw = unsafe { state.core.column_text(stmt, i) };
                        if raw.ptr.is_null() {
                            value_ptrs.push(null_mut());
                            value_bufs.push(Vec::new());
                            continue;
                        }
                        let mut buf = unsafe { raw.as_slice() }.to_vec();
                        buf.push(0);
                        value_ptrs.push(buf.as_mut_ptr() as *mut c_char);
                        value_bufs.push(buf);
                    }
                    let cb_rc = cb(
                        context,
                        col_count,
                        value_ptrs.as_mut_ptr(),
                        name_ptrs.as_mut_ptr(),
                    );
                    if cb_rc != SQLITE_OK {
                        rc = SQLITE_ABORT;
                        break;
                    }
                }
            }
            Ok(StepResult::Done) => {
                rc = SQLITE_OK;
                break;
            }
            Err(err) => {
                rc = err_code(&err);
                if let Some(msg) = err.message.as_deref() {
                    set_err_string(err_out, msg);
                }
                break;
            }
        }
    }
    let _ = unsafe { state.core.finalize(stmt) };
    if rc != SQLITE_OK && !err_out.is_null() {
        let msg_ptr = sqlite3_errmsg(db);
        if !msg_ptr.is_null() {
            let msg = unsafe { CStr::from_ptr(msg_ptr).to_bytes() };
            let out = sqlite3_malloc64((msg.len() + 1) as i32) as *mut c_char;
            if !out.is_null() {
                unsafe {
                    std::ptr::copy_nonoverlapping(msg.as_ptr(), out as *mut u8, msg.len());
                    *out.add(msg.len()) = 0;
                    *err_out = out;
                }
            }
        }
    }
    rc
}

#[no_mangle]
pub extern "C" fn sqlite3_changes(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.changes(db.db) },
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_changes64(db: *mut sqlite3) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.changes64(db.db) },
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_stmt_readonly(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.stmt_readonly(stmt.stmt) },
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_stmt_busy(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.stmt_busy(stmt.stmt) },
        None => {
            if stmt.last_step == StepResult::Row {
                1
            } else {
                0
            }
        }
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_next_stmt(db: *mut sqlite3, stmt: *mut sqlite3_stmt) -> *mut sqlite3_stmt {
    if db.is_null() {
        return null_mut();
    }
    let db = unsafe { &*db };
    if stmt.is_null() {
        return db.stmts;
    }
    let mut cur = db.stmts;
    while !cur.is_null() {
        if cur == stmt {
            return unsafe { (*cur).next };
        }
        cur = unsafe { (*cur).next };
    }
    null_mut()
}

#[no_mangle]
pub extern "C" fn sqlite3_get_autocommit(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 1,
    };
    if db.is_null() {
        return 1;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.get_autocommit(db.db) },
        None => 1,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_total_changes(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.total_changes(db.db) },
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.last_insert_rowid(db.db) },
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_interrupt(db: *mut sqlite3) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    if db.is_null() {
        return;
    }
    let db = unsafe { &*db };
    if let Some(extras) = state.extras {
        unsafe { extras.interrupt(db.db) };
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_db_config(db: *mut sqlite3, op: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.db_config(db.db, op) },
        None => SQLITE_MISUSE,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_limit(db: *mut sqlite3, id: i32, new_value: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return -1,
    };
    if db.is_null() {
        return -1;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.limit(db.db, id, new_value) },
        None => -1,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_db_handle(stmt: *mut sqlite3_stmt) -> *mut sqlite3 {
    if stmt.is_null() {
        return null_mut();
    }
    unsafe { (*stmt).db }
}

#[no_mangle]
pub extern "C" fn sqlite3_sleep(ms: i32) {
    if ms <= 0 {
        return;
    }
    std::thread::sleep(std::time::Duration::from_millis(ms as u64));
}

#[no_mangle]
pub extern "C" fn sqlite3_errcode(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    unsafe { state.core.errcode(db.db) }
}

#[no_mangle]
pub extern "C" fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if db.is_null() {
        return null();
    }
    let db = unsafe { &*db };
    unsafe { state.core.errmsg(db.db) }
}

#[no_mangle]
pub extern "C" fn sqlite3_extended_errcode(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    unsafe { state.core.extended_errcode(db.db) }.unwrap_or(SQLITE_ERROR)
}

#[no_mangle]
pub extern "C" fn sqlite3_errstr(code: i32) -> *const c_char {
    let msg: &'static [u8] = match code {
        SQLITE_OK => b"not an error\0",
        SQLITE_ERROR => b"SQL error or missing database\0",
        SQLITE_ABORT => b"abort\0",
        SQLITE_BUSY => b"database is locked\0",
        SQLITE_NOMEM => b"out of memory\0",
        SQLITE_INTERRUPT => b"interrupted\0",
        SQLITE_NOTFOUND => b"unknown operation\0",
        SQLITE_CANTOPEN => b"unable to open database\0",
        SQLITE_MISUSE => b"misuse\0",
        _ => b"unknown error\0",
    };
    unsafe { CStr::from_bytes_with_nul_unchecked(msg).as_ptr() }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_parameter_count(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.bind_parameter_count(stmt.stmt) },
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_parameter_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.bind_parameter_name(stmt.stmt, idx) }
            .map(|raw| raw.ptr as *const c_char)
            .unwrap_or(null()),
        None => null(),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_parameter_index(stmt: *mut sqlite3_stmt, name: *const c_char) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let name = unsafe { cstr_to_str(name) };
    let name = match name {
        Some(name) => name,
        None => return 0,
    };
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.bind_parameter_index(stmt.stmt, name) },
        None => 0,
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_null(stmt.stmt, idx) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, idx: i32, val: i64) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_int64(stmt.stmt, idx, val) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_double(stmt: *mut sqlite3_stmt, idx: i32, val: f64) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_double(stmt.stmt, idx, val) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_text(
    stmt: *mut sqlite3_stmt,
    idx: i32,
    text: *const c_char,
    len: i32,
    destroy: *mut c_void,
) -> i32 {
    let _ = destroy;
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    if text.is_null() {
        return sqlite3_bind_null(stmt, idx);
    }
    let text = unsafe { text_from_ptr(text, len) };
    let text = match text {
        Some(text) => text,
        None => return SQLITE_MISUSE,
    };
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_text(stmt.stmt, idx, text) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_bind_blob(
    stmt: *mut sqlite3_stmt,
    idx: i32,
    blob: *const c_void,
    len: i32,
    destroy: *mut c_void,
) -> i32 {
    let _ = destroy;
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let data = match unsafe { bytes_from_ptr(blob, len) } {
        Some(data) => data,
        None => return SQLITE_MISUSE,
    };
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_blob(stmt.stmt, idx, data) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_column_type(stmt: *mut sqlite3_stmt, idx: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_NULL,
    };
    if stmt.is_null() {
        return SQLITE_NULL;
    }
    let stmt = unsafe { &*stmt };
    value_type_to_code(unsafe { state.core.column_type(stmt.stmt, idx) })
}

#[no_mangle]
pub extern "C" fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    unsafe { state.core.column_count(stmt.stmt) }
}

#[no_mangle]
pub extern "C" fn sqlite3_column_decltype(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    unsafe { metadata.column_decltype(stmt.stmt, idx) }
        .map(|raw| raw.ptr as *const c_char)
        .unwrap_or(null())
}

#[no_mangle]
pub extern "C" fn sqlite3_column_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    unsafe { metadata.column_name(stmt.stmt, idx) }
        .map(|raw| raw.ptr as *const c_char)
        .unwrap_or(null())
}

#[no_mangle]
pub extern "C" fn sqlite3_column_table_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    unsafe { metadata.column_table_name(stmt.stmt, idx) }
        .map(|raw| raw.ptr as *const c_char)
        .unwrap_or(null())
}

#[no_mangle]
pub extern "C" fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, idx: i32) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    unsafe { state.core.column_int64(stmt.stmt, idx) }
}

#[no_mangle]
pub extern "C" fn sqlite3_column_double(stmt: *mut sqlite3_stmt, idx: i32) -> f64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0.0,
    };
    if stmt.is_null() {
        return 0.0;
    }
    let stmt = unsafe { &*stmt };
    unsafe { state.core.column_double(stmt.stmt, idx) }
}

#[no_mangle]
pub extern "C" fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    let raw = unsafe { state.core.column_blob(stmt.stmt, idx) };
    raw.ptr as *const c_void
}

#[no_mangle]
pub extern "C" fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, idx: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    let ty = unsafe { state.core.column_type(stmt.stmt, idx) };
    let raw = match ty {
        ValueType::Blob => unsafe { state.core.column_blob(stmt.stmt, idx) },
        _ => unsafe { state.core.column_text(stmt.stmt, idx) },
    };
    clamp_len(raw.len)
}

#[no_mangle]
pub extern "C" fn sqlite3_column_text(stmt: *mut sqlite3_stmt, idx: i32) -> *const u8 {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    let raw = unsafe { state.core.column_text(stmt.stmt, idx) };
    raw.ptr
}

#[no_mangle]
pub extern "C" fn sqlite3_value_type(value: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_NULL,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return SQLITE_NULL,
    };
    value_type_to_code(unsafe { state.core.value_type(value) })
}

#[no_mangle]
pub extern "C" fn sqlite3_value_int64(value: *mut c_void) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return 0,
    };
    unsafe { state.core.value_int64(value) }
}

#[no_mangle]
pub extern "C" fn sqlite3_value_double(value: *mut c_void) -> f64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0.0,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return 0.0,
    };
    unsafe { state.core.value_double(value) }
}

#[no_mangle]
pub extern "C" fn sqlite3_value_text(value: *mut c_void) -> *const u8 {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return null(),
    };
    let raw = unsafe { state.core.value_text(value) };
    raw.ptr
}

#[no_mangle]
pub extern "C" fn sqlite3_value_blob(value: *mut c_void) -> *const c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return null(),
    };
    let raw = unsafe { state.core.value_blob(value) };
    raw.ptr as *const c_void
}

#[no_mangle]
pub extern "C" fn sqlite3_value_bytes(value: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return 0,
    };
    let ty = unsafe { state.core.value_type(value) };
    let raw = match ty {
        ValueType::Blob => unsafe { state.core.value_blob(value) },
        _ => unsafe { state.core.value_text(value) },
    };
    clamp_len(raw.len)
}

#[no_mangle]
pub extern "C" fn sqlite3_get_table(
    db: *mut sqlite3,
    sql: *const c_char,
    paz_result: *mut *mut *mut c_char,
    pn_row: *mut i32,
    pn_column: *mut i32,
    pz_err_msg: *mut *mut c_char,
) -> i32 {
    if paz_result.is_null() || pn_row.is_null() || pn_column.is_null() {
        return SQLITE_MISUSE;
    }
    if !pz_err_msg.is_null() {
        unsafe { *pz_err_msg = null_mut() };
    }
    unsafe {
        *paz_result = null_mut();
        *pn_row = 0;
        *pn_column = 0;
    }
    let sql = match unsafe { cstr_to_str(sql) } {
        Some(sql) => sql,
        None => return SQLITE_MISUSE,
    };
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db_ref = unsafe { &mut *db };
    let stmt = match unsafe { state.core.prepare_v2(db_ref.db, sql) } {
        Ok(stmt) => stmt,
        Err(err) => {
            if let Some(msg) = err.message.as_deref() {
                set_err_string(pz_err_msg, msg);
            }
            return err_code(&err);
        }
    };
    let col_count = unsafe { state.core.column_count(stmt) } as usize;
    let mut cells: Vec<Option<Vec<u8>>> = Vec::new();
    cells.reserve(col_count);
    for i in 0..col_count {
        let name = if let Some(metadata) = state.metadata {
            unsafe { metadata.column_name(stmt, i as i32) }
        } else {
            None
        };
        let bytes = name
            .and_then(|raw| unsafe { raw.as_str() }.map(|s| s.as_bytes().to_vec()))
            .unwrap_or_default();
        cells.push(Some(bytes));
    }
    let mut rows = 0usize;
    loop {
        match unsafe { state.core.step(stmt) } {
            Ok(StepResult::Row) => {
                for i in 0..col_count {
                    let raw = unsafe { state.core.column_text(stmt, i as i32) };
                    if raw.ptr.is_null() {
                        cells.push(None);
                    } else {
                        let buf = unsafe { raw.as_slice() }.to_vec();
                        cells.push(Some(buf));
                    }
                }
                rows += 1;
            }
            Ok(StepResult::Done) => break,
            Err(err) => {
                let _ = unsafe { state.core.finalize(stmt) };
                if !pz_err_msg.is_null() {
                    if let Some(msg) = err.message.as_deref() {
                        set_err_string(pz_err_msg, msg);
                    }
                }
                return err_code(&err);
            }
        }
    }
    let _ = unsafe { state.core.finalize(stmt) };

    let total = (rows + 1) * col_count;
    let array_bytes = total
        .checked_add(1)
        .and_then(|v| v.checked_mul(std::mem::size_of::<*mut c_char>()))
        .unwrap_or(0);
    if array_bytes > i32::MAX as usize {
        return SQLITE_NOMEM;
    }
    let base = sqlite3_malloc64(array_bytes as i32) as *mut *mut c_char;
    if base.is_null() {
        return SQLITE_NOMEM;
    }
    let array_ptr = unsafe { base.add(1) };
    unsafe { *(base as *mut usize) = total };
    unsafe { *pn_row = rows as i32 };
    unsafe { *pn_column = col_count as i32 };
    let mut allocated = 0usize;
    for (idx, cell) in cells.into_iter().enumerate() {
        let slot = unsafe { array_ptr.add(idx) };
        match cell {
            Some(mut buf) => {
                buf.push(0);
                let ptr = sqlite3_malloc64(buf.len() as i32) as *mut c_char;
                if ptr.is_null() {
                    unsafe { *(base as *mut usize) = allocated };
                    sqlite3_free_table(array_ptr);
                    return SQLITE_NOMEM;
                }
                unsafe {
                    std::ptr::copy_nonoverlapping(buf.as_ptr(), ptr as *mut u8, buf.len());
                    *slot = ptr;
                }
            }
            None => unsafe {
                *slot = null_mut();
            },
        }
        allocated += 1;
    }
    unsafe { *paz_result = array_ptr };
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_free_table(paz_result: *mut *mut c_char) {
    if paz_result.is_null() {
        return;
    }
    unsafe {
        let base = (paz_result as *mut usize).offset(-1);
        let count = *base as usize;
        for idx in 0..count {
            let ptr = *paz_result.add(idx);
            if !ptr.is_null() {
                sqlite3_free(ptr as *mut c_void);
            }
        }
        sqlite3_free(base as *mut c_void);
    }
}

#[no_mangle]
pub extern "C" fn sqlite_get_table_cb(
    _context: *mut c_void,
    _n_column: i32,
    _argv: *mut *mut c_char,
    _colv: *mut *mut c_char,
) -> i32 {
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_result_null(context: *mut c_void) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_null(ctx) };
}

#[no_mangle]
pub extern "C" fn sqlite3_result_int64(context: *mut c_void, val: i64) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_int64(ctx, val) };
}

#[no_mangle]
pub extern "C" fn sqlite3_result_double(context: *mut c_void, val: f64) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_double(ctx, val) };
}

#[no_mangle]
pub extern "C" fn sqlite3_result_text(
    context: *mut c_void,
    text: *const c_char,
    len: i32,
    _destroy: *mut c_void,
) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    let text = unsafe { text_from_ptr(text, len) };
    match text {
        Some(text) => unsafe { state.core.result_text(ctx, text) },
        None => unsafe { state.core.result_error(ctx, "invalid utf-8") },
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_result_blob(
    context: *mut c_void,
    blob: *const c_void,
    len: i32,
    _destroy: *mut c_void,
) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    let blob = match unsafe { bytes_from_ptr(blob, len) } {
        Some(blob) => blob,
        None => {
            unsafe { state.core.result_error(ctx, "invalid blob") };
            return;
        }
    };
    unsafe { state.core.result_blob(ctx, blob) };
}

#[no_mangle]
pub extern "C" fn sqlite3_result_error_nomem(context: *mut c_void) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_error(ctx, "out of memory") };
}

#[no_mangle]
pub extern "C" fn sqlite3_result_error_toobig(context: *mut c_void) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_error(ctx, "string or blob too big") };
}

#[no_mangle]
pub extern "C" fn sqlite3_result_error(
    context: *mut c_void,
    err: *const c_char,
    len: i32,
) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    let err = unsafe { text_from_ptr(err, len) };
    match err {
        Some(msg) => unsafe { state.core.result_error(ctx, msg) },
        None => unsafe { state.core.result_error(ctx, "error") },
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_aggregate_context(context: *mut c_void, n: i32) -> *mut c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return null_mut(),
    };
    let n = if n < 0 { 0 } else { n as usize };
    unsafe { state.core.aggregate_context(ctx, n) }
}

#[no_mangle]
pub extern "C" fn sqlite3_user_data(context: *mut c_void) -> *mut c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return null_mut(),
    };
    unsafe { state.core.user_data(ctx) }
}

#[no_mangle]
pub extern "C" fn sqlite3_create_function_v2(
    db: *mut sqlite3,
    name: *const c_char,
    n_args: i32,
    enc: i32,
    context: *mut c_void,
    func: Option<extern "C" fn()>,
    step: Option<extern "C" fn()>,
    final_: Option<extern "C" fn()>,
    destroy: Option<extern "C" fn()>,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || name.is_null() {
        return SQLITE_MISUSE;
    }
    let name = unsafe { cstr_to_str(name) };
    let name = match name {
        Some(name) => name,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    let func = func.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void, i32, *mut *mut c_void)>(f) });
    let step = step.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void, i32, *mut *mut c_void)>(f) });
    let final_ = final_.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void)>(f) });
    let destroy = destroy.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void)>(f) });
    match unsafe {
        state.core.create_function_v2(
            db.db,
            name,
            n_args,
            FunctionFlags::from_bits(enc as u32),
            func,
            step,
            final_,
            context,
            destroy,
        )
    } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_create_window_function(
    db: *mut sqlite3,
    name: *const c_char,
    n_args: i32,
    enc: i32,
    context: *mut c_void,
    x_step: Option<extern "C" fn()>,
    x_final: Option<extern "C" fn()>,
    x_value: Option<extern "C" fn()>,
    x_inverse: Option<extern "C" fn()>,
    destroy: Option<extern "C" fn()>,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || name.is_null() {
        return SQLITE_MISUSE;
    }
    let name = unsafe { cstr_to_str(name) };
    let name = match name {
        Some(name) => name,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    let x_step = x_step.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void, i32, *mut *mut c_void)>(f) });
    let x_final = x_final.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void)>(f) });
    let x_value = x_value.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void)>(f) });
    let x_inverse = x_inverse.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void, i32, *mut *mut c_void)>(f) });
    let destroy = destroy.map(|f| unsafe { std::mem::transmute::<extern "C" fn(), extern "C" fn(*mut c_void)>(f) });
    match unsafe {
        state.core.create_window_function(
            db.db,
            name,
            n_args,
            FunctionFlags::from_bits(enc as u32),
            x_step,
            x_final,
            x_value,
            x_inverse,
            context,
            destroy,
        )
    } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_create_collation_v2(
    _db: *mut sqlite3,
    _name: *const c_char,
    _enc: i32,
    _context: *mut c_void,
    _cmp: Option<extern "C" fn()>,
    _destroy: Option<extern "C" fn()>,
) -> i32 {
    SQLITE_MISUSE
}

#[no_mangle]
pub extern "C" fn sqlite3_backup_init(
    dest_db: *mut sqlite3,
    dest_name: *const c_char,
    source_db: *mut sqlite3,
    source_name: *const c_char,
) -> *mut c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let backup = match state.backup {
        Some(backup) => backup,
        None => return null_mut(),
    };
    if dest_db.is_null() || source_db.is_null() {
        return null_mut();
    }
    let dest_name = unsafe { cstr_to_str(dest_name) }.unwrap_or("main");
    let source_name = unsafe { cstr_to_str(source_name) }.unwrap_or("main");
    let dest_db = unsafe { &*dest_db };
    let source_db = unsafe { &*source_db };
    match unsafe { backup.backup_init(dest_db.db, dest_name, source_db.db, source_name) } {
        Ok(handle) => handle.as_ptr(),
        Err(_) => null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_backup_step(backup: *mut c_void, pages: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    match unsafe { backup_api.backup_step(backup, pages) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_backup_remaining(backup: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return 0,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return 0,
    };
    unsafe { backup_api.backup_remaining(backup) }
}

#[no_mangle]
pub extern "C" fn sqlite3_backup_pagecount(backup: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return 0,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return 0,
    };
    unsafe { backup_api.backup_pagecount(backup) }
}

#[no_mangle]
pub extern "C" fn sqlite3_backup_finish(backup: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    match unsafe { backup_api.backup_finish(backup) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_serialize(
    db: *mut sqlite3,
    schema: *const c_char,
    out: *mut *mut c_void,
    out_bytes: *mut i32,
    flags: u32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let serialize = match state.serialize {
        Some(serialize) => serialize,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || out.is_null() || out_bytes.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    let schema = unsafe { cstr_to_str(schema) };
    let bytes = match unsafe { serialize.serialize(db.db, schema, flags) } {
        Ok(bytes) => bytes,
        Err(err) => return err_code(&err),
    };
    unsafe {
        *out = bytes.ptr.as_ptr() as *mut c_void;
        *out_bytes = clamp_len(bytes.len);
    }
    let key = bytes.ptr.as_ptr() as usize;
    let entry = SerializedEntry { provider: serialize, len: bytes.len };
    if let Ok(mut map) = serialized_map().lock() {
        map.insert(key, entry);
    } else {
        unsafe { serialize.free(bytes) };
        return SQLITE_MISUSE;
    }
    SQLITE_OK
}

#[no_mangle]
pub extern "C" fn sqlite3_deserialize(
    db: *mut sqlite3,
    schema: *const c_char,
    input: *const c_void,
    input_bytes: i32,
    flags: u32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let serialize = match state.serialize {
        Some(serialize) => serialize,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let input = match unsafe { bytes_from_ptr(input, input_bytes) } {
        Some(bytes) => bytes,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    let schema = unsafe { cstr_to_str(schema) };
    match unsafe { serialize.deserialize(db.db, schema, input, flags) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_blob_open(
    db: *mut sqlite3,
    db_name: *const c_char,
    table_name: *const c_char,
    column_name: *const c_char,
    rowid: i64,
    flags: i32,
    blob_out: *mut *mut c_void,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || blob_out.is_null() {
        return SQLITE_MISUSE;
    }
    let db_name = unsafe { cstr_to_str(db_name) }.unwrap_or("main");
    let table_name = unsafe { cstr_to_str(table_name) }.unwrap_or("");
    let column_name = unsafe { cstr_to_str(column_name) }.unwrap_or("");
    let db = unsafe { &*db };
    match unsafe { blob_api.blob_open(db.db, db_name, table_name, column_name, rowid, flags as u32) } {
        Ok(handle) => {
            unsafe { *blob_out = handle.as_ptr() };
            SQLITE_OK
        }
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_blob_read(blob: *mut c_void, data: *mut c_void, n: i32, offset: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let data = match unsafe { bytes_from_ptr_mut(data, n) } {
        Some(data) => data,
        None => return SQLITE_MISUSE,
    };
    match unsafe { blob_api.blob_read(blob, data, offset) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_blob_write(
    blob: *mut c_void,
    data: *const c_void,
    n: i32,
    offset: i32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let data = match unsafe { bytes_from_ptr(data, n) } {
        Some(data) => data,
        None => return SQLITE_MISUSE,
    };
    match unsafe { blob_api.blob_write(blob, data, offset) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_blob_bytes(blob: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return 0,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return 0,
    };
    unsafe { blob_api.blob_bytes(blob) }
}

#[no_mangle]
pub extern "C" fn sqlite3_blob_close(blob: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    match unsafe { blob_api.blob_close(blob) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_stricmp(a: *const c_char, b: *const c_char) -> i32 {
    if a.is_null() || b.is_null() {
        return if a.is_null() && b.is_null() { 0 } else if a.is_null() { -1 } else { 1 };
    }
    let a = unsafe { CStr::from_ptr(a).to_bytes() };
    let b = unsafe { CStr::from_ptr(b).to_bytes() };
    let mut i = 0;
    while i < a.len() && i < b.len() {
        let ca = a[i].to_ascii_lowercase();
        let cb = b[i].to_ascii_lowercase();
        if ca != cb {
            return ca as i32 - cb as i32;
        }
        i += 1;
    }
    a.len() as i32 - b.len() as i32
}

#[no_mangle]
pub extern "C" fn sqlite3_expanded_sql(stmt: *mut sqlite3_stmt) -> *mut c_char {
    if stmt.is_null() {
        return null_mut();
    }
    let stmt = unsafe { &*stmt };
    let sql = match stmt.sql.as_ref() {
        Some(sql) => sql,
        None => return null_mut(),
    };
    let bytes = sql.as_bytes_with_nul();
    let out = sqlite3_malloc64(bytes.len() as i32) as *mut c_char;
    if out.is_null() {
        return null_mut();
    }
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), out as *mut u8, bytes.len()) };
    out
}

#[no_mangle]
pub extern "C" fn sqlite3_data_count(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    if stmt.last_step != StepResult::Row {
        return 0;
    }
    unsafe { state.core.column_count(stmt.stmt) }
}

#[no_mangle]
pub extern "C" fn sqlite3_malloc64(n: i32) -> *mut c_void {
    if n <= 0 {
        return null_mut();
    }
    unsafe { libc::malloc(n as usize) }
}

#[no_mangle]
pub extern "C" fn sqlite3_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    if let Ok(mut map) = serialized_map().lock() {
        if let Some(entry) = map.remove(&(ptr as usize)) {
            let bytes = sqlite_provider::OwnedBytes {
                ptr: unsafe { NonNull::new_unchecked(ptr as *mut u8) },
                len: entry.len,
            };
            unsafe { entry.provider.free(bytes) };
            return;
        }
    }
    unsafe { libc::free(ptr) };
}

#[no_mangle]
pub extern "C" fn sqlite3_complete(sql: *const c_char) -> i32 {
    let sql = match unsafe { cstr_to_str(sql) } {
        Some(sql) => sql,
        None => return 0,
    };
    let trimmed = sql.trim_end();
    if trimmed.is_empty() {
        return 0;
    }
    if trimmed.ends_with(';') {
        1
    } else {
        0
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_threadsafe() -> i32 {
    1
}

#[no_mangle]
pub extern "C" fn sqlite3_libversion() -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    state.libversion.as_ptr()
}

#[no_mangle]
pub extern "C" fn sqlite3_libversion_number() -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    state.libversion_number
}

#[no_mangle]
pub extern "C" fn sqlite3_wal_checkpoint(db: *mut sqlite3, db_name: *const c_char) -> i32 {
    sqlite3_wal_checkpoint_v2(db, db_name, SQLITE_CHECKPOINT_PASSIVE, null_mut(), null_mut())
}

#[no_mangle]
pub extern "C" fn sqlite3_wal_checkpoint_v2(
    db: *mut sqlite3,
    db_name: *const c_char,
    mode: i32,
    log_size: *mut i32,
    checkpointed: *mut i32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let wal = match state.wal {
        Some(wal) => wal,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    let db_name = unsafe { cstr_to_str(db_name) };
    match unsafe { wal.wal_checkpoint_v2(db.db, db_name, mode) } {
        Ok((log, checkpoint)) => {
            if !log_size.is_null() {
                unsafe { *log_size = log };
            }
            if !checkpointed.is_null() {
                unsafe { *checkpointed = checkpoint };
            }
            SQLITE_OK
        }
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn libsql_wal_frame_count(db: *mut sqlite3, frame_count: *mut u32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let wal = match state.wal {
        Some(wal) => wal,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || frame_count.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { wal.wal_frame_count(db.db) } {
        Ok(Some(count)) => {
            unsafe { *frame_count = count };
            SQLITE_OK
        }
        Ok(None) => SQLITE_ERROR,
        Err(err) => err_code(&err),
    }
}

#[no_mangle]
pub extern "C" fn sqlite3_table_column_metadata(
    db: *mut sqlite3,
    db_name: *const c_char,
    table: *const c_char,
    column: *const c_char,
    data_type: *mut *const c_char,
    coll_seq: *mut *const c_char,
    not_null: *mut i32,
    primary_key: *mut i32,
    autoinc: *mut i32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || table.is_null() || column.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    let db_name = unsafe { cstr_to_str(db_name) };
    let table = unsafe { cstr_to_str(table) }.unwrap_or("");
    let column = unsafe { cstr_to_str(column) }.unwrap_or("");
    match unsafe { metadata.table_column_metadata(db.db, db_name, table, column) } {
        Ok(info) => {
            unsafe {
                if !data_type.is_null() {
                    *data_type = info.data_type.map(|raw| raw.ptr as *const c_char).unwrap_or(null());
                }
                if !coll_seq.is_null() {
                    *coll_seq = info.coll_seq.map(|raw| raw.ptr as *const c_char).unwrap_or(null());
                }
                if !not_null.is_null() {
                    *not_null = if info.not_null { 1 } else { 0 };
                }
                if !primary_key.is_null() {
                    *primary_key = if info.primary_key { 1 } else { 0 };
                }
                if !autoinc.is_null() {
                    *autoinc = if info.autoinc { 1 } else { 0 };
                }
            }
            SQLITE_OK
        }
        Err(err) => err_code(&err),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlite_provider::Sqlite3Api;

    struct MockDb;
    struct MockStmt;
    struct MockValue;
    struct MockContext;

    struct MockApi;

    unsafe impl Sqlite3Api for MockApi {
        type Db = MockDb;
        type Stmt = MockStmt;
        type Value = MockValue;
        type Context = MockContext;
        type VTab = u8;
        type VTabCursor = u8;

        fn api_version(&self) -> ApiVersion {
            ApiVersion::new(3, 45, 0)
        }

        fn feature_set(&self) -> FeatureSet {
            FeatureSet::empty()
        }

        fn backend_name(&self) -> &'static str {
            "mock"
        }

        fn backend_version(&self) -> Option<ApiVersion> {
            None
        }

        unsafe fn open(&self, _filename: &str, _options: OpenOptions<'_>) -> Result<NonNull<Self::Db>> {
            Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockDb))))
        }

        unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()> {
            drop(Box::from_raw(db.as_ptr()));
            Ok(())
        }

        unsafe fn prepare_v2(&self, _db: NonNull<Self::Db>, _sql: &str) -> Result<NonNull<Self::Stmt>> {
            Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockStmt))))
        }

        unsafe fn prepare_v3(
            &self,
            _db: NonNull<Self::Db>,
            _sql: &str,
            _flags: u32,
        ) -> Result<NonNull<Self::Stmt>> {
            Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockStmt))))
        }

        unsafe fn step(&self, _stmt: NonNull<Self::Stmt>) -> Result<StepResult> {
            Ok(StepResult::Done)
        }

        unsafe fn reset(&self, _stmt: NonNull<Self::Stmt>) -> Result<()> {
            Ok(())
        }

        unsafe fn finalize(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
            drop(Box::from_raw(stmt.as_ptr()));
            Ok(())
        }

        unsafe fn bind_null(&self, _stmt: NonNull<Self::Stmt>, _idx: i32) -> Result<()> {
            Ok(())
        }

        unsafe fn bind_int64(&self, _stmt: NonNull<Self::Stmt>, _idx: i32, _v: i64) -> Result<()> {
            Ok(())
        }

        unsafe fn bind_double(&self, _stmt: NonNull<Self::Stmt>, _idx: i32, _v: f64) -> Result<()> {
            Ok(())
        }

        unsafe fn bind_text(&self, _stmt: NonNull<Self::Stmt>, _idx: i32, _v: &str) -> Result<()> {
            Ok(())
        }

        unsafe fn bind_blob(&self, _stmt: NonNull<Self::Stmt>, _idx: i32, _v: &[u8]) -> Result<()> {
            Ok(())
        }

        unsafe fn column_count(&self, _stmt: NonNull<Self::Stmt>) -> i32 {
            0
        }

        unsafe fn column_type(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> ValueType {
            ValueType::Null
        }

        unsafe fn column_int64(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> i64 {
            0
        }

        unsafe fn column_double(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> f64 {
            0.0
        }

        unsafe fn column_text(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> RawBytes {
            RawBytes::empty()
        }

        unsafe fn column_blob(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> RawBytes {
            RawBytes::empty()
        }

        unsafe fn errcode(&self, _db: NonNull<Self::Db>) -> i32 {
            SQLITE_OK
        }

        unsafe fn errmsg(&self, _db: NonNull<Self::Db>) -> *const c_char {
            null()
        }

        unsafe fn extended_errcode(&self, _db: NonNull<Self::Db>) -> Option<i32> {
            None
        }

        unsafe fn create_function_v2(
            &self,
            _db: NonNull<Self::Db>,
            _name: &str,
            _n_args: i32,
            _flags: FunctionFlags,
            _x_func: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
            _x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
            _x_final: Option<extern "C" fn(*mut Self::Context)>,
            _user_data: *mut c_void,
            _drop_user_data: Option<extern "C" fn(*mut c_void)>,
        ) -> Result<()> {
            Ok(())
        }

        unsafe fn create_window_function(
            &self,
            _db: NonNull<Self::Db>,
            _name: &str,
            _n_args: i32,
            _flags: FunctionFlags,
            _x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
            _x_final: Option<extern "C" fn(*mut Self::Context)>,
            _x_value: Option<extern "C" fn(*mut Self::Context)>,
            _x_inverse: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
            _user_data: *mut c_void,
            _drop_user_data: Option<extern "C" fn(*mut c_void)>,
        ) -> Result<()> {
            Ok(())
        }

        unsafe fn aggregate_context(&self, _ctx: NonNull<Self::Context>, _bytes: usize) -> *mut c_void {
            null_mut()
        }

        unsafe fn result_null(&self, _ctx: NonNull<Self::Context>) {}

        unsafe fn result_int64(&self, _ctx: NonNull<Self::Context>, _v: i64) {}

        unsafe fn result_double(&self, _ctx: NonNull<Self::Context>, _v: f64) {}

        unsafe fn result_text(&self, _ctx: NonNull<Self::Context>, _v: &str) {}

        unsafe fn result_blob(&self, _ctx: NonNull<Self::Context>, _v: &[u8]) {}

        unsafe fn result_error(&self, _ctx: NonNull<Self::Context>, _msg: &str) {}

        unsafe fn user_data(_ctx: NonNull<Self::Context>) -> *mut c_void {
            null_mut()
        }

        unsafe fn value_type(&self, _v: NonNull<Self::Value>) -> ValueType {
            ValueType::Null
        }

        unsafe fn value_int64(&self, _v: NonNull<Self::Value>) -> i64 {
            0
        }

        unsafe fn value_double(&self, _v: NonNull<Self::Value>) -> f64 {
            0.0
        }

        unsafe fn value_text(&self, _v: NonNull<Self::Value>) -> RawBytes {
            RawBytes::empty()
        }

        unsafe fn value_blob(&self, _v: NonNull<Self::Value>) -> RawBytes {
            RawBytes::empty()
        }

        unsafe fn declare_vtab(&self, _db: NonNull<Self::Db>, _schema: &str) -> Result<()> {
            Ok(())
        }

        unsafe fn create_module_v2(
            &self,
            _db: NonNull<Self::Db>,
            _name: &str,
            _module: &'static sqlite_provider::sqlite3_module<Self>,
            _user_data: *mut c_void,
            _drop_user_data: Option<extern "C" fn(*mut c_void)>,
        ) -> Result<()> {
            Ok(())
        }
    }

    #[test]
    fn register_and_open() {
        static API: MockApi = MockApi;
        let state = ProviderState::new(&API);
        let _ = register_provider(state);

        let mut db: *mut sqlite3 = null_mut();
        let rc = sqlite3_open(CString::new(":memory:").unwrap().as_ptr(), &mut db);
        assert_eq!(rc, SQLITE_OK);
        assert!(!db.is_null());
        let rc = sqlite3_close(db);
        assert_eq!(rc, SQLITE_OK);
    }

    #[test]
    fn stricmp_basic() {
        let a = CString::new("Hello").unwrap();
        let b = CString::new("hello").unwrap();
        assert_eq!(sqlite3_stricmp(a.as_ptr(), b.as_ptr()), 0);
    }
}
