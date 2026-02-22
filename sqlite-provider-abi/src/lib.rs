//! SQLite3 C ABI shim over a registered `sqlite-provider` backend.
// The exported surface mirrors `sqlite3.h` and keeps C signatures verbatim.
// Pointer-deref and argument-count lints are intentionally relaxed here.
#![allow(unsafe_op_in_unsafe_fn)]
#![allow(clippy::missing_safety_doc)]
#![allow(clippy::not_unsafe_ptr_arg_deref)]
#![allow(clippy::too_many_arguments)]

use libc::{c_char, c_void};
use sqlite_provider::{
    ApiVersion, ColumnMetadata, Error, ErrorCode, FeatureSet, FunctionFlags, OpenFlags,
    OpenOptions, RawBytes, Result, Sqlite3Api, Sqlite3Backup, Sqlite3BlobIo, Sqlite3Hooks,
    Sqlite3Metadata, Sqlite3Serialize, Sqlite3Wal, StepResult, ValueType,
};
#[cfg(feature = "default-backend")]
use sqlite_provider_sqlite3::LibSqlite3;
use std::ffi::{CStr, CString};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr::{NonNull, null, null_mut};

const SQLITE_OK: i32 = 0;
const SQLITE_ERROR: i32 = 1;
const SQLITE_INTERNAL: i32 = 2;
const SQLITE_PERM: i32 = 3;
const SQLITE_ABORT: i32 = 4;
const SQLITE_BUSY: i32 = 5;
const SQLITE_LOCKED: i32 = 6;
const SQLITE_NOMEM: i32 = 7;
const SQLITE_READONLY: i32 = 8;
const SQLITE_INTERRUPT: i32 = 9;
const SQLITE_IOERR: i32 = 10;
const SQLITE_CORRUPT: i32 = 11;
const SQLITE_NOTFOUND: i32 = 12;
const SQLITE_FULL: i32 = 13;
const SQLITE_CANTOPEN: i32 = 14;
const SQLITE_PROTOCOL: i32 = 15;
const SQLITE_EMPTY: i32 = 16;
const SQLITE_SCHEMA: i32 = 17;
const SQLITE_TOOBIG: i32 = 18;
const SQLITE_CONSTRAINT: i32 = 19;
const SQLITE_MISMATCH: i32 = 20;
const SQLITE_MISUSE: i32 = 21;
const SQLITE_NOLFS: i32 = 22;
const SQLITE_AUTH: i32 = 23;
const SQLITE_FORMAT: i32 = 24;
const SQLITE_RANGE: i32 = 25;
const SQLITE_NOTADB: i32 = 26;
const SQLITE_NOTICE: i32 = 27;
const SQLITE_WARNING: i32 = 28;
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

const SQLITE_STATIC: isize = 0;
const SQLITE_TRANSIENT: isize = -1;
const SQLITE_OPEN_READONLY: i32 = 0x0000_0001;
const SQLITE_OPEN_READWRITE: i32 = 0x0000_0002;
const SQLITE_OPEN_CREATE: i32 = 0x0000_0004;
const SQLITE_OPEN_URI: i32 = 0x0000_0040;
const SQLITE_OPEN_NOMUTEX: i32 = 0x0000_8000;
const SQLITE_OPEN_FULLMUTEX: i32 = 0x0001_0000;
const SQLITE_OPEN_SHAREDCACHE: i32 = 0x0002_0000;
const SQLITE_OPEN_PRIVATECACHE: i32 = 0x0004_0000;
const SQLITE_OPEN_EXRESCODE: i32 = 0x0200_0000;
#[allow(dead_code)]
const SQLITE_UTF8: i32 = 0x0000_0001;
const SQLITE_DETERMINISTIC: i32 = 0x0000_0800;
const SQLITE_INNOCUOUS: i32 = 0x0002_0000;
const SQLITE_DIRECTONLY: i32 = 0x0008_0000;

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
    let bytes = std::slice::from_raw_parts(ptr.cast(), len as usize);
    std::str::from_utf8(bytes).ok()
}

unsafe fn text_bytes_from_ptr<'a>(ptr: *const c_char, len: i32) -> Option<&'a [u8]> {
    if ptr.is_null() {
        return None;
    }
    if len < 0 {
        return Some(CStr::from_ptr(ptr).to_bytes());
    }
    Some(std::slice::from_raw_parts(ptr.cast::<u8>(), len as usize))
}

#[derive(Clone, Copy)]
enum Destructor {
    Static,
    Transient,
    Custom(unsafe extern "C" fn(*mut c_void)),
}

fn decode_destructor(destroy: *mut c_void) -> Destructor {
    match destroy as isize {
        SQLITE_STATIC => Destructor::Static,
        SQLITE_TRANSIENT => Destructor::Transient,
        _ => {
            let drop_fn = unsafe {
                std::mem::transmute::<*mut c_void, unsafe extern "C" fn(*mut c_void)>(destroy)
            };
            Destructor::Custom(drop_fn)
        }
    }
}

unsafe fn call_destructor(destructor: Destructor, ptr: *mut c_void) {
    if let Destructor::Custom(drop_fn) = destructor
        && !ptr.is_null()
    {
        drop_fn(ptr);
    }
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

fn open_flags_from_sqlite(flags: i32) -> OpenFlags {
    let mut out = OpenFlags::empty();
    if (flags & SQLITE_OPEN_READONLY) != 0 {
        out |= OpenFlags::READ_ONLY;
    }
    if (flags & SQLITE_OPEN_READWRITE) != 0 {
        out |= OpenFlags::READ_WRITE;
    }
    if (flags & SQLITE_OPEN_CREATE) != 0 {
        out |= OpenFlags::CREATE;
    }
    if (flags & SQLITE_OPEN_URI) != 0 {
        out |= OpenFlags::URI;
    }
    if (flags & SQLITE_OPEN_NOMUTEX) != 0 {
        out |= OpenFlags::NO_MUTEX;
    }
    if (flags & SQLITE_OPEN_FULLMUTEX) != 0 {
        out |= OpenFlags::FULL_MUTEX;
    }
    if (flags & SQLITE_OPEN_SHAREDCACHE) != 0 {
        out |= OpenFlags::SHARED_CACHE;
    }
    if (flags & SQLITE_OPEN_PRIVATECACHE) != 0 {
        out |= OpenFlags::PRIVATE_CACHE;
    }
    if (flags & SQLITE_OPEN_EXRESCODE) != 0 {
        out |= OpenFlags::EXRESCODE;
    }
    out
}

fn function_flags_from_sqlite(flags: i32) -> FunctionFlags {
    let mut out = FunctionFlags::empty();
    if (flags & SQLITE_DETERMINISTIC) != 0 {
        out |= FunctionFlags::DETERMINISTIC;
    }
    if (flags & SQLITE_DIRECTONLY) != 0 {
        out |= FunctionFlags::DIRECT_ONLY;
    }
    if (flags & SQLITE_INNOCUOUS) != 0 {
        out |= FunctionFlags::INNOCUOUS;
    }
    out
}

fn api_version_number(version: ApiVersion) -> i32 {
    (version.major as i32) * 1_000_000 + (version.minor as i32) * 1_000 + (version.patch as i32)
}

fn set_err_string(err: *mut *mut c_char, msg: &str) {
    if err.is_null() {
        return;
    }
    let bytes = msg.as_bytes();
    let out = sqlite3_malloc64((bytes.len() + 1) as u64) as *mut c_char;
    if out.is_null() {
        unsafe { *err = null_mut() };
        return;
    }
    unsafe {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), out.cast(), bytes.len());
        *out.add(bytes.len()) = 0;
        *err = out;
    }
}

const GET_TABLE_INCOMPATIBLE_QUERY_MSG: &str =
    "sqlite3_get_table() called with two or more incompatible queries";
const EXEC_CALLBACK_ABORT_MSG: &[u8] = b"query aborted\0";
const NOMEM_MSG: &[u8] = b"out of memory\0";

fn provider_panic_error(op: &'static str) -> Error {
    Error::with_message(
        ErrorCode::Error,
        format!("panic in provider operation: {op}"),
    )
}

fn catch_provider_result<T, F>(op: &'static str, f: F) -> Result<T>
where
    F: FnOnce() -> Result<T>,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(result) => result,
        Err(_) => Err(provider_panic_error(op)),
    }
}

fn catch_provider_default<T, F>(default: T, f: F) -> T
where
    F: FnOnce() -> T,
{
    match catch_unwind(AssertUnwindSafe(f)) {
        Ok(value) => value,
        Err(_) => default,
    }
}

fn catch_provider_void<F>(f: F)
where
    F: FnOnce(),
{
    let _ = catch_unwind(AssertUnwindSafe(f));
}

fn push_column_name(
    storage: &mut Vec<u8>,
    offset: &mut Option<usize>,
    name: Option<RawBytes>,
    idx: usize,
) {
    if let Some(raw) = name
        && !raw.ptr.is_null()
    {
        let start = storage.len();
        storage.extend_from_slice(unsafe { raw.as_slice() });
        storage.push(0);
        *offset = Some(start);
        return;
    }
    let fallback = format!("column{}", idx + 1);
    let start = storage.len();
    storage.extend_from_slice(fallback.as_bytes());
    storage.push(0);
    *offset = Some(start);
}

#[doc(hidden)]
mod exports;
mod parser;
mod state;
#[cfg(test)]
mod tests;

use parser::split_first_statement;
pub use state::{ProviderState, RegisterError, register_provider, sqlite3, sqlite3_stmt};
pub(crate) use state::{
    SerializedEntry, TABLE_DATA_OWNER_NONE, TABLE_DATA_OWNER_RUST_VEC, TableResultAllocHeader,
    add_stmt, ensure_default_provider, free_table_data, provider, remove_stmt, serialized_map,
};

#[doc(hidden)]
pub use exports::*;

/// Object-safe core SPI using erased handles.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Api`.
///
/// This trait is intentionally a near 1:1 mirror of `Sqlite3Api`; method-level
/// details follow the upstream trait contracts.
pub unsafe trait AbiCore: Send + Sync + 'static {
    /// See [`Sqlite3Api::api_version`].
    fn api_version(&self) -> ApiVersion;
    /// See [`Sqlite3Api::feature_set`].
    fn feature_set(&self) -> FeatureSet;
    /// See [`Sqlite3Api::backend_name`].
    fn backend_name(&self) -> &'static str;
    /// See [`Sqlite3Api::backend_version`].
    fn backend_version(&self) -> Option<ApiVersion>;
    /// See [`Sqlite3Api::threadsafe`].
    fn threadsafe(&self) -> i32;
    /// See [`Sqlite3Api::malloc`].
    unsafe fn malloc(&self, size: usize) -> *mut c_void;
    /// See [`Sqlite3Api::free`].
    unsafe fn free(&self, ptr: *mut c_void);

    /// See [`Sqlite3Api::open`].
    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<c_void>>;
    /// See [`Sqlite3Api::close`].
    unsafe fn close(&self, db: NonNull<c_void>) -> Result<()>;

    /// See [`Sqlite3Api::prepare_v2`].
    unsafe fn prepare_v2(&self, db: NonNull<c_void>, sql: &str) -> Result<NonNull<c_void>>;
    /// See [`Sqlite3Api::prepare_v3`].
    unsafe fn prepare_v3(
        &self,
        db: NonNull<c_void>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<c_void>>;

    /// See [`Sqlite3Api::step`].
    unsafe fn step(&self, stmt: NonNull<c_void>) -> Result<StepResult>;
    /// See [`Sqlite3Api::reset`].
    unsafe fn reset(&self, stmt: NonNull<c_void>) -> Result<()>;
    /// See [`Sqlite3Api::finalize`].
    unsafe fn finalize(&self, stmt: NonNull<c_void>) -> Result<()>;

    /// See [`Sqlite3Api::bind_null`].
    unsafe fn bind_null(&self, stmt: NonNull<c_void>, idx: i32) -> Result<()>;
    /// See [`Sqlite3Api::bind_int64`].
    unsafe fn bind_int64(&self, stmt: NonNull<c_void>, idx: i32, v: i64) -> Result<()>;
    /// See [`Sqlite3Api::bind_double`].
    unsafe fn bind_double(&self, stmt: NonNull<c_void>, idx: i32, v: f64) -> Result<()>;
    /// See [`Sqlite3Api::bind_text`].
    unsafe fn bind_text(&self, stmt: NonNull<c_void>, idx: i32, v: &str) -> Result<()>;
    /// See [`Sqlite3Api::bind_text_bytes`].
    unsafe fn bind_text_bytes(&self, stmt: NonNull<c_void>, idx: i32, v: &[u8]) -> Result<()> {
        match core::str::from_utf8(v) {
            Ok(text) => unsafe { self.bind_text(stmt, idx, text) },
            Err(_) => Err(Error::with_message(ErrorCode::Misuse, "invalid utf-8 text")),
        }
    }
    /// See [`Sqlite3Api::bind_blob`].
    unsafe fn bind_blob(&self, stmt: NonNull<c_void>, idx: i32, v: &[u8]) -> Result<()>;

    /// See [`Sqlite3Api::column_count`].
    unsafe fn column_count(&self, stmt: NonNull<c_void>) -> i32;
    /// See [`Sqlite3Api::column_type`].
    unsafe fn column_type(&self, stmt: NonNull<c_void>, col: i32) -> ValueType;
    /// See [`Sqlite3Api::column_int64`].
    unsafe fn column_int64(&self, stmt: NonNull<c_void>, col: i32) -> i64;
    /// See [`Sqlite3Api::column_double`].
    unsafe fn column_double(&self, stmt: NonNull<c_void>, col: i32) -> f64;
    /// See [`Sqlite3Api::column_text`].
    unsafe fn column_text(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes;
    /// See [`Sqlite3Api::column_blob`].
    unsafe fn column_blob(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes;

    /// See [`Sqlite3Api::errcode`].
    unsafe fn errcode(&self, db: NonNull<c_void>) -> i32;
    /// See [`Sqlite3Api::errmsg`].
    unsafe fn errmsg(&self, db: NonNull<c_void>) -> *const c_char;
    /// See [`Sqlite3Api::extended_errcode`].
    unsafe fn extended_errcode(&self, db: NonNull<c_void>) -> Option<i32>;

    /// See [`Sqlite3Api::create_function_v2`].
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

    /// See [`Sqlite3Api::create_window_function`].
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

    /// See [`Sqlite3Api::create_collation_v2`].
    unsafe fn create_collation_v2(
        &self,
        db: NonNull<c_void>,
        name: &str,
        enc: i32,
        context: *mut c_void,
        cmp: Option<extern "C" fn(*mut c_void, i32, *const c_void, i32, *const c_void) -> i32>,
        destroy: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()>;

    /// See [`Sqlite3Api::aggregate_context`].
    unsafe fn aggregate_context(&self, ctx: NonNull<c_void>, bytes: usize) -> *mut c_void;

    /// See [`Sqlite3Api::result_null`].
    unsafe fn result_null(&self, ctx: NonNull<c_void>);
    /// See [`Sqlite3Api::result_int64`].
    unsafe fn result_int64(&self, ctx: NonNull<c_void>, v: i64);
    /// See [`Sqlite3Api::result_double`].
    unsafe fn result_double(&self, ctx: NonNull<c_void>, v: f64);
    /// See [`Sqlite3Api::result_text`].
    unsafe fn result_text(&self, ctx: NonNull<c_void>, v: &str);
    /// See [`Sqlite3Api::result_text_bytes`].
    unsafe fn result_text_bytes(&self, ctx: NonNull<c_void>, v: &[u8]) {
        match core::str::from_utf8(v) {
            Ok(text) => unsafe { self.result_text(ctx, text) },
            Err(_) => unsafe { self.result_error(ctx, "invalid utf-8") },
        }
    }
    /// See [`Sqlite3Api::result_blob`].
    unsafe fn result_blob(&self, ctx: NonNull<c_void>, v: &[u8]);
    /// See [`Sqlite3Api::result_error`].
    unsafe fn result_error(&self, ctx: NonNull<c_void>, msg: &str);
    /// See [`Sqlite3Api::user_data`].
    unsafe fn user_data(&self, ctx: NonNull<c_void>) -> *mut c_void;

    /// See [`Sqlite3Api::value_type`].
    unsafe fn value_type(&self, v: NonNull<c_void>) -> ValueType;
    /// See [`Sqlite3Api::value_int64`].
    unsafe fn value_int64(&self, v: NonNull<c_void>) -> i64;
    /// See [`Sqlite3Api::value_double`].
    unsafe fn value_double(&self, v: NonNull<c_void>) -> f64;
    /// See [`Sqlite3Api::value_text`].
    unsafe fn value_text(&self, v: NonNull<c_void>) -> RawBytes;
    /// See [`Sqlite3Api::value_blob`].
    unsafe fn value_blob(&self, v: NonNull<c_void>) -> RawBytes;
}

unsafe impl<P: Sqlite3Api> AbiCore for P {
    fn api_version(&self) -> ApiVersion {
        catch_provider_default(ApiVersion::new(0, 0, 0), || Sqlite3Api::api_version(self))
    }

    fn feature_set(&self) -> FeatureSet {
        catch_provider_default(FeatureSet::empty(), || Sqlite3Api::feature_set(self))
    }

    fn backend_name(&self) -> &'static str {
        catch_provider_default("panic", || Sqlite3Api::backend_name(self))
    }

    fn backend_version(&self) -> Option<ApiVersion> {
        catch_provider_default(None, || Sqlite3Api::backend_version(self))
    }

    fn threadsafe(&self) -> i32 {
        catch_provider_default(0, || Sqlite3Api::threadsafe(self))
    }

    unsafe fn malloc(&self, size: usize) -> *mut c_void {
        catch_provider_default(null_mut(), || unsafe { Sqlite3Api::malloc(self, size) })
    }

    unsafe fn free(&self, ptr: *mut c_void) {
        catch_provider_void(|| unsafe { Sqlite3Api::free(self, ptr) })
    }

    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<c_void>> {
        catch_provider_result("open", || {
            let db = Sqlite3Api::open(self, filename, options)?;
            Ok(NonNull::new_unchecked(db.as_ptr() as *mut c_void))
        })
    }

    unsafe fn close(&self, db: NonNull<c_void>) -> Result<()> {
        catch_provider_result("close", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Api::close(self, db)
        })
    }

    unsafe fn prepare_v2(&self, db: NonNull<c_void>, sql: &str) -> Result<NonNull<c_void>> {
        catch_provider_result("prepare_v2", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            let stmt = Sqlite3Api::prepare_v2(self, db, sql)?;
            Ok(NonNull::new_unchecked(stmt.as_ptr() as *mut c_void))
        })
    }

    unsafe fn prepare_v3(
        &self,
        db: NonNull<c_void>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<c_void>> {
        catch_provider_result("prepare_v3", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            let stmt = Sqlite3Api::prepare_v3(self, db, sql, flags)?;
            Ok(NonNull::new_unchecked(stmt.as_ptr() as *mut c_void))
        })
    }

    unsafe fn step(&self, stmt: NonNull<c_void>) -> Result<StepResult> {
        catch_provider_result("step", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::step(self, stmt)
        })
    }

    unsafe fn reset(&self, stmt: NonNull<c_void>) -> Result<()> {
        catch_provider_result("reset", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::reset(self, stmt)
        })
    }

    unsafe fn finalize(&self, stmt: NonNull<c_void>) -> Result<()> {
        catch_provider_result("finalize", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::finalize(self, stmt)
        })
    }

    unsafe fn bind_null(&self, stmt: NonNull<c_void>, idx: i32) -> Result<()> {
        catch_provider_result("bind_null", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::bind_null(self, stmt, idx)
        })
    }

    unsafe fn bind_int64(&self, stmt: NonNull<c_void>, idx: i32, v: i64) -> Result<()> {
        catch_provider_result("bind_int64", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::bind_int64(self, stmt, idx, v)
        })
    }

    unsafe fn bind_double(&self, stmt: NonNull<c_void>, idx: i32, v: f64) -> Result<()> {
        catch_provider_result("bind_double", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::bind_double(self, stmt, idx, v)
        })
    }

    unsafe fn bind_text(&self, stmt: NonNull<c_void>, idx: i32, v: &str) -> Result<()> {
        catch_provider_result("bind_text", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::bind_text(self, stmt, idx, v)
        })
    }

    unsafe fn bind_text_bytes(&self, stmt: NonNull<c_void>, idx: i32, v: &[u8]) -> Result<()> {
        catch_provider_result("bind_text_bytes", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::bind_text_bytes(self, stmt, idx, v)
        })
    }

    unsafe fn bind_blob(&self, stmt: NonNull<c_void>, idx: i32, v: &[u8]) -> Result<()> {
        catch_provider_result("bind_blob", || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::bind_blob(self, stmt, idx, v)
        })
    }

    unsafe fn column_count(&self, stmt: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::column_count(self, stmt)
        })
    }

    unsafe fn column_type(&self, stmt: NonNull<c_void>, col: i32) -> ValueType {
        catch_provider_default(ValueType::Null, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::column_type(self, stmt, col)
        })
    }

    unsafe fn column_int64(&self, stmt: NonNull<c_void>, col: i32) -> i64 {
        catch_provider_default(0, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::column_int64(self, stmt, col)
        })
    }

    unsafe fn column_double(&self, stmt: NonNull<c_void>, col: i32) -> f64 {
        catch_provider_default(0.0, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::column_double(self, stmt, col)
        })
    }

    unsafe fn column_text(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes {
        catch_provider_default(RawBytes::empty(), || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::column_text(self, stmt, col)
        })
    }

    unsafe fn column_blob(&self, stmt: NonNull<c_void>, col: i32) -> RawBytes {
        catch_provider_default(RawBytes::empty(), || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Api::column_blob(self, stmt, col)
        })
    }

    unsafe fn errcode(&self, db: NonNull<c_void>) -> i32 {
        catch_provider_default(SQLITE_ERROR, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Api::errcode(self, db)
        })
    }

    unsafe fn errmsg(&self, db: NonNull<c_void>) -> *const c_char {
        catch_provider_default(null(), || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Api::errmsg(self, db)
        })
    }

    unsafe fn extended_errcode(&self, db: NonNull<c_void>) -> Option<i32> {
        catch_provider_default(None, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Api::extended_errcode(self, db)
        })
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
        catch_provider_result("create_function_v2", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            let x_func = x_func.map(|f| {
                std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f)
            });
            let x_step = x_step.map(|f| {
                std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f)
            });
            let x_final =
                x_final.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context)>(f));
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
        })
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
        catch_provider_result("create_window_function", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            let x_step = x_step.map(|f| {
                std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f)
            });
            let x_final =
                x_final.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context)>(f));
            let x_value =
                x_value.map(|f| std::mem::transmute::<_, extern "C" fn(*mut P::Context)>(f));
            let x_inverse = x_inverse.map(|f| {
                std::mem::transmute::<_, extern "C" fn(*mut P::Context, i32, *mut *mut P::Value)>(f)
            });
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
        })
    }

    unsafe fn create_collation_v2(
        &self,
        db: NonNull<c_void>,
        name: &str,
        enc: i32,
        context: *mut c_void,
        cmp: Option<extern "C" fn(*mut c_void, i32, *const c_void, i32, *const c_void) -> i32>,
        destroy: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        catch_provider_result("create_collation_v2", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Api::create_collation_v2(self, db, name, enc, context, cmp, destroy)
        })
    }

    unsafe fn aggregate_context(&self, ctx: NonNull<c_void>, bytes: usize) -> *mut c_void {
        catch_provider_default(null_mut(), || {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::aggregate_context(self, ctx, bytes)
        })
    }

    unsafe fn result_null(&self, ctx: NonNull<c_void>) {
        catch_provider_void(|| {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::result_null(self, ctx)
        })
    }

    unsafe fn result_int64(&self, ctx: NonNull<c_void>, v: i64) {
        catch_provider_void(|| {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::result_int64(self, ctx, v)
        })
    }

    unsafe fn result_double(&self, ctx: NonNull<c_void>, v: f64) {
        catch_provider_void(|| {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::result_double(self, ctx, v)
        })
    }

    unsafe fn result_text(&self, ctx: NonNull<c_void>, v: &str) {
        catch_provider_void(|| {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::result_text(self, ctx, v)
        })
    }

    unsafe fn result_text_bytes(&self, ctx: NonNull<c_void>, v: &[u8]) {
        catch_provider_void(|| {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::result_text_bytes(self, ctx, v)
        })
    }

    unsafe fn result_blob(&self, ctx: NonNull<c_void>, v: &[u8]) {
        catch_provider_void(|| {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::result_blob(self, ctx, v)
        })
    }

    unsafe fn result_error(&self, ctx: NonNull<c_void>, msg: &str) {
        catch_provider_void(|| {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3Api::result_error(self, ctx, msg)
        })
    }

    unsafe fn user_data(&self, ctx: NonNull<c_void>) -> *mut c_void {
        catch_provider_default(null_mut(), || {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            P::user_data(ctx)
        })
    }

    unsafe fn value_type(&self, v: NonNull<c_void>) -> ValueType {
        catch_provider_default(ValueType::Null, || {
            let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
            Sqlite3Api::value_type(self, v)
        })
    }

    unsafe fn value_int64(&self, v: NonNull<c_void>) -> i64 {
        catch_provider_default(0, || {
            let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
            Sqlite3Api::value_int64(self, v)
        })
    }

    unsafe fn value_double(&self, v: NonNull<c_void>) -> f64 {
        catch_provider_default(0.0, || {
            let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
            Sqlite3Api::value_double(self, v)
        })
    }

    unsafe fn value_text(&self, v: NonNull<c_void>) -> RawBytes {
        catch_provider_default(RawBytes::empty(), || {
            let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
            Sqlite3Api::value_text(self, v)
        })
    }

    unsafe fn value_blob(&self, v: NonNull<c_void>) -> RawBytes {
        catch_provider_default(RawBytes::empty(), || {
            let v = NonNull::new_unchecked(v.as_ptr() as *mut P::Value);
            Sqlite3Api::value_blob(self, v)
        })
    }
}

/// Optional hook SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Hooks`.
pub unsafe trait AbiHooks: Send + Sync + 'static {
    /// See [`Sqlite3Hooks::trace_v2`].
    unsafe fn trace_v2(
        &self,
        db: NonNull<c_void>,
        mask: u32,
        callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
        context: *mut c_void,
    ) -> Result<()>;
    /// See [`Sqlite3Hooks::progress_handler`].
    unsafe fn progress_handler(
        &self,
        db: NonNull<c_void>,
        n: i32,
        callback: Option<extern "C" fn(*mut c_void) -> i32>,
        context: *mut c_void,
    ) -> Result<()>;
    /// See [`Sqlite3Hooks::busy_timeout`].
    unsafe fn busy_timeout(&self, db: NonNull<c_void>, ms: i32) -> Result<()>;
    /// See [`Sqlite3Hooks::set_authorizer`].
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
        catch_provider_result("trace_v2", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Hooks::trace_v2(self, db, mask, callback, context)
        })
    }

    unsafe fn progress_handler(
        &self,
        db: NonNull<c_void>,
        n: i32,
        callback: Option<extern "C" fn(*mut c_void) -> i32>,
        context: *mut c_void,
    ) -> Result<()> {
        catch_provider_result("progress_handler", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Hooks::progress_handler(self, db, n, callback, context)
        })
    }

    unsafe fn busy_timeout(&self, db: NonNull<c_void>, ms: i32) -> Result<()> {
        catch_provider_result("busy_timeout", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Hooks::busy_timeout(self, db, ms)
        })
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
        catch_provider_result("set_authorizer", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Hooks::set_authorizer(self, db, callback, context)
        })
    }
}

/// Optional backup SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Backup`.
pub unsafe trait AbiBackup: Send + Sync + 'static {
    /// See [`Sqlite3Backup::backup_init`].
    unsafe fn backup_init(
        &self,
        dest_db: NonNull<c_void>,
        dest_name: &str,
        source_db: NonNull<c_void>,
        source_name: &str,
    ) -> Result<NonNull<c_void>>;
    /// See [`Sqlite3Backup::backup_step`].
    unsafe fn backup_step(&self, backup: NonNull<c_void>, pages: i32) -> Result<()>;
    /// See [`Sqlite3Backup::backup_remaining`].
    unsafe fn backup_remaining(&self, backup: NonNull<c_void>) -> i32;
    /// See [`Sqlite3Backup::backup_pagecount`].
    unsafe fn backup_pagecount(&self, backup: NonNull<c_void>) -> i32;
    /// See [`Sqlite3Backup::backup_finish`].
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
        catch_provider_result("backup_init", || {
            let dest_db = NonNull::new_unchecked(dest_db.as_ptr() as *mut P::Db);
            let source_db = NonNull::new_unchecked(source_db.as_ptr() as *mut P::Db);
            let backup =
                Sqlite3Backup::backup_init(self, dest_db, dest_name, source_db, source_name)?;
            Ok(NonNull::new_unchecked(backup.as_ptr() as *mut c_void))
        })
    }

    unsafe fn backup_step(&self, backup: NonNull<c_void>, pages: i32) -> Result<()> {
        catch_provider_result("backup_step", || {
            let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
            Sqlite3Backup::backup_step(self, backup, pages)
        })
    }

    unsafe fn backup_remaining(&self, backup: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
            Sqlite3Backup::backup_remaining(self, backup)
        })
    }

    unsafe fn backup_pagecount(&self, backup: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
            Sqlite3Backup::backup_pagecount(self, backup)
        })
    }

    unsafe fn backup_finish(&self, backup: NonNull<c_void>) -> Result<()> {
        catch_provider_result("backup_finish", || {
            let backup = NonNull::new_unchecked(backup.as_ptr() as *mut P::Backup);
            Sqlite3Backup::backup_finish(self, backup)
        })
    }
}

/// Optional blob SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3BlobIo`.
pub unsafe trait AbiBlobIo: Send + Sync + 'static {
    /// See [`Sqlite3BlobIo::blob_open`].
    unsafe fn blob_open(
        &self,
        db: NonNull<c_void>,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<NonNull<c_void>>;
    /// See [`Sqlite3BlobIo::blob_read`].
    unsafe fn blob_read(&self, blob: NonNull<c_void>, data: &mut [u8], offset: i32) -> Result<()>;
    /// See [`Sqlite3BlobIo::blob_write`].
    unsafe fn blob_write(&self, blob: NonNull<c_void>, data: &[u8], offset: i32) -> Result<()>;
    /// See [`Sqlite3BlobIo::blob_bytes`].
    unsafe fn blob_bytes(&self, blob: NonNull<c_void>) -> i32;
    /// See [`Sqlite3BlobIo::blob_close`].
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
        catch_provider_result("blob_open", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            let blob = Sqlite3BlobIo::blob_open(self, db, db_name, table, column, rowid, flags)?;
            Ok(NonNull::new_unchecked(blob.as_ptr() as *mut c_void))
        })
    }

    unsafe fn blob_read(&self, blob: NonNull<c_void>, data: &mut [u8], offset: i32) -> Result<()> {
        catch_provider_result("blob_read", || {
            let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
            Sqlite3BlobIo::blob_read(self, blob, data, offset)
        })
    }

    unsafe fn blob_write(&self, blob: NonNull<c_void>, data: &[u8], offset: i32) -> Result<()> {
        catch_provider_result("blob_write", || {
            let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
            Sqlite3BlobIo::blob_write(self, blob, data, offset)
        })
    }

    unsafe fn blob_bytes(&self, blob: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
            Sqlite3BlobIo::blob_bytes(self, blob)
        })
    }

    unsafe fn blob_close(&self, blob: NonNull<c_void>) -> Result<()> {
        catch_provider_result("blob_close", || {
            let blob = NonNull::new_unchecked(blob.as_ptr() as *mut P::Blob);
            Sqlite3BlobIo::blob_close(self, blob)
        })
    }
}

/// Optional serialize SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Serialize`.
pub unsafe trait AbiSerialize: Send + Sync + 'static {
    /// See [`Sqlite3Serialize::serialize`].
    unsafe fn serialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        flags: u32,
    ) -> Result<sqlite_provider::OwnedBytes>;
    /// See [`Sqlite3Serialize::deserialize`].
    unsafe fn deserialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        data: &[u8],
        flags: u32,
    ) -> Result<()>;
    /// See [`Sqlite3Serialize::free`].
    unsafe fn free(&self, bytes: sqlite_provider::OwnedBytes);
}

unsafe impl<P: Sqlite3Serialize> AbiSerialize for P {
    unsafe fn serialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        flags: u32,
    ) -> Result<sqlite_provider::OwnedBytes> {
        catch_provider_result("serialize", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Serialize::serialize(self, db, schema, flags)
        })
    }

    unsafe fn deserialize(
        &self,
        db: NonNull<c_void>,
        schema: Option<&str>,
        data: &[u8],
        flags: u32,
    ) -> Result<()> {
        catch_provider_result("deserialize", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Serialize::deserialize(self, db, schema, data, flags)
        })
    }

    unsafe fn free(&self, bytes: sqlite_provider::OwnedBytes) {
        catch_provider_void(|| Sqlite3Serialize::free(self, bytes))
    }
}

/// Optional WAL SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Wal`.
pub unsafe trait AbiWal: Send + Sync + 'static {
    /// See [`Sqlite3Wal::wal_checkpoint`].
    unsafe fn wal_checkpoint(&self, db: NonNull<c_void>, db_name: Option<&str>) -> Result<()>;
    /// See [`Sqlite3Wal::wal_checkpoint_v2`].
    unsafe fn wal_checkpoint_v2(
        &self,
        db: NonNull<c_void>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)>;
    /// See [`Sqlite3Wal::wal_frame_count`].
    unsafe fn wal_frame_count(&self, db: NonNull<c_void>) -> Result<Option<u32>>;
}

unsafe impl<P: Sqlite3Wal> AbiWal for P {
    unsafe fn wal_checkpoint(&self, db: NonNull<c_void>, db_name: Option<&str>) -> Result<()> {
        catch_provider_result("wal_checkpoint", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Wal::wal_checkpoint(self, db, db_name)
        })
    }

    unsafe fn wal_checkpoint_v2(
        &self,
        db: NonNull<c_void>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)> {
        catch_provider_result("wal_checkpoint_v2", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Wal::wal_checkpoint_v2(self, db, db_name, mode)
        })
    }

    unsafe fn wal_frame_count(&self, db: NonNull<c_void>) -> Result<Option<u32>> {
        catch_provider_result("wal_frame_count", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Wal::wal_frame_count(self, db)
        })
    }
}

/// Optional metadata SPI for the ABI layer.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Metadata`.
pub unsafe trait AbiMetadata: Send + Sync + 'static {
    /// See [`Sqlite3Metadata::table_column_metadata`].
    unsafe fn table_column_metadata(
        &self,
        db: NonNull<c_void>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata>;
    /// See [`Sqlite3Metadata::column_decltype`].
    unsafe fn column_decltype(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes>;
    /// See [`Sqlite3Metadata::column_name`].
    unsafe fn column_name(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes>;
    /// See [`Sqlite3Metadata::column_table_name`].
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
        catch_provider_result("table_column_metadata", || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3Metadata::table_column_metadata(self, db, db_name, table, column)
        })
    }

    unsafe fn column_decltype(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes> {
        catch_provider_default(None, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Metadata::column_decltype(self, stmt, col)
        })
    }

    unsafe fn column_name(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes> {
        catch_provider_default(None, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Metadata::column_name(self, stmt, col)
        })
    }

    unsafe fn column_table_name(&self, stmt: NonNull<c_void>, col: i32) -> Option<RawBytes> {
        catch_provider_default(None, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3Metadata::column_table_name(self, stmt, col)
        })
    }
}

/// Extra optional SPI needed by the ABI layer for non-core SQLite helpers.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3Api`.
pub unsafe trait Sqlite3AbiExtras: Sqlite3Api {
    /// Resolve database filename bytes for `sqlite3_db_filename`.
    unsafe fn db_filename(&self, db: NonNull<Self::Db>, name: Option<&str>) -> Option<RawBytes>;
    /// Return autocommit state for `sqlite3_get_autocommit`.
    unsafe fn get_autocommit(&self, db: NonNull<Self::Db>) -> i32;
    /// Return cumulative row changes for `sqlite3_total_changes`.
    unsafe fn total_changes(&self, db: NonNull<Self::Db>) -> i32;
    /// Return statement row changes for `sqlite3_changes`.
    unsafe fn changes(&self, db: NonNull<Self::Db>) -> i32;
    /// Return statement row changes for `sqlite3_changes64`.
    unsafe fn changes64(&self, db: NonNull<Self::Db>) -> i64;
    /// Return last insert rowid for `sqlite3_last_insert_rowid`.
    unsafe fn last_insert_rowid(&self, db: NonNull<Self::Db>) -> i64;
    /// Interrupt execution for `sqlite3_interrupt`.
    unsafe fn interrupt(&self, db: NonNull<Self::Db>);
    /// Apply minimal fixed-arity `sqlite3_db_config` operations.
    unsafe fn db_config(&self, db: NonNull<Self::Db>, op: i32) -> i32;
    /// Forward `sqlite3_limit`.
    unsafe fn limit(&self, db: NonNull<Self::Db>, id: i32, new_value: i32) -> i32;
    /// Return read-only status for `sqlite3_stmt_readonly`.
    unsafe fn stmt_readonly(&self, stmt: NonNull<Self::Stmt>) -> i32;
    /// Return busy status for `sqlite3_stmt_busy`.
    unsafe fn stmt_busy(&self, stmt: NonNull<Self::Stmt>) -> i32;
    /// Return bind parameter count for `sqlite3_bind_parameter_count`.
    unsafe fn bind_parameter_count(&self, stmt: NonNull<Self::Stmt>) -> i32;
    /// Return bind parameter name for `sqlite3_bind_parameter_name`.
    unsafe fn bind_parameter_name(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Option<RawBytes>;
    /// Return bind parameter index for `sqlite3_bind_parameter_index`.
    unsafe fn bind_parameter_index(&self, stmt: NonNull<Self::Stmt>, name: &str) -> i32;
    /// Resolve owning database handle for `sqlite3_context_db_handle`.
    unsafe fn context_db_handle(&self, ctx: NonNull<Self::Context>) -> Option<NonNull<Self::Db>>;
}

/// Object-safe wrapper for `Sqlite3AbiExtras`.
///
/// # Safety
/// Implementations must uphold the same safety contracts as `Sqlite3AbiExtras`.
pub unsafe trait AbiExtras: Send + Sync + 'static {
    /// See [`Sqlite3AbiExtras::db_filename`].
    unsafe fn db_filename(&self, db: NonNull<c_void>, name: Option<&str>) -> Option<RawBytes>;
    /// See [`Sqlite3AbiExtras::get_autocommit`].
    unsafe fn get_autocommit(&self, db: NonNull<c_void>) -> i32;
    /// See [`Sqlite3AbiExtras::total_changes`].
    unsafe fn total_changes(&self, db: NonNull<c_void>) -> i32;
    /// See [`Sqlite3AbiExtras::changes`].
    unsafe fn changes(&self, db: NonNull<c_void>) -> i32;
    /// See [`Sqlite3AbiExtras::changes64`].
    unsafe fn changes64(&self, db: NonNull<c_void>) -> i64;
    /// See [`Sqlite3AbiExtras::last_insert_rowid`].
    unsafe fn last_insert_rowid(&self, db: NonNull<c_void>) -> i64;
    /// See [`Sqlite3AbiExtras::interrupt`].
    unsafe fn interrupt(&self, db: NonNull<c_void>);
    /// See [`Sqlite3AbiExtras::db_config`].
    unsafe fn db_config(&self, db: NonNull<c_void>, op: i32) -> i32;
    /// See [`Sqlite3AbiExtras::limit`].
    unsafe fn limit(&self, db: NonNull<c_void>, id: i32, new_value: i32) -> i32;
    /// See [`Sqlite3AbiExtras::stmt_readonly`].
    unsafe fn stmt_readonly(&self, stmt: NonNull<c_void>) -> i32;
    /// See [`Sqlite3AbiExtras::stmt_busy`].
    unsafe fn stmt_busy(&self, stmt: NonNull<c_void>) -> i32;
    /// See [`Sqlite3AbiExtras::bind_parameter_count`].
    unsafe fn bind_parameter_count(&self, stmt: NonNull<c_void>) -> i32;
    /// See [`Sqlite3AbiExtras::bind_parameter_name`].
    unsafe fn bind_parameter_name(&self, stmt: NonNull<c_void>, idx: i32) -> Option<RawBytes>;
    /// See [`Sqlite3AbiExtras::bind_parameter_index`].
    unsafe fn bind_parameter_index(&self, stmt: NonNull<c_void>, name: &str) -> i32;
    /// See [`Sqlite3AbiExtras::context_db_handle`].
    unsafe fn context_db_handle(&self, ctx: NonNull<c_void>) -> Option<NonNull<c_void>>;
}

unsafe impl<P: Sqlite3AbiExtras> AbiExtras for P {
    unsafe fn db_filename(&self, db: NonNull<c_void>, name: Option<&str>) -> Option<RawBytes> {
        catch_provider_default(None, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::db_filename(self, db, name)
        })
    }

    unsafe fn get_autocommit(&self, db: NonNull<c_void>) -> i32 {
        catch_provider_default(1, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::get_autocommit(self, db)
        })
    }

    unsafe fn total_changes(&self, db: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::total_changes(self, db)
        })
    }

    unsafe fn changes(&self, db: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::changes(self, db)
        })
    }

    unsafe fn changes64(&self, db: NonNull<c_void>) -> i64 {
        catch_provider_default(0, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::changes64(self, db)
        })
    }

    unsafe fn last_insert_rowid(&self, db: NonNull<c_void>) -> i64 {
        catch_provider_default(0, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::last_insert_rowid(self, db)
        })
    }

    unsafe fn interrupt(&self, db: NonNull<c_void>) {
        catch_provider_void(|| {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::interrupt(self, db)
        })
    }

    unsafe fn db_config(&self, db: NonNull<c_void>, op: i32) -> i32 {
        catch_provider_default(SQLITE_MISUSE, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::db_config(self, db, op)
        })
    }

    unsafe fn limit(&self, db: NonNull<c_void>, id: i32, new_value: i32) -> i32 {
        catch_provider_default(-1, || {
            let db = NonNull::new_unchecked(db.as_ptr() as *mut P::Db);
            Sqlite3AbiExtras::limit(self, db, id, new_value)
        })
    }

    unsafe fn stmt_readonly(&self, stmt: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3AbiExtras::stmt_readonly(self, stmt)
        })
    }

    unsafe fn stmt_busy(&self, stmt: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3AbiExtras::stmt_busy(self, stmt)
        })
    }

    unsafe fn bind_parameter_count(&self, stmt: NonNull<c_void>) -> i32 {
        catch_provider_default(0, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3AbiExtras::bind_parameter_count(self, stmt)
        })
    }

    unsafe fn bind_parameter_name(&self, stmt: NonNull<c_void>, idx: i32) -> Option<RawBytes> {
        catch_provider_default(None, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3AbiExtras::bind_parameter_name(self, stmt, idx)
        })
    }

    unsafe fn bind_parameter_index(&self, stmt: NonNull<c_void>, name: &str) -> i32 {
        catch_provider_default(0, || {
            let stmt = NonNull::new_unchecked(stmt.as_ptr() as *mut P::Stmt);
            Sqlite3AbiExtras::bind_parameter_index(self, stmt, name)
        })
    }

    unsafe fn context_db_handle(&self, ctx: NonNull<c_void>) -> Option<NonNull<c_void>> {
        catch_provider_default(None, || {
            let ctx = NonNull::new_unchecked(ctx.as_ptr() as *mut P::Context);
            Sqlite3AbiExtras::context_db_handle(self, ctx)
                .map(|db| NonNull::new_unchecked(db.as_ptr() as *mut c_void))
        })
    }
}

#[cfg(feature = "default-backend")]
unsafe impl Sqlite3AbiExtras for LibSqlite3 {
    unsafe fn db_filename(&self, db: NonNull<Self::Db>, name: Option<&str>) -> Option<RawBytes> {
        self.abi_db_filename(db.as_ptr(), name)
    }

    unsafe fn get_autocommit(&self, db: NonNull<Self::Db>) -> i32 {
        self.abi_get_autocommit(db.as_ptr())
    }

    unsafe fn total_changes(&self, db: NonNull<Self::Db>) -> i32 {
        self.abi_total_changes(db.as_ptr())
    }

    unsafe fn changes(&self, db: NonNull<Self::Db>) -> i32 {
        self.abi_changes(db.as_ptr())
    }

    unsafe fn changes64(&self, db: NonNull<Self::Db>) -> i64 {
        self.abi_changes64(db.as_ptr())
    }

    unsafe fn last_insert_rowid(&self, db: NonNull<Self::Db>) -> i64 {
        self.abi_last_insert_rowid(db.as_ptr())
    }

    unsafe fn interrupt(&self, db: NonNull<Self::Db>) {
        self.abi_interrupt(db.as_ptr());
    }

    unsafe fn db_config(&self, db: NonNull<Self::Db>, op: i32) -> i32 {
        self.abi_db_config(db.as_ptr(), op)
    }

    unsafe fn limit(&self, db: NonNull<Self::Db>, id: i32, new_value: i32) -> i32 {
        self.abi_limit(db.as_ptr(), id, new_value)
    }

    unsafe fn stmt_readonly(&self, stmt: NonNull<Self::Stmt>) -> i32 {
        self.abi_stmt_readonly(stmt.as_ptr())
    }

    unsafe fn stmt_busy(&self, stmt: NonNull<Self::Stmt>) -> i32 {
        self.abi_stmt_busy(stmt.as_ptr())
    }

    unsafe fn bind_parameter_count(&self, stmt: NonNull<Self::Stmt>) -> i32 {
        self.abi_bind_parameter_count(stmt.as_ptr())
    }

    unsafe fn bind_parameter_name(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Option<RawBytes> {
        self.abi_bind_parameter_name(stmt.as_ptr(), idx)
    }

    unsafe fn bind_parameter_index(&self, stmt: NonNull<Self::Stmt>, name: &str) -> i32 {
        self.abi_bind_parameter_index(stmt.as_ptr(), name)
    }

    unsafe fn context_db_handle(&self, ctx: NonNull<Self::Context>) -> Option<NonNull<Self::Db>> {
        self.abi_context_db_handle(ctx.as_ptr())
            .map(|db| NonNull::new_unchecked(db.as_ptr()))
    }
}
