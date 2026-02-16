//! Dynamic `libsqlite3` backend adapter for `sqlite-provider`.

#![allow(non_camel_case_types)]

use libc::{c_char, c_int, c_uchar, c_void};
use sqlite_provider::{
    ApiVersion, ColumnMetadata, Error, ErrorCode, FeatureSet, FunctionFlags, OpenFlags, OpenOptions,
    RawBytes, Result, Sqlite3Api, Sqlite3Metadata, StepResult, ValueType,
};
use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut, NonNull};
use std::sync::OnceLock;

#[cfg(target_os = "linux")]
#[link(name = "dl")]
extern "C" {}

const SQLITE_OK: i32 = 0;
const SQLITE_ROW: i32 = 100;
const SQLITE_DONE: i32 = 101;

const SQLITE_OPEN_READONLY: i32 = 0x0000_0001;
const SQLITE_OPEN_READWRITE: i32 = 0x0000_0002;
const SQLITE_OPEN_CREATE: i32 = 0x0000_0004;
const SQLITE_OPEN_URI: i32 = 0x0000_0040;
const SQLITE_OPEN_NOMUTEX: i32 = 0x0000_8000;
const SQLITE_OPEN_FULLMUTEX: i32 = 0x0001_0000;
const SQLITE_OPEN_SHAREDCACHE: i32 = 0x0002_0000;
const SQLITE_OPEN_PRIVATECACHE: i32 = 0x0004_0000;
const SQLITE_OPEN_EXRESCODE: i32 = 0x0200_0000;

const SQLITE_UTF8: i32 = 0x0000_0001;
const SQLITE_DETERMINISTIC: i32 = 0x0000_0800;
const SQLITE_INNOCUOUS: i32 = 0x0002_0000;
const SQLITE_DIRECTONLY: i32 = 0x0008_0000;
const EMPTY_BYTE: u8 = 0;

type sqlite3 = c_void;
type sqlite3_stmt = c_void;
type sqlite3_value = c_void;
type sqlite3_context = c_void;

type sqlite3_destructor_type = Option<unsafe extern "C" fn(*mut c_void)>;

type OpenV2 =
    unsafe extern "C" fn(*const c_char, *mut *mut sqlite3, c_int, *const c_char) -> c_int;
type Close = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type PrepareV2 =
    unsafe extern "C" fn(*mut sqlite3, *const c_char, c_int, *mut *mut sqlite3_stmt, *mut *const c_char) -> c_int;
type PrepareV3 =
    unsafe extern "C" fn(*mut sqlite3, *const c_char, c_int, u32, *mut *mut sqlite3_stmt, *mut *const c_char) -> c_int;
type Step = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type Reset = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type Finalize = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;

type BindNull = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_int;
type BindInt64 = unsafe extern "C" fn(*mut sqlite3_stmt, c_int, i64) -> c_int;
type BindDouble = unsafe extern "C" fn(*mut sqlite3_stmt, c_int, f64) -> c_int;
type BindText =
    unsafe extern "C" fn(*mut sqlite3_stmt, c_int, *const c_char, c_int, sqlite3_destructor_type) -> c_int;
type BindBlob =
    unsafe extern "C" fn(*mut sqlite3_stmt, c_int, *const c_void, c_int, sqlite3_destructor_type) -> c_int;

type ColumnCount = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type ColumnType = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_int;
type ColumnInt64 = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> i64;
type ColumnDouble = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> f64;
type ColumnText = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_uchar;
type ColumnBlob = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_void;
type ColumnBytes = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_int;

type ErrCode = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type ErrMsg = unsafe extern "C" fn(*mut sqlite3) -> *const c_char;
type ExtendedErrCode = unsafe extern "C" fn(*mut sqlite3) -> c_int;

type CreateFunctionV2 = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    c_int,
    c_int,
    *mut c_void,
    Option<extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value)>,
    Option<extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value)>,
    Option<extern "C" fn(*mut sqlite3_context)>,
    Option<extern "C" fn(*mut c_void)>,
) -> c_int;

type CreateWindowFunction = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    c_int,
    c_int,
    *mut c_void,
    Option<extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value)>,
    Option<extern "C" fn(*mut sqlite3_context)>,
    Option<extern "C" fn(*mut sqlite3_context)>,
    Option<extern "C" fn(*mut sqlite3_context, c_int, *mut *mut sqlite3_value)>,
    Option<extern "C" fn(*mut c_void)>,
) -> c_int;

type AggregateContext = unsafe extern "C" fn(*mut sqlite3_context, c_int) -> *mut c_void;
type ResultNull = unsafe extern "C" fn(*mut sqlite3_context);
type ResultInt64 = unsafe extern "C" fn(*mut sqlite3_context, i64);
type ResultDouble = unsafe extern "C" fn(*mut sqlite3_context, f64);
type ResultText =
    unsafe extern "C" fn(*mut sqlite3_context, *const c_char, c_int, sqlite3_destructor_type);
type ResultBlob =
    unsafe extern "C" fn(*mut sqlite3_context, *const c_void, c_int, sqlite3_destructor_type);
type ResultError = unsafe extern "C" fn(*mut sqlite3_context, *const c_char, c_int);
type UserData = unsafe extern "C" fn(*mut sqlite3_context) -> *mut c_void;

type ValueTypeFn = unsafe extern "C" fn(*mut sqlite3_value) -> c_int;
type ValueInt64Fn = unsafe extern "C" fn(*mut sqlite3_value) -> i64;
type ValueDoubleFn = unsafe extern "C" fn(*mut sqlite3_value) -> f64;
type ValueTextFn = unsafe extern "C" fn(*mut sqlite3_value) -> *const c_uchar;
type ValueBlobFn = unsafe extern "C" fn(*mut sqlite3_value) -> *const c_void;
type ValueBytesFn = unsafe extern "C" fn(*mut sqlite3_value) -> c_int;

type DeclareVTab = unsafe extern "C" fn(*mut sqlite3, *const c_char) -> c_int;
type CreateModuleV2 =
    unsafe extern "C" fn(*mut sqlite3, *const c_char, *const c_void, *mut c_void, Option<extern "C" fn(*mut c_void)>) -> c_int;

type LibversionNumber = unsafe extern "C" fn() -> c_int;

type Malloc = unsafe extern "C" fn(c_int) -> *mut c_void;
type Free = unsafe extern "C" fn(*mut c_void);

type ColumnDecltype = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_char;
type ColumnName = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_char;
type ColumnTableName = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_char;
type TableColumnMetadata = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    *const c_char,
    *const c_char,
    *mut *const c_char,
    *mut *const c_char,
    *mut c_int,
    *mut c_int,
    *mut c_int,
) -> c_int;

struct LibHandle {
    handle: *mut c_void,
}

unsafe impl Send for LibHandle {}
unsafe impl Sync for LibHandle {}

impl LibHandle {
    unsafe fn open() -> Option<Self> {
        let mut handle = null_mut();
        for name in lib_names() {
            let cstr = CStr::from_bytes_with_nul_unchecked(name);
            handle = libc::dlopen(cstr.as_ptr(), libc::RTLD_LAZY | libc::RTLD_LOCAL);
            if !handle.is_null() {
                break;
            }
        }
        if handle.is_null() {
            None
        } else {
            Some(Self { handle })
        }
    }

    unsafe fn symbol<T>(&self, name: &'static [u8]) -> Option<T>
    where
        T: Copy,
    {
        let sym = libc::dlsym(self.handle, name.as_ptr() as *const c_char);
        if sym.is_null() {
            None
        } else {
            debug_assert_eq!(std::mem::size_of::<T>(), std::mem::size_of::<*mut c_void>());
            Some(std::mem::transmute_copy(&sym))
        }
    }
}

struct LibSqlite3Fns {
    open_v2: OpenV2,
    close: Close,
    prepare_v2: PrepareV2,
    prepare_v3: Option<PrepareV3>,
    step: Step,
    reset: Reset,
    finalize: Finalize,
    bind_null: BindNull,
    bind_int64: BindInt64,
    bind_double: BindDouble,
    bind_text: BindText,
    bind_blob: BindBlob,
    column_count: ColumnCount,
    column_type: ColumnType,
    column_int64: ColumnInt64,
    column_double: ColumnDouble,
    column_text: ColumnText,
    column_blob: ColumnBlob,
    column_bytes: ColumnBytes,
    errcode: ErrCode,
    errmsg: ErrMsg,
    extended_errcode: Option<ExtendedErrCode>,
    create_function_v2: CreateFunctionV2,
    create_window_function: Option<CreateWindowFunction>,
    aggregate_context: AggregateContext,
    result_null: ResultNull,
    result_int64: ResultInt64,
    result_double: ResultDouble,
    result_text: ResultText,
    result_blob: ResultBlob,
    result_error: ResultError,
    user_data: UserData,
    value_type: ValueTypeFn,
    value_int64: ValueInt64Fn,
    value_double: ValueDoubleFn,
    value_text: ValueTextFn,
    value_blob: ValueBlobFn,
    value_bytes: ValueBytesFn,
    declare_vtab: DeclareVTab,
    create_module_v2: Option<CreateModuleV2>,
    libversion_number: LibversionNumber,
    malloc: Malloc,
    free: Free,
    column_decltype: ColumnDecltype,
    column_name: ColumnName,
    column_table_name: Option<ColumnTableName>,
    table_column_metadata: Option<TableColumnMetadata>,
}

impl LibSqlite3Fns {
    unsafe fn load(lib: &LibHandle) -> Option<Self> {
        Some(Self {
            open_v2: lib.symbol(b"sqlite3_open_v2\0")?,
            close: lib.symbol(b"sqlite3_close\0")?,
            prepare_v2: lib.symbol(b"sqlite3_prepare_v2\0")?,
            prepare_v3: lib.symbol(b"sqlite3_prepare_v3\0"),
            step: lib.symbol(b"sqlite3_step\0")?,
            reset: lib.symbol(b"sqlite3_reset\0")?,
            finalize: lib.symbol(b"sqlite3_finalize\0")?,
            bind_null: lib.symbol(b"sqlite3_bind_null\0")?,
            bind_int64: lib.symbol(b"sqlite3_bind_int64\0")?,
            bind_double: lib.symbol(b"sqlite3_bind_double\0")?,
            bind_text: lib.symbol(b"sqlite3_bind_text\0")?,
            bind_blob: lib.symbol(b"sqlite3_bind_blob\0")?,
            column_count: lib.symbol(b"sqlite3_column_count\0")?,
            column_type: lib.symbol(b"sqlite3_column_type\0")?,
            column_int64: lib.symbol(b"sqlite3_column_int64\0")?,
            column_double: lib.symbol(b"sqlite3_column_double\0")?,
            column_text: lib.symbol(b"sqlite3_column_text\0")?,
            column_blob: lib.symbol(b"sqlite3_column_blob\0")?,
            column_bytes: lib.symbol(b"sqlite3_column_bytes\0")?,
            errcode: lib.symbol(b"sqlite3_errcode\0")?,
            errmsg: lib.symbol(b"sqlite3_errmsg\0")?,
            extended_errcode: lib.symbol(b"sqlite3_extended_errcode\0"),
            create_function_v2: lib.symbol(b"sqlite3_create_function_v2\0")?,
            create_window_function: lib.symbol(b"sqlite3_create_window_function\0"),
            aggregate_context: lib.symbol(b"sqlite3_aggregate_context\0")?,
            result_null: lib.symbol(b"sqlite3_result_null\0")?,
            result_int64: lib.symbol(b"sqlite3_result_int64\0")?,
            result_double: lib.symbol(b"sqlite3_result_double\0")?,
            result_text: lib.symbol(b"sqlite3_result_text\0")?,
            result_blob: lib.symbol(b"sqlite3_result_blob\0")?,
            result_error: lib.symbol(b"sqlite3_result_error\0")?,
            user_data: lib.symbol(b"sqlite3_user_data\0")?,
            value_type: lib.symbol(b"sqlite3_value_type\0")?,
            value_int64: lib.symbol(b"sqlite3_value_int64\0")?,
            value_double: lib.symbol(b"sqlite3_value_double\0")?,
            value_text: lib.symbol(b"sqlite3_value_text\0")?,
            value_blob: lib.symbol(b"sqlite3_value_blob\0")?,
            value_bytes: lib.symbol(b"sqlite3_value_bytes\0")?,
            declare_vtab: lib.symbol(b"sqlite3_declare_vtab\0")?,
            create_module_v2: lib.symbol(b"sqlite3_create_module_v2\0"),
            libversion_number: lib.symbol(b"sqlite3_libversion_number\0")?,
            malloc: lib.symbol(b"sqlite3_malloc\0")?,
            free: lib.symbol(b"sqlite3_free\0")?,
            column_decltype: lib.symbol(b"sqlite3_column_decltype\0")?,
            column_name: lib.symbol(b"sqlite3_column_name\0")?,
            column_table_name: lib.symbol(b"sqlite3_column_table_name\0"),
            table_column_metadata: lib.symbol(b"sqlite3_table_column_metadata\0"),
        })
    }
}

static USER_DATA_FN: OnceLock<UserData> = OnceLock::new();

/// Dynamic `libsqlite3` backend adapter loaded via `dlopen`.
pub struct LibSqlite3 {
    fns: LibSqlite3Fns,
    features: FeatureSet,
    api_version: ApiVersion,
    _lib: LibHandle,
}

impl LibSqlite3 {
    /// Load `libsqlite3` and return a process-wide adapter instance.
    ///
    /// Returns `None` if the library or required symbols are unavailable.
    pub fn load() -> Option<&'static LibSqlite3> {
        unsafe {
            let lib = LibHandle::open()?;
            let fns = LibSqlite3Fns::load(&lib)?;
            let version_number = (fns.libversion_number)();
            let api_version = api_version_from_number(version_number);
            let mut features = FeatureSet::CREATE_FUNCTION_V2;
            if fns.prepare_v3.is_some() {
                features |= FeatureSet::PREPARE_V3;
            }
            if fns.create_window_function.is_some() {
                features |= FeatureSet::WINDOW_FUNCTIONS;
            }
            if fns.extended_errcode.is_some() {
                features |= FeatureSet::EXTENDED_ERRCODES;
            }
            if fns.create_module_v2.is_some() {
                features |= FeatureSet::VIRTUAL_TABLES;
            }
            let _ = USER_DATA_FN.set(fns.user_data);
            let adapter = LibSqlite3 {
                fns,
                features,
                api_version,
                _lib: lib,
            };
            Some(Box::leak(Box::new(adapter)))
        }
    }

    fn error_from_rc(&self, rc: i32, db: Option<NonNull<sqlite3>>) -> Error {
        let message = db
            .and_then(|db| unsafe { raw_cstr((self.fns.errmsg)(db.as_ptr())) })
            .map(|c| c.to_string_lossy().into_owned());
        let extended = db.and_then(|db| self.fns.extended_errcode.map(|f| unsafe { f(db.as_ptr()) }));
        Error::from_code(rc, message, extended)
    }

    // Allocate with sqlite3_malloc so SQLite can free via sqlite3_free.
    fn alloc_copy(&self, bytes: &[u8]) -> Result<(*const c_void, sqlite3_destructor_type)> {
        if bytes.is_empty() {
            return Ok((&EMPTY_BYTE as *const u8 as *const c_void, None));
        }
        if bytes.len() > i32::MAX as usize {
            return Err(Error::with_message(ErrorCode::Misuse, "value too large"));
        }
        let ptr = unsafe { (self.fns.malloc)(bytes.len() as i32) };
        if ptr.is_null() {
            return Err(Error::new(ErrorCode::NoMem));
        }
        unsafe {
            std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr as *mut u8, bytes.len());
        }
        Ok((ptr, Some(self.fns.free)))
    }
}

unsafe impl Sqlite3Api for LibSqlite3 {
    type Db = sqlite3;
    type Stmt = sqlite3_stmt;
    type Value = sqlite3_value;
    type Context = sqlite3_context;
    type VTab = c_void;
    type VTabCursor = c_void;

    fn api_version(&self) -> ApiVersion {
        self.api_version
    }

    fn feature_set(&self) -> FeatureSet {
        self.features
    }

    fn backend_name(&self) -> &'static str {
        "libsqlite3"
    }

    fn backend_version(&self) -> Option<ApiVersion> {
        Some(self.api_version)
    }

    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<Self::Db>> {
        let filename = CString::new(filename)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "filename contains NUL"))?;
        let vfs = match options.vfs {
            Some(vfs) => Some(
                CString::new(vfs).map_err(|_| Error::with_message(ErrorCode::Misuse, "vfs contains NUL"))?,
            ),
            None => None,
        };
        let mut db = null_mut();
        let flags = map_open_flags(options.flags);
        let vfs_ptr = vfs.as_ref().map(|s| s.as_ptr()).unwrap_or(null());
        let rc = (self.fns.open_v2)(filename.as_ptr(), &mut db, flags, vfs_ptr);
        if rc != SQLITE_OK {
            let err = self.error_from_rc(rc, NonNull::new(db));
            if !db.is_null() {
                let _ = (self.fns.close)(db);
            }
            return Err(err);
        }
        Ok(NonNull::new_unchecked(db))
    }

    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()> {
        let rc = (self.fns.close)(db.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn prepare_v2(&self, db: NonNull<Self::Db>, sql: &str) -> Result<NonNull<Self::Stmt>> {
        let mut stmt = null_mut();
        let sql_ptr = sql.as_ptr() as *const c_char;
        let sql_len = clamp_len(sql.len());
        let rc = (self.fns.prepare_v2)(db.as_ptr(), sql_ptr, sql_len, &mut stmt, null_mut());
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, Some(db)));
        }
        Ok(NonNull::new_unchecked(stmt))
    }

    unsafe fn prepare_v3(
        &self,
        db: NonNull<Self::Db>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<Self::Stmt>> {
        let prepare = match self.fns.prepare_v3 {
            Some(prepare) => prepare,
            None => return Err(Error::feature_unavailable("prepare_v3 not available")),
        };
        let mut stmt = null_mut();
        let sql_ptr = sql.as_ptr() as *const c_char;
        let sql_len = clamp_len(sql.len());
        let rc = prepare(db.as_ptr(), sql_ptr, sql_len, flags, &mut stmt, null_mut());
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, Some(db)));
        }
        Ok(NonNull::new_unchecked(stmt))
    }

    unsafe fn step(&self, stmt: NonNull<Self::Stmt>) -> Result<StepResult> {
        match (self.fns.step)(stmt.as_ptr()) {
            SQLITE_ROW => Ok(StepResult::Row),
            SQLITE_DONE => Ok(StepResult::Done),
            rc => Err(self.error_from_rc(rc, None)),
        }
    }

    unsafe fn reset(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        let rc = (self.fns.reset)(stmt.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn finalize(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        let rc = (self.fns.finalize)(stmt.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_null(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Result<()> {
        let rc = (self.fns.bind_null)(stmt.as_ptr(), idx);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_int64(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: i64) -> Result<()> {
        let rc = (self.fns.bind_int64)(stmt.as_ptr(), idx, v);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_double(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: f64) -> Result<()> {
        let rc = (self.fns.bind_double)(stmt.as_ptr(), idx, v);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_text(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &str) -> Result<()> {
        let (ptr, dtor) = self.alloc_copy(v.as_bytes())?;
        let rc = (self.fns.bind_text)(
            stmt.as_ptr(),
            idx,
            ptr as *const c_char,
            clamp_len(v.len()),
            dtor,
        );
        if rc != SQLITE_OK {
            if let Some(free) = dtor {
                free(ptr as *mut c_void);
            }
            return Err(self.error_from_rc(rc, None));
        }
        Ok(())
    }

    unsafe fn bind_blob(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &[u8]) -> Result<()> {
        let (ptr, dtor) = self.alloc_copy(v)?;
        let rc = (self.fns.bind_blob)(
            stmt.as_ptr(),
            idx,
            ptr as *const c_void,
            clamp_len(v.len()),
            dtor,
        );
        if rc != SQLITE_OK {
            if let Some(free) = dtor {
                free(ptr as *mut c_void);
            }
            return Err(self.error_from_rc(rc, None));
        }
        Ok(())
    }

    unsafe fn column_count(&self, stmt: NonNull<Self::Stmt>) -> i32 {
        (self.fns.column_count)(stmt.as_ptr())
    }

    unsafe fn column_type(&self, stmt: NonNull<Self::Stmt>, col: i32) -> ValueType {
        ValueType::from_code((self.fns.column_type)(stmt.as_ptr(), col))
    }

    unsafe fn column_int64(&self, stmt: NonNull<Self::Stmt>, col: i32) -> i64 {
        (self.fns.column_int64)(stmt.as_ptr(), col)
    }

    unsafe fn column_double(&self, stmt: NonNull<Self::Stmt>, col: i32) -> f64 {
        (self.fns.column_double)(stmt.as_ptr(), col)
    }

    unsafe fn column_text(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let ptr = (self.fns.column_text)(stmt.as_ptr(), col) as *const u8;
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.column_bytes)(stmt.as_ptr(), col);
        RawBytes { ptr, len: len as usize }
    }

    unsafe fn column_blob(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let ptr = (self.fns.column_blob)(stmt.as_ptr(), col) as *const u8;
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.column_bytes)(stmt.as_ptr(), col);
        RawBytes { ptr, len: len as usize }
    }

    unsafe fn errcode(&self, db: NonNull<Self::Db>) -> i32 {
        (self.fns.errcode)(db.as_ptr())
    }

    unsafe fn errmsg(&self, db: NonNull<Self::Db>) -> *const c_char {
        (self.fns.errmsg)(db.as_ptr())
    }

    unsafe fn extended_errcode(&self, db: NonNull<Self::Db>) -> Option<i32> {
        self.fns.extended_errcode.map(|f| f(db.as_ptr()))
    }

    unsafe fn create_function_v2(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_func: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_final: Option<extern "C" fn(*mut Self::Context)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let name = CString::new(name).map_err(|_| Error::with_message(ErrorCode::Misuse, "function name contains NUL"))?;
        let flags = map_function_flags(flags);
        let rc = (self.fns.create_function_v2)(
            db.as_ptr(),
            name.as_ptr(),
            n_args,
            flags,
            user_data,
            x_func,
            x_step,
            x_final,
            drop_user_data,
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn create_window_function(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_final: Option<extern "C" fn(*mut Self::Context)>,
        x_value: Option<extern "C" fn(*mut Self::Context)>,
        x_inverse: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let create = match self.fns.create_window_function {
            Some(create) => create,
            None => return Err(Error::feature_unavailable("create_window_function not available")),
        };
        let name = CString::new(name).map_err(|_| Error::with_message(ErrorCode::Misuse, "function name contains NUL"))?;
        let flags = map_function_flags(flags);
        let rc = create(
            db.as_ptr(),
            name.as_ptr(),
            n_args,
            flags,
            user_data,
            x_step,
            x_final,
            x_value,
            x_inverse,
            drop_user_data,
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn aggregate_context(&self, ctx: NonNull<Self::Context>, bytes: usize) -> *mut c_void {
        (self.fns.aggregate_context)(ctx.as_ptr(), clamp_len(bytes) as c_int)
    }

    unsafe fn result_null(&self, ctx: NonNull<Self::Context>) {
        (self.fns.result_null)(ctx.as_ptr());
    }

    unsafe fn result_int64(&self, ctx: NonNull<Self::Context>, v: i64) {
        (self.fns.result_int64)(ctx.as_ptr(), v);
    }

    unsafe fn result_double(&self, ctx: NonNull<Self::Context>, v: f64) {
        (self.fns.result_double)(ctx.as_ptr(), v);
    }

    unsafe fn result_text(&self, ctx: NonNull<Self::Context>, v: &str) {
        match self.alloc_copy(v.as_bytes()) {
            Ok((ptr, dtor)) => {
                (self.fns.result_text)(ctx.as_ptr(), ptr as *const c_char, clamp_len(v.len()), dtor);
            }
            Err(_) => {
                const OOM: &str = "out of memory";
                (self.fns.result_error)(ctx.as_ptr(), OOM.as_ptr() as *const c_char, clamp_len(OOM.len()));
            }
        }
    }

    unsafe fn result_blob(&self, ctx: NonNull<Self::Context>, v: &[u8]) {
        match self.alloc_copy(v) {
            Ok((ptr, dtor)) => {
                (self.fns.result_blob)(ctx.as_ptr(), ptr as *const c_void, clamp_len(v.len()), dtor);
            }
            Err(_) => {
                const OOM: &str = "out of memory";
                (self.fns.result_error)(ctx.as_ptr(), OOM.as_ptr() as *const c_char, clamp_len(OOM.len()));
            }
        }
    }

    unsafe fn result_error(&self, ctx: NonNull<Self::Context>, msg: &str) {
        (self.fns.result_error)(ctx.as_ptr(), msg.as_ptr() as *const c_char, clamp_len(msg.len()));
    }

    unsafe fn user_data(ctx: NonNull<Self::Context>) -> *mut c_void {
        match USER_DATA_FN.get() {
            Some(f) => f(ctx.as_ptr()),
            None => null_mut(),
        }
    }

    unsafe fn value_type(&self, v: NonNull<Self::Value>) -> ValueType {
        ValueType::from_code((self.fns.value_type)(v.as_ptr()))
    }

    unsafe fn value_int64(&self, v: NonNull<Self::Value>) -> i64 {
        (self.fns.value_int64)(v.as_ptr())
    }

    unsafe fn value_double(&self, v: NonNull<Self::Value>) -> f64 {
        (self.fns.value_double)(v.as_ptr())
    }

    unsafe fn value_text(&self, v: NonNull<Self::Value>) -> RawBytes {
        let ptr = (self.fns.value_text)(v.as_ptr()) as *const u8;
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.value_bytes)(v.as_ptr());
        RawBytes { ptr, len: len as usize }
    }

    unsafe fn value_blob(&self, v: NonNull<Self::Value>) -> RawBytes {
        let ptr = (self.fns.value_blob)(v.as_ptr()) as *const u8;
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.value_bytes)(v.as_ptr());
        RawBytes { ptr, len: len as usize }
    }

    unsafe fn declare_vtab(&self, db: NonNull<Self::Db>, schema: &str) -> Result<()> {
        let schema = CString::new(schema).map_err(|_| Error::with_message(ErrorCode::Misuse, "schema contains NUL"))?;
        let rc = (self.fns.declare_vtab)(db.as_ptr(), schema.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn create_module_v2(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        module: &'static sqlite_provider::sqlite3_module<Self>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let create = match self.fns.create_module_v2 {
            Some(create) => create,
            None => return Err(Error::feature_unavailable("create_module_v2 not available")),
        };
        let name = CString::new(name).map_err(|_| Error::with_message(ErrorCode::Misuse, "module name contains NUL"))?;
        let rc = create(
            db.as_ptr(),
            name.as_ptr(),
            module as *const _ as *const c_void,
            user_data,
            drop_user_data,
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }
}

unsafe impl Sqlite3Metadata for LibSqlite3 {
    unsafe fn table_column_metadata(
        &self,
        db: NonNull<Self::Db>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata> {
        let table = CString::new(table).map_err(|_| Error::with_message(ErrorCode::Misuse, "table contains NUL"))?;
        let column = CString::new(column).map_err(|_| Error::with_message(ErrorCode::Misuse, "column contains NUL"))?;
        let db_name = match db_name {
            Some(name) => Some(
                CString::new(name).map_err(|_| Error::with_message(ErrorCode::Misuse, "db name contains NUL"))?,
            ),
            None => None,
        };
        let func = match self.fns.table_column_metadata {
            Some(func) => func,
            None => return Err(Error::feature_unavailable("table_column_metadata not available")),
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

fn api_version_from_number(number: i32) -> ApiVersion {
    let major = (number / 1_000_000) as u16;
    let minor = ((number / 1_000) % 1_000) as u16;
    let patch = (number % 1_000) as u16;
    ApiVersion::new(major, minor, patch)
}

fn map_open_flags(flags: OpenFlags) -> i32 {
    let mut out = 0;
    if flags.contains(OpenFlags::READ_ONLY) {
        out |= SQLITE_OPEN_READONLY;
    }
    if flags.contains(OpenFlags::READ_WRITE) {
        out |= SQLITE_OPEN_READWRITE;
    }
    if flags.contains(OpenFlags::CREATE) {
        out |= SQLITE_OPEN_CREATE;
    }
    if flags.contains(OpenFlags::URI) {
        out |= SQLITE_OPEN_URI;
    }
    if flags.contains(OpenFlags::NO_MUTEX) {
        out |= SQLITE_OPEN_NOMUTEX;
    }
    if flags.contains(OpenFlags::FULL_MUTEX) {
        out |= SQLITE_OPEN_FULLMUTEX;
    }
    if flags.contains(OpenFlags::SHARED_CACHE) {
        out |= SQLITE_OPEN_SHAREDCACHE;
    }
    if flags.contains(OpenFlags::PRIVATE_CACHE) {
        out |= SQLITE_OPEN_PRIVATECACHE;
    }
    if flags.contains(OpenFlags::EXRESCODE) {
        out |= SQLITE_OPEN_EXRESCODE;
    }
    out
}

fn map_function_flags(flags: FunctionFlags) -> i32 {
    let mut out = SQLITE_UTF8;
    if flags.contains(FunctionFlags::DETERMINISTIC) {
        out |= SQLITE_DETERMINISTIC;
    }
    if flags.contains(FunctionFlags::DIRECT_ONLY) {
        out |= SQLITE_DIRECTONLY;
    }
    if flags.contains(FunctionFlags::INNOCUOUS) {
        out |= SQLITE_INNOCUOUS;
    }
    out
}

fn clamp_len(len: usize) -> i32 {
    if len > i32::MAX as usize {
        i32::MAX
    } else {
        len as i32
    }
}

unsafe fn raw_cstr<'a>(ptr: *const c_char) -> Option<&'a CStr> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr))
    }
}

fn raw_bytes_from_cstr(cstr: &CStr) -> RawBytes {
    RawBytes {
        ptr: cstr.as_ptr() as *const u8,
        len: cstr.to_bytes().len(),
    }
}

fn lib_names() -> &'static [&'static [u8]] {
    #[cfg(target_os = "macos")]
    const NAMES: [&[u8]; 3] = [
        b"libsqlite3.dylib\0",
        b"libsqlite3.so.0\0",
        b"libsqlite3.so\0",
    ];
    #[cfg(not(target_os = "macos"))]
    const NAMES: [&[u8]; 2] = [b"libsqlite3.so.0\0", b"libsqlite3.so\0"];
    &NAMES
}
