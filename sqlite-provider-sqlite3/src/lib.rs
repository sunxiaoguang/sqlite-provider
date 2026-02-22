//! Dynamic `libsqlite3` backend adapter for `sqlite-provider`.

#![allow(non_camel_case_types)]

use libc::{c_char, c_int, c_uchar, c_void};
use sqlite_provider::{
    ApiVersion, ColumnMetadata, Error, ErrorCode, FeatureSet, FunctionFlags, OpenFlags,
    OpenOptions, OwnedBytes, RawBytes, Result, Sqlite3Api, Sqlite3Backup, Sqlite3BlobIo,
    Sqlite3Hooks, Sqlite3Metadata, Sqlite3Serialize, Sqlite3Wal, StepResult, ValueType,
};
use std::ffi::{CStr, CString};
use std::ptr::{NonNull, null, null_mut};
use std::sync::OnceLock;

#[cfg(target_os = "linux")]
#[link(name = "dl")]
unsafe extern "C" {}

const SQLITE_OK: i32 = 0;
const SQLITE_MISUSE: i32 = 21;
const SQLITE_ROW: i32 = 100;
const SQLITE_DONE: i32 = 101;
const SQLITE_DESERIALIZE_FREEONCLOSE: u32 = 0x0000_0001;

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
type sqlite3_backup = c_void;
type sqlite3_blob = c_void;

type sqlite3_destructor_type = Option<unsafe extern "C" fn(*mut c_void)>;

type OpenV2 = unsafe extern "C" fn(*const c_char, *mut *mut sqlite3, c_int, *const c_char) -> c_int;
type Close = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type PrepareV2 = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    c_int,
    *mut *mut sqlite3_stmt,
    *mut *const c_char,
) -> c_int;
type PrepareV3 = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    c_int,
    u32,
    *mut *mut sqlite3_stmt,
    *mut *const c_char,
) -> c_int;
type Step = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type Reset = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type Finalize = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;

type BindNull = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> c_int;
type BindInt64 = unsafe extern "C" fn(*mut sqlite3_stmt, c_int, i64) -> c_int;
type BindDouble = unsafe extern "C" fn(*mut sqlite3_stmt, c_int, f64) -> c_int;
type BindText = unsafe extern "C" fn(
    *mut sqlite3_stmt,
    c_int,
    *const c_char,
    c_int,
    sqlite3_destructor_type,
) -> c_int;
type BindBlob = unsafe extern "C" fn(
    *mut sqlite3_stmt,
    c_int,
    *const c_void,
    c_int,
    sqlite3_destructor_type,
) -> c_int;

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
type CreateCollationV2 = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    c_int,
    *mut c_void,
    Option<extern "C" fn(*mut c_void, c_int, *const c_void, c_int, *const c_void) -> c_int>,
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
type CreateModuleV2 = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    *const c_void,
    *mut c_void,
    Option<extern "C" fn(*mut c_void)>,
) -> c_int;

type LibversionNumber = unsafe extern "C" fn() -> c_int;
type Threadsafe = unsafe extern "C" fn() -> c_int;

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
type DbFilename = unsafe extern "C" fn(*mut sqlite3, *const c_char) -> *const c_char;
type GetAutocommit = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type TotalChanges = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type Changes = unsafe extern "C" fn(*mut sqlite3) -> c_int;
type Changes64 = unsafe extern "C" fn(*mut sqlite3) -> i64;
type LastInsertRowid = unsafe extern "C" fn(*mut sqlite3) -> i64;
type Interrupt = unsafe extern "C" fn(*mut sqlite3);
type DbConfig = unsafe extern "C" fn(*mut sqlite3, c_int, ...) -> c_int;
type Limit = unsafe extern "C" fn(*mut sqlite3, c_int, c_int) -> c_int;
type StmtReadonly = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type StmtBusy = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type BindParameterCount = unsafe extern "C" fn(*mut sqlite3_stmt) -> c_int;
type BindParameterName = unsafe extern "C" fn(*mut sqlite3_stmt, c_int) -> *const c_char;
type BindParameterIndex = unsafe extern "C" fn(*mut sqlite3_stmt, *const c_char) -> c_int;
type ContextDbHandle = unsafe extern "C" fn(*mut sqlite3_context) -> *mut sqlite3;
type TraceV2 = unsafe extern "C" fn(
    *mut sqlite3,
    u32,
    Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
    *mut c_void,
) -> c_int;
type ProgressHandler = unsafe extern "C" fn(
    *mut sqlite3,
    c_int,
    Option<extern "C" fn(*mut c_void) -> c_int>,
    *mut c_void,
);
type BusyTimeout = unsafe extern "C" fn(*mut sqlite3, c_int) -> c_int;
type SetAuthorizer = unsafe extern "C" fn(
    *mut sqlite3,
    Option<
        extern "C" fn(
            *mut c_void,
            c_int,
            *const c_char,
            *const c_char,
            *const c_char,
            *const c_char,
        ) -> c_int,
    >,
    *mut c_void,
) -> c_int;
type BackupInit = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    *mut sqlite3,
    *const c_char,
) -> *mut sqlite3_backup;
type BackupStep = unsafe extern "C" fn(*mut sqlite3_backup, c_int) -> c_int;
type BackupRemaining = unsafe extern "C" fn(*mut sqlite3_backup) -> c_int;
type BackupPagecount = unsafe extern "C" fn(*mut sqlite3_backup) -> c_int;
type BackupFinish = unsafe extern "C" fn(*mut sqlite3_backup) -> c_int;
type BlobOpen = unsafe extern "C" fn(
    *mut sqlite3,
    *const c_char,
    *const c_char,
    *const c_char,
    i64,
    c_int,
    *mut *mut sqlite3_blob,
) -> c_int;
type BlobRead = unsafe extern "C" fn(*mut sqlite3_blob, *mut c_void, c_int, c_int) -> c_int;
type BlobWrite = unsafe extern "C" fn(*mut sqlite3_blob, *const c_void, c_int, c_int) -> c_int;
type BlobBytes = unsafe extern "C" fn(*mut sqlite3_blob) -> c_int;
type BlobClose = unsafe extern "C" fn(*mut sqlite3_blob) -> c_int;
type Serialize = unsafe extern "C" fn(*mut sqlite3, *const c_char, *mut i64, u32) -> *mut c_uchar;
type Deserialize =
    unsafe extern "C" fn(*mut sqlite3, *const c_char, *mut c_uchar, i64, i64, u32) -> c_int;
type WalCheckpoint = unsafe extern "C" fn(*mut sqlite3, *const c_char) -> c_int;
type WalCheckpointV2 =
    unsafe extern "C" fn(*mut sqlite3, *const c_char, c_int, *mut c_int, *mut c_int) -> c_int;
type LibsqlWalFrameCount = unsafe extern "C" fn(*mut sqlite3, *mut u32) -> c_int;

struct LibHandle {
    handle: *mut c_void,
}

unsafe impl Send for LibHandle {}
unsafe impl Sync for LibHandle {}

#[allow(unsafe_op_in_unsafe_fn)]
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
    create_collation_v2: Option<CreateCollationV2>,
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
    threadsafe: Option<Threadsafe>,
    malloc: Malloc,
    free: Free,
    column_decltype: ColumnDecltype,
    column_name: ColumnName,
    column_table_name: Option<ColumnTableName>,
    table_column_metadata: Option<TableColumnMetadata>,
    db_filename: Option<DbFilename>,
    get_autocommit: Option<GetAutocommit>,
    total_changes: Option<TotalChanges>,
    changes: Option<Changes>,
    changes64: Option<Changes64>,
    last_insert_rowid: Option<LastInsertRowid>,
    interrupt: Option<Interrupt>,
    db_config: Option<DbConfig>,
    limit: Option<Limit>,
    stmt_readonly: Option<StmtReadonly>,
    stmt_busy: Option<StmtBusy>,
    bind_parameter_count: Option<BindParameterCount>,
    bind_parameter_name: Option<BindParameterName>,
    bind_parameter_index: Option<BindParameterIndex>,
    context_db_handle: Option<ContextDbHandle>,
    trace_v2: Option<TraceV2>,
    progress_handler: Option<ProgressHandler>,
    busy_timeout: Option<BusyTimeout>,
    set_authorizer: Option<SetAuthorizer>,
    backup_init: Option<BackupInit>,
    backup_step: Option<BackupStep>,
    backup_remaining: Option<BackupRemaining>,
    backup_pagecount: Option<BackupPagecount>,
    backup_finish: Option<BackupFinish>,
    blob_open: Option<BlobOpen>,
    blob_read: Option<BlobRead>,
    blob_write: Option<BlobWrite>,
    blob_bytes: Option<BlobBytes>,
    blob_close: Option<BlobClose>,
    serialize: Option<Serialize>,
    deserialize: Option<Deserialize>,
    wal_checkpoint: Option<WalCheckpoint>,
    wal_checkpoint_v2: Option<WalCheckpointV2>,
    libsql_wal_frame_count: Option<LibsqlWalFrameCount>,
}

#[allow(unsafe_op_in_unsafe_fn)]
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
            create_collation_v2: lib.symbol(b"sqlite3_create_collation_v2\0"),
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
            threadsafe: lib.symbol(b"sqlite3_threadsafe\0"),
            malloc: lib.symbol(b"sqlite3_malloc\0")?,
            free: lib.symbol(b"sqlite3_free\0")?,
            column_decltype: lib.symbol(b"sqlite3_column_decltype\0")?,
            column_name: lib.symbol(b"sqlite3_column_name\0")?,
            column_table_name: lib.symbol(b"sqlite3_column_table_name\0"),
            table_column_metadata: lib.symbol(b"sqlite3_table_column_metadata\0"),
            db_filename: lib.symbol(b"sqlite3_db_filename\0"),
            get_autocommit: lib.symbol(b"sqlite3_get_autocommit\0"),
            total_changes: lib.symbol(b"sqlite3_total_changes\0"),
            changes: lib.symbol(b"sqlite3_changes\0"),
            changes64: lib.symbol(b"sqlite3_changes64\0"),
            last_insert_rowid: lib.symbol(b"sqlite3_last_insert_rowid\0"),
            interrupt: lib.symbol(b"sqlite3_interrupt\0"),
            db_config: lib.symbol(b"sqlite3_db_config\0"),
            limit: lib.symbol(b"sqlite3_limit\0"),
            stmt_readonly: lib.symbol(b"sqlite3_stmt_readonly\0"),
            stmt_busy: lib.symbol(b"sqlite3_stmt_busy\0"),
            bind_parameter_count: lib.symbol(b"sqlite3_bind_parameter_count\0"),
            bind_parameter_name: lib.symbol(b"sqlite3_bind_parameter_name\0"),
            bind_parameter_index: lib.symbol(b"sqlite3_bind_parameter_index\0"),
            context_db_handle: lib.symbol(b"sqlite3_context_db_handle\0"),
            trace_v2: lib.symbol(b"sqlite3_trace_v2\0"),
            progress_handler: lib.symbol(b"sqlite3_progress_handler\0"),
            busy_timeout: lib.symbol(b"sqlite3_busy_timeout\0"),
            set_authorizer: lib.symbol(b"sqlite3_set_authorizer\0"),
            backup_init: lib.symbol(b"sqlite3_backup_init\0"),
            backup_step: lib.symbol(b"sqlite3_backup_step\0"),
            backup_remaining: lib.symbol(b"sqlite3_backup_remaining\0"),
            backup_pagecount: lib.symbol(b"sqlite3_backup_pagecount\0"),
            backup_finish: lib.symbol(b"sqlite3_backup_finish\0"),
            blob_open: lib.symbol(b"sqlite3_blob_open\0"),
            blob_read: lib.symbol(b"sqlite3_blob_read\0"),
            blob_write: lib.symbol(b"sqlite3_blob_write\0"),
            blob_bytes: lib.symbol(b"sqlite3_blob_bytes\0"),
            blob_close: lib.symbol(b"sqlite3_blob_close\0"),
            serialize: lib.symbol(b"sqlite3_serialize\0"),
            deserialize: lib.symbol(b"sqlite3_deserialize\0"),
            wal_checkpoint: lib.symbol(b"sqlite3_wal_checkpoint\0"),
            wal_checkpoint_v2: lib.symbol(b"sqlite3_wal_checkpoint_v2\0"),
            libsql_wal_frame_count: lib.symbol(b"libsql_wal_frame_count\0"),
        })
    }
}

static USER_DATA_FN: OnceLock<UserData> = OnceLock::new();
static ADAPTER_INSTANCE: OnceLock<Option<&'static LibSqlite3>> = OnceLock::new();

/// Dynamic `libsqlite3` backend adapter loaded via `dlopen`.
pub struct LibSqlite3 {
    fns: LibSqlite3Fns,
    features: FeatureSet,
    api_version: ApiVersion,
    _lib: LibHandle,
}

#[allow(unsafe_op_in_unsafe_fn)]
impl LibSqlite3 {
    /// Load `libsqlite3` and return a process-wide adapter instance.
    ///
    /// Returns `None` if the library or required symbols are unavailable.
    pub fn load() -> Option<&'static LibSqlite3> {
        ADAPTER_INSTANCE
            .get_or_init(|| unsafe { Self::load_inner() })
            .as_ref()
            .copied()
    }

    unsafe fn load_inner() -> Option<&'static LibSqlite3> {
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
        let extended =
            db.and_then(|db| self.fns.extended_errcode.map(|f| unsafe { f(db.as_ptr()) }));
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

    /// ABI helper: `sqlite3_db_filename`.
    ///
    /// The returned bytes are backend-managed and follow SQLite pointer
    /// lifetime rules.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend, and must
    /// remain valid for the duration of the call.
    pub unsafe fn abi_db_filename(&self, db: *mut c_void, name: Option<&str>) -> Option<RawBytes> {
        let func = self.fns.db_filename?;
        let name = match name {
            Some(name) => Some(CString::new(name).ok()?),
            None => None,
        };
        let ptr = func(
            db as *mut sqlite3,
            name.as_ref().map(|s| s.as_ptr()).unwrap_or(null()),
        );
        raw_cstr(ptr).map(raw_bytes_from_cstr)
    }

    /// ABI helper: `sqlite3_get_autocommit`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_get_autocommit(&self, db: *mut c_void) -> i32 {
        self.fns
            .get_autocommit
            .map(|f| f(db as *mut sqlite3))
            .unwrap_or(1)
    }

    /// ABI helper: `sqlite3_total_changes`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_total_changes(&self, db: *mut c_void) -> i32 {
        self.fns
            .total_changes
            .map(|f| f(db as *mut sqlite3))
            .unwrap_or(0)
    }

    /// ABI helper: `sqlite3_changes`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_changes(&self, db: *mut c_void) -> i32 {
        self.fns.changes.map(|f| f(db as *mut sqlite3)).unwrap_or(0)
    }

    /// ABI helper: `sqlite3_changes64`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_changes64(&self, db: *mut c_void) -> i64 {
        self.fns
            .changes64
            .map(|f| f(db as *mut sqlite3))
            .unwrap_or(0)
    }

    /// ABI helper: `sqlite3_last_insert_rowid`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_last_insert_rowid(&self, db: *mut c_void) -> i64 {
        self.fns
            .last_insert_rowid
            .map(|f| f(db as *mut sqlite3))
            .unwrap_or(0)
    }

    /// ABI helper: `sqlite3_interrupt`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_interrupt(&self, db: *mut c_void) {
        if let Some(func) = self.fns.interrupt {
            func(db as *mut sqlite3);
        }
    }

    /// ABI helper: `sqlite3_db_config`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_db_config(&self, db: *mut c_void, op: i32) -> i32 {
        // The shim currently exposes the fixed-arity form (`db`, `op`) only.
        self.fns
            .db_config
            .map(|f| f(db as *mut sqlite3, op))
            .unwrap_or(SQLITE_MISUSE)
    }

    /// ABI helper: `sqlite3_limit`.
    ///
    /// # Safety
    /// `db` must be a valid `sqlite3*` from this loaded backend.
    pub unsafe fn abi_limit(&self, db: *mut c_void, id: i32, new_value: i32) -> i32 {
        self.fns
            .limit
            .map(|f| f(db as *mut sqlite3, id, new_value))
            .unwrap_or(-1)
    }

    /// ABI helper: `sqlite3_stmt_readonly`.
    ///
    /// # Safety
    /// `stmt` must be a valid `sqlite3_stmt*` from this loaded backend.
    pub unsafe fn abi_stmt_readonly(&self, stmt: *mut c_void) -> i32 {
        self.fns
            .stmt_readonly
            .map(|f| f(stmt as *mut sqlite3_stmt))
            .unwrap_or(0)
    }

    /// ABI helper: `sqlite3_stmt_busy`.
    ///
    /// # Safety
    /// `stmt` must be a valid `sqlite3_stmt*` from this loaded backend.
    pub unsafe fn abi_stmt_busy(&self, stmt: *mut c_void) -> i32 {
        self.fns
            .stmt_busy
            .map(|f| f(stmt as *mut sqlite3_stmt))
            .unwrap_or(0)
    }

    /// ABI helper: `sqlite3_bind_parameter_count`.
    ///
    /// # Safety
    /// `stmt` must be a valid `sqlite3_stmt*` from this loaded backend.
    pub unsafe fn abi_bind_parameter_count(&self, stmt: *mut c_void) -> i32 {
        self.fns
            .bind_parameter_count
            .map(|f| f(stmt as *mut sqlite3_stmt))
            .unwrap_or(0)
    }

    /// ABI helper: `sqlite3_bind_parameter_name`.
    ///
    /// The returned bytes are backend-managed and follow SQLite pointer
    /// lifetime rules.
    ///
    /// # Safety
    /// `stmt` must be a valid `sqlite3_stmt*` from this loaded backend.
    pub unsafe fn abi_bind_parameter_name(&self, stmt: *mut c_void, idx: i32) -> Option<RawBytes> {
        let func = self.fns.bind_parameter_name?;
        let ptr = func(stmt as *mut sqlite3_stmt, idx);
        raw_cstr(ptr).map(raw_bytes_from_cstr)
    }

    /// ABI helper: `sqlite3_bind_parameter_index`.
    ///
    /// # Safety
    /// `stmt` must be a valid `sqlite3_stmt*` from this loaded backend.
    pub unsafe fn abi_bind_parameter_index(&self, stmt: *mut c_void, name: &str) -> i32 {
        let func = match self.fns.bind_parameter_index {
            Some(func) => func,
            None => return 0,
        };
        let name = match CString::new(name) {
            Ok(name) => name,
            Err(_) => return 0,
        };
        func(stmt as *mut sqlite3_stmt, name.as_ptr())
    }

    /// ABI helper: `sqlite3_context_db_handle`.
    ///
    /// # Safety
    /// `ctx` must be a valid `sqlite3_context*` provided by SQLite for an
    /// active scalar/aggregate/window callback.
    pub unsafe fn abi_context_db_handle(&self, ctx: *mut c_void) -> Option<NonNull<c_void>> {
        let func = self.fns.context_db_handle?;
        NonNull::new(func(ctx as *mut sqlite3_context))
    }
}

mod core_impl;
mod extensions_impl;

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

#[allow(unsafe_op_in_unsafe_fn)]
unsafe fn raw_cstr<'a>(ptr: *const c_char) -> Option<&'a CStr> {
    if ptr.is_null() {
        None
    } else {
        Some(CStr::from_ptr(ptr))
    }
}

fn raw_bytes_from_cstr(cstr: &CStr) -> RawBytes {
    RawBytes {
        ptr: cstr.as_ptr(),
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

#[cfg(test)]
mod tests {
    use super::LibSqlite3;

    #[test]
    fn load_returns_stable_process_singleton() {
        let first = LibSqlite3::load();
        let second = LibSqlite3::load();
        match (first, second) {
            (Some(first), Some(second)) => {
                assert!(std::ptr::eq(first, second));
            }
            (None, None) => {}
            _ => panic!("LibSqlite3::load result changed across calls"),
        }
    }
}
