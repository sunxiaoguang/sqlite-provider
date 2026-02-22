use core::ffi::{c_char, c_void};
use core::ptr::NonNull;

use crate::error::{Error, ErrorCode, Result};

/// SQLite API version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ApiVersion {
    /// Major version component (X in X.Y.Z).
    pub major: u16,
    /// Minor version component (Y in X.Y.Z).
    pub minor: u16,
    /// Patch version component (Z in X.Y.Z).
    pub patch: u16,
}

impl ApiVersion {
    /// Build an API version from major/minor/patch components.
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

/// Backend feature flags exposed by the provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureSet {
    bits: u64,
}

impl FeatureSet {
    /// `prepare_v3` is available.
    pub const PREPARE_V3: FeatureSet = FeatureSet { bits: 1 << 0 };
    /// `create_function_v2` is available.
    pub const CREATE_FUNCTION_V2: FeatureSet = FeatureSet { bits: 1 << 1 };
    /// Virtual table APIs are available.
    pub const VIRTUAL_TABLES: FeatureSet = FeatureSet { bits: 1 << 2 };
    /// Extended error code APIs are available.
    pub const EXTENDED_ERRCODES: FeatureSet = FeatureSet { bits: 1 << 3 };
    /// Window-function APIs are available.
    pub const WINDOW_FUNCTIONS: FeatureSet = FeatureSet { bits: 1 << 4 };
    /// Backend keying/encryption APIs are available.
    pub const KEYING: FeatureSet = FeatureSet { bits: 1 << 5 };

    /// Build an empty flag set.
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// Build a flag set from raw bits.
    pub const fn from_bits(bits: u64) -> Self {
        Self { bits }
    }

    /// Return the raw bit representation.
    pub const fn bits(self) -> u64 {
        self.bits
    }

    /// Return whether `other` is fully contained in this set.
    pub const fn contains(self, other: FeatureSet) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for FeatureSet {
    type Output = FeatureSet;

    fn bitor(self, rhs: FeatureSet) -> FeatureSet {
        FeatureSet {
            bits: self.bits | rhs.bits,
        }
    }
}

impl core::ops::BitOrAssign for FeatureSet {
    fn bitor_assign(&mut self, rhs: FeatureSet) {
        self.bits |= rhs.bits;
    }
}

impl core::ops::BitAnd for FeatureSet {
    type Output = FeatureSet;

    fn bitand(self, rhs: FeatureSet) -> FeatureSet {
        FeatureSet {
            bits: self.bits & rhs.bits,
        }
    }
}

impl core::ops::BitAndAssign for FeatureSet {
    fn bitand_assign(&mut self, rhs: FeatureSet) {
        self.bits &= rhs.bits;
    }
}

impl core::ops::Not for FeatureSet {
    type Output = FeatureSet;

    fn not(self) -> FeatureSet {
        FeatureSet { bits: !self.bits }
    }
}

/// Flags for opening a database connection.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenFlags {
    bits: u32,
}

impl OpenFlags {
    /// Open database in read-only mode.
    pub const READ_ONLY: OpenFlags = OpenFlags { bits: 1 << 0 };
    /// Open database in read-write mode.
    pub const READ_WRITE: OpenFlags = OpenFlags { bits: 1 << 1 };
    /// Create database file when missing.
    pub const CREATE: OpenFlags = OpenFlags { bits: 1 << 2 };
    /// Treat filename as URI when supported.
    pub const URI: OpenFlags = OpenFlags { bits: 1 << 3 };
    /// Use connection-private mutex mode.
    pub const NO_MUTEX: OpenFlags = OpenFlags { bits: 1 << 4 };
    /// Use fully serialized mutex mode.
    pub const FULL_MUTEX: OpenFlags = OpenFlags { bits: 1 << 5 };
    /// Enable shared page cache.
    pub const SHARED_CACHE: OpenFlags = OpenFlags { bits: 1 << 6 };
    /// Force private page cache.
    pub const PRIVATE_CACHE: OpenFlags = OpenFlags { bits: 1 << 7 };
    /// Request extended result codes.
    pub const EXRESCODE: OpenFlags = OpenFlags { bits: 1 << 8 };

    /// Build an empty flag set.
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// Build a flag set from raw bits.
    pub const fn from_bits(bits: u32) -> Self {
        Self { bits }
    }

    /// Return the raw bit representation.
    pub const fn bits(self) -> u32 {
        self.bits
    }

    /// Return whether `other` is fully contained in this set.
    pub const fn contains(self, other: OpenFlags) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for OpenFlags {
    type Output = OpenFlags;

    fn bitor(self, rhs: OpenFlags) -> OpenFlags {
        OpenFlags {
            bits: self.bits | rhs.bits,
        }
    }
}

impl core::ops::BitOrAssign for OpenFlags {
    fn bitor_assign(&mut self, rhs: OpenFlags) {
        self.bits |= rhs.bits;
    }
}

impl core::ops::BitAnd for OpenFlags {
    type Output = OpenFlags;

    fn bitand(self, rhs: OpenFlags) -> OpenFlags {
        OpenFlags {
            bits: self.bits & rhs.bits,
        }
    }
}

impl core::ops::BitAndAssign for OpenFlags {
    fn bitand_assign(&mut self, rhs: OpenFlags) {
        self.bits &= rhs.bits;
    }
}

impl core::ops::Not for OpenFlags {
    type Output = OpenFlags;

    fn not(self) -> OpenFlags {
        OpenFlags { bits: !self.bits }
    }
}

/// Options passed to `Sqlite3Api::open`.
pub struct OpenOptions<'a> {
    /// Backend-open flags translated from caller intent.
    pub flags: OpenFlags,
    /// Optional VFS name passed through to backend open.
    pub vfs: Option<&'a str>,
}

/// Result of a `step` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepResult {
    /// The statement produced a row.
    Row,
    /// The statement has finished.
    Done,
}

/// SQLite storage class for a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    /// SQL NULL.
    Null,
    /// 64-bit integer.
    Integer,
    /// 64-bit floating value.
    Float,
    /// UTF-8 text.
    Text,
    /// Binary blob.
    Blob,
}

impl ValueType {
    /// Decode SQLite's integer storage-class code.
    pub const fn from_code(code: i32) -> ValueType {
        match code {
            1 => ValueType::Integer,
            2 => ValueType::Float,
            3 => ValueType::Text,
            4 => ValueType::Blob,
            _ => ValueType::Null,
        }
    }

    /// Encode this storage class into SQLite's integer code.
    pub const fn to_code(self) -> i32 {
        match self {
            ValueType::Null => 5,
            ValueType::Integer => 1,
            ValueType::Float => 2,
            ValueType::Text => 3,
            ValueType::Blob => 4,
        }
    }
}

/// Raw view into SQLite-managed bytes.
///
/// The pointer/length are tied to the lifetime of the current SQLite
/// row/value snapshot and are invalidated by lifecycle transitions such as
/// `step`, `reset`, or `finalize` on the owning statement/value context.
/// Implementations must keep returned bytes stable across repeated text/blob
/// reads within the same snapshot. Callers that need longer-lived data must
/// copy the bytes.
#[derive(Clone, Copy, Debug)]
pub struct RawBytes {
    /// Pointer to backend-owned bytes.
    pub ptr: *const u8,
    /// Byte length of `ptr`.
    pub len: usize,
}

impl RawBytes {
    /// Empty byte view.
    pub const fn empty() -> Self {
        Self {
            ptr: core::ptr::null(),
            len: 0,
        }
    }

    /// # Safety
    /// Caller must ensure the pointer/length remain valid for the returned slice.
    pub unsafe fn as_slice<'a>(self) -> &'a [u8] {
        if self.ptr.is_null() {
            return &[];
        }
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }

    /// # Safety
    /// Caller must ensure the bytes are valid UTF-8 and remain valid for `'a`.
    pub unsafe fn as_str<'a>(self) -> Option<&'a str> {
        core::str::from_utf8(unsafe { self.as_slice() }).ok()
    }

    /// # Safety
    /// Caller must ensure the bytes are valid UTF-8 and remain valid for `'a`.
    pub unsafe fn as_str_unchecked<'a>(self) -> &'a str {
        unsafe { core::str::from_utf8_unchecked(self.as_slice()) }
    }
}

/// Function flags passed to `create_function_v2` / `create_window_function`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FunctionFlags {
    bits: u32,
}

impl FunctionFlags {
    /// Mark UDF as deterministic.
    pub const DETERMINISTIC: FunctionFlags = FunctionFlags { bits: 1 << 0 };
    /// Restrict UDF to direct SQL only.
    pub const DIRECT_ONLY: FunctionFlags = FunctionFlags { bits: 1 << 1 };
    /// Mark UDF as innocuous.
    pub const INNOCUOUS: FunctionFlags = FunctionFlags { bits: 1 << 2 };

    /// Build an empty flag set.
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// Build a flag set from raw bits.
    pub const fn from_bits(bits: u32) -> Self {
        Self { bits }
    }

    /// Return the raw bit representation.
    pub const fn bits(self) -> u32 {
        self.bits
    }

    /// Return whether `other` is fully contained in this set.
    pub const fn contains(self, other: FunctionFlags) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for FunctionFlags {
    type Output = FunctionFlags;

    fn bitor(self, rhs: FunctionFlags) -> FunctionFlags {
        FunctionFlags {
            bits: self.bits | rhs.bits,
        }
    }
}

impl core::ops::BitOrAssign for FunctionFlags {
    fn bitor_assign(&mut self, rhs: FunctionFlags) {
        self.bits |= rhs.bits;
    }
}

impl core::ops::BitAnd for FunctionFlags {
    type Output = FunctionFlags;

    fn bitand(self, rhs: FunctionFlags) -> FunctionFlags {
        FunctionFlags {
            bits: self.bits & rhs.bits,
        }
    }
}

impl core::ops::BitAndAssign for FunctionFlags {
    fn bitand_assign(&mut self, rhs: FunctionFlags) {
        self.bits &= rhs.bits;
    }
}

impl core::ops::Not for FunctionFlags {
    type Output = FunctionFlags;

    fn not(self) -> FunctionFlags {
        FunctionFlags { bits: !self.bits }
    }
}

/// Provider SPI over a SQLite C API backend.
///
/// # Safety
/// Implementations must uphold the SQLite C ABI contracts.
#[allow(clippy::missing_safety_doc, clippy::too_many_arguments)]
pub unsafe trait Sqlite3Api: Send + Sync + 'static {
    /// Backend's opaque `sqlite3*` type.
    type Db;
    /// Backend's opaque `sqlite3_stmt*` type.
    type Stmt;
    /// Backend's opaque `sqlite3_value*` type.
    type Value;
    /// Backend's opaque `sqlite3_context*` type.
    type Context;
    /// Backend's opaque `sqlite3_vtab*` type.
    type VTab;
    /// Backend's opaque `sqlite3_vtab_cursor*` type.
    type VTabCursor;

    /// Declared SQLite API version supported by this provider implementation.
    fn api_version(&self) -> ApiVersion;
    /// Compile/runtime capability flags available through this provider.
    fn feature_set(&self) -> FeatureSet;
    /// Stable backend identifier (for diagnostics and capability routing).
    fn backend_name(&self) -> &'static str;
    /// Optional backend runtime version (if queryable).
    fn backend_version(&self) -> Option<ApiVersion>;
    /// Return SQLite allocator-compatible memory for cross-FFI ownership.
    ///
    /// Returned pointers must be releasable via `free` below and must match
    /// SQLite allocator expectations for this backend.
    unsafe fn malloc(&self, size: usize) -> *mut c_void;
    /// Free memory allocated through `malloc`.
    unsafe fn free(&self, ptr: *mut c_void);
    /// Thread-safety capability in `sqlite3_threadsafe()` form.
    fn threadsafe(&self) -> i32 {
        0
    }

    /// Open a database connection using backend-specific `open_v2` semantics.
    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<Self::Db>>;
    /// Close a database connection handle.
    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()>;

    /// Prepare SQL using legacy `prepare_v2` behavior.
    unsafe fn prepare_v2(&self, db: NonNull<Self::Db>, sql: &str) -> Result<NonNull<Self::Stmt>>;
    /// Prepare SQL with `prepare_v3` flags.
    unsafe fn prepare_v3(
        &self,
        db: NonNull<Self::Db>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<Self::Stmt>>;

    /// Execute one step of the virtual machine.
    unsafe fn step(&self, stmt: NonNull<Self::Stmt>) -> Result<StepResult>;
    /// Reset a prepared statement to run again.
    unsafe fn reset(&self, stmt: NonNull<Self::Stmt>) -> Result<()>;
    /// Finalize a prepared statement and release backend resources.
    unsafe fn finalize(&self, stmt: NonNull<Self::Stmt>) -> Result<()>;

    /// Bind SQL NULL at parameter index `idx` (1-based).
    unsafe fn bind_null(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Result<()>;
    /// Bind 64-bit integer at parameter index `idx` (1-based).
    unsafe fn bind_int64(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: i64) -> Result<()>;
    /// Bind floating value at parameter index `idx` (1-based).
    unsafe fn bind_double(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: f64) -> Result<()>;
    /// Bind UTF-8 text at parameter index `idx` (1-based).
    ///
    /// Implementations must copy `v` or retain it safely per SQLite lifetime rules.
    unsafe fn bind_text(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &str) -> Result<()>;
    /// Bind text bytes at parameter index `idx` (1-based).
    ///
    /// This hook lets ABI callers preserve SQLite's byte-oriented text semantics
    /// even when bytes are not valid UTF-8. The default implementation keeps
    /// compatibility with existing providers by requiring valid UTF-8.
    unsafe fn bind_text_bytes(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &[u8]) -> Result<()> {
        match core::str::from_utf8(v) {
            Ok(text) => unsafe { self.bind_text(stmt, idx, text) },
            Err(_) => Err(Error::with_message(ErrorCode::Misuse, "invalid utf-8 text")),
        }
    }
    /// Bind bytes at parameter index `idx` (1-based).
    ///
    /// Implementations must copy `v` or retain it safely per SQLite lifetime rules.
    unsafe fn bind_blob(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &[u8]) -> Result<()>;

    /// Number of columns in the current result row.
    unsafe fn column_count(&self, stmt: NonNull<Self::Stmt>) -> i32;
    /// SQLite storage class for `col` in the current row.
    unsafe fn column_type(&self, stmt: NonNull<Self::Stmt>, col: i32) -> ValueType;
    /// Integer value for `col` in the current row.
    unsafe fn column_int64(&self, stmt: NonNull<Self::Stmt>, col: i32) -> i64;
    /// Floating value for `col` in the current row.
    unsafe fn column_double(&self, stmt: NonNull<Self::Stmt>, col: i32) -> f64;
    /// Raw text bytes for `col` in the current row snapshot.
    unsafe fn column_text(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes;
    /// Raw blob bytes for `col` in the current row snapshot.
    unsafe fn column_blob(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes;

    /// Primary SQLite result code for a connection.
    unsafe fn errcode(&self, db: NonNull<Self::Db>) -> i32;
    /// Backend-provided UTF-8 error message pointer for a connection.
    unsafe fn errmsg(&self, db: NonNull<Self::Db>) -> *const c_char;
    /// Extended SQLite result code, if supported.
    unsafe fn extended_errcode(&self, db: NonNull<Self::Db>) -> Option<i32>;

    /// Register scalar/aggregate callbacks.
    ///
    /// # Ownership contract
    /// Ownership of `user_data` and `drop_user_data` is transferred to the
    /// provider at call entry.
    ///
    /// If registration succeeds, the provider must eventually invoke
    /// `drop_user_data` exactly once when the function definition is replaced,
    /// removed, or the owning connection is closed.
    ///
    /// If registration fails and this method returns `Err`, the provider must
    /// invoke `drop_user_data` exactly once before returning.
    ///
    /// Callers must not free or otherwise use `user_data` after calling this
    /// method.
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
    ) -> Result<()>;

    /// Register window-function callbacks.
    ///
    /// # Ownership contract
    /// Ownership of `user_data` and `drop_user_data` is transferred to the
    /// provider at call entry.
    ///
    /// If registration succeeds, the provider must eventually invoke
    /// `drop_user_data` exactly once when the window-function definition is
    /// replaced, removed, or the owning connection is closed.
    ///
    /// If registration fails and this method returns `Err`, the provider must
    /// invoke `drop_user_data` exactly once before returning.
    ///
    /// Callers must not free or otherwise use `user_data` after calling this
    /// method.
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
    ) -> Result<()>;

    /// Register or replace a collation sequence.
    ///
    /// Providers should map this to `sqlite3_create_collation_v2` semantics.
    /// The callback receives `(context, lhs_len, lhs_ptr, rhs_len, rhs_ptr)`
    /// and must return negative/zero/positive ordering as in SQLite.
    unsafe fn create_collation_v2(
        &self,
        _db: NonNull<Self::Db>,
        _name: &str,
        _enc: i32,
        _context: *mut c_void,
        _cmp: Option<extern "C" fn(*mut c_void, i32, *const c_void, i32, *const c_void) -> i32>,
        _destroy: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        Err(Error::feature_unavailable(
            "create_collation_v2 unsupported",
        ))
    }

    /// Fetch or allocate aggregate state memory for `ctx`.
    unsafe fn aggregate_context(&self, ctx: NonNull<Self::Context>, bytes: usize) -> *mut c_void;

    /// Set current function result to NULL.
    unsafe fn result_null(&self, ctx: NonNull<Self::Context>);
    /// Set current function result to integer.
    unsafe fn result_int64(&self, ctx: NonNull<Self::Context>, v: i64);
    /// Set current function result to floating value.
    unsafe fn result_double(&self, ctx: NonNull<Self::Context>, v: f64);
    /// Providers must ensure SQLite copies or retains the buffer for `v`.
    unsafe fn result_text(&self, ctx: NonNull<Self::Context>, v: &str);
    /// Providers must ensure SQLite copies or retains the buffer for `v`.
    ///
    /// The default implementation preserves the legacy UTF-8 contract by routing
    /// invalid bytes to `result_error`.
    unsafe fn result_text_bytes(&self, ctx: NonNull<Self::Context>, v: &[u8]) {
        match core::str::from_utf8(v) {
            Ok(text) => unsafe { self.result_text(ctx, text) },
            Err(_) => unsafe { self.result_error(ctx, "invalid utf-8") },
        }
    }
    /// Providers must ensure SQLite copies or retains the buffer for `v`.
    unsafe fn result_blob(&self, ctx: NonNull<Self::Context>, v: &[u8]);
    /// Set current function result to an error message.
    unsafe fn result_error(&self, ctx: NonNull<Self::Context>, msg: &str);
    /// Return function `user_data` for `ctx`.
    unsafe fn user_data(ctx: NonNull<Self::Context>) -> *mut c_void;

    /// SQLite storage class of a UDF argument value.
    unsafe fn value_type(&self, v: NonNull<Self::Value>) -> ValueType;
    /// Integer view of a UDF argument value.
    unsafe fn value_int64(&self, v: NonNull<Self::Value>) -> i64;
    /// Floating view of a UDF argument value.
    unsafe fn value_double(&self, v: NonNull<Self::Value>) -> f64;
    /// Raw text bytes view of a UDF argument value.
    unsafe fn value_text(&self, v: NonNull<Self::Value>) -> RawBytes;
    /// Raw blob bytes view of a UDF argument value.
    unsafe fn value_blob(&self, v: NonNull<Self::Value>) -> RawBytes;

    /// Declare a virtual table schema during xCreate/xConnect.
    unsafe fn declare_vtab(&self, db: NonNull<Self::Db>, schema: &str) -> Result<()>;

    /// Register a virtual table module.
    unsafe fn create_module_v2(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        module: &'static sqlite3_module<Self>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()>
    where
        Self: Sized;
}

/// Optional backend extension for SQLCipher-style keying.
///
/// # Safety
/// Implementations must uphold the same pointer and lifetime guarantees as
/// `Sqlite3Api` for key material and database handles.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Keying: Sqlite3Api {
    /// Apply an encryption key to an open database handle.
    unsafe fn key(&self, db: NonNull<Self::Db>, key: &[u8]) -> Result<()>;
    /// Change the encryption key for an already-keyed database.
    unsafe fn rekey(&self, db: NonNull<Self::Db>, key: &[u8]) -> Result<()>;
}

/// Bytes owned by the backend and freed via `Sqlite3Serialize::free`.
#[derive(Clone, Copy, Debug)]
pub struct OwnedBytes {
    /// Pointer returned by backend serialize allocator.
    pub ptr: NonNull<u8>,
    /// Length of serialized byte payload.
    pub len: usize,
}

/// Optional backend extension for hooks (trace/progress/busy/authorizer).
///
/// # Safety
/// Implementations must keep callback/context pointers valid per SQLite's hook
/// registration contract and ensure callback ABI compatibility.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Hooks: Sqlite3Api {
    /// Register or clear a `sqlite3_trace_v2` callback.
    unsafe fn trace_v2(
        &self,
        db: NonNull<Self::Db>,
        mask: u32,
        callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
        context: *mut c_void,
    ) -> Result<()>;
    /// Register or clear a progress handler callback.
    unsafe fn progress_handler(
        &self,
        db: NonNull<Self::Db>,
        n: i32,
        callback: Option<extern "C" fn(*mut c_void) -> i32>,
        context: *mut c_void,
    ) -> Result<()>;
    /// Set busy timeout in milliseconds.
    unsafe fn busy_timeout(&self, db: NonNull<Self::Db>, ms: i32) -> Result<()>;
    /// Register or clear an authorizer callback.
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
    ) -> Result<()>;
}

/// Optional backend extension for online backup.
///
/// # Safety
/// Implementations must return backup handles tied to the provided database
/// handles and honor SQLite's backup lifecycle requirements.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Backup: Sqlite3Api {
    /// Backend-specific backup-handle type.
    type Backup;
    /// Start a backup from `source_db/source_name` into `dest_db/dest_name`.
    unsafe fn backup_init(
        &self,
        dest_db: NonNull<Self::Db>,
        dest_name: &str,
        source_db: NonNull<Self::Db>,
        source_name: &str,
    ) -> Result<NonNull<Self::Backup>>;
    /// Copy up to `pages` pages from source to destination.
    unsafe fn backup_step(&self, backup: NonNull<Self::Backup>, pages: i32) -> Result<()>;
    /// Return the number of pages still remaining.
    unsafe fn backup_remaining(&self, backup: NonNull<Self::Backup>) -> i32;
    /// Return the total page count of the source database.
    unsafe fn backup_pagecount(&self, backup: NonNull<Self::Backup>) -> i32;
    /// Finish and release a backup handle.
    unsafe fn backup_finish(&self, backup: NonNull<Self::Backup>) -> Result<()>;
}

/// Optional backend extension for incremental blob I/O.
///
/// # Safety
/// Implementations must validate offsets/buffer sizes and preserve SQLite blob
/// handle validity across read/write operations.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3BlobIo: Sqlite3Api {
    /// Backend-specific incremental-blob handle type.
    type Blob;
    /// Open an incremental blob handle for a row/column cell.
    unsafe fn blob_open(
        &self,
        db: NonNull<Self::Db>,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<NonNull<Self::Blob>>;
    /// Read bytes from `blob` into `data` starting at `offset`.
    unsafe fn blob_read(
        &self,
        blob: NonNull<Self::Blob>,
        data: &mut [u8],
        offset: i32,
    ) -> Result<()>;
    /// Write `data` into `blob` starting at `offset`.
    unsafe fn blob_write(&self, blob: NonNull<Self::Blob>, data: &[u8], offset: i32) -> Result<()>;
    /// Return blob size in bytes.
    unsafe fn blob_bytes(&self, blob: NonNull<Self::Blob>) -> i32;
    /// Close and release a blob handle.
    unsafe fn blob_close(&self, blob: NonNull<Self::Blob>) -> Result<()>;
}

/// Optional backend extension for serialize/deserialize.
///
/// # Safety
/// Implementations must pair `serialize` allocations with `free`, and
/// `deserialize` must not retain references to caller-owned `data`.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Serialize: Sqlite3Api {
    /// Serialize a schema database into backend-owned bytes.
    unsafe fn serialize(
        &self,
        db: NonNull<Self::Db>,
        schema: Option<&str>,
        flags: u32,
    ) -> Result<OwnedBytes>;
    /// Replace schema contents from caller-owned bytes.
    unsafe fn deserialize(
        &self,
        db: NonNull<Self::Db>,
        schema: Option<&str>,
        data: &[u8],
        flags: u32,
    ) -> Result<()>;
    /// Free serialized bytes previously returned by `serialize`.
    unsafe fn free(&self, bytes: OwnedBytes);
}

/// Optional backend extension for WAL helpers.
///
/// # Safety
/// Implementations must only operate on valid database handles and obey SQLite
/// checkpoint and WAL-state semantics.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Wal: Sqlite3Api {
    /// Run a passive checkpoint for `db_name` (or main when `None`).
    unsafe fn wal_checkpoint(&self, db: NonNull<Self::Db>, db_name: Option<&str>) -> Result<()>;
    /// Run checkpoint mode `mode`, returning `(log_frames, checkpointed_frames)`.
    unsafe fn wal_checkpoint_v2(
        &self,
        db: NonNull<Self::Db>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)>;
    /// Return backend-specific WAL frame count when supported.
    unsafe fn wal_frame_count(&self, db: NonNull<Self::Db>) -> Result<Option<u32>>;
}

/// Optional backend extension for metadata helpers.
///
/// # Safety
/// Implementations must ensure returned raw metadata pointers are valid for the
/// documented SQLite lifetime and ownership rules.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Metadata: Sqlite3Api {
    /// Query table/column metadata for the specified schema object.
    unsafe fn table_column_metadata(
        &self,
        db: NonNull<Self::Db>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata>;
    /// Return declared type text for output column `col`.
    unsafe fn column_decltype(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes>;
    /// Return output column name for `col`.
    unsafe fn column_name(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes>;
    /// Return source table name for output column `col`, when available.
    unsafe fn column_table_name(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes>;
}

/// Column metadata returned by `Sqlite3Metadata`.
#[derive(Clone, Copy, Debug)]
pub struct ColumnMetadata {
    /// Declared column type as raw backend bytes, if available.
    pub data_type: Option<RawBytes>,
    /// Declared collation sequence as raw backend bytes, if available.
    pub coll_seq: Option<RawBytes>,
    /// Whether the column has a `NOT NULL` constraint.
    pub not_null: bool,
    /// Whether the column participates in the primary key.
    pub primary_key: bool,
    /// Whether the column is auto-incrementing.
    pub autoinc: bool,
}

/// Typed wrapper for `sqlite3_module`.
#[repr(C)]
pub struct sqlite3_module<P: Sqlite3Api> {
    /// Module ABI version.
    pub i_version: i32,
    /// `xCreate` callback for `CREATE VIRTUAL TABLE`.
    pub x_create: Option<
        extern "C" fn(
            *mut P::Db,
            *mut c_void,
            i32,
            *const *const u8,
            *mut *mut P::VTab,
            *mut *mut u8,
        ) -> i32,
    >,
    /// `xConnect` callback for `CONNECT`ing to an existing virtual table.
    pub x_connect: Option<
        extern "C" fn(
            *mut P::Db,
            *mut c_void,
            i32,
            *const *const u8,
            *mut *mut P::VTab,
            *mut *mut u8,
        ) -> i32,
    >,
    /// `xBestIndex` query-planning callback.
    pub x_best_index: Option<extern "C" fn(*mut P::VTab, *mut c_void) -> i32>,
    /// `xDisconnect` callback for disconnecting a table instance.
    pub x_disconnect: Option<extern "C" fn(*mut P::VTab) -> i32>,
    /// `xDestroy` callback for dropping a virtual table.
    pub x_destroy: Option<extern "C" fn(*mut P::VTab) -> i32>,
    /// `xOpen` callback for creating a cursor.
    pub x_open: Option<extern "C" fn(*mut P::VTab, *mut *mut P::VTabCursor) -> i32>,
    /// `xClose` callback for closing a cursor.
    pub x_close: Option<extern "C" fn(*mut P::VTabCursor) -> i32>,
    /// `xFilter` callback for starting a cursor scan.
    pub x_filter:
        Option<extern "C" fn(*mut P::VTabCursor, i32, *const u8, i32, *mut *mut P::Value) -> i32>,
    /// `xNext` callback for advancing a cursor.
    pub x_next: Option<extern "C" fn(*mut P::VTabCursor) -> i32>,
    /// `xEof` callback for cursor end-of-scan checks.
    pub x_eof: Option<extern "C" fn(*mut P::VTabCursor) -> i32>,
    /// `xColumn` callback for reading the current row value.
    pub x_column: Option<extern "C" fn(*mut P::VTabCursor, *mut P::Context, i32) -> i32>,
    /// `xRowid` callback for reading the current rowid.
    pub x_rowid: Option<extern "C" fn(*mut P::VTabCursor, *mut i64) -> i32>,
}
