use core::ffi::{c_char, c_void};
use core::ptr::NonNull;

use crate::error::Result;

/// SQLite API version.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ApiVersion {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

impl ApiVersion {
    pub const fn new(major: u16, minor: u16, patch: u16) -> Self {
        Self { major, minor, patch }
    }
}

/// Backend feature flags exposed by the provider.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FeatureSet {
    bits: u64,
}

impl FeatureSet {
    pub const PREPARE_V3: FeatureSet = FeatureSet { bits: 1 << 0 };
    pub const CREATE_FUNCTION_V2: FeatureSet = FeatureSet { bits: 1 << 1 };
    pub const VIRTUAL_TABLES: FeatureSet = FeatureSet { bits: 1 << 2 };
    pub const EXTENDED_ERRCODES: FeatureSet = FeatureSet { bits: 1 << 3 };
    pub const WINDOW_FUNCTIONS: FeatureSet = FeatureSet { bits: 1 << 4 };
    pub const KEYING: FeatureSet = FeatureSet { bits: 1 << 5 };

    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub const fn from_bits(bits: u64) -> Self {
        Self { bits }
    }

    pub const fn bits(self) -> u64 {
        self.bits
    }

    pub const fn contains(self, other: FeatureSet) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for FeatureSet {
    type Output = FeatureSet;

    fn bitor(self, rhs: FeatureSet) -> FeatureSet {
        FeatureSet { bits: self.bits | rhs.bits }
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
        FeatureSet { bits: self.bits & rhs.bits }
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
    pub const READ_ONLY: OpenFlags = OpenFlags { bits: 1 << 0 };
    pub const READ_WRITE: OpenFlags = OpenFlags { bits: 1 << 1 };
    pub const CREATE: OpenFlags = OpenFlags { bits: 1 << 2 };
    pub const URI: OpenFlags = OpenFlags { bits: 1 << 3 };
    pub const NO_MUTEX: OpenFlags = OpenFlags { bits: 1 << 4 };
    pub const FULL_MUTEX: OpenFlags = OpenFlags { bits: 1 << 5 };
    pub const SHARED_CACHE: OpenFlags = OpenFlags { bits: 1 << 6 };
    pub const PRIVATE_CACHE: OpenFlags = OpenFlags { bits: 1 << 7 };
    pub const EXRESCODE: OpenFlags = OpenFlags { bits: 1 << 8 };

    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub const fn from_bits(bits: u32) -> Self {
        Self { bits }
    }

    pub const fn bits(self) -> u32 {
        self.bits
    }

    pub const fn contains(self, other: OpenFlags) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for OpenFlags {
    type Output = OpenFlags;

    fn bitor(self, rhs: OpenFlags) -> OpenFlags {
        OpenFlags { bits: self.bits | rhs.bits }
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
        OpenFlags { bits: self.bits & rhs.bits }
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
    pub flags: OpenFlags,
    pub vfs: Option<&'a str>,
}

/// Result of a `step` call.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StepResult {
    Row,
    Done,
}

/// SQLite storage class for a value.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ValueType {
    Null,
    Integer,
    Float,
    Text,
    Blob,
}

impl ValueType {
    pub const fn from_code(code: i32) -> ValueType {
        match code {
            1 => ValueType::Integer,
            2 => ValueType::Float,
            3 => ValueType::Text,
            4 => ValueType::Blob,
            _ => ValueType::Null,
        }
    }

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
/// The pointer/length are only valid until the next text/blob access on the same
/// handle, or until the statement/value is reset or finalized. Callers that need
/// longer-lived data must copy the bytes.
#[derive(Clone, Copy, Debug)]
pub struct RawBytes {
    pub ptr: *const u8,
    pub len: usize,
}

impl RawBytes {
    pub const fn empty() -> Self {
        Self { ptr: core::ptr::null(), len: 0 }
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
    pub const DETERMINISTIC: FunctionFlags = FunctionFlags { bits: 1 << 0 };
    pub const DIRECT_ONLY: FunctionFlags = FunctionFlags { bits: 1 << 1 };
    pub const INNOCUOUS: FunctionFlags = FunctionFlags { bits: 1 << 2 };

    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub const fn from_bits(bits: u32) -> Self {
        Self { bits }
    }

    pub const fn bits(self) -> u32 {
        self.bits
    }

    pub const fn contains(self, other: FunctionFlags) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for FunctionFlags {
    type Output = FunctionFlags;

    fn bitor(self, rhs: FunctionFlags) -> FunctionFlags {
        FunctionFlags { bits: self.bits | rhs.bits }
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
        FunctionFlags { bits: self.bits & rhs.bits }
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
    type Db;
    type Stmt;
    type Value;
    type Context;
    type VTab;
    type VTabCursor;

    fn api_version(&self) -> ApiVersion;
    fn feature_set(&self) -> FeatureSet;
    fn backend_name(&self) -> &'static str;
    fn backend_version(&self) -> Option<ApiVersion>;

    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<Self::Db>>;
    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()>;

    unsafe fn prepare_v2(&self, db: NonNull<Self::Db>, sql: &str) -> Result<NonNull<Self::Stmt>>;
    unsafe fn prepare_v3(
        &self,
        db: NonNull<Self::Db>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<Self::Stmt>>;

    unsafe fn step(&self, stmt: NonNull<Self::Stmt>) -> Result<StepResult>;
    unsafe fn reset(&self, stmt: NonNull<Self::Stmt>) -> Result<()>;
    unsafe fn finalize(&self, stmt: NonNull<Self::Stmt>) -> Result<()>;

    unsafe fn bind_null(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Result<()>;
    unsafe fn bind_int64(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: i64) -> Result<()>;
    unsafe fn bind_double(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: f64) -> Result<()>;
    unsafe fn bind_text(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &str) -> Result<()>;
    unsafe fn bind_blob(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &[u8]) -> Result<()>;

    unsafe fn column_count(&self, stmt: NonNull<Self::Stmt>) -> i32;
    unsafe fn column_type(&self, stmt: NonNull<Self::Stmt>, col: i32) -> ValueType;
    unsafe fn column_int64(&self, stmt: NonNull<Self::Stmt>, col: i32) -> i64;
    unsafe fn column_double(&self, stmt: NonNull<Self::Stmt>, col: i32) -> f64;
    unsafe fn column_text(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes;
    unsafe fn column_blob(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes;

    unsafe fn errcode(&self, db: NonNull<Self::Db>) -> i32;
    unsafe fn errmsg(&self, db: NonNull<Self::Db>) -> *const c_char;
    unsafe fn extended_errcode(&self, db: NonNull<Self::Db>) -> Option<i32>;

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

    unsafe fn aggregate_context(&self, ctx: NonNull<Self::Context>, bytes: usize) -> *mut c_void;

    unsafe fn result_null(&self, ctx: NonNull<Self::Context>);
    unsafe fn result_int64(&self, ctx: NonNull<Self::Context>, v: i64);
    unsafe fn result_double(&self, ctx: NonNull<Self::Context>, v: f64);
    /// Providers must ensure SQLite copies or retains the buffer for `v`.
    unsafe fn result_text(&self, ctx: NonNull<Self::Context>, v: &str);
    /// Providers must ensure SQLite copies or retains the buffer for `v`.
    unsafe fn result_blob(&self, ctx: NonNull<Self::Context>, v: &[u8]);
    unsafe fn result_error(&self, ctx: NonNull<Self::Context>, msg: &str);
    unsafe fn user_data(ctx: NonNull<Self::Context>) -> *mut c_void;

    unsafe fn value_type(&self, v: NonNull<Self::Value>) -> ValueType;
    unsafe fn value_int64(&self, v: NonNull<Self::Value>) -> i64;
    unsafe fn value_double(&self, v: NonNull<Self::Value>) -> f64;
    unsafe fn value_text(&self, v: NonNull<Self::Value>) -> RawBytes;
    unsafe fn value_blob(&self, v: NonNull<Self::Value>) -> RawBytes;

    /// Declare a virtual table schema during xCreate/xConnect.
    unsafe fn declare_vtab(&self, db: NonNull<Self::Db>, schema: &str) -> Result<()>;

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
    unsafe fn key(&self, db: NonNull<Self::Db>, key: &[u8]) -> Result<()>;
    unsafe fn rekey(&self, db: NonNull<Self::Db>, key: &[u8]) -> Result<()>;
}

/// Bytes owned by the backend and freed via `Sqlite3Serialize::free`.
#[derive(Clone, Copy, Debug)]
pub struct OwnedBytes {
    pub ptr: NonNull<u8>,
    pub len: usize,
}

/// Optional backend extension for hooks (trace/progress/busy/authorizer).
///
/// # Safety
/// Implementations must keep callback/context pointers valid per SQLite's hook
/// registration contract and ensure callback ABI compatibility.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Hooks: Sqlite3Api {
    unsafe fn trace_v2(
        &self,
        db: NonNull<Self::Db>,
        mask: u32,
        callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
        context: *mut c_void,
    ) -> Result<()>;
    unsafe fn progress_handler(
        &self,
        db: NonNull<Self::Db>,
        n: i32,
        callback: Option<extern "C" fn() -> i32>,
        context: *mut c_void,
    ) -> Result<()>;
    unsafe fn busy_timeout(&self, db: NonNull<Self::Db>, ms: i32) -> Result<()>;
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
    type Backup;
    unsafe fn backup_init(
        &self,
        dest_db: NonNull<Self::Db>,
        dest_name: &str,
        source_db: NonNull<Self::Db>,
        source_name: &str,
    ) -> Result<NonNull<Self::Backup>>;
    unsafe fn backup_step(&self, backup: NonNull<Self::Backup>, pages: i32) -> Result<()>;
    unsafe fn backup_remaining(&self, backup: NonNull<Self::Backup>) -> i32;
    unsafe fn backup_pagecount(&self, backup: NonNull<Self::Backup>) -> i32;
    unsafe fn backup_finish(&self, backup: NonNull<Self::Backup>) -> Result<()>;
}

/// Optional backend extension for incremental blob I/O.
///
/// # Safety
/// Implementations must validate offsets/buffer sizes and preserve SQLite blob
/// handle validity across read/write operations.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3BlobIo: Sqlite3Api {
    type Blob;
    unsafe fn blob_open(
        &self,
        db: NonNull<Self::Db>,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<NonNull<Self::Blob>>;
    unsafe fn blob_read(&self, blob: NonNull<Self::Blob>, data: &mut [u8], offset: i32)
        -> Result<()>;
    unsafe fn blob_write(&self, blob: NonNull<Self::Blob>, data: &[u8], offset: i32)
        -> Result<()>;
    unsafe fn blob_bytes(&self, blob: NonNull<Self::Blob>) -> i32;
    unsafe fn blob_close(&self, blob: NonNull<Self::Blob>) -> Result<()>;
}

/// Optional backend extension for serialize/deserialize.
///
/// # Safety
/// Implementations must pair `serialize` allocations with `free`, and
/// `deserialize` must not retain references to caller-owned `data`.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Serialize: Sqlite3Api {
    unsafe fn serialize(
        &self,
        db: NonNull<Self::Db>,
        schema: Option<&str>,
        flags: u32,
    ) -> Result<OwnedBytes>;
    unsafe fn deserialize(
        &self,
        db: NonNull<Self::Db>,
        schema: Option<&str>,
        data: &[u8],
        flags: u32,
    ) -> Result<()>;
    unsafe fn free(&self, bytes: OwnedBytes);
}

/// Optional backend extension for WAL helpers.
///
/// # Safety
/// Implementations must only operate on valid database handles and obey SQLite
/// checkpoint and WAL-state semantics.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Wal: Sqlite3Api {
    unsafe fn wal_checkpoint(&self, db: NonNull<Self::Db>, db_name: Option<&str>) -> Result<()>;
    unsafe fn wal_checkpoint_v2(
        &self,
        db: NonNull<Self::Db>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)>;
    unsafe fn wal_frame_count(&self, db: NonNull<Self::Db>) -> Result<Option<u32>>;
}

/// Optional backend extension for metadata helpers.
///
/// # Safety
/// Implementations must ensure returned raw metadata pointers are valid for the
/// documented SQLite lifetime and ownership rules.
#[allow(clippy::missing_safety_doc)]
pub unsafe trait Sqlite3Metadata: Sqlite3Api {
    unsafe fn table_column_metadata(
        &self,
        db: NonNull<Self::Db>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata>;
    unsafe fn column_decltype(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes>;
    unsafe fn column_name(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes>;
    unsafe fn column_table_name(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes>;
}

/// Column metadata returned by `Sqlite3Metadata`.
#[derive(Clone, Copy, Debug)]
pub struct ColumnMetadata {
    pub data_type: Option<RawBytes>,
    pub coll_seq: Option<RawBytes>,
    pub not_null: bool,
    pub primary_key: bool,
    pub autoinc: bool,
}

/// Typed wrapper for `sqlite3_module`.
#[repr(C)]
pub struct sqlite3_module<P: Sqlite3Api> {
    pub i_version: i32,
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
    pub x_best_index: Option<extern "C" fn(*mut P::VTab, *mut c_void) -> i32>,
    pub x_disconnect: Option<extern "C" fn(*mut P::VTab) -> i32>,
    pub x_destroy: Option<extern "C" fn(*mut P::VTab) -> i32>,
    pub x_open: Option<extern "C" fn(*mut P::VTab, *mut *mut P::VTabCursor) -> i32>,
    pub x_close: Option<extern "C" fn(*mut P::VTabCursor) -> i32>,
    pub x_filter: Option<
        extern "C" fn(*mut P::VTabCursor, i32, *const u8, i32, *mut *mut P::Value) -> i32,
    >,
    pub x_next: Option<extern "C" fn(*mut P::VTabCursor) -> i32>,
    pub x_eof: Option<extern "C" fn(*mut P::VTabCursor) -> i32>,
    pub x_column: Option<extern "C" fn(*mut P::VTabCursor, *mut P::Context, i32) -> i32>,
    pub x_rowid: Option<extern "C" fn(*mut P::VTabCursor, *mut i64) -> i32>,
}
