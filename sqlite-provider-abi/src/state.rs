use super::*;
use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, OnceLock};

/// Provider registration state for the ABI shim.
#[derive(Clone, Copy)]
pub struct ProviderState {
    pub(crate) core: &'static dyn AbiCore,
    pub(crate) hooks: Option<&'static dyn AbiHooks>,
    pub(crate) backup: Option<&'static dyn AbiBackup>,
    pub(crate) blob: Option<&'static dyn AbiBlobIo>,
    pub(crate) serialize: Option<&'static dyn AbiSerialize>,
    pub(crate) wal: Option<&'static dyn AbiWal>,
    pub(crate) metadata: Option<&'static dyn AbiMetadata>,
    pub(crate) extras: Option<&'static dyn AbiExtras>,
    pub(crate) libversion: &'static CStr,
    pub(crate) libversion_number: i32,
}

impl ProviderState {
    /// Creates a provider registration bundle from the required core ABI.
    ///
    /// The backend version is captured immediately and used to serve
    /// `sqlite3_libversion*` without extra backend calls.
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

    /// Enables optional hook callbacks (`trace`, progress, authorizer, busy timeout).
    pub fn with_hooks(mut self, hooks: &'static dyn AbiHooks) -> Self {
        self.hooks = Some(hooks);
        self
    }

    /// Enables optional backup APIs.
    pub fn with_backup(mut self, backup: &'static dyn AbiBackup) -> Self {
        self.backup = Some(backup);
        self
    }

    /// Enables optional incremental blob I/O APIs.
    pub fn with_blob(mut self, blob: &'static dyn AbiBlobIo) -> Self {
        self.blob = Some(blob);
        self
    }

    /// Enables optional serialize/deserialize APIs.
    pub fn with_serialize(mut self, serialize: &'static dyn AbiSerialize) -> Self {
        self.serialize = Some(serialize);
        self
    }

    /// Enables optional WAL checkpoint APIs.
    pub fn with_wal(mut self, wal: &'static dyn AbiWal) -> Self {
        self.wal = Some(wal);
        self
    }

    /// Enables optional table/column metadata APIs.
    pub fn with_metadata(mut self, metadata: &'static dyn AbiMetadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// Enables optional non-core helper APIs used by compatibility entrypoints.
    pub fn with_extras(mut self, extras: &'static dyn AbiExtras) -> Self {
        self.extras = Some(extras);
        self
    }
}

/// Provider registration errors.
#[derive(Debug)]
pub enum RegisterError {
    /// A provider was already registered for this process.
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

pub(crate) fn provider() -> Option<&'static ProviderState> {
    PROVIDER.get()
}

pub(crate) fn ensure_default_provider() -> i32 {
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
            let state = ProviderState::new(api)
                .with_hooks(api)
                .with_backup(api)
                .with_blob(api)
                .with_serialize(api)
                .with_wal(api)
                .with_metadata(api)
                .with_extras(api);
            match register_provider(state) {
                Ok(()) | Err(RegisterError::AlreadySet) => SQLITE_OK,
            }
        });
        if rc == SQLITE_OK || provider().is_some() {
            return SQLITE_OK;
        }
        rc
    }

    #[cfg(not(feature = "default-backend"))]
    {
        SQLITE_MISUSE
    }
}

#[repr(C)]
/// Opaque C ABI wrapper for a backend database handle.
///
/// This matches SQLite's `sqlite3` handle semantics on the exported ABI.
pub struct sqlite3 {
    pub(crate) db: NonNull<c_void>,
    pub(crate) stmts: *mut sqlite3_stmt,
    pub(crate) close_pending: bool,
    pub(crate) filename: Option<CString>,
    pub(crate) exec_callback_aborted: bool,
}

#[repr(C)]
/// Opaque C ABI wrapper for a backend prepared statement handle.
///
/// This matches SQLite's `sqlite3_stmt` handle semantics on the exported ABI.
pub struct sqlite3_stmt {
    pub(crate) stmt: NonNull<c_void>,
    pub(crate) db: *mut sqlite3,
    pub(crate) next: *mut sqlite3_stmt,
    pub(crate) last_step: StepResult,
    pub(crate) sql: Option<CString>,
}

#[repr(C)]
pub(crate) struct TableResultAllocHeader {
    pub(crate) count: usize,
    pub(crate) data_ptr: *mut c_void,
    pub(crate) data_len: usize,
    pub(crate) data_cap: usize,
    pub(crate) data_owner: u8,
}

pub(crate) const TABLE_DATA_OWNER_NONE: u8 = 0;
pub(crate) const TABLE_DATA_OWNER_RUST_VEC: u8 = 1;

pub(crate) unsafe fn free_table_data(
    owner: u8,
    data_ptr: *mut c_void,
    data_len: usize,
    data_cap: usize,
) {
    if data_ptr.is_null() {
        return;
    }
    if owner == TABLE_DATA_OWNER_RUST_VEC {
        drop(Vec::from_raw_parts(data_ptr as *mut u8, data_len, data_cap));
    }
}

pub(crate) unsafe fn add_stmt(db: &mut sqlite3, stmt: *mut sqlite3_stmt) {
    (*stmt).next = db.stmts;
    db.stmts = stmt;
}

pub(crate) unsafe fn remove_stmt(db: &mut sqlite3, stmt: *mut sqlite3_stmt) {
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

pub(crate) struct SerializedEntry {
    pub(crate) provider: &'static dyn AbiSerialize,
    pub(crate) len: usize,
}

#[derive(Clone, Copy)]
pub(crate) enum MallocAllocOwner {
    Provider(&'static dyn AbiCore),
    Libc,
}

static SERIALIZED: OnceLock<Mutex<std::collections::HashMap<usize, SerializedEntry>>> =
    OnceLock::new();
static DB_HANDLE_MAP: OnceLock<Mutex<HashMap<usize, usize>>> = OnceLock::new();
static MALLOC_ALLOCS: OnceLock<Mutex<HashMap<usize, MallocAllocOwner>>> = OnceLock::new();
static UDF_USER_DATA_MAP: OnceLock<Mutex<HashMap<usize, usize>>> = OnceLock::new();
static TABLE_RESULT_ALLOCS: OnceLock<Mutex<HashSet<usize>>> = OnceLock::new();

pub(crate) fn serialized_map() -> &'static Mutex<std::collections::HashMap<usize, SerializedEntry>>
{
    SERIALIZED.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

fn db_handle_map() -> &'static Mutex<HashMap<usize, usize>> {
    DB_HANDLE_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn malloc_alloc_map() -> &'static Mutex<HashMap<usize, MallocAllocOwner>> {
    MALLOC_ALLOCS.get_or_init(|| Mutex::new(HashMap::new()))
}

fn udf_user_data_map() -> &'static Mutex<HashMap<usize, usize>> {
    UDF_USER_DATA_MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn table_result_allocs() -> &'static Mutex<HashSet<usize>> {
    TABLE_RESULT_ALLOCS.get_or_init(|| Mutex::new(HashSet::new()))
}

pub(crate) fn register_db_handle(raw_db: NonNull<c_void>, handle: *mut sqlite3) {
    if let Ok(mut map) = db_handle_map().lock() {
        map.insert(raw_db.as_ptr() as usize, handle as usize);
    }
}

pub(crate) fn unregister_db_handle(raw_db: NonNull<c_void>) {
    if let Ok(mut map) = db_handle_map().lock() {
        map.remove(&(raw_db.as_ptr() as usize));
    }
}

pub(crate) fn lookup_db_handle(raw_db: NonNull<c_void>) -> Option<*mut sqlite3> {
    let map = db_handle_map().lock().ok()?;
    map.get(&(raw_db.as_ptr() as usize))
        .copied()
        .map(|ptr| ptr as *mut sqlite3)
}

pub(crate) fn register_malloc_alloc(
    ptr: *mut c_void,
    owner: MallocAllocOwner,
) -> std::result::Result<(), ()> {
    let mut map = malloc_alloc_map().lock().map_err(|_| ())?;
    map.insert(ptr as usize, owner);
    Ok(())
}

pub(crate) fn take_malloc_alloc(ptr: *mut c_void) -> Option<MallocAllocOwner> {
    let mut map = malloc_alloc_map().lock().ok()?;
    map.remove(&(ptr as usize))
}

pub(crate) fn register_udf_user_data(wrapper: *mut c_void, user_data: *mut c_void) {
    if wrapper.is_null() {
        return;
    }
    if let Ok(mut map) = udf_user_data_map().lock() {
        map.insert(wrapper as usize, user_data as usize);
    }
}

pub(crate) fn lookup_udf_user_data(wrapper: *mut c_void) -> Option<*mut c_void> {
    if wrapper.is_null() {
        return None;
    }
    let map = udf_user_data_map().lock().ok()?;
    map.get(&(wrapper as usize))
        .copied()
        .map(|ptr| ptr as *mut c_void)
}

pub(crate) fn unregister_udf_user_data(wrapper: *mut c_void) -> Option<*mut c_void> {
    if wrapper.is_null() {
        return None;
    }
    let mut map = udf_user_data_map().lock().ok()?;
    map.remove(&(wrapper as usize))
        .map(|ptr| ptr as *mut c_void)
}

pub(crate) fn register_table_result_alloc(ptr: *mut *mut c_char) {
    if ptr.is_null() {
        return;
    }
    if let Ok(mut allocs) = table_result_allocs().lock() {
        allocs.insert(ptr as usize);
    }
}

pub(crate) fn take_table_result_alloc(ptr: *mut *mut c_char) -> bool {
    if ptr.is_null() {
        return false;
    }
    match table_result_allocs().lock() {
        Ok(mut allocs) => allocs.remove(&(ptr as usize)),
        Err(_) => false,
    }
}
