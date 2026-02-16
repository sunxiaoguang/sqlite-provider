use core::ffi::{c_char, c_void};
use core::ptr::NonNull;

use crate::error::{Error, Result};
use crate::provider::{
    ColumnMetadata, FeatureSet, OpenOptions, OwnedBytes, RawBytes, Sqlite3Api, Sqlite3Backup,
    Sqlite3BlobIo, Sqlite3Hooks, Sqlite3Keying, Sqlite3Metadata, Sqlite3Serialize, Sqlite3Wal,
};
use crate::statement::Statement;

/// Safe wrapper around a `sqlite3*` connection.
pub struct Connection<'p, P: Sqlite3Api> {
    pub(crate) api: &'p P,
    pub(crate) db: NonNull<P::Db>,
}

impl<'p, P: Sqlite3Api> Connection<'p, P> {
    /// Open a connection using the provider SPI.
    pub fn open(api: &'p P, filename: &str, options: OpenOptions<'_>) -> Result<Self> {
        let db = unsafe { api.open(filename, options)? };
        Ok(Self { api, db })
    }

    /// Prepare a statement, using prepare_v3 when available.
    pub fn prepare(&self, sql: &str) -> Result<Statement<'_, 'p, P>> {
        let stmt = unsafe {
            if self.api.feature_set().contains(FeatureSet::PREPARE_V3) {
                self.api.prepare_v3(self.db, sql, 0)?
            } else {
                self.api.prepare_v2(self.db, sql)?
            }
        };
        Ok(Statement::new(self, stmt))
    }

    /// Prepare a statement with flags (requires prepare_v3 support).
    pub fn prepare_with_flags(&self, sql: &str, flags: u32) -> Result<Statement<'_, 'p, P>> {
        if !self.api.feature_set().contains(FeatureSet::PREPARE_V3) {
            return Err(Error::feature_unavailable("prepare_v3 unsupported"));
        }
        let stmt = unsafe { self.api.prepare_v3(self.db, sql, flags)? };
        Ok(Statement::new(self, stmt))
    }

    /// Expose the raw database handle.
    pub fn raw_handle(&self) -> NonNull<P::Db> {
        self.db
    }
}

impl<'p, P: Sqlite3Keying> Connection<'p, P> {
    /// Open a connection and immediately apply a key (SQLCipher-style).
    pub fn open_with_key(
        api: &'p P,
        filename: &str,
        options: OpenOptions<'_>,
        key: &[u8],
    ) -> Result<Self> {
        let db = unsafe { api.open(filename, options)? };
        if let Err(err) = unsafe { api.key(db, key) } {
            let _ = unsafe { api.close(db) };
            return Err(err);
        }
        Ok(Self { api, db })
    }

    /// Rekey the database (SQLCipher-style).
    pub fn rekey(&self, key: &[u8]) -> Result<()> {
        unsafe { self.api.rekey(self.db, key) }
    }
}

impl<'p, P: Sqlite3Api> Drop for Connection<'p, P> {
    fn drop(&mut self) {
        let _ = unsafe { self.api.close(self.db) };
    }
}

/// Bitmask for trace_v2 callbacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceMask {
    bits: u32,
}

impl TraceMask {
    pub const STMT: TraceMask = TraceMask { bits: 0x01 };
    pub const PROFILE: TraceMask = TraceMask { bits: 0x02 };
    pub const ROW: TraceMask = TraceMask { bits: 0x04 };
    pub const CLOSE: TraceMask = TraceMask { bits: 0x08 };

    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    pub const fn bits(self) -> u32 {
        self.bits
    }

    pub const fn contains(self, other: TraceMask) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for TraceMask {
    type Output = TraceMask;

    fn bitor(self, rhs: TraceMask) -> TraceMask {
        TraceMask { bits: self.bits | rhs.bits }
    }
}

impl core::ops::BitOrAssign for TraceMask {
    fn bitor_assign(&mut self, rhs: TraceMask) {
        self.bits |= rhs.bits;
    }
}

/// Decoded trace callback event.
pub enum TraceEvent<'a, P: Sqlite3Api> {
    Stmt { stmt: NonNull<P::Stmt>, sql: Option<&'a str> },
    Profile { stmt: NonNull<P::Stmt>, nsec: i64 },
    Row { stmt: NonNull<P::Stmt> },
    Close { db: NonNull<P::Db> },
    Raw { mask: u32, p1: *mut c_void, p2: *mut c_void },
}

type TraceCallback<P> = dyn for<'a> FnMut(TraceEvent<'a, P>) + Send;

struct TraceState<P: Sqlite3Api> {
    cb: Box<TraceCallback<P>>,
}

extern "C" fn trace_trampoline<P: Sqlite3Api>(
    mask: u32,
    ctx: *mut c_void,
    p1: *mut c_void,
    p2: *mut c_void,
) {
    let state = unsafe { &mut *(ctx as *mut TraceState<P>) };
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let event = decode_trace::<P>(mask, p1, p2);
        (state.cb)(event);
    }));
}

fn decode_trace<'a, P: Sqlite3Api>(
    mask: u32,
    p1: *mut c_void,
    p2: *mut c_void,
) -> TraceEvent<'a, P> {
    if (mask & TraceMask::STMT.bits()) != 0 {
        let stmt = match NonNull::new(p1 as *mut P::Stmt) {
            Some(stmt) => stmt,
            None => return TraceEvent::Raw { mask, p1, p2 },
        };
        let sql = unsafe { cstr_to_opt(p2 as *const c_char) };
        return TraceEvent::Stmt { stmt, sql };
    }
    if (mask & TraceMask::PROFILE.bits()) != 0 {
        let stmt = match NonNull::new(p1 as *mut P::Stmt) {
            Some(stmt) => stmt,
            None => return TraceEvent::Raw { mask, p1, p2 },
        };
        if p2.is_null() {
            return TraceEvent::Raw { mask, p1, p2 };
        }
        let nsec = unsafe { *(p2 as *const i64) };
        return TraceEvent::Profile { stmt, nsec };
    }
    if (mask & TraceMask::ROW.bits()) != 0 {
        let stmt = match NonNull::new(p1 as *mut P::Stmt) {
            Some(stmt) => stmt,
            None => return TraceEvent::Raw { mask, p1, p2 },
        };
        return TraceEvent::Row { stmt };
    }
    if (mask & TraceMask::CLOSE.bits()) != 0 {
        let db = match NonNull::new(p1 as *mut P::Db) {
            Some(db) => db,
            None => return TraceEvent::Raw { mask, p1, p2 },
        };
        return TraceEvent::Close { db };
    }
    TraceEvent::Raw { mask, p1, p2 }
}

/// Authorizer action codes (SQLite values).
pub mod authorizer {
    pub const CREATE_INDEX: i32 = 1;
    pub const CREATE_TABLE: i32 = 2;
    pub const CREATE_TEMP_INDEX: i32 = 3;
    pub const CREATE_TEMP_TABLE: i32 = 4;
    pub const CREATE_TEMP_TRIGGER: i32 = 5;
    pub const CREATE_TEMP_VIEW: i32 = 6;
    pub const CREATE_TRIGGER: i32 = 7;
    pub const CREATE_VIEW: i32 = 8;
    pub const DELETE: i32 = 9;
    pub const DROP_INDEX: i32 = 10;
    pub const DROP_TABLE: i32 = 11;
    pub const DROP_TEMP_INDEX: i32 = 12;
    pub const DROP_TEMP_TABLE: i32 = 13;
    pub const DROP_TEMP_TRIGGER: i32 = 14;
    pub const DROP_TEMP_VIEW: i32 = 15;
    pub const DROP_TRIGGER: i32 = 16;
    pub const DROP_VIEW: i32 = 17;
    pub const INSERT: i32 = 18;
    pub const PRAGMA: i32 = 19;
    pub const READ: i32 = 20;
    pub const SELECT: i32 = 21;
    pub const TRANSACTION: i32 = 22;
    pub const UPDATE: i32 = 23;
    pub const ATTACH: i32 = 24;
    pub const DETACH: i32 = 25;
    pub const ALTER_TABLE: i32 = 26;
    pub const REINDEX: i32 = 27;
    pub const ANALYZE: i32 = 28;
    pub const CREATE_VTABLE: i32 = 29;
    pub const DROP_VTABLE: i32 = 30;
    pub const FUNCTION: i32 = 31;
    pub const SAVEPOINT: i32 = 32;
    pub const RECURSIVE: i32 = 33;
}

/// Authorizer action decoded from the SQLite action code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizerAction {
    CreateIndex,
    CreateTable,
    CreateTempIndex,
    CreateTempTable,
    CreateTempTrigger,
    CreateTempView,
    CreateTrigger,
    CreateView,
    Delete,
    DropIndex,
    DropTable,
    DropTempIndex,
    DropTempTable,
    DropTempTrigger,
    DropTempView,
    DropTrigger,
    DropView,
    Insert,
    Pragma,
    Read,
    Select,
    Transaction,
    Update,
    Attach,
    Detach,
    AlterTable,
    Reindex,
    Analyze,
    CreateVTable,
    DropVTable,
    Function,
    Savepoint,
    Recursive,
    Unknown(i32),
}

impl AuthorizerAction {
    pub fn from_code(code: i32) -> Self {
        match code {
            authorizer::CREATE_INDEX => Self::CreateIndex,
            authorizer::CREATE_TABLE => Self::CreateTable,
            authorizer::CREATE_TEMP_INDEX => Self::CreateTempIndex,
            authorizer::CREATE_TEMP_TABLE => Self::CreateTempTable,
            authorizer::CREATE_TEMP_TRIGGER => Self::CreateTempTrigger,
            authorizer::CREATE_TEMP_VIEW => Self::CreateTempView,
            authorizer::CREATE_TRIGGER => Self::CreateTrigger,
            authorizer::CREATE_VIEW => Self::CreateView,
            authorizer::DELETE => Self::Delete,
            authorizer::DROP_INDEX => Self::DropIndex,
            authorizer::DROP_TABLE => Self::DropTable,
            authorizer::DROP_TEMP_INDEX => Self::DropTempIndex,
            authorizer::DROP_TEMP_TABLE => Self::DropTempTable,
            authorizer::DROP_TEMP_TRIGGER => Self::DropTempTrigger,
            authorizer::DROP_TEMP_VIEW => Self::DropTempView,
            authorizer::DROP_TRIGGER => Self::DropTrigger,
            authorizer::DROP_VIEW => Self::DropView,
            authorizer::INSERT => Self::Insert,
            authorizer::PRAGMA => Self::Pragma,
            authorizer::READ => Self::Read,
            authorizer::SELECT => Self::Select,
            authorizer::TRANSACTION => Self::Transaction,
            authorizer::UPDATE => Self::Update,
            authorizer::ATTACH => Self::Attach,
            authorizer::DETACH => Self::Detach,
            authorizer::ALTER_TABLE => Self::AlterTable,
            authorizer::REINDEX => Self::Reindex,
            authorizer::ANALYZE => Self::Analyze,
            authorizer::CREATE_VTABLE => Self::CreateVTable,
            authorizer::DROP_VTABLE => Self::DropVTable,
            authorizer::FUNCTION => Self::Function,
            authorizer::SAVEPOINT => Self::Savepoint,
            authorizer::RECURSIVE => Self::Recursive,
            other => Self::Unknown(other),
        }
    }
}

/// Return value for authorizer callbacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizerResult {
    Ok,
    Ignore,
    Deny,
}

impl AuthorizerResult {
    pub fn into_code(self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::Ignore => 1,
            Self::Deny => 2,
        }
    }
}

/// Arguments passed to the authorizer callback.
pub struct AuthorizerEvent<'a> {
    pub action: AuthorizerAction,
    pub code: i32,
    pub arg1: Option<&'a str>,
    pub arg2: Option<&'a str>,
    pub db_name: Option<&'a str>,
    pub trigger_or_view: Option<&'a str>,
}

struct AuthorizerState {
    cb: Box<dyn for<'a> FnMut(AuthorizerEvent<'a>) -> AuthorizerResult + Send>,
}

extern "C" fn authorizer_trampoline(
    ctx: *mut c_void,
    action: i32,
    arg1: *const c_char,
    arg2: *const c_char,
    db_name: *const c_char,
    trigger_or_view: *const c_char,
) -> i32 {
    let state = unsafe { &mut *(ctx as *mut AuthorizerState) };
    let mut out = 0;
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let event = AuthorizerEvent {
            action: AuthorizerAction::from_code(action),
            code: action,
            arg1: unsafe { cstr_to_opt(arg1) },
            arg2: unsafe { cstr_to_opt(arg2) },
            db_name: unsafe { cstr_to_opt(db_name) },
            trigger_or_view: unsafe { cstr_to_opt(trigger_or_view) },
        };
        out = (state.cb)(event).into_code();
    }));
    out
}

unsafe fn cstr_to_opt<'a>(ptr: *const c_char) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    unsafe { core::ffi::CStr::from_ptr(ptr) }.to_str().ok()
}

/// RAII handle for registered callbacks (trace/authorizer).
pub struct CallbackHandle<'p, P: Sqlite3Hooks> {
    api: &'p P,
    db: NonNull<P::Db>,
    kind: CallbackKind,
    ctx: *mut c_void,
}

enum CallbackKind {
    Trace,
    Authorizer,
}

impl<'p, P: Sqlite3Hooks> CallbackHandle<'p, P> {
    fn new_trace(api: &'p P, db: NonNull<P::Db>, ctx: *mut c_void) -> Self {
        Self { api, db, kind: CallbackKind::Trace, ctx }
    }

    fn new_authorizer(api: &'p P, db: NonNull<P::Db>, ctx: *mut c_void) -> Self {
        Self { api, db, kind: CallbackKind::Authorizer, ctx }
    }
}

impl<'p, P: Sqlite3Hooks> Drop for CallbackHandle<'p, P> {
    fn drop(&mut self) {
        unsafe {
            match self.kind {
                CallbackKind::Trace => {
                    let _ = self.api.trace_v2(self.db, 0, None, core::ptr::null_mut());
                    drop(Box::from_raw(self.ctx as *mut TraceState<P>));
                }
                CallbackKind::Authorizer => {
                    let _ = self.api.set_authorizer(self.db, None, core::ptr::null_mut());
                    drop(Box::from_raw(self.ctx as *mut AuthorizerState));
                }
            }
        }
    }
}

impl<'p, P: Sqlite3Hooks> Connection<'p, P> {
    /// Set busy timeout.
    pub fn busy_timeout(&self, ms: i32) -> Result<()> {
        unsafe { self.api.busy_timeout(self.db, ms) }
    }

    /// Register a progress handler.
    ///
    /// # Safety
    /// `ctx` must point to callback state that remains valid until the progress
    /// handler is replaced/cleared, and `cb` must uphold SQLite's callback ABI.
    pub unsafe fn progress_handler(
        &self,
        n: i32,
        cb: Option<extern "C" fn() -> i32>,
        ctx: *mut c_void,
    ) -> Result<()> {
        unsafe { self.api.progress_handler(self.db, n, cb, ctx) }
    }

    /// Register a typed trace callback.
    pub fn register_trace<F>(&self, mask: TraceMask, f: F) -> Result<CallbackHandle<'p, P>>
    where
        F: for<'a> FnMut(TraceEvent<'a, P>) + Send + 'static,
    {
        let state = Box::new(TraceState::<P> { cb: Box::new(f) });
        let ctx = Box::into_raw(state) as *mut c_void;
        unsafe { self.api.trace_v2(self.db, mask.bits(), Some(trace_trampoline::<P>), ctx)? };
        Ok(CallbackHandle::new_trace(self.api, self.db, ctx))
    }

    /// Register a typed authorizer callback.
    pub fn register_authorizer<F>(&self, f: F) -> Result<CallbackHandle<'p, P>>
    where
        F: FnMut(AuthorizerEvent<'_>) -> AuthorizerResult + Send + 'static,
    {
        let state = Box::new(AuthorizerState { cb: Box::new(f) });
        let ctx = Box::into_raw(state) as *mut c_void;
        unsafe { self.api.set_authorizer(self.db, Some(authorizer_trampoline), ctx)? };
        Ok(CallbackHandle::new_authorizer(self.api, self.db, ctx))
    }
}

/// RAII wrapper around an online backup handle.
pub struct Backup<'p, P: Sqlite3Backup> {
    api: &'p P,
    handle: NonNull<P::Backup>,
}

impl<'p, P: Sqlite3Backup> Backup<'p, P> {
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

impl<'p, P: Sqlite3Backup> Drop for Backup<'p, P> {
    fn drop(&mut self) {
        let _ = unsafe { self.api.backup_finish(self.handle) };
    }
}

impl<'p, P: Sqlite3Backup> Connection<'p, P> {
    /// Start a backup from this database to `dest`.
    pub fn backup_to(&self, dest: &Connection<'p, P>, name: &str) -> Result<Backup<'p, P>> {
        let handle = unsafe { self.api.backup_init(dest.db, name, self.db, "main")? };
        Ok(Backup { api: self.api, handle })
    }
}

/// RAII wrapper around an incremental blob handle.
pub struct Blob<'p, P: Sqlite3BlobIo> {
    api: &'p P,
    handle: NonNull<P::Blob>,
}

impl<'p, P: Sqlite3BlobIo> Blob<'p, P> {
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

impl<'p, P: Sqlite3BlobIo> Drop for Blob<'p, P> {
    fn drop(&mut self) {
        let _ = unsafe { self.api.blob_close(self.handle) };
    }
}

impl<'p, P: Sqlite3BlobIo> Connection<'p, P> {
    /// Open an incremental blob handle.
    pub fn open_blob(
        &self,
        db_name: &str,
        table: &str,
        column: &str,
        rowid: i64,
        flags: u32,
    ) -> Result<Blob<'p, P>> {
        let handle = unsafe { self.api.blob_open(self.db, db_name, table, column, rowid, flags)? };
        Ok(Blob { api: self.api, handle })
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
        unsafe { me.api.free(me.bytes) };
        vec
    }
}

impl<'p, P: Sqlite3Serialize> Drop for SerializedDb<'p, P> {
    fn drop(&mut self) {
        unsafe { self.api.free(self.bytes) };
    }
}

impl<'p, P: Sqlite3Serialize> Connection<'p, P> {
    /// Serialize the database into an owned buffer.
    pub fn serialize(&self, schema: Option<&str>, flags: u32) -> Result<SerializedDb<'p, P>> {
        let bytes = unsafe { self.api.serialize(self.db, schema, flags)? };
        Ok(SerializedDb { api: self.api, bytes })
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
        unsafe { self.api.table_column_metadata(self.db, db_name, table, column) }
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

#[cfg(test)]
mod tests {
    use super::authorizer;
    use super::{AuthorizerAction, AuthorizerResult, TraceMask};

    #[test]
    fn trace_mask_bits() {
        let mask = TraceMask::STMT | TraceMask::PROFILE;
        assert!(mask.contains(TraceMask::STMT));
        assert!(mask.contains(TraceMask::PROFILE));
    }

    #[test]
    fn authorizer_action_from_code() {
        assert_eq!(AuthorizerAction::from_code(authorizer::READ), AuthorizerAction::Read);
        assert_eq!(AuthorizerAction::from_code(999), AuthorizerAction::Unknown(999));
    }

    #[test]
    fn authorizer_result_codes() {
        assert_eq!(AuthorizerResult::Ok.into_code(), 0);
        assert_eq!(AuthorizerResult::Ignore.into_code(), 1);
        assert_eq!(AuthorizerResult::Deny.into_code(), 2);
    }
}
