use core::ffi::{c_char, c_void};
use core::marker::PhantomData;
use core::ptr::NonNull;
use std::collections::HashMap;
use std::sync::{Mutex, MutexGuard, OnceLock};

use crate::error::Result;
use crate::provider::{Sqlite3Api, Sqlite3Hooks};

use super::core::Connection;

/// Bitmask for trace_v2 callbacks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TraceMask {
    bits: u32,
}

impl TraceMask {
    /// Trace SQL text before execution.
    pub const STMT: TraceMask = TraceMask { bits: 0x01 };
    /// Trace statement execution timing samples.
    pub const PROFILE: TraceMask = TraceMask { bits: 0x02 };
    /// Trace each produced row.
    pub const ROW: TraceMask = TraceMask { bits: 0x04 };
    /// Trace connection close events.
    pub const CLOSE: TraceMask = TraceMask { bits: 0x08 };

    /// Empty trace mask.
    pub const fn empty() -> Self {
        Self { bits: 0 }
    }

    /// Raw SQLite mask bits.
    pub const fn bits(self) -> u32 {
        self.bits
    }

    /// Whether `other` is a subset of this mask.
    pub const fn contains(self, other: TraceMask) -> bool {
        (self.bits & other.bits) == other.bits
    }
}

impl core::ops::BitOr for TraceMask {
    type Output = TraceMask;

    fn bitor(self, rhs: TraceMask) -> TraceMask {
        TraceMask {
            bits: self.bits | rhs.bits,
        }
    }
}

impl core::ops::BitOrAssign for TraceMask {
    fn bitor_assign(&mut self, rhs: TraceMask) {
        self.bits |= rhs.bits;
    }
}

/// Decoded trace callback event.
pub enum TraceEvent<'a, P: Sqlite3Api> {
    /// Statement trace event.
    Stmt {
        /// Statement handle associated with the trace event.
        stmt: NonNull<P::Stmt>,
        /// SQL text pointer decoded as UTF-8 when available.
        sql: Option<&'a str>,
    },
    /// Profile trace event.
    Profile {
        /// Statement handle associated with the profile sample.
        stmt: NonNull<P::Stmt>,
        /// Execution time in nanoseconds.
        nsec: i64,
    },
    /// Row trace event.
    Row {
        /// Statement handle currently producing a row.
        stmt: NonNull<P::Stmt>,
    },
    /// Connection-close trace event.
    Close {
        /// Database handle being closed.
        db: NonNull<P::Db>,
    },
    /// Unrecognized/raw trace event payload.
    Raw {
        /// Original SQLite trace mask bits.
        mask: u32,
        /// Raw first pointer payload supplied by SQLite.
        p1: *mut c_void,
        /// Raw second pointer payload supplied by SQLite.
        p2: *mut c_void,
    },
}

type TraceCallback<P> = dyn for<'a> FnMut(TraceEvent<'a, P>) + Send;

struct TraceState<P: Sqlite3Api> {
    cb: Box<TraceCallback<P>>,
}

type ProgressCallback = dyn FnMut() -> i32 + Send;

struct ProgressState {
    cb: Box<ProgressCallback>,
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
    /// `SQLITE_CREATE_INDEX`.
    pub const CREATE_INDEX: i32 = 1;
    /// `SQLITE_CREATE_TABLE`.
    pub const CREATE_TABLE: i32 = 2;
    /// `SQLITE_CREATE_TEMP_INDEX`.
    pub const CREATE_TEMP_INDEX: i32 = 3;
    /// `SQLITE_CREATE_TEMP_TABLE`.
    pub const CREATE_TEMP_TABLE: i32 = 4;
    /// `SQLITE_CREATE_TEMP_TRIGGER`.
    pub const CREATE_TEMP_TRIGGER: i32 = 5;
    /// `SQLITE_CREATE_TEMP_VIEW`.
    pub const CREATE_TEMP_VIEW: i32 = 6;
    /// `SQLITE_CREATE_TRIGGER`.
    pub const CREATE_TRIGGER: i32 = 7;
    /// `SQLITE_CREATE_VIEW`.
    pub const CREATE_VIEW: i32 = 8;
    /// `SQLITE_DELETE`.
    pub const DELETE: i32 = 9;
    /// `SQLITE_DROP_INDEX`.
    pub const DROP_INDEX: i32 = 10;
    /// `SQLITE_DROP_TABLE`.
    pub const DROP_TABLE: i32 = 11;
    /// `SQLITE_DROP_TEMP_INDEX`.
    pub const DROP_TEMP_INDEX: i32 = 12;
    /// `SQLITE_DROP_TEMP_TABLE`.
    pub const DROP_TEMP_TABLE: i32 = 13;
    /// `SQLITE_DROP_TEMP_TRIGGER`.
    pub const DROP_TEMP_TRIGGER: i32 = 14;
    /// `SQLITE_DROP_TEMP_VIEW`.
    pub const DROP_TEMP_VIEW: i32 = 15;
    /// `SQLITE_DROP_TRIGGER`.
    pub const DROP_TRIGGER: i32 = 16;
    /// `SQLITE_DROP_VIEW`.
    pub const DROP_VIEW: i32 = 17;
    /// `SQLITE_INSERT`.
    pub const INSERT: i32 = 18;
    /// `SQLITE_PRAGMA`.
    pub const PRAGMA: i32 = 19;
    /// `SQLITE_READ`.
    pub const READ: i32 = 20;
    /// `SQLITE_SELECT`.
    pub const SELECT: i32 = 21;
    /// `SQLITE_TRANSACTION`.
    pub const TRANSACTION: i32 = 22;
    /// `SQLITE_UPDATE`.
    pub const UPDATE: i32 = 23;
    /// `SQLITE_ATTACH`.
    pub const ATTACH: i32 = 24;
    /// `SQLITE_DETACH`.
    pub const DETACH: i32 = 25;
    /// `SQLITE_ALTER_TABLE`.
    pub const ALTER_TABLE: i32 = 26;
    /// `SQLITE_REINDEX`.
    pub const REINDEX: i32 = 27;
    /// `SQLITE_ANALYZE`.
    pub const ANALYZE: i32 = 28;
    /// `SQLITE_CREATE_VTABLE`.
    pub const CREATE_VTABLE: i32 = 29;
    /// `SQLITE_DROP_VTABLE`.
    pub const DROP_VTABLE: i32 = 30;
    /// `SQLITE_FUNCTION`.
    pub const FUNCTION: i32 = 31;
    /// `SQLITE_SAVEPOINT`.
    pub const SAVEPOINT: i32 = 32;
    /// `SQLITE_RECURSIVE`.
    pub const RECURSIVE: i32 = 33;
}

/// Authorizer action decoded from the SQLite action code.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AuthorizerAction {
    /// Create index operation.
    CreateIndex,
    /// Create table operation.
    CreateTable,
    /// Create temporary index operation.
    CreateTempIndex,
    /// Create temporary table operation.
    CreateTempTable,
    /// Create temporary trigger operation.
    CreateTempTrigger,
    /// Create temporary view operation.
    CreateTempView,
    /// Create trigger operation.
    CreateTrigger,
    /// Create view operation.
    CreateView,
    /// Delete operation.
    Delete,
    /// Drop index operation.
    DropIndex,
    /// Drop table operation.
    DropTable,
    /// Drop temporary index operation.
    DropTempIndex,
    /// Drop temporary table operation.
    DropTempTable,
    /// Drop temporary trigger operation.
    DropTempTrigger,
    /// Drop temporary view operation.
    DropTempView,
    /// Drop trigger operation.
    DropTrigger,
    /// Drop view operation.
    DropView,
    /// Insert operation.
    Insert,
    /// PRAGMA operation.
    Pragma,
    /// Read operation.
    Read,
    /// Select operation.
    Select,
    /// Transaction control operation.
    Transaction,
    /// Update operation.
    Update,
    /// Attach database operation.
    Attach,
    /// Detach database operation.
    Detach,
    /// Alter-table operation.
    AlterTable,
    /// Reindex operation.
    Reindex,
    /// Analyze operation.
    Analyze,
    /// Create virtual-table operation.
    CreateVTable,
    /// Drop virtual-table operation.
    DropVTable,
    /// Function invocation operation.
    Function,
    /// Savepoint operation.
    Savepoint,
    /// Recursive query operation.
    Recursive,
    /// Unrecognized authorizer action code.
    Unknown(i32),
}

impl AuthorizerAction {
    /// Decode a raw SQLite authorizer action code into a typed action.
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
    /// Allow the operation.
    Ok,
    /// Allow but suppress specific reads where SQLite supports it.
    Ignore,
    /// Deny the operation.
    Deny,
}

impl AuthorizerResult {
    /// Encode the typed authorizer decision into the SQLite return code.
    pub fn into_code(self) -> i32 {
        match self {
            Self::Ok => 0,
            Self::Ignore => 2,
            Self::Deny => 1,
        }
    }
}

/// Arguments passed to the authorizer callback.
pub struct AuthorizerEvent<'a> {
    /// Action decoded into a typed enum when recognized.
    pub action: AuthorizerAction,
    /// Original numeric SQLite authorizer action code.
    pub code: i32,
    /// First optional action argument.
    pub arg1: Option<&'a str>,
    /// Second optional action argument.
    pub arg2: Option<&'a str>,
    /// Optional database name for the action.
    pub db_name: Option<&'a str>,
    /// Optional trigger or view name for the action.
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
    if ctx.is_null() {
        return AuthorizerResult::Deny.into_code();
    }
    let state = unsafe { &mut *(ctx as *mut AuthorizerState) };
    // Fail closed if callback panics.
    let mut out = AuthorizerResult::Deny.into_code();
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

extern "C" fn progress_trampoline(ctx: *mut c_void) -> i32 {
    if ctx.is_null() {
        return 0;
    }
    let state = unsafe { &mut *(ctx as *mut ProgressState) };
    // Fail closed if callback panics.
    let mut out = 1;
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        out = (state.cb)();
    }));
    out
}

type CallbackRegistry = HashMap<CallbackRegistryKey, usize>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
struct CallbackRegistryKey {
    db_addr: usize,
    kind: CallbackKind,
}

fn callback_registry() -> &'static Mutex<CallbackRegistry> {
    static REGISTRY: OnceLock<Mutex<CallbackRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_callback_registry() -> MutexGuard<'static, CallbackRegistry> {
    callback_registry()
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn callback_registry_key<T>(db: NonNull<T>, kind: CallbackKind) -> CallbackRegistryKey {
    CallbackRegistryKey {
        db_addr: db.as_ptr() as usize,
        kind,
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
enum CallbackKind {
    Trace,
    Authorizer,
    Progress,
}

/// RAII handle for registered callbacks (trace/authorizer/progress).
///
/// The handle borrows the originating connection so callback state cannot
/// outlive the database handle.
pub struct CallbackHandle<'c, 'p, P: Sqlite3Hooks> {
    api: &'p P,
    db: NonNull<P::Db>,
    kind: CallbackKind,
    ctx: *mut c_void,
    _conn: PhantomData<&'c Connection<'p, P>>,
}

unsafe fn drop_callback_context<P: Sqlite3Hooks>(kind: CallbackKind, ctx: *mut c_void) {
    if ctx.is_null() {
        return;
    }
    match kind {
        CallbackKind::Trace => unsafe { drop(Box::from_raw(ctx as *mut TraceState<P>)) },
        CallbackKind::Authorizer => unsafe { drop(Box::from_raw(ctx as *mut AuthorizerState)) },
        CallbackKind::Progress => unsafe { drop(Box::from_raw(ctx as *mut ProgressState)) },
    }
}

unsafe fn unregister_callback<P: Sqlite3Hooks>(
    api: &P,
    db: NonNull<P::Db>,
    kind: CallbackKind,
) -> Result<()> {
    match kind {
        CallbackKind::Trace => unsafe { api.trace_v2(db, 0, None, core::ptr::null_mut()) },
        CallbackKind::Authorizer => unsafe { api.set_authorizer(db, None, core::ptr::null_mut()) },
        CallbackKind::Progress => unsafe {
            api.progress_handler(db, 0, None, core::ptr::null_mut())
        },
    }
}

impl<'c, 'p, P: Sqlite3Hooks> CallbackHandle<'c, 'p, P> {
    fn new_trace(conn: &'c Connection<'p, P>, ctx: *mut c_void) -> Self {
        Self {
            api: conn.api,
            db: conn.db,
            kind: CallbackKind::Trace,
            ctx,
            _conn: PhantomData,
        }
    }

    fn new_authorizer(conn: &'c Connection<'p, P>, ctx: *mut c_void) -> Self {
        Self {
            api: conn.api,
            db: conn.db,
            kind: CallbackKind::Authorizer,
            ctx,
            _conn: PhantomData,
        }
    }

    fn new_progress(conn: &'c Connection<'p, P>, ctx: *mut c_void) -> Self {
        Self {
            api: conn.api,
            db: conn.db,
            kind: CallbackKind::Progress,
            ctx,
            _conn: PhantomData,
        }
    }
}

impl<'c, 'p, P: Sqlite3Hooks> Drop for CallbackHandle<'c, 'p, P> {
    fn drop(&mut self) {
        let key = callback_registry_key(self.db, self.kind);
        let mut registry = lock_callback_registry();
        let is_active = registry.get(&key).copied() == Some(self.ctx as usize);
        if !is_active {
            drop(registry);
            unsafe { drop_callback_context::<P>(self.kind, self.ctx) };
            return;
        }

        // If unregister fails, leak state to avoid callback UAF.
        if unsafe { unregister_callback(self.api, self.db, self.kind) }.is_ok() {
            if registry.get(&key).copied() == Some(self.ctx as usize) {
                registry.remove(&key);
            }
            drop(registry);
            unsafe { drop_callback_context::<P>(self.kind, self.ctx) };
        }
    }
}

impl<'p, P: Sqlite3Hooks> Connection<'p, P> {
    /// Set busy timeout.
    pub fn busy_timeout(&self, ms: i32) -> Result<()> {
        unsafe { self.api.busy_timeout(self.db, ms) }
    }

    /// Register or clear a raw progress callback.
    ///
    /// # Safety
    /// Callers must ensure the callback/context pair remains valid until it is
    /// explicitly cleared and that the callback never unwinds.
    pub unsafe fn progress_handler_raw(
        &self,
        n: i32,
        cb: Option<extern "C" fn(*mut c_void) -> i32>,
        context: *mut c_void,
    ) -> Result<()> {
        let key = callback_registry_key(self.db, CallbackKind::Progress);
        let mut registry = lock_callback_registry();
        let out = unsafe { self.api.progress_handler(self.db, n, cb, context) };
        if out.is_ok() {
            registry.remove(&key);
        }
        out
    }

    /// Register a panic-contained progress callback.
    ///
    /// The callback is invoked through a trampoline that catches panics and
    /// returns `1` on unwind so SQLite interrupts execution (fail-closed).
    pub fn register_progress_handler<'c, F>(
        &'c self,
        n: i32,
        f: F,
    ) -> Result<CallbackHandle<'c, 'p, P>>
    where
        F: FnMut() -> i32 + Send + 'static,
    {
        let state = Box::new(ProgressState { cb: Box::new(f) });
        let ctx = Box::into_raw(state) as *mut c_void;
        let key = callback_registry_key(self.db, CallbackKind::Progress);
        let mut registry = lock_callback_registry();
        if let Err(err) = unsafe {
            self.api
                .progress_handler(self.db, n, Some(progress_trampoline), ctx)
        } {
            unsafe { drop(Box::from_raw(ctx as *mut ProgressState)) };
            return Err(err);
        }
        registry.insert(key, ctx as usize);
        Ok(CallbackHandle::new_progress(self, ctx))
    }

    /// Remove any previously registered progress callback.
    pub fn clear_progress_handler(&self) -> Result<()> {
        let key = callback_registry_key(self.db, CallbackKind::Progress);
        let mut registry = lock_callback_registry();
        let out = unsafe {
            self.api
                .progress_handler(self.db, 0, None, core::ptr::null_mut())
        };
        if out.is_ok() {
            registry.remove(&key);
        }
        out
    }

    /// Register a typed trace callback.
    pub fn register_trace<'c, F>(
        &'c self,
        mask: TraceMask,
        f: F,
    ) -> Result<CallbackHandle<'c, 'p, P>>
    where
        F: for<'a> FnMut(TraceEvent<'a, P>) + Send + 'static,
    {
        let state = Box::new(TraceState::<P> { cb: Box::new(f) });
        let ctx = Box::into_raw(state) as *mut c_void;
        let key = callback_registry_key(self.db, CallbackKind::Trace);
        let mut registry = lock_callback_registry();
        if let Err(err) = unsafe {
            self.api
                .trace_v2(self.db, mask.bits(), Some(trace_trampoline::<P>), ctx)
        } {
            unsafe { drop(Box::from_raw(ctx as *mut TraceState<P>)) };
            return Err(err);
        }
        registry.insert(key, ctx as usize);
        Ok(CallbackHandle::new_trace(self, ctx))
    }

    /// Register a typed authorizer callback.
    pub fn register_authorizer<'c, F>(&'c self, f: F) -> Result<CallbackHandle<'c, 'p, P>>
    where
        F: FnMut(AuthorizerEvent<'_>) -> AuthorizerResult + Send + 'static,
    {
        let state = Box::new(AuthorizerState { cb: Box::new(f) });
        let ctx = Box::into_raw(state) as *mut c_void;
        let key = callback_registry_key(self.db, CallbackKind::Authorizer);
        let mut registry = lock_callback_registry();
        if let Err(err) = unsafe {
            self.api
                .set_authorizer(self.db, Some(authorizer_trampoline), ctx)
        } {
            unsafe { drop(Box::from_raw(ctx as *mut AuthorizerState)) };
            return Err(err);
        }
        registry.insert(key, ctx as usize);
        Ok(CallbackHandle::new_authorizer(self, ctx))
    }
}
