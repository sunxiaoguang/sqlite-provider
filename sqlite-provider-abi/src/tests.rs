use super::*;
use crate::state::{MallocAllocOwner, register_malloc_alloc, take_malloc_alloc};
use sqlite_provider::Sqlite3Api;
use std::cell::Cell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

struct MockDb;
struct MockStmt {
    stepped: bool,
}
struct MockValue;
struct MockContext;

struct MockApi;

static DESTRUCTOR_CALLS: AtomicUsize = AtomicUsize::new(0);
static DESTRUCTOR_TEST_LOCK: Mutex<()> = Mutex::new(());
static PANIC_TEST_LOCK: Mutex<()> = Mutex::new(());
std::thread_local! {
    static PANIC_ON_OPEN: Cell<bool> = const { Cell::new(false) };
    static PANIC_ON_ERRCODE: Cell<bool> = const { Cell::new(false) };
}

fn set_panic_on_open(value: bool) {
    PANIC_ON_OPEN.with(|flag| flag.set(value));
}

fn panic_on_open() -> bool {
    PANIC_ON_OPEN.with(Cell::get)
}

fn set_panic_on_errcode(value: bool) {
    PANIC_ON_ERRCODE.with(|flag| flag.set(value));
}

fn panic_on_errcode() -> bool {
    PANIC_ON_ERRCODE.with(Cell::get)
}

struct PanicToggleResetGuard;

impl PanicToggleResetGuard {
    fn new() -> Self {
        set_panic_on_open(false);
        set_panic_on_errcode(false);
        Self
    }
}

impl Drop for PanicToggleResetGuard {
    fn drop(&mut self) {
        set_panic_on_open(false);
        set_panic_on_errcode(false);
    }
}

extern "C" fn counting_destructor(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe { libc::free(ptr) };
    }
    DESTRUCTOR_CALLS.fetch_add(1, Ordering::SeqCst);
}

fn reset_destructor_calls() {
    DESTRUCTOR_CALLS.store(0, Ordering::SeqCst);
}

fn destructor_calls() -> usize {
    DESTRUCTOR_CALLS.load(Ordering::SeqCst)
}

fn custom_destructor_ptr() -> *mut c_void {
    counting_destructor as *const () as *mut c_void
}

extern "C" fn aborting_exec_callback(
    _ctx: *mut c_void,
    _cols: i32,
    _values: *mut *mut c_char,
    _names: *mut *mut c_char,
) -> i32 {
    1
}

unsafe fn malloc_copy(bytes: &[u8]) -> *mut c_void {
    let size = bytes.len().max(1);
    let ptr = libc::malloc(size) as *mut u8;
    assert!(!ptr.is_null(), "malloc failed in test");
    if !bytes.is_empty() {
        std::ptr::copy_nonoverlapping(bytes.as_ptr(), ptr, bytes.len());
    }
    ptr as *mut c_void
}

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

    unsafe fn malloc(&self, size: usize) -> *mut c_void {
        libc::malloc(size.max(1))
    }

    unsafe fn free(&self, ptr: *mut c_void) {
        libc::free(ptr);
    }

    fn threadsafe(&self) -> i32 {
        7
    }

    unsafe fn open(&self, _filename: &str, _options: OpenOptions<'_>) -> Result<NonNull<Self::Db>> {
        if panic_on_open() {
            panic!("mock open panic");
        }
        Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockDb))))
    }

    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()> {
        drop(Box::from_raw(db.as_ptr()));
        Ok(())
    }

    unsafe fn prepare_v2(&self, _db: NonNull<Self::Db>, _sql: &str) -> Result<NonNull<Self::Stmt>> {
        Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockStmt {
            stepped: false,
        }))))
    }

    unsafe fn prepare_v3(
        &self,
        _db: NonNull<Self::Db>,
        _sql: &str,
        _flags: u32,
    ) -> Result<NonNull<Self::Stmt>> {
        Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockStmt {
            stepped: false,
        }))))
    }

    unsafe fn step(&self, stmt: NonNull<Self::Stmt>) -> Result<StepResult> {
        let stmt = &mut *stmt.as_ptr();
        if stmt.stepped {
            Ok(StepResult::Done)
        } else {
            stmt.stepped = true;
            Ok(StepResult::Row)
        }
    }

    unsafe fn reset(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.stepped = false;
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
        2
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

    unsafe fn column_text(&self, _stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        const V0: &[u8] = b"v0";
        const V1: &[u8] = b"v1";
        match col {
            0 => RawBytes {
                ptr: V0.as_ptr(),
                len: V0.len(),
            },
            1 => RawBytes {
                ptr: V1.as_ptr(),
                len: V1.len(),
            },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn column_blob(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> RawBytes {
        RawBytes::empty()
    }

    unsafe fn errcode(&self, _db: NonNull<Self::Db>) -> i32 {
        if panic_on_errcode() {
            panic!("mock errcode panic");
        }
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
    let _guard = PANIC_TEST_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let _reset = PanicToggleResetGuard::new();
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
fn provider_panic_in_open_is_contained() {
    static API: MockApi = MockApi;
    let _guard = PANIC_TEST_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let _reset = PanicToggleResetGuard::new();
    set_panic_on_open(true);
    let open = unsafe {
        <MockApi as AbiCore>::open(
            &API,
            ":memory:",
            OpenOptions {
                flags: OpenFlags::READ_WRITE | OpenFlags::CREATE,
                vfs: None,
            },
        )
    };
    let err = open.expect_err("panic should be converted into Error");
    assert_eq!(err.code, ErrorCode::Error);
    assert_eq!(
        err.message.as_deref(),
        Some("panic in provider operation: open")
    );
}

#[test]
fn provider_panic_in_errcode_is_contained() {
    static API: MockApi = MockApi;
    let _guard = PANIC_TEST_LOCK
        .lock()
        .unwrap_or_else(|poison| poison.into_inner());
    let _reset = PanicToggleResetGuard::new();
    let db = unsafe {
        <MockApi as AbiCore>::open(
            &API,
            ":memory:",
            OpenOptions {
                flags: OpenFlags::READ_WRITE | OpenFlags::CREATE,
                vfs: None,
            },
        )
    }
    .expect("open should succeed when panic toggle is disabled");

    set_panic_on_errcode(true);
    let err = unsafe { <MockApi as AbiCore>::errcode(&API, db) };
    assert_eq!(err, SQLITE_ERROR);

    let close = unsafe { <MockApi as AbiCore>::close(&API, db) };
    assert!(close.is_ok());
}

#[test]
fn null_handle_cleanup_entrypoints_return_ok() {
    assert_eq!(sqlite3_finalize(null_mut()), SQLITE_OK);
    assert_eq!(sqlite3_reset(null_mut()), SQLITE_OK);
    assert_eq!(sqlite3_close(null_mut()), SQLITE_OK);
    assert_eq!(sqlite3_close_v2(null_mut()), SQLITE_OK);
}

#[test]
fn null_handle_error_entrypoints_match_sqlite() {
    assert_eq!(sqlite3_errcode(null_mut()), SQLITE_NOMEM);
    assert_eq!(sqlite3_extended_errcode(null_mut()), SQLITE_NOMEM);
    let msg = unsafe { CStr::from_ptr(sqlite3_errmsg(null_mut())) };
    assert_eq!(msg.to_str().unwrap(), "out of memory");
}

#[test]
fn sleep_returns_elapsed_millis() {
    assert_eq!(sqlite3_sleep(0), 0);
    assert_eq!(sqlite3_sleep(1), 1);
}

#[test]
fn malloc64_rejects_oversized_request() {
    let ptr = sqlite3_malloc64(8);
    assert!(!ptr.is_null());
    sqlite3_free(ptr);

    let oversized = (i32::MAX as u64) + 1;
    assert!(sqlite3_malloc64(oversized).is_null());
}

#[test]
fn malloc64_tracks_owner_and_free_consumes_entry() {
    let _ = ensure_default_provider();
    let ptr = sqlite3_malloc64(16);
    assert!(!ptr.is_null());

    let owner = take_malloc_alloc(ptr).expect("malloc64 should track pointer owner");
    match (provider(), owner) {
        (Some(_), MallocAllocOwner::Provider(_)) => {}
        (None, MallocAllocOwner::Libc) => {}
        _ => panic!("malloc owner did not match provider availability"),
    }
    register_malloc_alloc(ptr, owner).expect("reinsert malloc owner");

    sqlite3_free(ptr);
    assert!(take_malloc_alloc(ptr).is_none());
}

#[test]
fn close_v2_defers_close_until_statement_finalize() {
    static API: MockApi = MockApi;
    let _ = register_provider(ProviderState::new(&API));

    let mut db: *mut sqlite3 = null_mut();
    let rc = sqlite3_open(CString::new(":memory:").unwrap().as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select 1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());

    let rc = sqlite3_close_v2(db);
    assert_eq!(rc, SQLITE_OK);

    let rc = sqlite3_step(stmt);
    assert_eq!(rc, SQLITE_ROW);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn stricmp_basic() {
    let a = CString::new("Hello").unwrap();
    let b = CString::new("hello").unwrap();
    assert_eq!(sqlite3_stricmp(a.as_ptr(), b.as_ptr()), 0);
}

#[test]
fn open_v2_clears_db_out_on_error() {
    static API: MockApi = MockApi;
    let _ = register_provider(ProviderState::new(&API));
    let mut db = 1 as *mut sqlite3;
    let rc = sqlite3_open_v2(null(), &mut db, SQLITE_OPEN_READWRITE, null());
    assert_eq!(rc, SQLITE_MISUSE);
    assert!(db.is_null());
}

#[test]
fn blob_open_clears_blob_out_on_error() {
    static API: MockApi = MockApi;
    let _ = register_provider(ProviderState::new(&API));

    let mut blob = 1 as *mut c_void;
    let rc = sqlite3_blob_open(null_mut(), null(), null(), null(), 1, 0, &mut blob);
    assert_eq!(rc, SQLITE_MISUSE);
    assert!(blob.is_null());
}

#[test]
fn serialize_clears_outputs_on_error() {
    static API: MockApi = MockApi;
    let _ = register_provider(ProviderState::new(&API));

    let mut out = 1 as *mut c_void;
    let mut out_bytes = 777;
    let rc = sqlite3_serialize(null_mut(), null(), &mut out, &mut out_bytes, 0);
    assert_eq!(rc, SQLITE_MISUSE);
    assert!(out.is_null());
    assert_eq!(out_bytes, 0);
}

#[test]
fn exec_callback_abort_sets_errcode_errmsg_and_err_out() {
    static API: MockApi = MockApi;
    let _ = register_provider(ProviderState::new(&API));

    let mut db: *mut sqlite3 = null_mut();
    let open_rc = sqlite3_open(CString::new(":memory:").unwrap().as_ptr(), &mut db);
    assert_eq!(open_rc, SQLITE_OK);

    let mut err_out: *mut c_char = null_mut();
    let sql = CString::new("SELECT 1;").unwrap();
    let rc = sqlite3_exec(
        db,
        sql.as_ptr(),
        Some(aborting_exec_callback),
        null_mut(),
        &mut err_out,
    );
    assert_eq!(rc, SQLITE_ABORT);
    assert_eq!(sqlite3_errcode(db), SQLITE_ABORT);
    assert_eq!(sqlite3_extended_errcode(db), SQLITE_ABORT);

    let msg = unsafe { CStr::from_ptr(sqlite3_errmsg(db)) }
        .to_str()
        .unwrap();
    assert_eq!(msg, "query aborted");
    assert!(!err_out.is_null());
    let err_out_msg = unsafe { CStr::from_ptr(err_out) }.to_str().unwrap();
    assert_eq!(err_out_msg, "query aborted");
    sqlite3_free(err_out.cast());

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());
    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(sqlite3_errcode(db), SQLITE_OK);

    let close_rc = sqlite3_close(db);
    assert_eq!(close_rc, SQLITE_OK);
}

#[test]
fn push_column_name_uses_fallback_when_missing() {
    let mut buf = Vec::new();
    let mut first = None;
    let mut second = None;
    push_column_name(&mut buf, &mut first, None, 0);
    push_column_name(&mut buf, &mut second, None, 1);
    let first = first.unwrap();
    let second = second.unwrap();
    let first_name = std::str::from_utf8(&buf[first..second - 1]).unwrap();
    let second_name = std::str::from_utf8(&buf[second..buf.len() - 1]).unwrap();
    assert_eq!(first_name, "column1");
    assert_eq!(second_name, "column2");
}

#[test]
fn sqlite3_threadsafe_uses_provider_capability() {
    let _ = ensure_default_provider();
    let expected = provider().map(|state| state.core.threadsafe()).unwrap_or(0);
    assert_eq!(sqlite3_threadsafe(), expected);
}

#[test]
fn open_flags_from_sqlite_maps_expected_bits() {
    let sqlite_flags = SQLITE_OPEN_READWRITE
        | SQLITE_OPEN_CREATE
        | SQLITE_OPEN_URI
        | SQLITE_OPEN_FULLMUTEX
        | SQLITE_OPEN_EXRESCODE;
    let mapped = open_flags_from_sqlite(sqlite_flags);
    assert!(mapped.contains(OpenFlags::READ_WRITE));
    assert!(mapped.contains(OpenFlags::CREATE));
    assert!(mapped.contains(OpenFlags::URI));
    assert!(mapped.contains(OpenFlags::FULL_MUTEX));
    assert!(mapped.contains(OpenFlags::EXRESCODE));
    assert!(!mapped.contains(OpenFlags::READ_ONLY));
}

#[test]
fn function_flags_from_sqlite_maps_expected_bits() {
    let sqlite_flags = SQLITE_UTF8 | SQLITE_DETERMINISTIC | SQLITE_DIRECTONLY | SQLITE_INNOCUOUS;
    let mapped = function_flags_from_sqlite(sqlite_flags);
    assert!(mapped.contains(FunctionFlags::DETERMINISTIC));
    assert!(mapped.contains(FunctionFlags::DIRECT_ONLY));
    assert!(mapped.contains(FunctionFlags::INNOCUOUS));
}

#[test]
fn split_first_statement_keeps_trigger_body_intact() {
    let sql = b"CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t(id) VALUES (new.id + 1); END; SELECT 1;";
    let first = split_first_statement(sql, 0);
    assert!(first.complete);
    assert!(first.terminated);
    let (start, end) = first.range.expect("first statement range");
    let first_stmt = std::str::from_utf8(&sql[start..end]).expect("utf-8");
    assert!(first_stmt.trim_end().ends_with("END;"));

    let second = split_first_statement(sql, first.tail);
    assert!(second.complete);
    let (start, end) = second.range.expect("second statement range");
    let second_stmt = std::str::from_utf8(&sql[start..end]).expect("utf-8");
    assert_eq!(second_stmt.trim(), "SELECT 1;");
}

#[test]
fn split_first_statement_skips_leading_empty_statements() {
    let sql = b"; ; --x\n SELECT 1;";
    let split = split_first_statement(sql, 0);
    assert!(split.complete);
    assert!(split.terminated);
    let (start, end) = split.range.expect("statement range");
    let stmt = std::str::from_utf8(&sql[start..end]).expect("utf-8");
    assert_eq!(stmt.trim(), "SELECT 1;");
}

#[test]
fn split_first_statement_keeps_trigger_case_body_intact() {
    let sql = b"CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT CASE WHEN NEW.id > 0 THEN 'a;b' ELSE 'z' END; INSERT INTO t(id) VALUES (NEW.id + 1); END; SELECT 99;";
    let first = split_first_statement(sql, 0);
    assert!(first.complete);
    assert!(first.terminated);
    let (start, end) = first.range.expect("first statement range");
    let first_stmt = std::str::from_utf8(&sql[start..end]).expect("utf-8");
    assert!(first_stmt.trim_end().ends_with("END;"));

    let second = split_first_statement(sql, first.tail);
    assert!(second.complete);
    let (start, end) = second.range.expect("second statement range");
    let second_stmt = std::str::from_utf8(&sql[start..end]).expect("utf-8");
    assert_eq!(second_stmt.trim(), "SELECT 99;");
}

#[test]
fn split_first_statement_rejects_trigger_missing_end_terminator_boundary() {
    let sql = b"CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); END SELECT 1;";
    let split = split_first_statement(sql, 0);
    assert!(!split.complete);
    assert!(!split.terminated);
}

#[test]
fn complete_accepts_trigger_body_semicolons() {
    let sql =
        CString::new("CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); END;")
            .unwrap();
    assert_eq!(sqlite3_complete(sql.as_ptr()), 1);
}

#[test]
fn complete_rejects_trigger_missing_end_terminator_boundary() {
    let sql = CString::new(
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); END SELECT 1;",
    )
    .unwrap();
    assert_eq!(sqlite3_complete(sql.as_ptr()), 0);
}

#[test]
fn complete_accepts_trigger_case_body_semicolons() {
    let sql = CString::new(
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT CASE WHEN NEW.id > 0 THEN 'x;y' ELSE 'z' END; END;",
    )
    .unwrap();
    assert_eq!(sqlite3_complete(sql.as_ptr()), 1);
}

#[test]
fn errstr_matches_sqlite_text_for_common_codes() {
    let msg = unsafe { CStr::from_ptr(sqlite3_errstr(SQLITE_ERROR)) };
    assert_eq!(msg.to_str().unwrap(), "SQL logic error");

    let msg = unsafe { CStr::from_ptr(sqlite3_errstr(SQLITE_ABORT)) };
    assert_eq!(msg.to_str().unwrap(), "query aborted");

    let msg = unsafe { CStr::from_ptr(sqlite3_errstr(SQLITE_MISUSE)) };
    assert_eq!(msg.to_str().unwrap(), "bad parameter or other API misuse");

    let msg = unsafe { CStr::from_ptr(sqlite3_errstr(SQLITE_DONE)) };
    assert_eq!(msg.to_str().unwrap(), "no more rows available");

    let msg = unsafe { CStr::from_ptr(sqlite3_errstr((1 << 8) | SQLITE_IOERR)) };
    assert_eq!(msg.to_str().unwrap(), "disk I/O error");
}

#[test]
fn bind_text_calls_custom_destructor_on_misuse_path() {
    let _guard = DESTRUCTOR_TEST_LOCK.lock().unwrap();
    reset_destructor_calls();
    let text = unsafe { malloc_copy(b"abc") };
    let rc = sqlite3_bind_text(
        null_mut(),
        1,
        text as *const c_char,
        3,
        custom_destructor_ptr(),
    );
    assert_eq!(rc, SQLITE_MISUSE);
    assert_eq!(destructor_calls(), 1);
}

#[test]
fn bind_blob_calls_custom_destructor_on_misuse_path() {
    let _guard = DESTRUCTOR_TEST_LOCK.lock().unwrap();
    reset_destructor_calls();
    let blob = unsafe { malloc_copy(&[1_u8, 2_u8, 3_u8]) };
    let rc = sqlite3_bind_blob(null_mut(), 1, blob, 3, custom_destructor_ptr());
    assert_eq!(rc, SQLITE_MISUSE);
    assert_eq!(destructor_calls(), 1);
}

#[test]
fn result_text_calls_custom_destructor_on_misuse_path() {
    let _guard = DESTRUCTOR_TEST_LOCK.lock().unwrap();
    reset_destructor_calls();
    let text = unsafe { malloc_copy(b"xyz") };
    sqlite3_result_text(
        null_mut(),
        text as *const c_char,
        3,
        custom_destructor_ptr(),
    );
    assert_eq!(destructor_calls(), 1);
}

#[test]
fn result_blob_calls_custom_destructor_on_misuse_path() {
    let _guard = DESTRUCTOR_TEST_LOCK.lock().unwrap();
    reset_destructor_calls();
    let blob = unsafe { malloc_copy(&[9_u8, 8_u8, 7_u8]) };
    sqlite3_result_blob(null_mut(), blob, 3, custom_destructor_ptr());
    assert_eq!(destructor_calls(), 1);
}

#[test]
fn bind_text_calls_custom_destructor_on_success_path() {
    let _guard = DESTRUCTOR_TEST_LOCK.lock().unwrap();
    reset_destructor_calls();
    let mut db: *mut sqlite3 = null_mut();
    let rc = sqlite3_open(CString::new(":memory:").unwrap().as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select ?1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);

    let text = unsafe { malloc_copy(b"abc") };
    let rc = sqlite3_bind_text(stmt, 1, text as *const c_char, 3, custom_destructor_ptr());
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(destructor_calls(), 1);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn bind_blob_calls_custom_destructor_on_success_path() {
    let _guard = DESTRUCTOR_TEST_LOCK.lock().unwrap();
    reset_destructor_calls();
    let mut db: *mut sqlite3 = null_mut();
    let rc = sqlite3_open(CString::new(":memory:").unwrap().as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select ?1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);

    let blob = unsafe { malloc_copy(&[1_u8, 2_u8, 3_u8]) };
    let rc = sqlite3_bind_blob(stmt, 1, blob, 3, custom_destructor_ptr());
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(destructor_calls(), 1);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn bind_blob_null_pointer_binds_sql_null() {
    let mut db: *mut sqlite3 = null_mut();
    let rc = sqlite3_open(CString::new(":memory:").unwrap().as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select ?1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);

    let rc = sqlite3_bind_blob(stmt, 1, null(), 0, SQLITE_TRANSIENT as *mut c_void);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_step(stmt);
    assert_eq!(rc, SQLITE_ROW);
    assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_NULL);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

extern "C" fn result_blob_null_scalar(ctx: *mut c_void, _argc: i32, _argv: *mut *mut c_void) {
    sqlite3_result_blob(ctx, null(), 0, SQLITE_TRANSIENT as *mut c_void);
}

#[test]
fn result_blob_null_pointer_returns_sql_null() {
    let mut db: *mut sqlite3 = null_mut();
    let rc = sqlite3_open(CString::new(":memory:").unwrap().as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);
    let fn_name = CString::new("result_blob_null").unwrap();
    let rc = sqlite3_create_function_v2(
        db,
        fn_name.as_ptr(),
        0,
        SQLITE_UTF8,
        null_mut(),
        Some(result_blob_null_scalar),
        None,
        None,
        None,
    );
    assert_eq!(rc, SQLITE_OK);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select result_blob_null()").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_step(stmt);
    assert_eq!(rc, SQLITE_ROW);
    assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_NULL);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn bind_text_transient_does_not_invoke_destructor() {
    let _guard = DESTRUCTOR_TEST_LOCK.lock().unwrap();
    reset_destructor_calls();
    let text = b"abc";
    let rc = sqlite3_bind_text(
        null_mut(),
        1,
        text.as_ptr() as *const c_char,
        text.len() as i32,
        SQLITE_TRANSIENT as *mut c_void,
    );
    assert_eq!(rc, SQLITE_MISUSE);
    assert_eq!(destructor_calls(), 0);
}
