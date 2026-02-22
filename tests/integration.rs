#![allow(unsafe_op_in_unsafe_fn)]

use std::collections::HashMap;
use std::ffi::{CStr, CString};
use std::ptr::NonNull;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use sqlite_provider::*;

#[derive(Clone, Debug, PartialEq)]
enum MockValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    TextBytes(Vec<u8>),
    Blob(Vec<u8>),
}

#[derive(Clone, Debug, PartialEq)]
enum MockResult {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Blob(Vec<u8>),
    Error(String),
}

struct MockContext {
    user_data: *mut std::ffi::c_void,
    result: Option<MockResult>,
    agg_storage: Option<Vec<usize>>,
    max_agg_request: usize,
}

impl MockContext {
    fn new(user_data: *mut std::ffi::c_void) -> Self {
        Self {
            user_data,
            result: None,
            agg_storage: None,
            max_agg_request: 0,
        }
    }
}

struct MockStmt {
    rows: Vec<Vec<MockValue>>,
    row_index: usize,
    current_row: Option<usize>,
    binds: Vec<(i32, MockValue)>,
}

impl MockStmt {
    fn new(rows: Vec<Vec<MockValue>>) -> Self {
        Self {
            rows,
            row_index: 0,
            current_row: None,
            binds: Vec::new(),
        }
    }

    fn current_value(&self, col: i32) -> Option<&MockValue> {
        let row = self.current_row?;
        self.rows.get(row)?.get(col as usize)
    }
}

struct MockBackup {
    remaining: i32,
    pagecount: i32,
}

struct MockBlob {
    data: Vec<u8>,
}

struct DummyVTab;

struct DummyCursor {
    done: bool,
}

#[derive(Clone, Copy)]
enum DummyPanicSite {
    None = 0,
    Connect = 1,
    BestIndex = 2,
    Disconnect = 3,
    Open = 4,
    Filter = 5,
    Next = 6,
    Eof = 7,
    Column = 8,
    Rowid = 9,
}

static DUMMY_PANIC_SITE: AtomicUsize = AtomicUsize::new(DummyPanicSite::None as usize);
static DUMMY_CONNECT_ERROR: AtomicBool = AtomicBool::new(false);
static DUMMY_STATE_LOCK: Mutex<()> = Mutex::new(());

struct DummyStateGuard {
    _lock: std::sync::MutexGuard<'static, ()>,
}

impl DummyStateGuard {
    fn new() -> Self {
        let lock = DUMMY_STATE_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        set_dummy_panic(DummyPanicSite::None);
        set_dummy_connect_error(false);
        Self { _lock: lock }
    }
}

impl Drop for DummyStateGuard {
    fn drop(&mut self) {
        set_dummy_panic(DummyPanicSite::None);
        set_dummy_connect_error(false);
    }
}

fn set_dummy_panic(site: DummyPanicSite) {
    DUMMY_PANIC_SITE.store(site as usize, Ordering::SeqCst);
}

fn set_dummy_connect_error(enable: bool) {
    DUMMY_CONNECT_ERROR.store(enable, Ordering::SeqCst);
}

fn panic_if(site: DummyPanicSite) {
    if DUMMY_PANIC_SITE.load(Ordering::SeqCst) == site as usize {
        panic!("dummy vtab panic");
    }
}

impl VirtualTable<MockApi> for DummyVTab {
    type Cursor = DummyCursor;
    type Error = Error;

    fn connect(_args: &[&str]) -> core::result::Result<(Self, String), Self::Error> {
        if DUMMY_CONNECT_ERROR.load(Ordering::SeqCst) {
            return Err(Error::with_message(
                ErrorCode::Error,
                "dummy connect failed",
            ));
        }
        panic_if(DummyPanicSite::Connect);
        Ok((DummyVTab, "create table x(a int)".to_string()))
    }

    fn disconnect(self) -> core::result::Result<(), Self::Error> {
        panic_if(DummyPanicSite::Disconnect);
        Ok(())
    }

    fn best_index(&self, _info: &mut BestIndexInfo) -> core::result::Result<(), Self::Error> {
        panic_if(DummyPanicSite::BestIndex);
        Ok(())
    }

    fn open(&self) -> core::result::Result<Self::Cursor, Self::Error> {
        panic_if(DummyPanicSite::Open);
        Ok(DummyCursor { done: true })
    }
}

impl VTabCursor<MockApi> for DummyCursor {
    type Error = Error;

    fn filter(
        &mut self,
        _idx_num: i32,
        _idx_str: Option<&str>,
        _args: &[ValueRef<'_>],
    ) -> core::result::Result<(), Self::Error> {
        panic_if(DummyPanicSite::Filter);
        self.done = true;
        Ok(())
    }

    fn next(&mut self) -> core::result::Result<(), Self::Error> {
        panic_if(DummyPanicSite::Next);
        self.done = true;
        Ok(())
    }

    fn eof(&self) -> bool {
        panic_if(DummyPanicSite::Eof);
        self.done
    }

    fn column(
        &self,
        ctx: &Context<'_, MockApi>,
        _col: i32,
    ) -> core::result::Result<(), Self::Error> {
        panic_if(DummyPanicSite::Column);
        ctx.result_null();
        Ok(())
    }

    fn rowid(&self) -> core::result::Result<i64, Self::Error> {
        panic_if(DummyPanicSite::Rowid);
        Ok(0)
    }
}

#[derive(Default)]
struct FunctionRegistry {
    last: Option<FunctionRegistration>,
}

#[allow(dead_code)]
struct FunctionRegistration {
    x_func: Option<extern "C" fn(*mut MockContext, i32, *mut *mut MockValue)>,
    x_step: Option<extern "C" fn(*mut MockContext, i32, *mut *mut MockValue)>,
    x_final: Option<extern "C" fn(*mut MockContext)>,
    x_value: Option<extern "C" fn(*mut MockContext)>,
    x_inverse: Option<extern "C" fn(*mut MockContext, i32, *mut *mut MockValue)>,
    user_data: *mut std::ffi::c_void,
    drop_user_data: Option<extern "C" fn(*mut std::ffi::c_void)>,
}

#[derive(Clone, Copy, Default)]
#[allow(dead_code)]
struct TraceReg {
    mask: u32,
    callback: Option<
        extern "C" fn(u32, *mut std::ffi::c_void, *mut std::ffi::c_void, *mut std::ffi::c_void),
    >,
    context: *mut std::ffi::c_void,
}

#[derive(Clone, Copy, Default)]
#[allow(dead_code)]
struct AuthorizerReg {
    callback: Option<
        extern "C" fn(
            *mut std::ffi::c_void,
            i32,
            *const std::ffi::c_char,
            *const std::ffi::c_char,
            *const std::ffi::c_char,
            *const std::ffi::c_char,
        ) -> i32,
    >,
    context: *mut std::ffi::c_void,
}

#[derive(Clone, Copy, Default)]
#[allow(dead_code)]
struct ProgressReg {
    n: i32,
    callback: Option<extern "C" fn(*mut std::ffi::c_void) -> i32>,
    context: *mut std::ffi::c_void,
}

struct MockDb {
    functions: Mutex<FunctionRegistry>,
    trace: Mutex<TraceReg>,
    authorizer: Mutex<AuthorizerReg>,
    progress: Mutex<ProgressReg>,
}

impl MockDb {
    fn new() -> Self {
        Self {
            functions: Mutex::new(FunctionRegistry::default()),
            trace: Mutex::new(TraceReg::default()),
            authorizer: Mutex::new(AuthorizerReg::default()),
            progress: Mutex::new(ProgressReg::default()),
        }
    }
}

struct MockApi {
    features: FeatureSet,
    prepare_v2_calls: AtomicUsize,
    prepare_v3_calls: AtomicUsize,
    close_calls: AtomicUsize,
    key_calls: AtomicUsize,
    rekey_calls: AtomicUsize,
    key_should_fail: AtomicBool,
    next_rows: Mutex<Vec<Vec<MockValue>>>,
    serialize_frees: AtomicUsize,
    create_module_calls: AtomicUsize,
    module_ptrs: Mutex<Vec<usize>>,
    function_registration_should_fail: AtomicBool,
    window_registration_should_fail: AtomicBool,
    trace_should_fail: AtomicBool,
    authorizer_should_fail: AtomicBool,
    progress_should_fail: AtomicBool,
    declare_vtab_should_fail: AtomicBool,
    backup_finish_calls: AtomicUsize,
    blob_close_calls: AtomicUsize,
    wal_checkpoint_calls: Mutex<Vec<Option<String>>>,
    wal_checkpoint_v2_calls: Mutex<Vec<(Option<String>, i32)>>,
    metadata_calls: Mutex<Vec<(Option<String>, String, String)>>,
    allocations: Mutex<HashMap<usize, Box<[u8]>>>,
}

impl MockApi {
    fn new(features: FeatureSet) -> Self {
        Self {
            features,
            prepare_v2_calls: AtomicUsize::new(0),
            prepare_v3_calls: AtomicUsize::new(0),
            close_calls: AtomicUsize::new(0),
            key_calls: AtomicUsize::new(0),
            rekey_calls: AtomicUsize::new(0),
            key_should_fail: AtomicBool::new(false),
            next_rows: Mutex::new(Vec::new()),
            serialize_frees: AtomicUsize::new(0),
            create_module_calls: AtomicUsize::new(0),
            module_ptrs: Mutex::new(Vec::new()),
            function_registration_should_fail: AtomicBool::new(false),
            window_registration_should_fail: AtomicBool::new(false),
            trace_should_fail: AtomicBool::new(false),
            authorizer_should_fail: AtomicBool::new(false),
            progress_should_fail: AtomicBool::new(false),
            declare_vtab_should_fail: AtomicBool::new(false),
            backup_finish_calls: AtomicUsize::new(0),
            blob_close_calls: AtomicUsize::new(0),
            wal_checkpoint_calls: Mutex::new(Vec::new()),
            wal_checkpoint_v2_calls: Mutex::new(Vec::new()),
            metadata_calls: Mutex::new(Vec::new()),
            allocations: Mutex::new(HashMap::new()),
        }
    }

    fn prepare_v2_count(&self) -> usize {
        self.prepare_v2_calls.load(Ordering::SeqCst)
    }

    fn prepare_v3_count(&self) -> usize {
        self.prepare_v3_calls.load(Ordering::SeqCst)
    }

    fn close_count(&self) -> usize {
        self.close_calls.load(Ordering::SeqCst)
    }

    fn key_count(&self) -> usize {
        self.key_calls.load(Ordering::SeqCst)
    }

    fn rekey_count(&self) -> usize {
        self.rekey_calls.load(Ordering::SeqCst)
    }

    fn set_key_failure(&self, fail: bool) {
        self.key_should_fail.store(fail, Ordering::SeqCst);
    }

    fn set_rows(&self, rows: Vec<Vec<MockValue>>) {
        let mut guard = self.next_rows.lock().unwrap();
        *guard = rows;
    }

    fn serialize_free_count(&self) -> usize {
        self.serialize_frees.load(Ordering::SeqCst)
    }

    fn create_module_count(&self) -> usize {
        self.create_module_calls.load(Ordering::SeqCst)
    }

    fn backup_finish_count(&self) -> usize {
        self.backup_finish_calls.load(Ordering::SeqCst)
    }

    fn blob_close_count(&self) -> usize {
        self.blob_close_calls.load(Ordering::SeqCst)
    }

    fn wal_checkpoint_log(&self) -> Vec<Option<String>> {
        self.wal_checkpoint_calls.lock().unwrap().clone()
    }

    fn wal_checkpoint_v2_log(&self) -> Vec<(Option<String>, i32)> {
        self.wal_checkpoint_v2_calls.lock().unwrap().clone()
    }

    fn metadata_log(&self) -> Vec<(Option<String>, String, String)> {
        self.metadata_calls.lock().unwrap().clone()
    }

    fn module_ptrs(&self) -> Vec<usize> {
        self.module_ptrs.lock().unwrap().clone()
    }

    fn set_function_registration_failure(&self, fail: bool) {
        self.function_registration_should_fail
            .store(fail, Ordering::SeqCst);
    }

    fn set_window_registration_failure(&self, fail: bool) {
        self.window_registration_should_fail
            .store(fail, Ordering::SeqCst);
    }

    fn set_trace_failure(&self, fail: bool) {
        self.trace_should_fail.store(fail, Ordering::SeqCst);
    }

    fn set_authorizer_failure(&self, fail: bool) {
        self.authorizer_should_fail.store(fail, Ordering::SeqCst);
    }

    fn set_progress_failure(&self, fail: bool) {
        self.progress_should_fail.store(fail, Ordering::SeqCst);
    }

    fn set_declare_vtab_failure(&self, fail: bool) {
        self.declare_vtab_should_fail.store(fail, Ordering::SeqCst);
    }
}

unsafe impl Sqlite3Api for MockApi {
    type Db = MockDb;
    type Stmt = MockStmt;
    type Value = MockValue;
    type Context = MockContext;
    type VTab = VTab<MockApi, DummyVTab>;
    type VTabCursor = Cursor<MockApi, DummyCursor>;

    fn api_version(&self) -> ApiVersion {
        ApiVersion::new(3, 44, 0)
    }

    fn feature_set(&self) -> FeatureSet {
        self.features
    }

    fn backend_name(&self) -> &'static str {
        "mock"
    }

    fn backend_version(&self) -> Option<ApiVersion> {
        None
    }

    unsafe fn malloc(&self, size: usize) -> *mut std::ffi::c_void {
        let mut block = vec![0_u8; size.max(1)].into_boxed_slice();
        let ptr = block.as_mut_ptr();
        self.allocations.lock().unwrap().insert(ptr as usize, block);
        ptr as *mut std::ffi::c_void
    }

    unsafe fn free(&self, ptr: *mut std::ffi::c_void) {
        if ptr.is_null() {
            return;
        }
        let _ = self.allocations.lock().unwrap().remove(&(ptr as usize));
    }

    unsafe fn open(&self, _filename: &str, _options: OpenOptions<'_>) -> Result<NonNull<Self::Db>> {
        let db = Box::new(MockDb::new());
        Ok(NonNull::new_unchecked(Box::into_raw(db)))
    }

    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()> {
        self.close_calls.fetch_add(1, Ordering::SeqCst);
        drop(Box::from_raw(db.as_ptr()));
        Ok(())
    }

    unsafe fn prepare_v2(&self, _db: NonNull<Self::Db>, _sql: &str) -> Result<NonNull<Self::Stmt>> {
        self.prepare_v2_calls.fetch_add(1, Ordering::SeqCst);
        let rows = std::mem::take(&mut *self.next_rows.lock().unwrap());
        let stmt = Box::new(MockStmt::new(rows));
        Ok(NonNull::new_unchecked(Box::into_raw(stmt)))
    }

    unsafe fn prepare_v3(
        &self,
        _db: NonNull<Self::Db>,
        _sql: &str,
        _flags: u32,
    ) -> Result<NonNull<Self::Stmt>> {
        self.prepare_v3_calls.fetch_add(1, Ordering::SeqCst);
        let rows = std::mem::take(&mut *self.next_rows.lock().unwrap());
        let stmt = Box::new(MockStmt::new(rows));
        Ok(NonNull::new_unchecked(Box::into_raw(stmt)))
    }

    unsafe fn step(&self, stmt: NonNull<Self::Stmt>) -> Result<StepResult> {
        let stmt = &mut *stmt.as_ptr();
        if stmt.row_index < stmt.rows.len() {
            stmt.current_row = Some(stmt.row_index);
            stmt.row_index += 1;
            return Ok(StepResult::Row);
        }
        stmt.current_row = None;
        Ok(StepResult::Done)
    }

    unsafe fn reset(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.row_index = 0;
        stmt.current_row = None;
        Ok(())
    }

    unsafe fn finalize(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        drop(Box::from_raw(stmt.as_ptr()));
        Ok(())
    }

    unsafe fn bind_null(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.binds.push((idx, MockValue::Null));
        Ok(())
    }

    unsafe fn bind_int64(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: i64) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.binds.push((idx, MockValue::Integer(v)));
        Ok(())
    }

    unsafe fn bind_double(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: f64) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.binds.push((idx, MockValue::Float(v)));
        Ok(())
    }

    unsafe fn bind_text(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &str) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.binds.push((idx, MockValue::Text(v.to_owned())));
        Ok(())
    }

    unsafe fn bind_blob(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &[u8]) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.binds.push((idx, MockValue::Blob(v.to_vec())));
        Ok(())
    }

    unsafe fn column_count(&self, stmt: NonNull<Self::Stmt>) -> i32 {
        let stmt = &*stmt.as_ptr();
        stmt.rows.first().map(|row| row.len() as i32).unwrap_or(0)
    }

    unsafe fn column_type(&self, stmt: NonNull<Self::Stmt>, col: i32) -> ValueType {
        let stmt = &*stmt.as_ptr();
        match stmt.current_value(col) {
            Some(MockValue::Null) | None => ValueType::Null,
            Some(MockValue::Integer(_)) => ValueType::Integer,
            Some(MockValue::Float(_)) => ValueType::Float,
            Some(MockValue::Text(_)) | Some(MockValue::TextBytes(_)) => ValueType::Text,
            Some(MockValue::Blob(_)) => ValueType::Blob,
        }
    }

    unsafe fn column_int64(&self, stmt: NonNull<Self::Stmt>, col: i32) -> i64 {
        let stmt = &*stmt.as_ptr();
        match stmt.current_value(col) {
            Some(MockValue::Integer(v)) => *v,
            _ => 0,
        }
    }

    unsafe fn column_double(&self, stmt: NonNull<Self::Stmt>, col: i32) -> f64 {
        let stmt = &*stmt.as_ptr();
        match stmt.current_value(col) {
            Some(MockValue::Float(v)) => *v,
            _ => 0.0,
        }
    }

    unsafe fn column_text(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let stmt = &*stmt.as_ptr();
        match stmt.current_value(col) {
            Some(MockValue::Text(v)) => RawBytes {
                ptr: v.as_ptr(),
                len: v.len(),
            },
            Some(MockValue::TextBytes(v)) => RawBytes {
                ptr: v.as_ptr(),
                len: v.len(),
            },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn column_blob(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let stmt = &*stmt.as_ptr();
        match stmt.current_value(col) {
            Some(MockValue::Blob(v)) => RawBytes {
                ptr: v.as_ptr(),
                len: v.len(),
            },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn errcode(&self, _db: NonNull<Self::Db>) -> i32 {
        0
    }

    unsafe fn errmsg(&self, _db: NonNull<Self::Db>) -> *const std::ffi::c_char {
        std::ptr::null()
    }

    unsafe fn extended_errcode(&self, _db: NonNull<Self::Db>) -> Option<i32> {
        None
    }

    unsafe fn create_function_v2(
        &self,
        db: NonNull<Self::Db>,
        _name: &str,
        _n_args: i32,
        _flags: FunctionFlags,
        x_func: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_final: Option<extern "C" fn(*mut Self::Context)>,
        user_data: *mut std::ffi::c_void,
        drop_user_data: Option<extern "C" fn(*mut std::ffi::c_void)>,
    ) -> Result<()> {
        if self
            .function_registration_should_fail
            .load(Ordering::SeqCst)
        {
            if let Some(drop_user_data) = drop_user_data {
                drop_user_data(user_data);
            }
            return Err(Error::with_message(
                ErrorCode::Busy,
                "function registration failed",
            ));
        }
        let reg = FunctionRegistration {
            x_func,
            x_step,
            x_final,
            x_value: None,
            x_inverse: None,
            user_data,
            drop_user_data,
        };
        let db = &mut *db.as_ptr();
        db.functions.lock().unwrap().last = Some(reg);
        Ok(())
    }

    unsafe fn create_window_function(
        &self,
        db: NonNull<Self::Db>,
        _name: &str,
        _n_args: i32,
        _flags: FunctionFlags,
        x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_final: Option<extern "C" fn(*mut Self::Context)>,
        x_value: Option<extern "C" fn(*mut Self::Context)>,
        x_inverse: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        user_data: *mut std::ffi::c_void,
        drop_user_data: Option<extern "C" fn(*mut std::ffi::c_void)>,
    ) -> Result<()> {
        if self.window_registration_should_fail.load(Ordering::SeqCst) {
            if let Some(drop_user_data) = drop_user_data {
                drop_user_data(user_data);
            }
            return Err(Error::with_message(
                ErrorCode::Busy,
                "window function registration failed",
            ));
        }
        let reg = FunctionRegistration {
            x_func: None,
            x_step,
            x_final,
            x_value,
            x_inverse,
            user_data,
            drop_user_data,
        };
        let db = &mut *db.as_ptr();
        db.functions.lock().unwrap().last = Some(reg);
        Ok(())
    }

    unsafe fn aggregate_context(
        &self,
        ctx: NonNull<Self::Context>,
        bytes: usize,
    ) -> *mut std::ffi::c_void {
        let ctx = &mut *ctx.as_ptr();
        ctx.max_agg_request = ctx.max_agg_request.max(bytes);
        if bytes == 0 {
            return ctx
                .agg_storage
                .as_mut()
                .map(|buf| buf.as_mut_ptr() as *mut std::ffi::c_void)
                .unwrap_or(std::ptr::null_mut());
        }
        let need = bytes.max(1);
        let elem = std::mem::size_of::<usize>();
        let elems = need.div_ceil(elem);
        let replace = match ctx.agg_storage.as_ref() {
            Some(buf) => buf.len() < elems,
            None => true,
        };
        if replace {
            ctx.agg_storage = Some(vec![0usize; elems]);
        }
        ctx.agg_storage
            .as_mut()
            .map(|buf| buf.as_mut_ptr() as *mut std::ffi::c_void)
            .unwrap_or(std::ptr::null_mut())
    }

    unsafe fn result_null(&self, ctx: NonNull<Self::Context>) {
        let ctx = &mut *ctx.as_ptr();
        ctx.result = Some(MockResult::Null);
    }

    unsafe fn result_int64(&self, ctx: NonNull<Self::Context>, v: i64) {
        let ctx = &mut *ctx.as_ptr();
        ctx.result = Some(MockResult::Integer(v));
    }

    unsafe fn result_double(&self, ctx: NonNull<Self::Context>, v: f64) {
        let ctx = &mut *ctx.as_ptr();
        ctx.result = Some(MockResult::Float(v));
    }

    unsafe fn result_text(&self, ctx: NonNull<Self::Context>, v: &str) {
        let ctx = &mut *ctx.as_ptr();
        ctx.result = Some(MockResult::Text(v.to_owned()));
    }

    unsafe fn result_blob(&self, ctx: NonNull<Self::Context>, v: &[u8]) {
        let ctx = &mut *ctx.as_ptr();
        ctx.result = Some(MockResult::Blob(v.to_vec()));
    }

    unsafe fn result_error(&self, ctx: NonNull<Self::Context>, msg: &str) {
        let ctx = &mut *ctx.as_ptr();
        ctx.result = Some(MockResult::Error(msg.to_owned()));
    }

    unsafe fn user_data(ctx: NonNull<Self::Context>) -> *mut std::ffi::c_void {
        let ctx = &*ctx.as_ptr();
        ctx.user_data
    }

    unsafe fn value_type(&self, v: NonNull<Self::Value>) -> ValueType {
        match &*v.as_ptr() {
            MockValue::Null => ValueType::Null,
            MockValue::Integer(_) => ValueType::Integer,
            MockValue::Float(_) => ValueType::Float,
            MockValue::Text(_) | MockValue::TextBytes(_) => ValueType::Text,
            MockValue::Blob(_) => ValueType::Blob,
        }
    }

    unsafe fn value_int64(&self, v: NonNull<Self::Value>) -> i64 {
        match &*v.as_ptr() {
            MockValue::Integer(val) => *val,
            _ => 0,
        }
    }

    unsafe fn value_double(&self, v: NonNull<Self::Value>) -> f64 {
        match &*v.as_ptr() {
            MockValue::Float(val) => *val,
            _ => 0.0,
        }
    }

    unsafe fn value_text(&self, v: NonNull<Self::Value>) -> RawBytes {
        match &*v.as_ptr() {
            MockValue::Text(val) => RawBytes {
                ptr: val.as_ptr(),
                len: val.len(),
            },
            MockValue::TextBytes(val) => RawBytes {
                ptr: val.as_ptr(),
                len: val.len(),
            },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn value_blob(&self, v: NonNull<Self::Value>) -> RawBytes {
        match &*v.as_ptr() {
            MockValue::Blob(val) => RawBytes {
                ptr: val.as_ptr(),
                len: val.len(),
            },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn declare_vtab(&self, _db: NonNull<Self::Db>, _schema: &str) -> Result<()> {
        if self.declare_vtab_should_fail.load(Ordering::SeqCst) {
            return Err(Error::with_message(ErrorCode::Error, "declare_vtab failed"));
        }
        Ok(())
    }

    unsafe fn create_module_v2(
        &self,
        _db: NonNull<Self::Db>,
        _name: &str,
        module: &'static sqlite3_module<Self>,
        _user_data: *mut std::ffi::c_void,
        _drop_user_data: Option<extern "C" fn(*mut std::ffi::c_void)>,
    ) -> Result<()> {
        self.create_module_calls.fetch_add(1, Ordering::SeqCst);
        self.module_ptrs
            .lock()
            .unwrap()
            .push(module as *const sqlite3_module<Self> as usize);
        Ok(())
    }
}

unsafe impl Sqlite3Keying for MockApi {
    unsafe fn key(&self, _db: NonNull<Self::Db>, _key: &[u8]) -> Result<()> {
        self.key_calls.fetch_add(1, Ordering::SeqCst);
        if self.key_should_fail.load(Ordering::SeqCst) {
            return Err(Error::with_message(ErrorCode::Auth, "keying failed"));
        }
        Ok(())
    }

    unsafe fn rekey(&self, _db: NonNull<Self::Db>, _key: &[u8]) -> Result<()> {
        self.rekey_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

unsafe impl Sqlite3Hooks for MockApi {
    unsafe fn trace_v2(
        &self,
        db: NonNull<Self::Db>,
        mask: u32,
        callback: Option<
            extern "C" fn(u32, *mut std::ffi::c_void, *mut std::ffi::c_void, *mut std::ffi::c_void),
        >,
        context: *mut std::ffi::c_void,
    ) -> Result<()> {
        if self.trace_should_fail.load(Ordering::SeqCst) {
            return Err(Error::with_message(
                ErrorCode::Busy,
                "trace registration failed",
            ));
        }
        let db = &mut *db.as_ptr();
        *db.trace.lock().unwrap() = TraceReg {
            mask,
            callback,
            context,
        };
        Ok(())
    }

    unsafe fn progress_handler(
        &self,
        db: NonNull<Self::Db>,
        n: i32,
        callback: Option<extern "C" fn(*mut std::ffi::c_void) -> i32>,
        context: *mut std::ffi::c_void,
    ) -> Result<()> {
        if self.progress_should_fail.load(Ordering::SeqCst) {
            return Err(Error::with_message(
                ErrorCode::Busy,
                "progress registration failed",
            ));
        }
        let db = &mut *db.as_ptr();
        *db.progress.lock().unwrap() = ProgressReg {
            n,
            callback,
            context,
        };
        Ok(())
    }

    unsafe fn busy_timeout(&self, _db: NonNull<Self::Db>, _ms: i32) -> Result<()> {
        Ok(())
    }

    unsafe fn set_authorizer(
        &self,
        db: NonNull<Self::Db>,
        callback: Option<
            extern "C" fn(
                *mut std::ffi::c_void,
                i32,
                *const std::ffi::c_char,
                *const std::ffi::c_char,
                *const std::ffi::c_char,
                *const std::ffi::c_char,
            ) -> i32,
        >,
        context: *mut std::ffi::c_void,
    ) -> Result<()> {
        if self.authorizer_should_fail.load(Ordering::SeqCst) {
            return Err(Error::with_message(
                ErrorCode::Busy,
                "authorizer registration failed",
            ));
        }
        let db = &mut *db.as_ptr();
        *db.authorizer.lock().unwrap() = AuthorizerReg { callback, context };
        Ok(())
    }
}

unsafe impl Sqlite3Backup for MockApi {
    type Backup = MockBackup;

    unsafe fn backup_init(
        &self,
        _dest_db: NonNull<Self::Db>,
        _dest_name: &str,
        _source_db: NonNull<Self::Db>,
        _source_name: &str,
    ) -> Result<NonNull<Self::Backup>> {
        let backup = Box::new(MockBackup {
            remaining: 12,
            pagecount: 34,
        });
        Ok(NonNull::new_unchecked(Box::into_raw(backup)))
    }

    unsafe fn backup_step(&self, backup: NonNull<Self::Backup>, pages: i32) -> Result<()> {
        let backup = &mut *backup.as_ptr();
        if pages > 0 {
            backup.remaining = backup.remaining.saturating_sub(pages);
        }
        Ok(())
    }

    unsafe fn backup_remaining(&self, backup: NonNull<Self::Backup>) -> i32 {
        (*backup.as_ptr()).remaining
    }

    unsafe fn backup_pagecount(&self, backup: NonNull<Self::Backup>) -> i32 {
        (*backup.as_ptr()).pagecount
    }

    unsafe fn backup_finish(&self, backup: NonNull<Self::Backup>) -> Result<()> {
        self.backup_finish_calls.fetch_add(1, Ordering::SeqCst);
        drop(Box::from_raw(backup.as_ptr()));
        Ok(())
    }
}

unsafe impl Sqlite3BlobIo for MockApi {
    type Blob = MockBlob;

    unsafe fn blob_open(
        &self,
        _db: NonNull<Self::Db>,
        _db_name: &str,
        _table: &str,
        _column: &str,
        _rowid: i64,
        _flags: u32,
    ) -> Result<NonNull<Self::Blob>> {
        let blob = Box::new(MockBlob {
            data: vec![1_u8, 2_u8, 3_u8, 4_u8],
        });
        Ok(NonNull::new_unchecked(Box::into_raw(blob)))
    }

    unsafe fn blob_read(
        &self,
        blob: NonNull<Self::Blob>,
        data: &mut [u8],
        offset: i32,
    ) -> Result<()> {
        let blob = &*blob.as_ptr();
        let start = if offset < 0 {
            return Err(Error::with_message(
                ErrorCode::Range,
                "blob read out of range",
            ));
        } else {
            offset as usize
        };
        let end = start.saturating_add(data.len());
        if end > blob.data.len() {
            return Err(Error::with_message(
                ErrorCode::Range,
                "blob read out of range",
            ));
        }
        data.copy_from_slice(&blob.data[start..end]);
        Ok(())
    }

    unsafe fn blob_write(&self, blob: NonNull<Self::Blob>, data: &[u8], offset: i32) -> Result<()> {
        let blob = &mut *blob.as_ptr();
        let start = if offset < 0 {
            return Err(Error::with_message(
                ErrorCode::Range,
                "blob write out of range",
            ));
        } else {
            offset as usize
        };
        let end = start.saturating_add(data.len());
        if end > blob.data.len() {
            return Err(Error::with_message(
                ErrorCode::Range,
                "blob write out of range",
            ));
        }
        blob.data[start..end].copy_from_slice(data);
        Ok(())
    }

    unsafe fn blob_bytes(&self, blob: NonNull<Self::Blob>) -> i32 {
        (*blob.as_ptr()).data.len() as i32
    }

    unsafe fn blob_close(&self, blob: NonNull<Self::Blob>) -> Result<()> {
        self.blob_close_calls.fetch_add(1, Ordering::SeqCst);
        drop(Box::from_raw(blob.as_ptr()));
        Ok(())
    }
}

unsafe impl Sqlite3Wal for MockApi {
    unsafe fn wal_checkpoint(&self, _db: NonNull<Self::Db>, db_name: Option<&str>) -> Result<()> {
        self.wal_checkpoint_calls
            .lock()
            .unwrap()
            .push(db_name.map(str::to_owned));
        Ok(())
    }

    unsafe fn wal_checkpoint_v2(
        &self,
        _db: NonNull<Self::Db>,
        db_name: Option<&str>,
        mode: i32,
    ) -> Result<(i32, i32)> {
        self.wal_checkpoint_v2_calls
            .lock()
            .unwrap()
            .push((db_name.map(str::to_owned), mode));
        Ok((8, 5))
    }

    unsafe fn wal_frame_count(&self, _db: NonNull<Self::Db>) -> Result<Option<u32>> {
        Ok(Some(42))
    }
}

unsafe impl Sqlite3Metadata for MockApi {
    unsafe fn table_column_metadata(
        &self,
        _db: NonNull<Self::Db>,
        db_name: Option<&str>,
        table: &str,
        column: &str,
    ) -> Result<ColumnMetadata> {
        self.metadata_calls.lock().unwrap().push((
            db_name.map(str::to_owned),
            table.to_owned(),
            column.to_owned(),
        ));
        Ok(ColumnMetadata {
            data_type: Some(RawBytes {
                ptr: b"INTEGER".as_ptr(),
                len: b"INTEGER".len(),
            }),
            coll_seq: Some(RawBytes {
                ptr: b"BINARY".as_ptr(),
                len: b"BINARY".len(),
            }),
            not_null: true,
            primary_key: false,
            autoinc: false,
        })
    }

    unsafe fn column_decltype(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> Option<RawBytes> {
        Some(RawBytes {
            ptr: b"INTEGER".as_ptr(),
            len: b"INTEGER".len(),
        })
    }

    unsafe fn column_name(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> Option<RawBytes> {
        Some(RawBytes {
            ptr: b"value".as_ptr(),
            len: b"value".len(),
        })
    }

    unsafe fn column_table_name(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> Option<RawBytes> {
        Some(RawBytes {
            ptr: b"dummy_table".as_ptr(),
            len: b"dummy_table".len(),
        })
    }
}

unsafe impl Sqlite3Serialize for MockApi {
    unsafe fn serialize(
        &self,
        _db: NonNull<Self::Db>,
        _schema: Option<&str>,
        _flags: u32,
    ) -> Result<OwnedBytes> {
        let mut bytes = vec![1u8, 2, 3];
        let ptr = NonNull::new(bytes.as_mut_ptr()).unwrap();
        let len = bytes.len();
        std::mem::forget(bytes);
        Ok(OwnedBytes { ptr, len })
    }

    unsafe fn deserialize(
        &self,
        _db: NonNull<Self::Db>,
        _schema: Option<&str>,
        _data: &[u8],
        _flags: u32,
    ) -> Result<()> {
        Ok(())
    }

    unsafe fn free(&self, bytes: OwnedBytes) {
        let _ = Vec::from_raw_parts(bytes.ptr.as_ptr(), bytes.len, bytes.len);
        self.serialize_frees.fetch_add(1, Ordering::SeqCst);
    }
}

fn open_conn(api: &MockApi) -> Connection<'_, MockApi> {
    Connection::open(
        api,
        ":memory:",
        OpenOptions {
            flags: OpenFlags::empty(),
            vfs: None,
        },
    )
    .unwrap()
}

fn module_for_dummy(
    api: &MockApi,
    conn: &Connection<'_, MockApi>,
) -> &'static sqlite3_module<MockApi> {
    conn.create_module::<DummyVTab>("dummy").unwrap();
    let ptr = *api.module_ptrs().last().expect("module pointer") as *const sqlite3_module<MockApi>;
    unsafe { &*ptr }
}

struct DropCounter(Arc<AtomicUsize>);

impl Drop for DropCounter {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[repr(align(64))]
struct OverAlignedState {
    total: i64,
}

#[test]
fn prepare_uses_v3_when_available() {
    let api = MockApi::new(FeatureSet::PREPARE_V3);
    let conn = open_conn(&api);
    let _stmt = conn.prepare("select 1").unwrap();
    assert_eq!(api.prepare_v3_count(), 1);
    assert_eq!(api.prepare_v2_count(), 0);
}

#[test]
fn prepare_falls_back_to_v2() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let _stmt = conn.prepare("select 1").unwrap();
    assert_eq!(api.prepare_v3_count(), 0);
    assert_eq!(api.prepare_v2_count(), 1);
}

#[test]
fn open_with_key_and_rekey_forward_to_keying_spi() {
    let api = MockApi::new(FeatureSet::empty());
    {
        let conn = Connection::open_with_key(
            &api,
            ":memory:",
            OpenOptions {
                flags: OpenFlags::empty(),
                vfs: None,
            },
            b"initial-key",
        )
        .unwrap();
        assert_eq!(api.key_count(), 1);
        assert_eq!(api.rekey_count(), 0);
        assert_eq!(api.close_count(), 0);

        conn.rekey(b"next-key").unwrap();
        assert_eq!(api.rekey_count(), 1);
    }
    assert_eq!(api.close_count(), 1);
}

#[test]
fn open_with_key_failure_closes_connection() {
    let api = MockApi::new(FeatureSet::empty());
    api.set_key_failure(true);
    let open = Connection::open_with_key(
        &api,
        ":memory:",
        OpenOptions {
            flags: OpenFlags::empty(),
            vfs: None,
        },
        b"bad-key",
    );
    let err = match open {
        Ok(_) => panic!("expected key failure"),
        Err(err) => err,
    };
    assert_eq!(err.code, ErrorCode::Auth);
    assert_eq!(api.key_count(), 1);
    assert_eq!(api.close_count(), 1);
}

#[test]
fn step_and_read_row() {
    let api = MockApi::new(FeatureSet::empty());
    api.set_rows(vec![vec![
        MockValue::Integer(7),
        MockValue::Text("hi".to_owned()),
    ]]);
    let conn = open_conn(&api);
    let mut stmt = conn.prepare("select 7, 'hi'").unwrap();
    let row = stmt.step().unwrap().expect("row");
    assert_eq!(row.column_int64(0), 7);
    assert_eq!(row.column_text(1), Some("hi"));
    assert!(stmt.step().unwrap().is_none());
}

#[test]
fn scalar_function_trampoline_sets_result() {
    let api = MockApi::new(FeatureSet::CREATE_FUNCTION_V2);
    let conn = open_conn(&api);
    let called = Arc::new(AtomicUsize::new(0));
    let called_ref = called.clone();

    conn.create_scalar_function("add1", 1, move |_ctx, args| {
        called_ref.fetch_add(1, Ordering::SeqCst);
        let v = args.first().and_then(|v| v.as_i64()).unwrap_or(0);
        Ok(Value::Integer(v + 1))
    })
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let func = reg.x_func.expect("x_func");

    let mut arg0 = MockValue::Integer(41);
    let mut argv = vec![&mut arg0 as *mut MockValue];
    let mut ctx = MockContext::new(reg.user_data);
    func(&mut ctx, 1, argv.as_mut_ptr());

    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(ctx.result, Some(MockResult::Integer(42)));
}

#[test]
fn aggregate_function_uses_context_state() {
    let api = MockApi::new(FeatureSet::CREATE_FUNCTION_V2);
    let conn = open_conn(&api);

    conn.create_aggregate_function(
        "sum",
        1,
        || 0i64,
        |_ctx, state, args| {
            *state += args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(())
        },
        |_ctx, state| Ok(Value::Integer(state)),
    )
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let step = reg.x_step.expect("x_step");
    let final_fn = reg.x_final.expect("x_final");

    let mut ctx = MockContext::new(reg.user_data);

    let mut arg0 = MockValue::Integer(10);
    let mut argv = vec![&mut arg0 as *mut MockValue];
    step(&mut ctx, 1, argv.as_mut_ptr());

    let mut arg1 = MockValue::Integer(5);
    let mut argv2 = vec![&mut arg1 as *mut MockValue];
    step(&mut ctx, 1, argv2.as_mut_ptr());

    final_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(15)));
}

#[test]
fn window_function_trampoline_updates_state() {
    let api = MockApi::new(FeatureSet::WINDOW_FUNCTIONS);
    let conn = open_conn(&api);

    conn.create_window_function(
        "winsum",
        1,
        || 0i64,
        |_ctx, state, args| {
            *state += args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(())
        },
        |_ctx, state, args| {
            *state -= args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(())
        },
        |_ctx, state| Ok(Value::Integer(*state)),
        |_ctx, state| Ok(Value::Integer(state)),
    )
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let step = reg.x_step.expect("x_step");
    let inverse = reg.x_inverse.expect("x_inverse");
    let value_fn = reg.x_value.expect("x_value");
    let final_fn = reg.x_final.expect("x_final");

    let mut ctx = MockContext::new(reg.user_data);

    let mut arg0 = MockValue::Integer(2);
    let mut argv = vec![&mut arg0 as *mut MockValue];
    step(&mut ctx, 1, argv.as_mut_ptr());

    let mut arg1 = MockValue::Integer(3);
    let mut argv2 = vec![&mut arg1 as *mut MockValue];
    step(&mut ctx, 1, argv2.as_mut_ptr());

    value_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(5)));

    let mut arg2 = MockValue::Integer(2);
    let mut argv3 = vec![&mut arg2 as *mut MockValue];
    inverse(&mut ctx, 1, argv3.as_mut_ptr());

    value_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(3)));

    final_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(3)));
}

#[test]
fn aggregate_function_supports_over_aligned_state() {
    let api = MockApi::new(FeatureSet::CREATE_FUNCTION_V2);
    let conn = open_conn(&api);

    conn.create_aggregate_function(
        "aligned_sum",
        1,
        || OverAlignedState { total: 0 },
        |_ctx, state, args| {
            state.total += args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(())
        },
        |_ctx, state| Ok(Value::Integer(state.total)),
    )
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let step = reg.x_step.expect("x_step");
    let final_fn = reg.x_final.expect("x_final");

    let mut ctx = MockContext::new(reg.user_data);

    let mut arg0 = MockValue::Integer(3);
    let mut argv = vec![&mut arg0 as *mut MockValue];
    step(&mut ctx, 1, argv.as_mut_ptr());

    let mut arg1 = MockValue::Integer(4);
    let mut argv2 = vec![&mut arg1 as *mut MockValue];
    step(&mut ctx, 1, argv2.as_mut_ptr());

    final_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(7)));
    assert_eq!(
        ctx.max_agg_request,
        std::mem::size_of::<*mut OverAlignedState>()
    );
}

#[test]
fn window_function_supports_over_aligned_state() {
    let api = MockApi::new(FeatureSet::WINDOW_FUNCTIONS);
    let conn = open_conn(&api);

    conn.create_window_function(
        "aligned_window",
        1,
        || OverAlignedState { total: 0 },
        |_ctx, state, args| {
            state.total += args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(())
        },
        |_ctx, state, args| {
            state.total -= args.first().and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(())
        },
        |_ctx, state| Ok(Value::Integer(state.total)),
        |_ctx, state| Ok(Value::Integer(state.total)),
    )
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let step = reg.x_step.expect("x_step");
    let inverse = reg.x_inverse.expect("x_inverse");
    let value_fn = reg.x_value.expect("x_value");
    let final_fn = reg.x_final.expect("x_final");

    let mut ctx = MockContext::new(reg.user_data);

    let mut arg0 = MockValue::Integer(10);
    let mut argv = vec![&mut arg0 as *mut MockValue];
    step(&mut ctx, 1, argv.as_mut_ptr());

    let mut arg1 = MockValue::Integer(1);
    let mut argv2 = vec![&mut arg1 as *mut MockValue];
    step(&mut ctx, 1, argv2.as_mut_ptr());

    value_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(11)));

    let mut arg2 = MockValue::Integer(10);
    let mut argv3 = vec![&mut arg2 as *mut MockValue];
    inverse(&mut ctx, 1, argv3.as_mut_ptr());

    value_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(1)));

    final_fn(&mut ctx);
    assert_eq!(ctx.result, Some(MockResult::Integer(1)));
    assert_eq!(
        ctx.max_agg_request,
        std::mem::size_of::<*mut OverAlignedState>()
    );
}

#[test]
fn window_function_requires_feature() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);

    let err = conn
        .create_window_function(
            "winsum",
            1,
            || 0i64,
            |_ctx, _state, _args| Ok(()),
            |_ctx, _state, _args| Ok(()),
            |_ctx, _state| Ok(Value::Null),
            |_ctx, _state| Ok(Value::Null),
        )
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::FeatureUnavailable);
}

#[test]
fn serialize_into_vec_frees_buffer() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let serialized = conn.serialize(None, 0).unwrap();
    assert_eq!(serialized.as_slice(), &[1, 2, 3]);
    let vec = serialized.into_vec();
    assert_eq!(vec, vec![1, 2, 3]);
    assert_eq!(api.serialize_free_count(), 1);
}

#[test]
fn backup_wrapper_forwards_to_backup_spi() {
    let api = MockApi::new(FeatureSet::empty());
    let src = open_conn(&api);
    let dest = open_conn(&api);

    {
        let backup = src.backup_to(&dest, "main").unwrap();
        assert_eq!(backup.pagecount(), 34);
        assert_eq!(backup.remaining(), 12);
        backup.step(5).unwrap();
        assert_eq!(backup.remaining(), 7);
    }

    assert_eq!(api.backup_finish_count(), 1);
}

#[test]
fn blob_wrapper_forwards_to_blob_spi() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);

    {
        let blob = conn.open_blob("main", "t", "c", 1, 0).unwrap();
        assert_eq!(blob.len(), 4);
        assert!(!blob.is_empty());

        let mut part = [0_u8; 2];
        blob.read(&mut part, 1).unwrap();
        assert_eq!(part, [2, 3]);

        blob.write(&[9, 8], 1).unwrap();
        let mut full = [0_u8; 4];
        blob.read(&mut full, 0).unwrap();
        assert_eq!(full, [1, 9, 8, 4]);
    }

    assert_eq!(api.blob_close_count(), 1);
}

#[test]
fn wal_wrappers_forward_to_wal_spi() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);

    conn.wal_checkpoint(Some("main")).unwrap();
    let (log, checkpointed) = conn.wal_checkpoint_v2(Some("temp"), 7).unwrap();
    assert_eq!((log, checkpointed), (8, 5));
    assert_eq!(conn.wal_frame_count().unwrap(), Some(42));

    assert_eq!(api.wal_checkpoint_log(), vec![Some("main".to_owned())]);
    assert_eq!(
        api.wal_checkpoint_v2_log(),
        vec![(Some("temp".to_owned()), 7)]
    );
}

#[test]
fn metadata_wrappers_forward_to_metadata_spi() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);

    let metadata = conn.table_column_metadata(Some("main"), "t", "c").unwrap();
    assert_eq!(
        unsafe { metadata.data_type.unwrap().as_str() },
        Some("INTEGER")
    );
    assert_eq!(
        unsafe { metadata.coll_seq.unwrap().as_str() },
        Some("BINARY")
    );
    assert!(metadata.not_null);
    assert!(!metadata.primary_key);
    assert!(!metadata.autoinc);
    assert_eq!(
        api.metadata_log(),
        vec![(Some("main".to_owned()), "t".to_owned(), "c".to_owned())]
    );

    let stmt = conn.prepare("select 1").unwrap();
    assert_eq!(
        unsafe { stmt.column_decltype_raw(0).unwrap().as_str() },
        Some("INTEGER")
    );
    assert_eq!(
        unsafe { stmt.column_name_raw(0).unwrap().as_str() },
        Some("value")
    );
    assert_eq!(
        unsafe { stmt.column_table_name_raw(0).unwrap().as_str() },
        Some("dummy_table")
    );
}

#[test]
fn trace_and_authorizer_unregister_on_drop() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);

    let trace = conn.register_trace(TraceMask::STMT, |_event| {}).unwrap();
    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let trace_reg = *db.trace.lock().unwrap();
    assert_eq!(trace_reg.mask, TraceMask::STMT.bits());
    assert!(trace_reg.callback.is_some());
    drop(trace);
    let trace_reg = *db.trace.lock().unwrap();
    assert_eq!(trace_reg.mask, 0);
    assert!(trace_reg.callback.is_none());

    let auth = conn
        .register_authorizer(|_event| AuthorizerResult::Ok)
        .unwrap();
    let auth_reg = *db.authorizer.lock().unwrap();
    assert!(auth_reg.callback.is_some());
    drop(auth);
    let auth_reg = *db.authorizer.lock().unwrap();
    assert!(auth_reg.callback.is_none());
}

#[test]
fn progress_handler_unregisters_on_drop() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let db = unsafe { &mut *conn.raw_handle().as_ptr() };

    let handle = conn.register_progress_handler(7, || 0).unwrap();
    let progress_reg = *db.progress.lock().unwrap();
    assert_eq!(progress_reg.n, 7);
    assert!(progress_reg.callback.is_some());
    assert!(!progress_reg.context.is_null());
    assert_eq!(progress_reg.callback.unwrap()(progress_reg.context), 0);

    drop(handle);
    let progress_reg = *db.progress.lock().unwrap();
    assert_eq!(progress_reg.n, 0);
    assert!(progress_reg.callback.is_none());
    assert!(progress_reg.context.is_null());
}

#[test]
fn progress_handler_drop_of_stale_handle_keeps_newer_registration() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));

    let first_counter = Arc::clone(&first_calls);
    let first = conn
        .register_progress_handler(3, move || {
            first_counter.fetch_add(1, Ordering::SeqCst);
            0
        })
        .unwrap();

    let second_counter = Arc::clone(&second_calls);
    let second = conn
        .register_progress_handler(5, move || {
            second_counter.fetch_add(1, Ordering::SeqCst);
            0
        })
        .unwrap();

    let progress_reg = *db.progress.lock().unwrap();
    assert_eq!(progress_reg.n, 5);
    let callback = progress_reg.callback.expect("progress callback");
    assert_eq!(callback(progress_reg.context), 0);
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);

    drop(first);

    let progress_reg = *db.progress.lock().unwrap();
    assert_eq!(progress_reg.n, 5);
    assert!(progress_reg.callback.is_some());
    assert!(!progress_reg.context.is_null());
    let callback = progress_reg.callback.expect("progress callback");
    assert_eq!(callback(progress_reg.context), 0);
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);

    drop(second);

    let progress_reg = *db.progress.lock().unwrap();
    assert_eq!(progress_reg.n, 0);
    assert!(progress_reg.callback.is_none());
    assert!(progress_reg.context.is_null());
}

#[test]
fn trace_drop_of_stale_handle_keeps_newer_registration() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));

    let first_counter = Arc::clone(&first_calls);
    let first = conn
        .register_trace(TraceMask::STMT, move |_event| {
            first_counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();

    let second_counter = Arc::clone(&second_calls);
    let second = conn
        .register_trace(TraceMask::STMT, move |_event| {
            second_counter.fetch_add(1, Ordering::SeqCst);
        })
        .unwrap();

    let trace_reg = *db.trace.lock().unwrap();
    assert_eq!(trace_reg.mask, TraceMask::STMT.bits());
    let callback = trace_reg.callback.expect("trace callback");
    callback(
        trace_reg.mask,
        trace_reg.context,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);

    drop(first);

    let trace_reg = *db.trace.lock().unwrap();
    assert_eq!(trace_reg.mask, TraceMask::STMT.bits());
    assert!(trace_reg.callback.is_some());
    let callback = trace_reg.callback.expect("trace callback");
    callback(
        trace_reg.mask,
        trace_reg.context,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);

    drop(second);

    let trace_reg = *db.trace.lock().unwrap();
    assert_eq!(trace_reg.mask, 0);
    assert!(trace_reg.callback.is_none());
    assert!(trace_reg.context.is_null());
}

#[test]
fn authorizer_drop_of_stale_handle_keeps_newer_registration() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let first_calls = Arc::new(AtomicUsize::new(0));
    let second_calls = Arc::new(AtomicUsize::new(0));

    let first_counter = Arc::clone(&first_calls);
    let first = conn
        .register_authorizer(move |_event| {
            first_counter.fetch_add(1, Ordering::SeqCst);
            AuthorizerResult::Ok
        })
        .unwrap();

    let second_counter = Arc::clone(&second_calls);
    let second = conn
        .register_authorizer(move |_event| {
            second_counter.fetch_add(1, Ordering::SeqCst);
            AuthorizerResult::Deny
        })
        .unwrap();

    let auth_reg = *db.authorizer.lock().unwrap();
    let callback = auth_reg.callback.expect("authorizer callback");
    let rc = callback(
        auth_reg.context,
        authorizer::SELECT,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
    );
    assert_eq!(rc, 1);
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 1);

    drop(first);

    let auth_reg = *db.authorizer.lock().unwrap();
    assert!(auth_reg.callback.is_some());
    let callback = auth_reg.callback.expect("authorizer callback");
    let rc = callback(
        auth_reg.context,
        authorizer::SELECT,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
    );
    assert_eq!(rc, 1);
    assert_eq!(first_calls.load(Ordering::SeqCst), 0);
    assert_eq!(second_calls.load(Ordering::SeqCst), 2);

    drop(second);

    let auth_reg = *db.authorizer.lock().unwrap();
    assert!(auth_reg.callback.is_none());
    assert!(auth_reg.context.is_null());
}

#[test]
fn register_progress_handler_error_does_not_register_callback() {
    let api = MockApi::new(FeatureSet::empty());
    api.set_progress_failure(true);
    let conn = open_conn(&api);

    let err = match conn.register_progress_handler(3, || 0) {
        Ok(_) => panic!("expected progress registration failure"),
        Err(err) => err,
    };
    assert_eq!(err.code, ErrorCode::Busy);

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let progress_reg = *db.progress.lock().unwrap();
    assert_eq!(progress_reg.n, 0);
    assert!(progress_reg.callback.is_none());
    assert!(progress_reg.context.is_null());
}

#[test]
fn progress_handler_panic_is_contained() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let db = unsafe { &mut *conn.raw_handle().as_ptr() };

    let _handle = conn
        .register_progress_handler(5, || -> i32 {
            panic!("progress panic");
        })
        .unwrap();
    let progress_reg = *db.progress.lock().unwrap();
    let callback = progress_reg.callback.expect("progress callback");
    assert_eq!(callback(progress_reg.context), 1);
}

#[test]
fn authorizer_callback_returns_sqlite_codes() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);

    let _auth = conn
        .register_authorizer(|event| match event.action {
            AuthorizerAction::Read => AuthorizerResult::Ignore,
            AuthorizerAction::Select => AuthorizerResult::Deny,
            _ => AuthorizerResult::Ok,
        })
        .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let auth_reg = *db.authorizer.lock().unwrap();
    let callback = auth_reg.callback.expect("authorizer callback");

    let t = CString::new("t").unwrap();
    let c = CString::new("c").unwrap();
    let m = CString::new("main").unwrap();
    let trigger = CString::new("trigger").unwrap();

    let ok_rc = callback(
        auth_reg.context,
        authorizer::INSERT,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
    );
    assert_eq!(ok_rc, 0);

    let ignore_rc = callback(
        auth_reg.context,
        authorizer::READ,
        t.as_ptr(),
        c.as_ptr(),
        m.as_ptr(),
        trigger.as_ptr(),
    );
    assert_eq!(ignore_rc, 2);

    let deny_rc = callback(
        auth_reg.context,
        authorizer::SELECT,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
    );
    assert_eq!(deny_rc, 1);
}

#[test]
fn authorizer_panic_is_fail_closed() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);

    let _auth = conn
        .register_authorizer(|_event| -> AuthorizerResult { panic!("authorizer panic") })
        .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let auth_reg = *db.authorizer.lock().unwrap();
    let callback = auth_reg.callback.expect("authorizer callback");

    let rc = callback(
        auth_reg.context,
        authorizer::SELECT,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
    );
    assert_eq!(rc, 1);
}

#[test]
fn create_module_requires_feature() {
    let api = MockApi::new(FeatureSet::empty());
    let conn = open_conn(&api);
    let err = conn.create_module::<DummyVTab>("dummy").unwrap_err();
    assert_eq!(err.code, ErrorCode::FeatureUnavailable);
}

#[test]
fn create_module_calls_provider() {
    let api = MockApi::new(FeatureSet::VIRTUAL_TABLES);
    let conn = open_conn(&api);
    conn.create_module::<DummyVTab>("dummy").unwrap();
    assert_eq!(api.create_module_count(), 1);
}

#[test]
fn create_module_reuses_cached_module_descriptor() {
    let api = MockApi::new(FeatureSet::VIRTUAL_TABLES);
    let conn = open_conn(&api);
    conn.create_module::<DummyVTab>("dummy1").unwrap();
    conn.create_module::<DummyVTab>("dummy2").unwrap();
    let ptrs = api.module_ptrs();
    assert_eq!(ptrs.len(), 2);
    assert_eq!(ptrs[0], ptrs[1]);
}

#[test]
fn invalid_utf8_row_text_falls_back_to_blob() {
    let api = MockApi::new(FeatureSet::empty());
    api.set_rows(vec![vec![MockValue::TextBytes(vec![0xff_u8, 0xfe_u8])]]);
    let conn = open_conn(&api);
    let mut stmt = conn.prepare("select x").unwrap();
    let row = stmt.step().unwrap().expect("row");
    assert_eq!(row.column_text(0), None);
    assert_eq!(row.column_value_ref(0), ValueRef::Blob(&[0xff_u8, 0xfe_u8]));
}

#[test]
fn scalar_function_sees_invalid_utf8_as_blob() {
    let api = MockApi::new(FeatureSet::CREATE_FUNCTION_V2);
    let conn = open_conn(&api);
    let saw_blob = Arc::new(AtomicBool::new(false));
    let saw_blob_ref = saw_blob.clone();

    conn.create_scalar_function("bloby", 1, move |_ctx, args| {
        if let Some(ValueRef::Blob(bytes)) = args.first()
            && *bytes == [0xff_u8, 0xfe_u8]
        {
            saw_blob_ref.store(true, Ordering::SeqCst);
        }
        Ok(Value::Null)
    })
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let func = reg.x_func.expect("x_func");

    let mut arg0 = MockValue::TextBytes(vec![0xff_u8, 0xfe_u8]);
    let mut argv = vec![&mut arg0 as *mut MockValue];
    let mut ctx = MockContext::new(reg.user_data);
    func(&mut ctx, 1, argv.as_mut_ptr());

    assert!(saw_blob.load(Ordering::SeqCst));
    assert_eq!(ctx.result, Some(MockResult::Null));
}

#[test]
fn aggregate_init_panic_is_caught() {
    let api = MockApi::new(FeatureSet::CREATE_FUNCTION_V2);
    let conn = open_conn(&api);

    conn.create_aggregate_function(
        "agg_init_panic",
        1,
        || -> i64 { panic!("init panic") },
        |_ctx, _state, _args| Ok(()),
        |_ctx, state| Ok(Value::Integer(state)),
    )
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let step = reg.x_step.expect("x_step");
    let mut ctx = MockContext::new(reg.user_data);
    let mut arg0 = MockValue::Integer(1);
    let mut argv = vec![&mut arg0 as *mut MockValue];
    step(&mut ctx, 1, argv.as_mut_ptr());

    assert_eq!(
        ctx.result,
        Some(MockResult::Error(
            "panic in sqlite aggregate init".to_owned()
        ))
    );
}

#[test]
fn window_init_panic_is_caught_for_step_and_inverse() {
    let api = MockApi::new(FeatureSet::WINDOW_FUNCTIONS);
    let conn = open_conn(&api);

    conn.create_window_function(
        "window_init_panic",
        1,
        || -> i64 { panic!("init panic") },
        |_ctx, _state, _args| Ok(()),
        |_ctx, _state, _args| Ok(()),
        |_ctx, state| Ok(Value::Integer(*state)),
        |_ctx, state| Ok(Value::Integer(state)),
    )
    .unwrap();

    let db = unsafe { &mut *conn.raw_handle().as_ptr() };
    let reg_guard = db.functions.lock().unwrap();
    let reg = reg_guard.last.as_ref().unwrap();
    let step = reg.x_step.expect("x_step");
    let inverse = reg.x_inverse.expect("x_inverse");
    let mut arg0 = MockValue::Integer(1);
    let mut argv = vec![&mut arg0 as *mut MockValue];

    let mut ctx_step = MockContext::new(reg.user_data);
    step(&mut ctx_step, 1, argv.as_mut_ptr());
    assert_eq!(
        ctx_step.result,
        Some(MockResult::Error("panic in sqlite window init".to_owned()))
    );

    let mut ctx_inverse = MockContext::new(reg.user_data);
    inverse(&mut ctx_inverse, 1, argv.as_mut_ptr());
    assert_eq!(
        ctx_inverse.result,
        Some(MockResult::Error("panic in sqlite window init".to_owned()))
    );
}

#[test]
fn scalar_function_registration_error_drops_callback_state() {
    let api = MockApi::new(FeatureSet::CREATE_FUNCTION_V2);
    api.set_function_registration_failure(true);
    let conn = open_conn(&api);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(drops.clone());

    let err = conn
        .create_scalar_function("fail_scalar", 1, move |_ctx, _args| {
            let _keep_alive = &drop_counter;
            Ok(Value::Null)
        })
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::Busy);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn window_function_registration_error_drops_callback_state() {
    let api = MockApi::new(FeatureSet::WINDOW_FUNCTIONS);
    api.set_window_registration_failure(true);
    let conn = open_conn(&api);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(drops.clone());

    let err = conn
        .create_window_function(
            "fail_window",
            1,
            move || {
                let _keep_alive = &drop_counter;
                0i64
            },
            |_ctx, _state, _args| Ok(()),
            |_ctx, _state, _args| Ok(()),
            |_ctx, state| Ok(Value::Integer(*state)),
            |_ctx, state| Ok(Value::Integer(state)),
        )
        .unwrap_err();

    assert_eq!(err.code, ErrorCode::Busy);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn register_trace_error_drops_callback_state() {
    let api = MockApi::new(FeatureSet::empty());
    api.set_trace_failure(true);
    let conn = open_conn(&api);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(drops.clone());

    let err = match conn.register_trace(TraceMask::STMT, move |_event| {
        let _keep_alive = &drop_counter;
    }) {
        Ok(_) => panic!("expected trace registration failure"),
        Err(err) => err,
    };

    assert_eq!(err.code, ErrorCode::Busy);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn register_authorizer_error_drops_callback_state() {
    let api = MockApi::new(FeatureSet::empty());
    api.set_authorizer_failure(true);
    let conn = open_conn(&api);
    let drops = Arc::new(AtomicUsize::new(0));
    let drop_counter = DropCounter(drops.clone());

    let err = match conn.register_authorizer(move |_event| {
        let _keep_alive = &drop_counter;
        AuthorizerResult::Ok
    }) {
        Ok(_) => panic!("expected authorizer registration failure"),
        Err(err) => err,
    };

    assert_eq!(err.code, ErrorCode::Busy);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
}

#[test]
fn vtab_create_and_open_panics_are_caught() {
    let _guard = DummyStateGuard::new();
    let api = MockApi::new(FeatureSet::VIRTUAL_TABLES);
    let conn = open_conn(&api);
    let module = module_for_dummy(&api, &conn);
    let create = module.x_create.expect("x_create");
    let open = module.x_open.expect("x_open");
    let disconnect = module.x_disconnect.expect("x_disconnect");

    let mut vtab: *mut VTab<MockApi, DummyVTab> = std::ptr::null_mut();
    set_dummy_panic(DummyPanicSite::Connect);
    let rc = create(
        conn.raw_handle().as_ptr(),
        &api as *const MockApi as *mut std::ffi::c_void,
        0,
        std::ptr::null(),
        &mut vtab,
        std::ptr::null_mut(),
    );
    assert_ne!(rc, 0);
    assert!(vtab.is_null());

    set_dummy_panic(DummyPanicSite::None);
    let rc = create(
        conn.raw_handle().as_ptr(),
        &api as *const MockApi as *mut std::ffi::c_void,
        0,
        std::ptr::null(),
        &mut vtab,
        std::ptr::null_mut(),
    );
    assert_eq!(rc, 0);
    assert!(!vtab.is_null());

    let mut cursor: *mut Cursor<MockApi, DummyCursor> = std::ptr::null_mut();
    set_dummy_panic(DummyPanicSite::Open);
    let rc = open(vtab, &mut cursor);
    assert_ne!(rc, 0);
    assert!(cursor.is_null());

    set_dummy_panic(DummyPanicSite::None);
    let rc = disconnect(vtab);
    assert_eq!(rc, 0);
}

#[test]
fn vtab_create_sets_out_err_on_connect_error() {
    let _guard = DummyStateGuard::new();
    let api = MockApi::new(FeatureSet::VIRTUAL_TABLES);
    let conn = open_conn(&api);
    let module = module_for_dummy(&api, &conn);
    let create = module.x_create.expect("x_create");

    let mut vtab: *mut VTab<MockApi, DummyVTab> = std::ptr::null_mut();
    let mut out_err: *mut u8 = std::ptr::null_mut();
    set_dummy_connect_error(true);
    let rc = create(
        conn.raw_handle().as_ptr(),
        &api as *const MockApi as *mut std::ffi::c_void,
        0,
        std::ptr::null(),
        &mut vtab,
        &mut out_err,
    );
    set_dummy_connect_error(false);

    assert_ne!(rc, 0);
    assert!(vtab.is_null());
    assert!(!out_err.is_null());
    let message = unsafe { CStr::from_ptr(out_err.cast::<std::ffi::c_char>()) };
    assert_eq!(message.to_str().unwrap(), "dummy connect failed");
    unsafe { sqlite_provider::Sqlite3Api::free(&api, out_err.cast::<std::ffi::c_void>()) };
}

#[test]
fn vtab_create_sets_out_err_on_declare_vtab_error() {
    let _guard = DummyStateGuard::new();
    let api = MockApi::new(FeatureSet::VIRTUAL_TABLES);
    api.set_declare_vtab_failure(true);
    let conn = open_conn(&api);
    let module = module_for_dummy(&api, &conn);
    let create = module.x_create.expect("x_create");

    let mut vtab: *mut VTab<MockApi, DummyVTab> = std::ptr::null_mut();
    let mut out_err: *mut u8 = std::ptr::null_mut();
    let rc = create(
        conn.raw_handle().as_ptr(),
        &api as *const MockApi as *mut std::ffi::c_void,
        0,
        std::ptr::null(),
        &mut vtab,
        &mut out_err,
    );

    assert_ne!(rc, 0);
    assert!(vtab.is_null());
    assert!(!out_err.is_null());
    let message = unsafe { CStr::from_ptr(out_err.cast::<std::ffi::c_char>()) };
    assert_eq!(message.to_str().unwrap(), "declare_vtab failed");
    unsafe { sqlite_provider::Sqlite3Api::free(&api, out_err.cast::<std::ffi::c_void>()) };
}

#[test]
fn vtab_cursor_callback_panics_are_caught() {
    let _guard = DummyStateGuard::new();
    let api = MockApi::new(FeatureSet::VIRTUAL_TABLES);
    let conn = open_conn(&api);
    let module = module_for_dummy(&api, &conn);
    let create = module.x_create.expect("x_create");
    let best_index = module.x_best_index.expect("x_best_index");
    let open = module.x_open.expect("x_open");
    let filter = module.x_filter.expect("x_filter");
    let next = module.x_next.expect("x_next");
    let eof = module.x_eof.expect("x_eof");
    let column = module.x_column.expect("x_column");
    let rowid = module.x_rowid.expect("x_rowid");
    let close = module.x_close.expect("x_close");
    let disconnect = module.x_disconnect.expect("x_disconnect");

    let mut vtab: *mut VTab<MockApi, DummyVTab> = std::ptr::null_mut();
    let rc = create(
        conn.raw_handle().as_ptr(),
        &api as *const MockApi as *mut std::ffi::c_void,
        0,
        std::ptr::null(),
        &mut vtab,
        std::ptr::null_mut(),
    );
    assert_eq!(rc, 0);

    let mut cursor: *mut Cursor<MockApi, DummyCursor> = std::ptr::null_mut();
    let rc = open(vtab, &mut cursor);
    assert_eq!(rc, 0);

    set_dummy_panic(DummyPanicSite::BestIndex);
    assert_ne!(best_index(vtab, std::ptr::null_mut()), 0);

    set_dummy_panic(DummyPanicSite::Filter);
    assert_ne!(
        filter(cursor, 0, std::ptr::null(), 0, std::ptr::null_mut()),
        0
    );

    set_dummy_panic(DummyPanicSite::Next);
    assert_ne!(next(cursor), 0);

    set_dummy_panic(DummyPanicSite::Eof);
    assert_eq!(eof(cursor), 1);

    let mut ctx = MockContext::new(std::ptr::null_mut());
    set_dummy_panic(DummyPanicSite::Column);
    assert_ne!(column(cursor, &mut ctx, 0), 0);
    assert_eq!(
        ctx.result,
        Some(MockResult::Error(
            "panic in virtual table column".to_owned()
        ))
    );

    let mut rowid_out = 0_i64;
    set_dummy_panic(DummyPanicSite::Rowid);
    assert_ne!(rowid(cursor, &mut rowid_out), 0);

    set_dummy_panic(DummyPanicSite::None);
    assert_eq!(close(cursor), 0);
    assert_eq!(disconnect(vtab), 0);
}

#[test]
fn vtab_disconnect_panic_is_caught() {
    let _guard = DummyStateGuard::new();
    let api = MockApi::new(FeatureSet::VIRTUAL_TABLES);
    let conn = open_conn(&api);
    let module = module_for_dummy(&api, &conn);
    let create = module.x_create.expect("x_create");
    let disconnect = module.x_disconnect.expect("x_disconnect");

    let mut vtab: *mut VTab<MockApi, DummyVTab> = std::ptr::null_mut();
    let rc = create(
        conn.raw_handle().as_ptr(),
        &api as *const MockApi as *mut std::ffi::c_void,
        0,
        std::ptr::null(),
        &mut vtab,
        std::ptr::null_mut(),
    );
    assert_eq!(rc, 0);

    set_dummy_panic(DummyPanicSite::Disconnect);
    assert_ne!(disconnect(vtab), 0);
    set_dummy_panic(DummyPanicSite::None);
}
