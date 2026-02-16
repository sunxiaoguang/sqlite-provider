use std::ptr::NonNull;
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicUsize, Ordering};

use sqlite_provider::*;

#[derive(Clone, Debug, PartialEq)]
enum MockValue {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
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
}

impl MockContext {
    fn new(user_data: *mut std::ffi::c_void) -> Self {
        Self { user_data, result: None, agg_storage: None }
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
        Self { rows, row_index: 0, current_row: None, binds: Vec::new() }
    }

    fn current_value(&self, col: i32) -> Option<&MockValue> {
        let row = self.current_row?;
        self.rows.get(row)?.get(col as usize)
    }
}

struct DummyVTab;

struct DummyCursor {
    done: bool,
}

impl VirtualTable<MockApi> for DummyVTab {
    type Cursor = DummyCursor;
    type Error = Error;

    fn connect(_args: &[&str]) -> core::result::Result<(Self, String), Self::Error> {
        Ok((DummyVTab, "create table x(a int)".to_string()))
    }

    fn disconnect(self) -> core::result::Result<(), Self::Error> {
        Ok(())
    }

    fn open(&self) -> core::result::Result<Self::Cursor, Self::Error> {
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
        self.done = true;
        Ok(())
    }

    fn next(&mut self) -> core::result::Result<(), Self::Error> {
        self.done = true;
        Ok(())
    }

    fn eof(&self) -> bool {
        self.done
    }

    fn column(&self, ctx: &Context<'_, MockApi>, _col: i32) -> core::result::Result<(), Self::Error> {
        ctx.result_null();
        Ok(())
    }

    fn rowid(&self) -> core::result::Result<i64, Self::Error> {
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
    callback: Option<extern "C" fn(u32, *mut std::ffi::c_void, *mut std::ffi::c_void, *mut std::ffi::c_void)>,
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

struct MockDb {
    functions: Mutex<FunctionRegistry>,
    trace: Mutex<TraceReg>,
    authorizer: Mutex<AuthorizerReg>,
}

impl MockDb {
    fn new() -> Self {
        Self {
            functions: Mutex::new(FunctionRegistry::default()),
            trace: Mutex::new(TraceReg::default()),
            authorizer: Mutex::new(AuthorizerReg::default()),
        }
    }
}

struct MockApi {
    features: FeatureSet,
    prepare_v2_calls: AtomicUsize,
    prepare_v3_calls: AtomicUsize,
    next_rows: Mutex<Vec<Vec<MockValue>>>,
    serialize_frees: AtomicUsize,
    create_module_calls: AtomicUsize,
}

impl MockApi {
    fn new(features: FeatureSet) -> Self {
        Self {
            features,
            prepare_v2_calls: AtomicUsize::new(0),
            prepare_v3_calls: AtomicUsize::new(0),
            next_rows: Mutex::new(Vec::new()),
            serialize_frees: AtomicUsize::new(0),
            create_module_calls: AtomicUsize::new(0),
        }
    }

    fn prepare_v2_count(&self) -> usize {
        self.prepare_v2_calls.load(Ordering::SeqCst)
    }

    fn prepare_v3_count(&self) -> usize {
        self.prepare_v3_calls.load(Ordering::SeqCst)
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

    unsafe fn open(&self, _filename: &str, _options: OpenOptions<'_>) -> Result<NonNull<Self::Db>> {
        let db = Box::new(MockDb::new());
        Ok(NonNull::new_unchecked(Box::into_raw(db)))
    }

    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()> {
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
        stmt.rows.get(0).map(|row| row.len() as i32).unwrap_or(0)
    }

    unsafe fn column_type(&self, stmt: NonNull<Self::Stmt>, col: i32) -> ValueType {
        let stmt = &*stmt.as_ptr();
        match stmt.current_value(col) {
            Some(MockValue::Null) | None => ValueType::Null,
            Some(MockValue::Integer(_)) => ValueType::Integer,
            Some(MockValue::Float(_)) => ValueType::Float,
            Some(MockValue::Text(_)) => ValueType::Text,
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
            Some(MockValue::Text(v)) => RawBytes { ptr: v.as_ptr(), len: v.len() },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn column_blob(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let stmt = &*stmt.as_ptr();
        match stmt.current_value(col) {
            Some(MockValue::Blob(v)) => RawBytes { ptr: v.as_ptr(), len: v.len() },
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

    unsafe fn aggregate_context(&self, ctx: NonNull<Self::Context>, bytes: usize) -> *mut std::ffi::c_void {
        let ctx = &mut *ctx.as_ptr();
        if bytes == 0 {
            return ctx
                .agg_storage
                .as_mut()
                .map(|buf| buf.as_mut_ptr() as *mut std::ffi::c_void)
                .unwrap_or(std::ptr::null_mut());
        }
        let need = bytes.max(1);
        let elem = std::mem::size_of::<usize>();
        let elems = (need + elem - 1) / elem;
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
            MockValue::Text(_) => ValueType::Text,
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
            MockValue::Text(val) => RawBytes { ptr: val.as_ptr(), len: val.len() },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn value_blob(&self, v: NonNull<Self::Value>) -> RawBytes {
        match &*v.as_ptr() {
            MockValue::Blob(val) => RawBytes { ptr: val.as_ptr(), len: val.len() },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn declare_vtab(&self, _db: NonNull<Self::Db>, _schema: &str) -> Result<()> {
        Ok(())
    }

    unsafe fn create_module_v2(
        &self,
        _db: NonNull<Self::Db>,
        _name: &str,
        _module: &'static sqlite3_module<Self>,
        _user_data: *mut std::ffi::c_void,
        _drop_user_data: Option<extern "C" fn(*mut std::ffi::c_void)>,
    ) -> Result<()> {
        self.create_module_calls.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

unsafe impl Sqlite3Hooks for MockApi {
    unsafe fn trace_v2(
        &self,
        db: NonNull<Self::Db>,
        mask: u32,
        callback: Option<extern "C" fn(u32, *mut std::ffi::c_void, *mut std::ffi::c_void, *mut std::ffi::c_void)>,
        context: *mut std::ffi::c_void,
    ) -> Result<()> {
        let db = &mut *db.as_ptr();
        *db.trace.lock().unwrap() = TraceReg { mask, callback, context };
        Ok(())
    }

    unsafe fn progress_handler(
        &self,
        _db: NonNull<Self::Db>,
        _n: i32,
        _callback: Option<extern "C" fn() -> i32>,
        _context: *mut std::ffi::c_void,
    ) -> Result<()> {
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
        let db = &mut *db.as_ptr();
        *db.authorizer.lock().unwrap() = AuthorizerReg { callback, context };
        Ok(())
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
        OpenOptions { flags: OpenFlags::empty(), vfs: None },
    )
    .unwrap()
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
fn step_and_read_row() {
    let api = MockApi::new(FeatureSet::empty());
    api.set_rows(vec![vec![MockValue::Integer(7), MockValue::Text("hi".to_owned())]]);
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
        let v = args.get(0).and_then(|v| v.as_i64()).unwrap_or(0);
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
            *state += args.get(0).and_then(|v| v.as_i64()).unwrap_or(0);
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
            *state += args.get(0).and_then(|v| v.as_i64()).unwrap_or(0);
            Ok(())
        },
        |_ctx, state, args| {
            *state -= args.get(0).and_then(|v| v.as_i64()).unwrap_or(0);
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

    let auth = conn.register_authorizer(|_event| AuthorizerResult::Ok).unwrap();
    let auth_reg = *db.authorizer.lock().unwrap();
    assert!(auth_reg.callback.is_some());
    drop(auth);
    let auth_reg = *db.authorizer.lock().unwrap();
    assert!(auth_reg.callback.is_none());
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
