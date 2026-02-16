use libc::{c_char, c_void};
use sqlite_provider::{
    ApiVersion, ColumnMetadata, FeatureSet, FunctionFlags, OpenOptions, RawBytes, Result,
    Sqlite3Api, Sqlite3Metadata, StepResult, ValueType,
};
use sqlite_provider_abi::{
    register_provider, sqlite3, sqlite3_close, sqlite3_exec, sqlite3_free_table, sqlite3_get_table,
    sqlite3_open, ProviderState,
};
use std::ffi::{CStr, CString};
use std::ptr::{null, null_mut, NonNull};
use std::sync::OnceLock;

const SQLITE_OK: i32 = 0;

struct MockDb;

struct MockStmt {
    cols: Vec<Vec<u8>>,
    rows: Vec<Vec<Option<Vec<u8>>>>,
    next_row: usize,
    current_row: Option<usize>,
}

struct MockValue;
struct MockContext;

struct MockApi;

impl MockStmt {
    fn sample() -> Self {
        Self {
            cols: vec![b"id".to_vec(), b"name".to_vec()],
            rows: vec![
                vec![Some(b"1".to_vec()), Some(b"alice".to_vec())],
                vec![Some(b"2".to_vec()), Some(b"bob".to_vec())],
            ],
            next_row: 0,
            current_row: None,
        }
    }
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

    unsafe fn open(&self, _filename: &str, _options: OpenOptions<'_>) -> Result<NonNull<Self::Db>> {
        Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockDb))))
    }

    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()> {
        drop(Box::from_raw(db.as_ptr()));
        Ok(())
    }

    unsafe fn prepare_v2(&self, _db: NonNull<Self::Db>, _sql: &str) -> Result<NonNull<Self::Stmt>> {
        Ok(NonNull::new_unchecked(Box::into_raw(Box::new(MockStmt::sample()))))
    }

    unsafe fn prepare_v3(
        &self,
        db: NonNull<Self::Db>,
        sql: &str,
        _flags: u32,
    ) -> Result<NonNull<Self::Stmt>> {
        self.prepare_v2(db, sql)
    }

    unsafe fn step(&self, stmt: NonNull<Self::Stmt>) -> Result<StepResult> {
        let stmt = &mut *stmt.as_ptr();
        if stmt.next_row < stmt.rows.len() {
            stmt.current_row = Some(stmt.next_row);
            stmt.next_row += 1;
            Ok(StepResult::Row)
        } else {
            stmt.current_row = None;
            Ok(StepResult::Done)
        }
    }

    unsafe fn reset(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        let stmt = &mut *stmt.as_ptr();
        stmt.next_row = 0;
        stmt.current_row = None;
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

    unsafe fn column_count(&self, stmt: NonNull<Self::Stmt>) -> i32 {
        let stmt = &*stmt.as_ptr();
        stmt.cols.len() as i32
    }

    unsafe fn column_type(&self, stmt: NonNull<Self::Stmt>, col: i32) -> ValueType {
        let stmt = &*stmt.as_ptr();
        let row = match stmt.current_row {
            Some(row) => row,
            None => return ValueType::Null,
        };
        match stmt.rows.get(row).and_then(|row| row.get(col as usize)) {
            Some(Some(_)) => ValueType::Text,
            _ => ValueType::Null,
        }
    }

    unsafe fn column_int64(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> i64 {
        0
    }

    unsafe fn column_double(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> f64 {
        0.0
    }

    unsafe fn column_text(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let stmt = &*stmt.as_ptr();
        let row = match stmt.current_row {
            Some(row) => row,
            None => return RawBytes::empty(),
        };
        match stmt.rows.get(row).and_then(|row| row.get(col as usize)) {
            Some(Some(buf)) => RawBytes { ptr: buf.as_ptr(), len: buf.len() },
            _ => RawBytes::empty(),
        }
    }

    unsafe fn column_blob(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        self.column_text(stmt, col)
    }

    unsafe fn errcode(&self, _db: NonNull<Self::Db>) -> i32 {
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

unsafe impl Sqlite3Metadata for MockApi {
    unsafe fn table_column_metadata(
        &self,
        _db: NonNull<Self::Db>,
        _db_name: Option<&str>,
        _table: &str,
        _column: &str,
    ) -> Result<ColumnMetadata> {
        Ok(ColumnMetadata {
            data_type: None,
            coll_seq: None,
            not_null: false,
            primary_key: false,
            autoinc: false,
        })
    }

    unsafe fn column_decltype(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> Option<RawBytes> {
        None
    }

    unsafe fn column_name(&self, stmt: NonNull<Self::Stmt>, col: i32) -> Option<RawBytes> {
        let stmt = &*stmt.as_ptr();
        let name = stmt.cols.get(col as usize)?;
        Some(RawBytes { ptr: name.as_ptr(), len: name.len() })
    }

    unsafe fn column_table_name(&self, _stmt: NonNull<Self::Stmt>, _col: i32) -> Option<RawBytes> {
        None
    }
}

fn ensure_provider() {
    static INIT: OnceLock<()> = OnceLock::new();
    INIT.get_or_init(|| {
        static API: MockApi = MockApi;
        let state = ProviderState::new(&API).with_metadata(&API);
        let _ = register_provider(state);
    });
}

struct ExecCapture {
    names: Vec<String>,
    rows: Vec<Vec<Option<String>>>,
}

extern "C" fn exec_cb(
    ctx: *mut c_void,
    ncol: i32,
    values: *mut *mut c_char,
    names: *mut *mut c_char,
) -> i32 {
    if ctx.is_null() {
        return SQLITE_OK;
    }
    let capture = unsafe { &mut *(ctx as *mut ExecCapture) };
    if capture.names.is_empty() {
        let name_slice = unsafe { std::slice::from_raw_parts(names, ncol as usize) };
        for &ptr in name_slice {
            let name = if ptr.is_null() {
                String::new()
            } else {
                unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned()
            };
            capture.names.push(name);
        }
    }
    let value_slice = unsafe { std::slice::from_raw_parts(values, ncol as usize) };
    let mut row = Vec::with_capacity(ncol as usize);
    for &ptr in value_slice {
        if ptr.is_null() {
            row.push(None);
        } else {
            let text = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
            row.push(Some(text));
        }
    }
    capture.rows.push(row);
    SQLITE_OK
}

#[test]
fn exec_uses_callback_data() {
    ensure_provider();
    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("select * from t").unwrap();
    let mut capture = ExecCapture { names: Vec::new(), rows: Vec::new() };
    let rc = sqlite3_exec(
        db,
        sql.as_ptr(),
        Some(exec_cb),
        &mut capture as *mut ExecCapture as *mut c_void,
        null_mut(),
    );
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(capture.names, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(
        capture.rows,
        vec![
            vec![Some("1".to_string()), Some("alice".to_string())],
            vec![Some("2".to_string()), Some("bob".to_string())],
        ]
    );

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn get_table_builds_cells() {
    ensure_provider();
    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("select * from t").unwrap();
    let mut table: *mut *mut c_char = null_mut();
    let mut rows = 0;
    let mut cols = 0;
    let rc = sqlite3_get_table(
        db,
        sql.as_ptr(),
        &mut table,
        &mut rows,
        &mut cols,
        null_mut(),
    );
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(rows, 2);
    assert_eq!(cols, 2);

    let total = ((rows + 1) * cols) as usize;
    let slice = unsafe { std::slice::from_raw_parts(table, total) };
    let values: Vec<Option<String>> = slice
        .iter()
        .map(|ptr| {
            if ptr.is_null() {
                None
            } else {
                Some(unsafe { CStr::from_ptr(*ptr) }.to_string_lossy().into_owned())
            }
        })
        .collect();
    assert_eq!(
        values,
        vec![
            Some("id".to_string()),
            Some("name".to_string()),
            Some("1".to_string()),
            Some("alice".to_string()),
            Some("2".to_string()),
            Some("bob".to_string()),
        ]
    );

    sqlite3_free_table(table);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}
