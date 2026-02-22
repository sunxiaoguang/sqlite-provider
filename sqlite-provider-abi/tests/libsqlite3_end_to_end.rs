#![cfg(feature = "default-backend")]

use libc::{c_char, c_void};
use sqlite_provider_abi::{
    ProviderState, register_provider, sqlite3, sqlite3_backup_finish, sqlite3_backup_init,
    sqlite3_backup_pagecount, sqlite3_backup_remaining, sqlite3_backup_step, sqlite3_bind_blob,
    sqlite3_bind_parameter_count, sqlite3_bind_parameter_index, sqlite3_bind_text,
    sqlite3_blob_bytes, sqlite3_blob_close, sqlite3_blob_open, sqlite3_blob_read,
    sqlite3_blob_write, sqlite3_busy_timeout, sqlite3_changes, sqlite3_close, sqlite3_close_v2,
    sqlite3_column_blob, sqlite3_column_bytes, sqlite3_column_type, sqlite3_complete,
    sqlite3_context_db_handle, sqlite3_create_collation_v2, sqlite3_create_function_v2,
    sqlite3_deserialize, sqlite3_errcode, sqlite3_errmsg, sqlite3_exec, sqlite3_finalize,
    sqlite3_free, sqlite3_free_table, sqlite3_get_table, sqlite3_last_insert_rowid, sqlite3_open,
    sqlite3_prepare_v2, sqlite3_progress_handler, sqlite3_reset, sqlite3_result_int64,
    sqlite3_result_text, sqlite3_serialize, sqlite3_step, sqlite3_stmt, sqlite3_stmt_readonly,
    sqlite3_table_column_metadata, sqlite3_total_changes, sqlite3_user_data,
    sqlite3_wal_checkpoint_v2,
};
use sqlite_provider_sqlite3::LibSqlite3;
use std::ffi::{CStr, CString};
use std::fs;
use std::ptr::{null, null_mut};
use std::sync::OnceLock;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

const SQLITE_OK: i32 = 0;
const SQLITE_ABORT: i32 = 4;
const SQLITE_MISUSE: i32 = 21;
const SQLITE_RANGE: i32 = 25;
const SQLITE_ROW: i32 = 100;
const SQLITE_TEXT: i32 = 3;
const SQLITE_UTF8: i32 = 1;
const SQLITE_TRANSIENT: *mut c_void = -1_isize as *mut c_void;
const SQLITE_CHECKPOINT_PASSIVE: i32 = 0;

type SystemSqlite3Complete = unsafe extern "C" fn(*const c_char) -> i32;

static CONTEXT_DB_HANDLE: AtomicUsize = AtomicUsize::new(0);
static COLLATION_DESTROY_CALLED: AtomicBool = AtomicBool::new(false);
static PROGRESS_CALLS: AtomicUsize = AtomicUsize::new(0);
static FUNCTION_DESTROY_CALLED: AtomicBool = AtomicBool::new(false);
static FUNCTION_USER_DATA_VALUE: AtomicUsize = AtomicUsize::new(0);
static FUNCTION_DESTROY_USER_DATA: AtomicUsize = AtomicUsize::new(0);
static TEMP_DB_COUNTER: AtomicUsize = AtomicUsize::new(0);

extern "C" fn aborting_exec_callback(
    _ctx: *mut c_void,
    _cols: i32,
    _values: *mut *mut c_char,
    _names: *mut *mut c_char,
) -> i32 {
    1
}

extern "C" fn capture_context_db_handle(ctx: *mut c_void, _argc: i32, _argv: *mut *mut c_void) {
    CONTEXT_DB_HANDLE.store(sqlite3_context_db_handle(ctx) as usize, Ordering::SeqCst);
    sqlite3_result_int64(ctx, 1);
}

extern "C" fn counting_progress_handler(context: *mut c_void) -> i32 {
    if !context.is_null() {
        let calls = unsafe { &*(context as *const AtomicUsize) };
        calls.fetch_add(1, Ordering::SeqCst);
    }
    0
}

extern "C" fn reverse_binary_collation(
    _ctx: *mut c_void,
    lhs_len: i32,
    lhs: *const c_void,
    rhs_len: i32,
    rhs: *const c_void,
) -> i32 {
    let lhs_len = lhs_len.max(0) as usize;
    let rhs_len = rhs_len.max(0) as usize;
    let lhs = if lhs.is_null() || lhs_len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(lhs as *const u8, lhs_len) }
    };
    let rhs = if rhs.is_null() || rhs_len == 0 {
        &[][..]
    } else {
        unsafe { std::slice::from_raw_parts(rhs as *const u8, rhs_len) }
    };
    match lhs.cmp(rhs) {
        std::cmp::Ordering::Less => 1,
        std::cmp::Ordering::Equal => 0,
        std::cmp::Ordering::Greater => -1,
    }
}

extern "C" fn collation_destroy(_ctx: *mut c_void) {
    COLLATION_DESTROY_CALLED.store(true, Ordering::SeqCst);
}

extern "C" fn capture_function_user_data(ctx: *mut c_void, _argc: i32, _argv: *mut *mut c_void) {
    let user_data = sqlite3_user_data(ctx) as *const usize;
    let value = if user_data.is_null() {
        0
    } else {
        unsafe { *user_data }
    };
    FUNCTION_USER_DATA_VALUE.store(value, Ordering::SeqCst);
    sqlite3_result_int64(ctx, value as i64);
}

extern "C" fn function_destroy_with_context(ctx: *mut c_void) {
    FUNCTION_DESTROY_CALLED.store(true, Ordering::SeqCst);
    FUNCTION_DESTROY_USER_DATA.store(ctx as usize, Ordering::SeqCst);
}

fn ensure_provider() -> bool {
    static INIT: OnceLock<bool> = OnceLock::new();
    *INIT.get_or_init(|| {
        if let Some(api) = LibSqlite3::load() {
            let state = ProviderState::new(api)
                .with_hooks(api)
                .with_backup(api)
                .with_blob(api)
                .with_serialize(api)
                .with_wal(api)
                .with_metadata(api)
                .with_extras(api);
            let _ = register_provider(state);
            true
        } else {
            false
        }
    })
}

fn load_system_sqlite3_complete() -> Option<SystemSqlite3Complete> {
    #[cfg(target_os = "macos")]
    const LIB_NAMES: [&[u8]; 3] = [
        b"libsqlite3.dylib\0",
        b"libsqlite3.so.0\0",
        b"libsqlite3.so\0",
    ];
    #[cfg(not(target_os = "macos"))]
    const LIB_NAMES: [&[u8]; 2] = [b"libsqlite3.so.0\0", b"libsqlite3.so\0"];

    for name in LIB_NAMES {
        let handle = unsafe {
            libc::dlopen(
                name.as_ptr().cast(),
                libc::RTLD_LAZY | getattr_rtld_local_default(),
            )
        };
        if handle.is_null() {
            continue;
        }
        let sym = unsafe { libc::dlsym(handle, b"sqlite3_complete\0".as_ptr().cast()) };
        if sym.is_null() {
            continue;
        }
        // Keep the library handle open for process lifetime.
        return Some(unsafe { std::mem::transmute::<*mut c_void, SystemSqlite3Complete>(sym) });
    }
    None
}

const fn getattr_rtld_local_default() -> i32 {
    #[cfg(any(target_os = "linux", target_os = "android", target_os = "macos"))]
    {
        libc::RTLD_LOCAL
    }
    #[cfg(not(any(target_os = "linux", target_os = "android", target_os = "macos")))]
    {
        0
    }
}

fn system_sqlite3_complete() -> Option<SystemSqlite3Complete> {
    static COMPLETE: OnceLock<Option<SystemSqlite3Complete>> = OnceLock::new();
    *COMPLETE.get_or_init(load_system_sqlite3_complete)
}

fn collect_table_values(table: *mut *mut c_char, rows: i32, cols: i32) -> Vec<Option<String>> {
    let total = ((rows + 1) * cols) as usize;
    let slice = unsafe { std::slice::from_raw_parts(table, total) };
    slice
        .iter()
        .map(|ptr| {
            if ptr.is_null() {
                None
            } else {
                Some(
                    unsafe { CStr::from_ptr(*ptr) }
                        .to_string_lossy()
                        .into_owned(),
                )
            }
        })
        .collect()
}

fn unique_temp_db_path(prefix: &str) -> CString {
    let seq = TEMP_DB_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut path = std::env::temp_dir();
    path.push(format!(
        "sqlite_provider_{prefix}_{}_{}.db",
        std::process::id(),
        seq
    ));
    CString::new(path.to_string_lossy().into_owned()).unwrap()
}

#[test]
fn exec_and_get_table_round_trip() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let create_sql = CString::new("CREATE TABLE t(id INTEGER, name TEXT);").unwrap();
    let rc = sqlite3_exec(db, create_sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let insert_sql = CString::new("INSERT INTO t VALUES(1,'alice'),(2,'bob');").unwrap();
    let rc = sqlite3_exec(db, insert_sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let select_sql = CString::new("SELECT id, name FROM t ORDER BY id;").unwrap();
    let mut table: *mut *mut c_char = null_mut();
    let mut rows = 0;
    let mut cols = 0;
    let rc = sqlite3_get_table(
        db,
        select_sql.as_ptr(),
        &mut table,
        &mut rows,
        &mut cols,
        null_mut(),
    );
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(rows, 2);
    assert_eq!(cols, 2);

    let values = collect_table_values(table, rows, cols);

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

    assert!(!table.is_null());
    let table_ptr = table;
    sqlite3_free_table(table);
    assert_eq!(table, table_ptr);
    sqlite3_free_table(table);
    assert_eq!(table, table_ptr);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn extras_helpers_follow_backend_values() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let create_sql = CString::new("CREATE TABLE t(id INTEGER PRIMARY KEY, name TEXT);").unwrap();
    let rc = sqlite3_exec(db, create_sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let insert1_sql = CString::new("INSERT INTO t(name) VALUES('alice');").unwrap();
    let rc = sqlite3_exec(db, insert1_sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let insert2_sql = CString::new("INSERT INTO t(name) VALUES('bob');").unwrap();
    let rc = sqlite3_exec(db, insert2_sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    assert_eq!(sqlite3_changes(db), 1);
    assert_eq!(sqlite3_total_changes(db), 2);
    assert_eq!(sqlite3_last_insert_rowid(db), 2);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("SELECT :foo, ?, @bar;").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());

    assert_eq!(sqlite3_stmt_readonly(stmt), 1);
    assert_eq!(sqlite3_bind_parameter_count(stmt), 3);
    let p3 = CString::new("@bar").unwrap();
    assert_eq!(sqlite3_bind_parameter_index(stmt, p3.as_ptr()), 3);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn get_table_runs_setup_statements_before_select() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("create table t(v integer); insert into t values(7); select v from t;")
        .unwrap();
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
    assert_eq!(rows, 1);
    assert_eq!(cols, 1);
    assert_eq!(
        collect_table_values(table, rows, cols),
        vec![Some("v".to_string()), Some("7".to_string())]
    );

    sqlite3_free_table(table);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn get_table_aggregates_rows_across_select_statements() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("select 1 as a; select 2 as b;").unwrap();
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
    assert_eq!(cols, 1);
    assert_eq!(
        collect_table_values(table, rows, cols),
        vec![
            Some("a".to_string()),
            Some("1".to_string()),
            Some("2".to_string()),
        ]
    );

    sqlite3_free_table(table);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn prepare_v2_tail_skips_trigger_body_semicolons() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let create_sql = CString::new("CREATE TABLE t(id INTEGER);").unwrap();
    let rc = sqlite3_exec(db, create_sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new(
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t(id) VALUES (new.id + 1); END; SELECT 1;",
    )
    .unwrap();
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let mut tail: *const c_char = null();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, &mut tail);
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());
    assert!(!tail.is_null());
    let tail_text = unsafe { CStr::from_ptr(tail) }.to_str().unwrap();
    assert!(tail_text.trim_start().starts_with("SELECT 1"));

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn prepare_v2_tail_skips_trigger_case_body_semicolons() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let create_sql = CString::new("CREATE TABLE t(id INTEGER);").unwrap();
    let rc = sqlite3_exec(db, create_sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new(
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT CASE WHEN NEW.id > 0 THEN 'a;b' ELSE 'z' END; INSERT INTO t(id) VALUES (NEW.id + 1); END; SELECT 2;",
    )
    .unwrap();
    let mut stmt: *mut sqlite3_stmt = null_mut();
    let mut tail: *const c_char = null();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, &mut tail);
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());
    assert!(!tail.is_null());
    let tail_text = unsafe { CStr::from_ptr(tail) }.to_str().unwrap();
    assert!(tail_text.trim_start().starts_with("SELECT 2"));

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn exec_handles_trigger_body_semicolons() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new(
        "CREATE TABLE t(id INTEGER); \
         CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t(id) VALUES (new.id + 1); END; \
         INSERT INTO t(id) VALUES (1);",
    )
    .unwrap();
    let rc = sqlite3_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let select_sql = CString::new("SELECT id FROM t ORDER BY id;").unwrap();
    let mut table: *mut *mut c_char = null_mut();
    let mut rows = 0;
    let mut cols = 0;
    let rc = sqlite3_get_table(
        db,
        select_sql.as_ptr(),
        &mut table,
        &mut rows,
        &mut cols,
        null_mut(),
    );
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(rows, 2);
    assert_eq!(cols, 1);
    assert_eq!(
        collect_table_values(table, rows, cols),
        vec![
            Some("id".to_string()),
            Some("1".to_string()),
            Some("2".to_string()),
        ]
    );

    sqlite3_free_table(table);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn get_table_runs_trigger_setup_script_before_select() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new(
        "CREATE TABLE t(id INTEGER); \
         CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t(id) VALUES (new.id + 1); END; \
         INSERT INTO t(id) VALUES (5); \
         SELECT id FROM t ORDER BY id;",
    )
    .unwrap();
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
    assert_eq!(cols, 1);
    assert_eq!(
        collect_table_values(table, rows, cols),
        vec![
            Some("id".to_string()),
            Some("5".to_string()),
            Some("6".to_string()),
        ]
    );

    sqlite3_free_table(table);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn exec_callback_abort_updates_errcode_and_errmsg() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("SELECT 1;").unwrap();
    let mut err_out: *mut c_char = null_mut();
    let rc = sqlite3_exec(
        db,
        sql.as_ptr(),
        Some(aborting_exec_callback),
        null_mut(),
        &mut err_out,
    );
    assert_eq!(rc, SQLITE_ABORT);
    assert_eq!(sqlite3_errcode(db), SQLITE_ABORT);
    let msg = unsafe { CStr::from_ptr(sqlite3_errmsg(db)) }
        .to_str()
        .unwrap();
    assert_eq!(msg, "query aborted");
    assert!(!err_out.is_null());
    let err_out_msg = unsafe { CStr::from_ptr(err_out) }.to_str().unwrap();
    assert_eq!(err_out_msg, "query aborted");
    sqlite3_free(err_out.cast());

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let mut tail: *const c_char = null();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, &mut tail);
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());
    assert_eq!(sqlite3_errcode(db), SQLITE_OK);
    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn bind_text_accepts_invalid_utf8_bytes() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select ?1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());

    let text = [0xff_u8];
    let rc = sqlite3_bind_text(
        stmt,
        1,
        text.as_ptr() as *const c_char,
        text.len() as i32,
        SQLITE_TRANSIENT,
    );
    assert_eq!(rc, SQLITE_OK);

    let rc = sqlite3_step(stmt);
    assert_eq!(rc, SQLITE_ROW);
    assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_TEXT);
    assert_eq!(sqlite3_column_bytes(stmt, 0), 1);
    let ptr = sqlite3_column_blob(stmt, 0) as *const u8;
    assert!(!ptr.is_null());
    assert_eq!(unsafe { *ptr }, 0xff_u8);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn bind_text_invalid_index_returns_range_without_crash() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select ?1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());

    let text = b"abc";
    let rc = sqlite3_bind_text(
        stmt,
        2,
        text.as_ptr() as *const c_char,
        text.len() as i32,
        SQLITE_TRANSIENT,
    );
    assert_eq!(rc, SQLITE_RANGE);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn bind_blob_invalid_index_returns_range_without_crash() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select ?1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());

    let blob = [1_u8, 2, 3];
    let rc = sqlite3_bind_blob(
        stmt,
        2,
        blob.as_ptr() as *const c_void,
        blob.len() as i32,
        SQLITE_TRANSIENT,
    );
    assert_eq!(rc, SQLITE_RANGE);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

extern "C" fn invalid_utf8_text_scalar(ctx: *mut c_void, _argc: i32, _argv: *mut *mut c_void) {
    let text = [0xff_u8];
    sqlite3_result_text(
        ctx,
        text.as_ptr() as *const c_char,
        text.len() as i32,
        SQLITE_TRANSIENT,
    );
}

#[test]
fn result_text_accepts_invalid_utf8_bytes() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let fn_name = CString::new("invalid_utf8_text").unwrap();
    let rc = sqlite3_create_function_v2(
        db,
        fn_name.as_ptr(),
        0,
        SQLITE_UTF8,
        null_mut(),
        Some(invalid_utf8_text_scalar),
        None,
        None,
        None,
    );
    assert_eq!(rc, SQLITE_OK);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select invalid_utf8_text()").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());

    let rc = sqlite3_step(stmt);
    assert_eq!(rc, SQLITE_ROW);
    assert_eq!(sqlite3_column_type(stmt, 0), SQLITE_TEXT);
    assert_eq!(sqlite3_column_bytes(stmt, 0), 1);
    let ptr = sqlite3_column_blob(stmt, 0) as *const u8;
    assert!(!ptr.is_null());
    assert_eq!(unsafe { *ptr }, 0xff_u8);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn close_v2_keeps_statement_usable_until_finalize() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let mut stmt: *mut sqlite3_stmt = null_mut();
    let sql = CString::new("select 1").unwrap();
    let rc = sqlite3_prepare_v2(db, sql.as_ptr(), -1, &mut stmt, null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(!stmt.is_null());

    let rc = sqlite3_close_v2(db);
    assert_eq!(rc, SQLITE_OK);
    // Statement should remain usable after close_v2.
    let rc = sqlite3_step(stmt);
    assert_eq!(rc, SQLITE_ROW);

    let rc = sqlite3_finalize(stmt);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn context_db_handle_returns_shim_handle() {
    if !ensure_provider() {
        return;
    }
    CONTEXT_DB_HANDLE.store(0, Ordering::SeqCst);

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let fn_name = CString::new("ctx_db_handle_probe").unwrap();
    let rc = sqlite3_create_function_v2(
        db,
        fn_name.as_ptr(),
        0,
        SQLITE_UTF8,
        null_mut(),
        Some(capture_context_db_handle),
        None,
        None,
        None,
    );
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("SELECT ctx_db_handle_probe();").unwrap();
    let rc = sqlite3_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    assert_eq!(CONTEXT_DB_HANDLE.load(Ordering::SeqCst), db as usize);

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn create_function_v2_destroy_callback_matches_sqlite_signature_and_preserves_user_data() {
    if !ensure_provider() {
        return;
    }
    FUNCTION_DESTROY_CALLED.store(false, Ordering::SeqCst);
    FUNCTION_USER_DATA_VALUE.store(0, Ordering::SeqCst);
    FUNCTION_DESTROY_USER_DATA.store(0, Ordering::SeqCst);

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let mut function_user_data: usize = 0x5A17;
    let fn_name = CString::new("capture_function_user_data").unwrap();
    let rc = sqlite3_create_function_v2(
        db,
        fn_name.as_ptr(),
        0,
        SQLITE_UTF8,
        &mut function_user_data as *mut usize as *mut c_void,
        Some(capture_function_user_data),
        None,
        None,
        Some(function_destroy_with_context),
    );
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("SELECT capture_function_user_data();").unwrap();
    let rc = sqlite3_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(
        FUNCTION_USER_DATA_VALUE.load(Ordering::SeqCst),
        function_user_data
    );

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
    assert!(FUNCTION_DESTROY_CALLED.load(Ordering::SeqCst));
    assert_eq!(
        FUNCTION_DESTROY_USER_DATA.load(Ordering::SeqCst),
        &mut function_user_data as *mut usize as usize
    );
}

#[test]
fn progress_handler_registers_via_void_abi_signature() {
    if !ensure_provider() {
        return;
    }
    PROGRESS_CALLS.store(0, Ordering::SeqCst);

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    sqlite3_progress_handler(
        db,
        1,
        Some(counting_progress_handler),
        &PROGRESS_CALLS as *const AtomicUsize as *mut c_void,
    );

    let sql = CString::new(
        "WITH RECURSIVE seq(x) AS \
         (VALUES(1) UNION ALL SELECT x + 1 FROM seq WHERE x < 4000) \
         SELECT sum(x) FROM seq;",
    )
    .unwrap();
    let rc = sqlite3_exec(db, sql.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);
    assert!(PROGRESS_CALLS.load(Ordering::SeqCst) > 0);

    sqlite3_progress_handler(db, 0, None, null_mut());
    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn create_collation_v2_registers_and_orders_rows() {
    if !ensure_provider() {
        return;
    }
    COLLATION_DESTROY_CALLED.store(false, Ordering::SeqCst);

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let setup =
        CString::new("CREATE TABLE t(name TEXT); INSERT INTO t VALUES('a'),('c'),('b');").unwrap();
    let rc = sqlite3_exec(db, setup.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let name = CString::new("reverse_bin").unwrap();
    let rc = sqlite3_create_collation_v2(
        db,
        name.as_ptr(),
        SQLITE_UTF8,
        null_mut(),
        Some(reverse_binary_collation),
        Some(collation_destroy),
    );
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("SELECT name FROM t ORDER BY name COLLATE reverse_bin;").unwrap();
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
    assert_eq!(rows, 3);
    assert_eq!(cols, 1);
    assert_eq!(
        collect_table_values(table, rows, cols),
        vec![
            Some("name".to_string()),
            Some("c".to_string()),
            Some("b".to_string()),
            Some("a".to_string()),
        ]
    );
    sqlite3_free_table(table);

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
    assert!(COLLATION_DESTROY_CALLED.load(Ordering::SeqCst));
}

#[test]
fn complete_matches_system_sqlite3_for_edge_case_corpus() {
    if !ensure_provider() {
        return;
    }
    let system_complete = match system_sqlite3_complete() {
        Some(f) => f,
        None => return,
    };

    let corpus = [
        "SELECT 1;",
        ";",
        ";;",
        "-- x\n;",
        "/*x*/ ;",
        "SELECT 'a;';",
        "SELECT [a;b];",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); END;",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT CASE WHEN 1 THEN 'a;b' ELSE 'z' END; END;",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); INSERT INTO t VALUES (2); END; SELECT 1;",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); END SELECT 1;",
        "SELECT 1",
        "/* unterminated",
    ];

    for sql in corpus {
        let sql = CString::new(sql).unwrap();
        let shim = sqlite3_complete(sql.as_ptr());
        let system = unsafe { system_complete(sql.as_ptr()) };
        assert_eq!(shim, system, "sqlite3_complete mismatch for SQL: {sql:?}");
    }
}

#[test]
fn complete_matches_system_sqlite3_for_generated_suffix_matrix() {
    if !ensure_provider() {
        return;
    }
    let system_complete = match system_sqlite3_complete() {
        Some(f) => f,
        None => return,
    };

    let bases = [
        "SELECT 1",
        "SELECT 'a;''b'",
        "SELECT \"x;y\"",
        "SELECT [a;b]",
        "WITH c(x) AS (SELECT 1) SELECT x FROM c",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); END",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN SELECT CASE WHEN NEW.id > 0 THEN 'a;b' ELSE 'z' END; END",
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN INSERT INTO t VALUES (1); END SELECT 1",
    ];
    let suffixes = [
        "",
        ";",
        " ; ",
        " ; -- trailing comment\n",
        " /* trailing comment */ ;",
        ";\nSELECT 2;",
    ];

    for base in bases {
        for suffix in suffixes {
            let sql = CString::new(format!("{base}{suffix}")).unwrap();
            let shim = sqlite3_complete(sql.as_ptr());
            let system = unsafe { system_complete(sql.as_ptr()) };
            assert_eq!(
                shim, system,
                "sqlite3_complete mismatch for generated SQL: {sql:?}"
            );
        }
    }
}

#[test]
fn null_cleanup_entrypoints_match_sqlite_ok_semantics() {
    assert_eq!(sqlite3_finalize(null_mut()), SQLITE_OK);
    assert_eq!(sqlite3_reset(null_mut()), SQLITE_OK);
    assert_eq!(sqlite3_close(null_mut()), SQLITE_OK);
    assert_eq!(sqlite3_close_v2(null_mut()), SQLITE_OK);
}

#[test]
fn busy_timeout_is_available_with_adapter_backend() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let rc = sqlite3_busy_timeout(db, 50);
    assert_eq!(rc, SQLITE_OK);

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn backup_api_smoke_copies_rows_between_connections() {
    if !ensure_provider() {
        return;
    }

    let mut source: *mut sqlite3 = null_mut();
    let source_name = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(source_name.as_ptr(), &mut source);
    assert_eq!(rc, SQLITE_OK);

    let mut dest: *mut sqlite3 = null_mut();
    let dest_name = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(dest_name.as_ptr(), &mut dest);
    assert_eq!(rc, SQLITE_OK);

    let setup = CString::new("CREATE TABLE t(v INTEGER); INSERT INTO t VALUES(7),(9);").unwrap();
    let rc = sqlite3_exec(source, setup.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let main = CString::new("main").unwrap();
    let backup = sqlite3_backup_init(dest, main.as_ptr(), source, main.as_ptr());
    assert!(!backup.is_null());
    assert!(sqlite3_backup_pagecount(backup) >= 0);
    assert!(sqlite3_backup_remaining(backup) >= 0);
    let rc = sqlite3_backup_step(backup, -1);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_backup_finish(backup);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("SELECT count(*) FROM t;").unwrap();
    let mut table: *mut *mut c_char = null_mut();
    let mut rows = 0;
    let mut cols = 0;
    let rc = sqlite3_get_table(
        dest,
        sql.as_ptr(),
        &mut table,
        &mut rows,
        &mut cols,
        null_mut(),
    );
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(
        collect_table_values(table, rows, cols)[1].as_deref(),
        Some("2")
    );
    sqlite3_free_table(table);

    let rc = sqlite3_close(source);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(dest);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn optional_api_failure_paths_return_non_ok_and_keep_out_params_sanitized() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let setup = CString::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB); \
         INSERT INTO t(id, payload) VALUES(1, zeroblob(4));",
    )
    .unwrap();
    let rc = sqlite3_exec(db, setup.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let mut blob: *mut c_void = 1 as *mut c_void;
    let main = CString::new("main").unwrap();
    let table_name = CString::new("t").unwrap();
    let column_name = CString::new("payload").unwrap();
    let rc = sqlite3_blob_open(
        db,
        main.as_ptr(),
        table_name.as_ptr(),
        column_name.as_ptr(),
        9999,
        1,
        &mut blob,
    );
    assert_ne!(rc, SQLITE_OK);
    assert!(blob.is_null());

    let mut image: *mut c_void = 1 as *mut c_void;
    let mut image_len = 777;
    let missing_schema = CString::new("missing_schema").unwrap();
    let rc = sqlite3_serialize(db, missing_schema.as_ptr(), &mut image, &mut image_len, 0);
    assert_ne!(rc, SQLITE_OK);
    assert!(image.is_null());
    assert_eq!(image_len, 0);

    assert_eq!(sqlite3_backup_step(null_mut(), 1), SQLITE_MISUSE);
    assert_eq!(sqlite3_backup_finish(null_mut()), SQLITE_MISUSE);
    assert_eq!(
        sqlite3_blob_read(null_mut(), null_mut(), 4, 0),
        SQLITE_MISUSE
    );
    assert_eq!(sqlite3_blob_write(null_mut(), null(), 4, 0), SQLITE_MISUSE);

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn blob_api_smoke_reads_and_writes_payload() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let setup = CString::new(
        "CREATE TABLE t(id INTEGER PRIMARY KEY, payload BLOB); \
         INSERT INTO t(id, payload) VALUES(1, zeroblob(4));",
    )
    .unwrap();
    let rc = sqlite3_exec(db, setup.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let mut blob: *mut c_void = null_mut();
    let main = CString::new("main").unwrap();
    let table_name = CString::new("t").unwrap();
    let column_name = CString::new("payload").unwrap();
    let rc = sqlite3_blob_open(
        db,
        main.as_ptr(),
        table_name.as_ptr(),
        column_name.as_ptr(),
        1,
        1,
        &mut blob,
    );
    assert_eq!(rc, SQLITE_OK);
    assert!(!blob.is_null());
    assert_eq!(sqlite3_blob_bytes(blob), 4);

    let written = [1_u8, 2, 3, 4];
    let rc = sqlite3_blob_write(blob, written.as_ptr().cast(), written.len() as i32, 0);
    assert_eq!(rc, SQLITE_OK);

    let mut read = [0_u8; 4];
    let rc = sqlite3_blob_read(blob, read.as_mut_ptr().cast(), read.len() as i32, 0);
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(read, written);

    let rc = sqlite3_blob_close(blob);
    assert_eq!(rc, SQLITE_OK);

    let sql = CString::new("SELECT hex(payload) FROM t WHERE id = 1;").unwrap();
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
    assert_eq!(
        collect_table_values(table, rows, cols)[1].as_deref(),
        Some("01020304")
    );
    sqlite3_free_table(table);

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn serialize_and_deserialize_smoke_round_trip() {
    if !ensure_provider() {
        return;
    }

    let mut source: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut source);
    assert_eq!(rc, SQLITE_OK);

    let setup = CString::new("CREATE TABLE s(v INTEGER); INSERT INTO s VALUES(42);").unwrap();
    let rc = sqlite3_exec(source, setup.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let mut image: *mut c_void = null_mut();
    let mut image_len = 0;
    let rc = sqlite3_serialize(source, null(), &mut image, &mut image_len, 0);
    assert_eq!(rc, SQLITE_OK);
    assert!(!image.is_null());
    assert!(image_len > 0);

    let mut dest: *mut sqlite3 = null_mut();
    let rc = sqlite3_open(filename.as_ptr(), &mut dest);
    assert_eq!(rc, SQLITE_OK);

    let rc = sqlite3_deserialize(dest, null(), image, image_len, 0);
    assert_eq!(rc, SQLITE_OK);

    let query = CString::new("SELECT v FROM s;").unwrap();
    let mut table: *mut *mut c_char = null_mut();
    let mut rows = 0;
    let mut cols = 0;
    let rc = sqlite3_get_table(
        dest,
        query.as_ptr(),
        &mut table,
        &mut rows,
        &mut cols,
        null_mut(),
    );
    assert_eq!(rc, SQLITE_OK);
    assert_eq!(
        collect_table_values(table, rows, cols)[1].as_deref(),
        Some("42")
    );
    sqlite3_free_table(table);

    sqlite3_free(image);
    let rc = sqlite3_close(source);
    assert_eq!(rc, SQLITE_OK);
    let rc = sqlite3_close(dest);
    assert_eq!(rc, SQLITE_OK);
}

#[test]
fn wal_checkpoint_smoke_on_file_database() {
    if !ensure_provider() {
        return;
    }

    let filename = unique_temp_db_path("wal_checkpoint");
    let mut db: *mut sqlite3 = null_mut();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let setup = CString::new(
        "PRAGMA journal_mode=WAL; CREATE TABLE t(v INTEGER); INSERT INTO t VALUES(1);",
    )
    .unwrap();
    let rc = sqlite3_exec(db, setup.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let mut log_size = -1;
    let mut checkpointed = -1;
    let rc = sqlite3_wal_checkpoint_v2(
        db,
        null(),
        SQLITE_CHECKPOINT_PASSIVE,
        &mut log_size,
        &mut checkpointed,
    );
    assert_eq!(rc, SQLITE_OK);
    assert!(log_size >= 0);
    assert!(checkpointed >= 0);

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);

    let filename = filename.into_string().unwrap();
    let _ = fs::remove_file(&filename);
    let _ = fs::remove_file(format!("{filename}-wal"));
    let _ = fs::remove_file(format!("{filename}-shm"));
}

#[test]
fn table_column_metadata_smoke_reports_declared_schema() {
    if !ensure_provider() {
        return;
    }

    let mut db: *mut sqlite3 = null_mut();
    let filename = CString::new(":memory:").unwrap();
    let rc = sqlite3_open(filename.as_ptr(), &mut db);
    assert_eq!(rc, SQLITE_OK);

    let setup = CString::new(
        "CREATE TABLE m(id INTEGER PRIMARY KEY AUTOINCREMENT, \
         name TEXT COLLATE NOCASE NOT NULL);",
    )
    .unwrap();
    let rc = sqlite3_exec(db, setup.as_ptr(), None, null_mut(), null_mut());
    assert_eq!(rc, SQLITE_OK);

    let table = CString::new("m").unwrap();
    let column = CString::new("name").unwrap();
    let mut data_type: *const c_char = null();
    let mut coll_seq: *const c_char = null();
    let mut not_null = -1;
    let mut primary_key = -1;
    let mut autoinc = -1;
    let rc = sqlite3_table_column_metadata(
        db,
        null(),
        table.as_ptr(),
        column.as_ptr(),
        &mut data_type,
        &mut coll_seq,
        &mut not_null,
        &mut primary_key,
        &mut autoinc,
    );
    assert_eq!(rc, SQLITE_OK);
    assert!(!data_type.is_null());
    assert!(!coll_seq.is_null());
    assert_eq!(not_null, 1);
    assert_eq!(primary_key, 0);
    assert_eq!(autoinc, 0);

    let data_type = unsafe { CStr::from_ptr(data_type) }.to_string_lossy();
    let coll_seq = unsafe { CStr::from_ptr(coll_seq) }.to_string_lossy();
    assert!(data_type.eq_ignore_ascii_case("text"));
    assert!(coll_seq.eq_ignore_ascii_case("nocase"));

    let rc = sqlite3_close(db);
    assert_eq!(rc, SQLITE_OK);
}
