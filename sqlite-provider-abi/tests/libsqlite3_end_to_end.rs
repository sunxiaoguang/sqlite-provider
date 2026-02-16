use libc::c_char;
use sqlite_provider_abi::{
    register_provider, sqlite3, sqlite3_close, sqlite3_exec, sqlite3_free_table, sqlite3_get_table,
    sqlite3_open, ProviderState,
};
use sqlite_provider_sqlite3::LibSqlite3;
use std::ffi::{CStr, CString};
use std::ptr::null_mut;
use std::sync::OnceLock;

const SQLITE_OK: i32 = 0;

fn ensure_provider() -> bool {
    static INIT: OnceLock<bool> = OnceLock::new();
    *INIT.get_or_init(|| {
        if let Some(api) = LibSqlite3::load() {
            let state = ProviderState::new(api).with_metadata(api);
            let _ = register_provider(state);
            true
        } else {
            false
        }
    })
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
