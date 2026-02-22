#![cfg(not(feature = "default-backend"))]

use sqlite_provider_abi::{sqlite3, sqlite3_open_v2};
use std::ffi::CString;
use std::ptr::null;

const SQLITE_MISUSE: i32 = 21;
const SQLITE_OPEN_READWRITE: i32 = 0x0000_0002;

#[test]
fn open_v2_clears_db_out_when_default_bootstrap_is_unavailable() {
    let filename = CString::new(":memory:").expect("valid filename");
    let mut db = 1 as *mut sqlite3;
    let rc = sqlite3_open_v2(filename.as_ptr(), &mut db, SQLITE_OPEN_READWRITE, null());
    assert_eq!(rc, SQLITE_MISUSE);
    assert!(db.is_null());
}
