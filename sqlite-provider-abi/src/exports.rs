//! C ABI entrypoints exported by the shim.
//!
//! The public symbols in this module intentionally mirror `sqlite3.h` names
//! and signatures.

use super::*;
use crate::state::{
    MallocAllocOwner, lookup_db_handle, lookup_udf_user_data, register_db_handle,
    register_malloc_alloc, register_table_result_alloc, register_udf_user_data, take_malloc_alloc,
    take_table_result_alloc, unregister_db_handle, unregister_udf_user_data,
};

unsafe fn free_with_owner(owner: MallocAllocOwner, ptr: *mut c_void) {
    match owner {
        MallocAllocOwner::Provider(core) => unsafe { core.free(ptr) },
        MallocAllocOwner::Libc => unsafe { libc::free(ptr) },
    }
}

#[repr(C)]
struct WrappedUdfDestroy {
    destroy: Option<extern "C" fn(*mut c_void)>,
}

fn wrap_udf_user_data(
    user_data: *mut c_void,
    destroy: Option<extern "C" fn(*mut c_void)>,
) -> *mut c_void {
    let wrapped = Box::new(WrappedUdfDestroy { destroy });
    let wrapped_ptr = Box::into_raw(wrapped) as *mut c_void;
    register_udf_user_data(wrapped_ptr, user_data);
    wrapped_ptr
}

extern "C" fn destroy_wrapped_udf_user_data(wrapped: *mut c_void) {
    if wrapped.is_null() {
        return;
    }
    let user_data = unregister_udf_user_data(wrapped).unwrap_or(null_mut());
    let wrapped = unsafe { Box::from_raw(wrapped as *mut WrappedUdfDestroy) };
    if let Some(destroy) = wrapped.destroy {
        destroy(user_data);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_initialize() -> i32 {
    ensure_default_provider()
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_shutdown() -> i32 {
    SQLITE_OK
}

/// Initialize the default backend provider.
///
/// Returns `SQLITE_OK` if a provider is already registered or if the default
/// `libsqlite3` backend initializes successfully.
#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_provider_init_default() -> i32 {
    ensure_default_provider()
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_open(filename: *const c_char, db_out: *mut *mut sqlite3) -> i32 {
    sqlite3_open_v2(
        filename,
        db_out,
        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
        null(),
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_open_v2(
    filename: *const c_char,
    db_out: *mut *mut sqlite3,
    flags: i32,
    vfs: *const c_char,
) -> i32 {
    if db_out.is_null() {
        return SQLITE_MISUSE;
    }
    unsafe { *db_out = null_mut() };
    let init_rc = ensure_default_provider();
    if init_rc != SQLITE_OK {
        return init_rc;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let filename = unsafe { cstr_to_str(filename) };
    let filename = match filename {
        Some(name) => name,
        None => return SQLITE_MISUSE,
    };
    let vfs = unsafe { cstr_to_str(vfs) };
    let options = OpenOptions {
        flags: open_flags_from_sqlite(flags),
        vfs,
    };
    let db = match unsafe { state.core.open(filename, options) } {
        Ok(db) => db,
        Err(err) => return err_code(&err),
    };
    let filename = CString::new(filename).ok();
    let handle = Box::new(sqlite3 {
        db,
        stmts: null_mut(),
        close_pending: false,
        filename,
        exec_callback_aborted: false,
    });
    let handle = Box::into_raw(handle);
    register_db_handle(db, handle);
    unsafe { *db_out = handle };
    SQLITE_OK
}

fn close_db_handle(state: &ProviderState, db: *mut sqlite3) -> i32 {
    let db_ref = unsafe { &mut *db };
    let rc = match unsafe { state.core.close(db_ref.db) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    };
    if rc == SQLITE_OK {
        unregister_db_handle(db_ref.db);
        unsafe { drop(Box::from_raw(db)) };
    }
    rc
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_close(db: *mut sqlite3) -> i32 {
    if db.is_null() {
        return SQLITE_OK;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let db_ref = unsafe { &mut *db };
    if !db_ref.stmts.is_null() {
        return SQLITE_BUSY;
    }
    close_db_handle(state, db)
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_close_v2(db: *mut sqlite3) -> i32 {
    if db.is_null() {
        return SQLITE_OK;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    {
        let db_ref = unsafe { &mut *db };
        db_ref.close_pending = true;
        if !db_ref.stmts.is_null() {
            // SQLite close_v2 enters zombie mode and defers actual close
            // until outstanding statements are finalized.
            return SQLITE_OK;
        }
    }
    close_db_handle(state, db)
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_db_filename(db: *mut sqlite3, db_name: *const c_char) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if db.is_null() {
        return null();
    }
    let db_ref = unsafe { &*db };
    let db_name = unsafe { cstr_to_str(db_name) };
    if let Some(extras) = state.extras
        && let Some(raw) = unsafe { extras.db_filename(db_ref.db, db_name) }
    {
        return raw.ptr as *const c_char;
    }
    db_ref
        .filename
        .as_ref()
        .map(|name| name.as_ptr())
        .unwrap_or(null())
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_trace_v2(
    db: *mut sqlite3,
    mask: u32,
    callback: Option<extern "C" fn(u32, *mut c_void, *mut c_void, *mut c_void)>,
    context: *mut c_void,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { hooks.trace_v2(db.db, mask, callback, context) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_progress_handler(
    db: *mut sqlite3,
    n: i32,
    callback: Option<extern "C" fn(*mut c_void) -> i32>,
    context: *mut c_void,
) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return,
    };
    if db.is_null() {
        return;
    }
    let db = unsafe { &*db };
    let _ = unsafe { hooks.progress_handler(db.db, n, callback, context) };
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_busy_timeout(db: *mut sqlite3, ms: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { hooks.busy_timeout(db.db, ms) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_set_authorizer(
    db: *mut sqlite3,
    callback: Option<
        extern "C" fn(
            *mut c_void,
            i32,
            *const c_char,
            *const c_char,
            *const c_char,
            *const c_char,
        ) -> i32,
    >,
    context: *mut c_void,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let hooks = match state.hooks {
        Some(hooks) => hooks,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { hooks.set_authorizer(db.db, callback, context) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_context_db_handle(context: *mut c_void) -> *mut sqlite3 {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let extras = match state.extras {
        Some(extras) => extras,
        None => return null_mut(),
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return null_mut(),
    };
    match unsafe { extras.context_db_handle(ctx) } {
        Some(db) => lookup_db_handle(db).unwrap_or(null_mut()),
        None => null_mut(),
    }
}

fn prepare_statement(
    state: &ProviderState,
    db: *mut sqlite3,
    sql: *const c_char,
    len: i32,
    out_stmt: *mut *mut sqlite3_stmt,
    tail: *mut *const c_char,
    prep_flags: Option<u32>,
) -> i32 {
    if db.is_null() || out_stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let db_ref = unsafe { &mut *db };
    db_ref.exec_callback_aborted = false;
    let sql_bytes = unsafe {
        if sql.is_null() {
            return SQLITE_MISUSE;
        }
        if len < 0 {
            CStr::from_ptr(sql).to_bytes()
        } else {
            std::slice::from_raw_parts(sql.cast(), len as usize)
        }
    };
    let split = split_first_statement(sql_bytes, 0);
    unsafe { *out_stmt = null_mut() };
    if !tail.is_null() {
        unsafe {
            *tail = if split.tail <= sql_bytes.len() {
                sql.add(split.tail)
            } else {
                null()
            };
        }
    }
    if !split.complete {
        return SQLITE_ERROR;
    }
    let (stmt_start, stmt_end) = match split.range {
        Some(range) => range,
        None => return SQLITE_OK,
    };
    let stmt_sql = match std::str::from_utf8(&sql_bytes[stmt_start..stmt_end]) {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let stmt = if let Some(flags) = prep_flags {
        if state.core.feature_set().contains(FeatureSet::PREPARE_V3) {
            match unsafe { state.core.prepare_v3(db_ref.db, stmt_sql, flags) } {
                Ok(stmt) => stmt,
                Err(err) => return err_code(&err),
            }
        } else {
            match unsafe { state.core.prepare_v2(db_ref.db, stmt_sql) } {
                Ok(stmt) => stmt,
                Err(err) => return err_code(&err),
            }
        }
    } else {
        match unsafe { state.core.prepare_v2(db_ref.db, stmt_sql) } {
            Ok(stmt) => stmt,
            Err(err) => return err_code(&err),
        }
    };
    let sql_cstr = CString::new(stmt_sql).ok();
    let stmt_handle = Box::new(sqlite3_stmt {
        stmt,
        db,
        next: null_mut(),
        last_step: StepResult::Done,
        sql: sql_cstr,
    });
    let stmt_ptr = Box::into_raw(stmt_handle);
    unsafe { add_stmt(db_ref, stmt_ptr) };
    unsafe { *out_stmt = stmt_ptr };
    SQLITE_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_prepare_v2(
    db: *mut sqlite3,
    sql: *const c_char,
    len: i32,
    out_stmt: *mut *mut sqlite3_stmt,
    tail: *mut *const c_char,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    prepare_statement(state, db, sql, len, out_stmt, tail, None)
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_prepare_v3(
    db: *mut sqlite3,
    sql: *const c_char,
    len: i32,
    prep_flags: u32,
    out_stmt: *mut *mut sqlite3_stmt,
    tail: *mut *const c_char,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    prepare_statement(state, db, sql, len, out_stmt, tail, Some(prep_flags))
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_finalize(stmt: *mut sqlite3_stmt) -> i32 {
    if stmt.is_null() {
        return SQLITE_OK;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let stmt_ref = unsafe { &mut *stmt };
    let rc = match unsafe { state.core.finalize(stmt_ref.stmt) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    };
    let db = stmt_ref.db;
    if !db.is_null() {
        let should_close = unsafe {
            let db_ref = &mut *db;
            remove_stmt(db_ref, stmt);
            db_ref.close_pending && db_ref.stmts.is_null()
        };
        if should_close {
            let _ = close_db_handle(state, db);
        }
    }
    unsafe { drop(Box::from_raw(stmt)) };
    rc
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_step(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt_ref = unsafe { &mut *stmt };
    match unsafe { state.core.step(stmt_ref.stmt) } {
        Ok(StepResult::Row) => {
            stmt_ref.last_step = StepResult::Row;
            SQLITE_ROW
        }
        Ok(StepResult::Done) => {
            stmt_ref.last_step = StepResult::Done;
            SQLITE_DONE
        }
        Err(err) => {
            stmt_ref.last_step = StepResult::Done;
            err_code(&err)
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_reset(stmt: *mut sqlite3_stmt) -> i32 {
    if stmt.is_null() {
        return SQLITE_OK;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let stmt_ref = unsafe { &mut *stmt };
    stmt_ref.last_step = StepResult::Done;
    match unsafe { state.core.reset(stmt_ref.stmt) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_exec(
    db: *mut sqlite3,
    sql: *const c_char,
    callback: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_char, *mut *mut c_char) -> i32>,
    context: *mut c_void,
    err_out: *mut *mut c_char,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || sql.is_null() {
        return SQLITE_MISUSE;
    }
    if !err_out.is_null() {
        unsafe { *err_out = null_mut() };
    }
    let sql_str = unsafe { CStr::from_ptr(sql) };
    let sql_str = match sql_str.to_str() {
        Ok(s) => s,
        Err(_) => return SQLITE_MISUSE,
    };
    let sql_bytes = sql_str.as_bytes();
    let db_ref = unsafe { &mut *db };
    db_ref.exec_callback_aborted = false;
    let mut offset = 0usize;
    let mut rc = SQLITE_OK;
    while offset <= sql_bytes.len() {
        let split = split_first_statement(sql_bytes, offset);
        if !split.complete {
            rc = SQLITE_ERROR;
            break;
        }
        let (stmt_start, stmt_end) = match split.range {
            Some(range) => range,
            None => {
                if split.tail <= offset || split.tail > sql_bytes.len() {
                    break;
                }
                offset = split.tail;
                continue;
            }
        };
        let stmt_sql = match std::str::from_utf8(&sql_bytes[stmt_start..stmt_end]) {
            Ok(s) => s,
            Err(_) => {
                rc = SQLITE_MISUSE;
                break;
            }
        };
        let stmt = match unsafe { state.core.prepare_v2(db_ref.db, stmt_sql) } {
            Ok(stmt) => stmt,
            Err(err) => {
                rc = err_code(&err);
                if let Some(msg) = err.message.as_deref() {
                    set_err_string(err_out, msg);
                }
                break;
            }
        };

        let col_count = unsafe { state.core.column_count(stmt) };
        let col_count = if col_count > 0 { col_count as usize } else { 0 };
        let mut name_storage: Vec<u8> = Vec::new();
        let mut name_offsets: Vec<Option<usize>> = vec![None; col_count];
        let mut name_ptrs: Vec<*mut c_char> = vec![null_mut(); col_count];
        if callback.is_some() && col_count > 0 {
            for (i, name_offset) in name_offsets.iter_mut().enumerate().take(col_count) {
                let name = state
                    .metadata
                    .and_then(|metadata| unsafe { metadata.column_name(stmt, i as i32) });
                push_column_name(&mut name_storage, name_offset, name, i);
            }
            let base = name_storage.as_mut_ptr();
            for (idx, offset) in name_offsets.iter().enumerate() {
                if let Some(offset) = *offset {
                    name_ptrs[idx] = unsafe { base.add(offset) as *mut c_char };
                }
            }
        }

        let mut value_storage: Vec<u8> = Vec::new();
        let mut value_offsets: Vec<Option<usize>> = vec![None; col_count];
        let mut value_ptrs: Vec<*mut c_char> = vec![null_mut(); col_count];
        let stmt_rc = loop {
            match unsafe { state.core.step(stmt) } {
                Ok(StepResult::Row) => {
                    if let Some(cb) = callback {
                        value_storage.clear();
                        value_offsets.fill(None);
                        value_ptrs.fill(null_mut());
                        for (i, value_offset) in
                            value_offsets.iter_mut().enumerate().take(col_count)
                        {
                            let raw = unsafe { state.core.column_text(stmt, i as i32) };
                            if raw.ptr.is_null() {
                                continue;
                            }
                            let start = value_storage.len();
                            value_storage.extend_from_slice(unsafe { raw.as_slice() });
                            value_storage.push(0);
                            *value_offset = Some(start);
                        }
                        if !value_storage.is_empty() {
                            let base = value_storage.as_mut_ptr();
                            for (idx, offset) in value_offsets.iter().enumerate() {
                                if let Some(offset) = *offset {
                                    value_ptrs[idx] = unsafe { base.add(offset) as *mut c_char };
                                }
                            }
                        }
                        let cb_rc = cb(
                            context,
                            col_count as i32,
                            value_ptrs.as_mut_ptr(),
                            name_ptrs.as_mut_ptr(),
                        );
                        if cb_rc != SQLITE_OK {
                            db_ref.exec_callback_aborted = true;
                            break SQLITE_ABORT;
                        }
                    }
                }
                Ok(StepResult::Done) => {
                    break SQLITE_OK;
                }
                Err(err) => {
                    let stmt_rc = err_code(&err);
                    if let Some(msg) = err.message.as_deref() {
                        set_err_string(err_out, msg);
                    }
                    break stmt_rc;
                }
            }
        };
        let _ = unsafe { state.core.finalize(stmt) };
        if stmt_rc != SQLITE_OK {
            rc = stmt_rc;
            break;
        }
        if split.tail <= offset || split.tail > sql_bytes.len() {
            break;
        }
        offset = split.tail;
        if offset >= sql_bytes.len() {
            break;
        }
    }
    if rc != SQLITE_OK && !err_out.is_null() {
        let needs_msg = unsafe { (*err_out).is_null() };
        if needs_msg {
            let msg_ptr = sqlite3_errmsg(db);
            if !msg_ptr.is_null() {
                let msg = unsafe { CStr::from_ptr(msg_ptr).to_bytes() };
                let out = sqlite3_malloc64((msg.len() + 1) as u64) as *mut c_char;
                if !out.is_null() {
                    unsafe {
                        std::ptr::copy_nonoverlapping(msg.as_ptr(), out.cast(), msg.len());
                        *out.add(msg.len()) = 0;
                        *err_out = out;
                    }
                }
            }
        }
    }
    rc
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_changes(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.changes(db.db) },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_changes64(db: *mut sqlite3) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.changes64(db.db) },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_stmt_readonly(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.stmt_readonly(stmt.stmt) },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_stmt_busy(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.stmt_busy(stmt.stmt) },
        None => {
            if stmt.last_step == StepResult::Row {
                1
            } else {
                0
            }
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_next_stmt(
    db: *mut sqlite3,
    stmt: *mut sqlite3_stmt,
) -> *mut sqlite3_stmt {
    if db.is_null() {
        return null_mut();
    }
    let db = unsafe { &*db };
    if stmt.is_null() {
        return db.stmts;
    }
    let mut cur = db.stmts;
    while !cur.is_null() {
        if cur == stmt {
            return unsafe { (*cur).next };
        }
        cur = unsafe { (*cur).next };
    }
    null_mut()
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_get_autocommit(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 1,
    };
    if db.is_null() {
        return 1;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.get_autocommit(db.db) },
        None => 1,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_total_changes(db: *mut sqlite3) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.total_changes(db.db) },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_last_insert_rowid(db: *mut sqlite3) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if db.is_null() {
        return 0;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.last_insert_rowid(db.db) },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_interrupt(db: *mut sqlite3) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    if db.is_null() {
        return;
    }
    let db = unsafe { &*db };
    if let Some(extras) = state.extras {
        unsafe { extras.interrupt(db.db) };
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_db_config(db: *mut sqlite3, op: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.db_config(db.db, op) },
        None => SQLITE_MISUSE,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_limit(db: *mut sqlite3, id: i32, new_value: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return -1,
    };
    if db.is_null() {
        return -1;
    }
    let db = unsafe { &*db };
    match state.extras {
        Some(extras) => unsafe { extras.limit(db.db, id, new_value) },
        None => -1,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_db_handle(stmt: *mut sqlite3_stmt) -> *mut sqlite3 {
    if stmt.is_null() {
        return null_mut();
    }
    unsafe { (*stmt).db }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_sleep(ms: i32) -> i32 {
    if ms <= 0 {
        return 0;
    }
    std::thread::sleep(std::time::Duration::from_millis(ms as u64));
    ms
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_errcode(db: *mut sqlite3) -> i32 {
    if db.is_null() {
        return SQLITE_NOMEM;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    if db.exec_callback_aborted {
        return SQLITE_ABORT;
    }
    unsafe { state.core.errcode(db.db) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_errmsg(db: *mut sqlite3) -> *const c_char {
    if db.is_null() {
        return NOMEM_MSG.as_ptr().cast();
    }
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let db = unsafe { &*db };
    if db.exec_callback_aborted {
        return EXEC_CALLBACK_ABORT_MSG.as_ptr() as *const c_char;
    }
    unsafe { state.core.errmsg(db.db) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_extended_errcode(db: *mut sqlite3) -> i32 {
    if db.is_null() {
        return SQLITE_NOMEM;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    if db.exec_callback_aborted {
        return SQLITE_ABORT;
    }
    unsafe { state.core.extended_errcode(db.db) }.unwrap_or(SQLITE_ERROR)
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_errstr(code: i32) -> *const c_char {
    let primary = match code {
        SQLITE_ROW | SQLITE_DONE => code,
        _ => code & 0xff,
    };
    let msg: &'static [u8] = match primary {
        SQLITE_OK => b"not an error\0",
        SQLITE_ERROR => b"SQL logic error\0",
        SQLITE_INTERNAL => b"unknown error\0",
        SQLITE_PERM => b"access permission denied\0",
        SQLITE_ABORT => b"query aborted\0",
        SQLITE_BUSY => b"database is locked\0",
        SQLITE_LOCKED => b"database table is locked\0",
        SQLITE_NOMEM => b"out of memory\0",
        SQLITE_READONLY => b"attempt to write a readonly database\0",
        SQLITE_INTERRUPT => b"interrupted\0",
        SQLITE_IOERR => b"disk I/O error\0",
        SQLITE_CORRUPT => b"database disk image is malformed\0",
        SQLITE_NOTFOUND => b"unknown operation\0",
        SQLITE_FULL => b"database or disk is full\0",
        SQLITE_CANTOPEN => b"unable to open database file\0",
        SQLITE_PROTOCOL => b"locking protocol\0",
        SQLITE_EMPTY => b"unknown error\0",
        SQLITE_SCHEMA => b"database schema has changed\0",
        SQLITE_TOOBIG => b"string or blob too big\0",
        SQLITE_CONSTRAINT => b"constraint failed\0",
        SQLITE_MISMATCH => b"datatype mismatch\0",
        SQLITE_MISUSE => b"bad parameter or other API misuse\0",
        SQLITE_NOLFS => b"unknown error\0",
        SQLITE_AUTH => b"authorization denied\0",
        SQLITE_FORMAT => b"unknown error\0",
        SQLITE_RANGE => b"column index out of range\0",
        SQLITE_NOTADB => b"file is not a database\0",
        SQLITE_NOTICE => b"notification message\0",
        SQLITE_WARNING => b"warning message\0",
        SQLITE_ROW => b"another row available\0",
        SQLITE_DONE => b"no more rows available\0",
        _ => b"unknown error\0",
    };
    unsafe { CStr::from_bytes_with_nul_unchecked(msg).as_ptr() }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_bind_parameter_count(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.bind_parameter_count(stmt.stmt) },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_bind_parameter_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.bind_parameter_name(stmt.stmt, idx) }
            .map(|raw| raw.ptr as *const c_char)
            .unwrap_or(null()),
        None => null(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_bind_parameter_index(
    stmt: *mut sqlite3_stmt,
    name: *const c_char,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let name = unsafe { cstr_to_str(name) };
    let name = match name {
        Some(name) => name,
        None => return 0,
    };
    let stmt = unsafe { &*stmt };
    match state.extras {
        Some(extras) => unsafe { extras.bind_parameter_index(stmt.stmt, name) },
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_bind_null(stmt: *mut sqlite3_stmt, idx: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_null(stmt.stmt, idx) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_bind_int64(stmt: *mut sqlite3_stmt, idx: i32, val: i64) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_int64(stmt.stmt, idx, val) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_bind_double(stmt: *mut sqlite3_stmt, idx: i32, val: f64) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if stmt.is_null() {
        return SQLITE_MISUSE;
    }
    let stmt = unsafe { &*stmt };
    match unsafe { state.core.bind_double(stmt.stmt, idx, val) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
/// SQLite-compatible bind wrapper.
///
/// The shim forwards bytes into the registered provider and then applies the
/// caller-provided destructor contract (`SQLITE_STATIC`/`SQLITE_TRANSIENT`/custom).
pub extern "C" fn sqlite3_bind_text(
    stmt: *mut sqlite3_stmt,
    idx: i32,
    text: *const c_char,
    len: i32,
    destroy: *mut c_void,
) -> i32 {
    let destructor = decode_destructor(destroy);
    let text_ptr = text as *mut c_void;
    let state = match provider() {
        Some(state) => state,
        None => {
            unsafe { call_destructor(destructor, text_ptr) };
            return SQLITE_MISUSE;
        }
    };
    if stmt.is_null() {
        unsafe { call_destructor(destructor, text_ptr) };
        return SQLITE_MISUSE;
    }
    if text.is_null() {
        return sqlite3_bind_null(stmt, idx);
    }
    let text = unsafe { text_bytes_from_ptr(text, len) };
    let text = match text {
        Some(text) => text,
        None => {
            unsafe { call_destructor(destructor, text_ptr) };
            return SQLITE_MISUSE;
        }
    };
    let stmt = unsafe { &*stmt };
    let rc = match unsafe { state.core.bind_text_bytes(stmt.stmt, idx, text) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    };
    unsafe { call_destructor(destructor, text_ptr) };
    rc
}

#[unsafe(no_mangle)]
/// SQLite-compatible bind wrapper.
///
/// The shim forwards bytes into the registered provider and then applies the
/// caller-provided destructor contract (`SQLITE_STATIC`/`SQLITE_TRANSIENT`/custom).
pub extern "C" fn sqlite3_bind_blob(
    stmt: *mut sqlite3_stmt,
    idx: i32,
    blob: *const c_void,
    len: i32,
    destroy: *mut c_void,
) -> i32 {
    let destructor = decode_destructor(destroy);
    let blob_ptr = blob as *mut c_void;
    let state = match provider() {
        Some(state) => state,
        None => {
            unsafe { call_destructor(destructor, blob_ptr) };
            return SQLITE_MISUSE;
        }
    };
    if stmt.is_null() {
        unsafe { call_destructor(destructor, blob_ptr) };
        return SQLITE_MISUSE;
    }
    if blob.is_null() {
        return sqlite3_bind_null(stmt, idx);
    }
    let data = match unsafe { bytes_from_ptr(blob, len) } {
        Some(data) => data,
        None => {
            unsafe { call_destructor(destructor, blob_ptr) };
            return SQLITE_MISUSE;
        }
    };
    let stmt = unsafe { &*stmt };
    let rc = match unsafe { state.core.bind_blob(stmt.stmt, idx, data) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    };
    unsafe { call_destructor(destructor, blob_ptr) };
    rc
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_type(stmt: *mut sqlite3_stmt, idx: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_NULL,
    };
    if stmt.is_null() {
        return SQLITE_NULL;
    }
    let stmt = unsafe { &*stmt };
    value_type_to_code(unsafe { state.core.column_type(stmt.stmt, idx) })
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_count(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    unsafe { state.core.column_count(stmt.stmt) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_decltype(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    unsafe { metadata.column_decltype(stmt.stmt, idx) }
        .map(|raw| raw.ptr as *const c_char)
        .unwrap_or(null())
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    unsafe { metadata.column_name(stmt.stmt, idx) }
        .map(|raw| raw.ptr as *const c_char)
        .unwrap_or(null())
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_table_name(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_char {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    unsafe { metadata.column_table_name(stmt.stmt, idx) }
        .map(|raw| raw.ptr as *const c_char)
        .unwrap_or(null())
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_int64(stmt: *mut sqlite3_stmt, idx: i32) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    unsafe { state.core.column_int64(stmt.stmt, idx) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_double(stmt: *mut sqlite3_stmt, idx: i32) -> f64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0.0,
    };
    if stmt.is_null() {
        return 0.0;
    }
    let stmt = unsafe { &*stmt };
    unsafe { state.core.column_double(stmt.stmt, idx) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_blob(stmt: *mut sqlite3_stmt, idx: i32) -> *const c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    let raw = unsafe { state.core.column_blob(stmt.stmt, idx) };
    raw.ptr as *const c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_bytes(stmt: *mut sqlite3_stmt, idx: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    let ty = unsafe { state.core.column_type(stmt.stmt, idx) };
    let raw = match ty {
        ValueType::Blob => unsafe { state.core.column_blob(stmt.stmt, idx) },
        _ => unsafe { state.core.column_text(stmt.stmt, idx) },
    };
    clamp_len(raw.len)
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_column_text(stmt: *mut sqlite3_stmt, idx: i32) -> *const u8 {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    if stmt.is_null() {
        return null();
    }
    let stmt = unsafe { &*stmt };
    let raw = unsafe { state.core.column_text(stmt.stmt, idx) };
    raw.ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_value_type(value: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_NULL,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return SQLITE_NULL,
    };
    value_type_to_code(unsafe { state.core.value_type(value) })
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_value_int64(value: *mut c_void) -> i64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return 0,
    };
    unsafe { state.core.value_int64(value) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_value_double(value: *mut c_void) -> f64 {
    let state = match provider() {
        Some(state) => state,
        None => return 0.0,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return 0.0,
    };
    unsafe { state.core.value_double(value) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_value_text(value: *mut c_void) -> *const u8 {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return null(),
    };
    let raw = unsafe { state.core.value_text(value) };
    raw.ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_value_blob(value: *mut c_void) -> *const c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return null(),
    };
    let raw = unsafe { state.core.value_blob(value) };
    raw.ptr as *const c_void
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_value_bytes(value: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let value = match NonNull::new(value) {
        Some(value) => value,
        None => return 0,
    };
    let ty = unsafe { state.core.value_type(value) };
    let raw = match ty {
        ValueType::Blob => unsafe { state.core.value_blob(value) },
        _ => unsafe { state.core.value_text(value) },
    };
    clamp_len(raw.len)
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_get_table(
    db: *mut sqlite3,
    sql: *const c_char,
    paz_result: *mut *mut *mut c_char,
    pn_row: *mut i32,
    pn_column: *mut i32,
    pz_err_msg: *mut *mut c_char,
) -> i32 {
    if paz_result.is_null() || pn_row.is_null() || pn_column.is_null() {
        return SQLITE_MISUSE;
    }
    if !pz_err_msg.is_null() {
        unsafe { *pz_err_msg = null_mut() };
    }
    unsafe {
        *paz_result = null_mut();
        *pn_row = 0;
        *pn_column = 0;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db_ref = unsafe { &mut *db };
    db_ref.exec_callback_aborted = false;
    let sql = match unsafe { cstr_to_str(sql) } {
        Some(sql) => sql,
        None => return SQLITE_MISUSE,
    };
    let sql_bytes = sql.as_bytes();
    let mut offset = 0usize;
    let mut rc = SQLITE_OK;
    let mut rows = 0usize;
    let mut col_count = 0usize;
    let mut offsets: Vec<Option<usize>> = Vec::new();
    let mut data_buf: Vec<u8> = Vec::new();
    while offset <= sql_bytes.len() {
        let split = split_first_statement(sql_bytes, offset);
        if !split.complete {
            rc = SQLITE_ERROR;
            break;
        }
        let (stmt_start, stmt_end) = match split.range {
            Some(range) => range,
            None => {
                if split.tail <= offset || split.tail > sql_bytes.len() {
                    break;
                }
                offset = split.tail;
                continue;
            }
        };
        let stmt_sql = match std::str::from_utf8(&sql_bytes[stmt_start..stmt_end]) {
            Ok(s) => s,
            Err(_) => {
                rc = SQLITE_MISUSE;
                break;
            }
        };
        let stmt = match unsafe { state.core.prepare_v2(db_ref.db, stmt_sql) } {
            Ok(stmt) => stmt,
            Err(err) => {
                if let Some(msg) = err.message.as_deref() {
                    set_err_string(pz_err_msg, msg);
                }
                rc = err_code(&err);
                break;
            }
        };
        let stmt_cols_raw = unsafe { state.core.column_count(stmt) };
        let stmt_cols = if stmt_cols_raw > 0 {
            stmt_cols_raw as usize
        } else {
            0
        };
        let mut stmt_rc = SQLITE_OK;
        loop {
            match unsafe { state.core.step(stmt) } {
                Ok(StepResult::Row) => {
                    if col_count == 0 {
                        col_count = stmt_cols;
                        offsets.reserve(col_count);
                        for i in 0..col_count {
                            let mut name_offset = None;
                            let name = state.metadata.and_then(|metadata| unsafe {
                                metadata.column_name(stmt, i as i32)
                            });
                            push_column_name(&mut data_buf, &mut name_offset, name, i);
                            offsets.push(name_offset);
                        }
                    } else if stmt_cols != col_count {
                        set_err_string(pz_err_msg, GET_TABLE_INCOMPATIBLE_QUERY_MSG);
                        stmt_rc = SQLITE_ERROR;
                        break;
                    }
                    for i in 0..col_count {
                        let raw = unsafe { state.core.column_text(stmt, i as i32) };
                        if raw.ptr.is_null() {
                            offsets.push(None);
                        } else {
                            let start = data_buf.len();
                            data_buf.extend_from_slice(unsafe { raw.as_slice() });
                            data_buf.push(0);
                            offsets.push(Some(start));
                        }
                    }
                    rows += 1;
                }
                Ok(StepResult::Done) => break,
                Err(err) => {
                    if let Some(msg) = err.message.as_deref() {
                        set_err_string(pz_err_msg, msg);
                    }
                    stmt_rc = err_code(&err);
                    break;
                }
            }
        }
        let _ = unsafe { state.core.finalize(stmt) };
        if stmt_rc != SQLITE_OK {
            rc = stmt_rc;
            break;
        }
        if split.tail <= offset || split.tail > sql_bytes.len() {
            break;
        }
        offset = split.tail;
        if offset >= sql_bytes.len() {
            break;
        }
    }

    if rc != SQLITE_OK {
        return rc;
    }

    let total = (rows + 1) * col_count;
    if offsets.len() != total {
        return SQLITE_ERROR;
    }
    let (data_ptr, data_len, data_cap, data_owner) = if data_buf.is_empty() {
        (null_mut(), 0usize, 0usize, TABLE_DATA_OWNER_NONE)
    } else {
        data_buf.shrink_to_fit();
        let len = data_buf.len();
        let cap = data_buf.capacity();
        let ptr = data_buf.as_mut_ptr() as *mut c_void;
        std::mem::forget(data_buf);
        (ptr, len, cap, TABLE_DATA_OWNER_RUST_VEC)
    };
    let array_bytes = std::mem::size_of::<TableResultAllocHeader>()
        .checked_add(
            total
                .checked_mul(std::mem::size_of::<*mut c_char>())
                .unwrap_or(0),
        )
        .unwrap_or(0);
    if array_bytes > i32::MAX as usize {
        unsafe { free_table_data(data_owner, data_ptr, data_len, data_cap) };
        return SQLITE_NOMEM;
    }
    let base = sqlite3_malloc64(array_bytes as u64) as *mut TableResultAllocHeader;
    if base.is_null() {
        unsafe { free_table_data(data_owner, data_ptr, data_len, data_cap) };
        return SQLITE_NOMEM;
    }
    unsafe {
        (*base).count = total;
        (*base).data_ptr = data_ptr;
        (*base).data_len = data_len;
        (*base).data_cap = data_cap;
        (*base).data_owner = data_owner;
    }
    let array_ptr = unsafe { base.add(1) as *mut *mut c_char };
    unsafe { *pn_row = rows as i32 };
    unsafe { *pn_column = col_count as i32 };
    for (idx, offset) in offsets.into_iter().enumerate() {
        let slot = unsafe { array_ptr.add(idx) };
        match offset {
            Some(offset) => unsafe {
                *slot = (data_ptr as *mut u8).add(offset) as *mut c_char;
            },
            None => unsafe {
                *slot = null_mut();
            },
        }
    }
    unsafe { *paz_result = array_ptr };
    register_table_result_alloc(array_ptr);
    SQLITE_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_free_table(paz_result: *mut *mut c_char) {
    if paz_result.is_null() {
        return;
    }
    if !take_table_result_alloc(paz_result) {
        return;
    }
    unsafe {
        let header = (paz_result as *mut TableResultAllocHeader).offset(-1);
        free_table_data(
            (*header).data_owner,
            (*header).data_ptr,
            (*header).data_len,
            (*header).data_cap,
        );
        sqlite3_free(header as *mut c_void);
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite_get_table_cb(
    _context: *mut c_void,
    _n_column: i32,
    _argv: *mut *mut c_char,
    _colv: *mut *mut c_char,
) -> i32 {
    SQLITE_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_result_null(context: *mut c_void) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_null(ctx) };
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_result_int64(context: *mut c_void, val: i64) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_int64(ctx, val) };
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_result_double(context: *mut c_void, val: f64) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_double(ctx, val) };
}

#[unsafe(no_mangle)]
/// SQLite-compatible result wrapper.
///
/// The shim forwards bytes into the registered provider and then applies the
/// caller-provided destructor contract (`SQLITE_STATIC`/`SQLITE_TRANSIENT`/custom).
pub extern "C" fn sqlite3_result_text(
    context: *mut c_void,
    text: *const c_char,
    len: i32,
    destroy: *mut c_void,
) {
    let destructor = decode_destructor(destroy);
    let text_ptr = text as *mut c_void;
    let state = match provider() {
        Some(state) => state,
        None => {
            unsafe { call_destructor(destructor, text_ptr) };
            return;
        }
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => {
            unsafe { call_destructor(destructor, text_ptr) };
            return;
        }
    };
    if text.is_null() {
        unsafe { state.core.result_null(ctx) };
        return;
    }
    let text = unsafe { text_bytes_from_ptr(text, len) };
    match text {
        Some(text) => unsafe { state.core.result_text_bytes(ctx, text) },
        None => unsafe { state.core.result_error(ctx, "invalid text") },
    }
    unsafe { call_destructor(destructor, text_ptr) };
}

#[unsafe(no_mangle)]
/// SQLite-compatible result wrapper.
///
/// The shim forwards bytes into the registered provider and then applies the
/// caller-provided destructor contract (`SQLITE_STATIC`/`SQLITE_TRANSIENT`/custom).
pub extern "C" fn sqlite3_result_blob(
    context: *mut c_void,
    blob: *const c_void,
    len: i32,
    destroy: *mut c_void,
) {
    let destructor = decode_destructor(destroy);
    let blob_ptr = blob as *mut c_void;
    let state = match provider() {
        Some(state) => state,
        None => {
            unsafe { call_destructor(destructor, blob_ptr) };
            return;
        }
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => {
            unsafe { call_destructor(destructor, blob_ptr) };
            return;
        }
    };
    if blob.is_null() {
        unsafe { state.core.result_null(ctx) };
        return;
    }
    let blob = match unsafe { bytes_from_ptr(blob, len) } {
        Some(blob) => blob,
        None => {
            unsafe { state.core.result_error(ctx, "invalid blob") };
            unsafe { call_destructor(destructor, blob_ptr) };
            return;
        }
    };
    unsafe { state.core.result_blob(ctx, blob) };
    unsafe { call_destructor(destructor, blob_ptr) };
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_result_error_nomem(context: *mut c_void) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_error(ctx, "out of memory") };
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_result_error_toobig(context: *mut c_void) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    unsafe { state.core.result_error(ctx, "string or blob too big") };
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_result_error(context: *mut c_void, err: *const c_char, len: i32) {
    let state = match provider() {
        Some(state) => state,
        None => return,
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return,
    };
    let err = unsafe { text_from_ptr(err, len) };
    match err {
        Some(msg) => unsafe { state.core.result_error(ctx, msg) },
        None => unsafe { state.core.result_error(ctx, "error") },
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_aggregate_context(context: *mut c_void, n: i32) -> *mut c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return null_mut(),
    };
    let n = if n < 0 { 0 } else { n as usize };
    unsafe { state.core.aggregate_context(ctx, n) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_user_data(context: *mut c_void) -> *mut c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let ctx = match NonNull::new(context) {
        Some(ctx) => ctx,
        None => return null_mut(),
    };
    let user_data = unsafe { state.core.user_data(ctx) };
    lookup_udf_user_data(user_data).unwrap_or(user_data)
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_create_function_v2(
    db: *mut sqlite3,
    name: *const c_char,
    n_args: i32,
    enc: i32,
    context: *mut c_void,
    func: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
    step: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
    final_: Option<extern "C" fn(*mut c_void)>,
    destroy: Option<extern "C" fn(*mut c_void)>,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || name.is_null() {
        return SQLITE_MISUSE;
    }
    let name = unsafe { cstr_to_str(name) };
    let name = match name {
        Some(name) => name,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    let (user_data, drop_user_data): (*mut c_void, Option<extern "C" fn(*mut c_void)>) =
        match destroy {
            Some(destroy) => {
                let wrapped = wrap_udf_user_data(context, Some(destroy));
                (wrapped, Some(destroy_wrapped_udf_user_data))
            }
            None => (context, None),
        };
    match unsafe {
        state.core.create_function_v2(
            db.db,
            name,
            n_args,
            function_flags_from_sqlite(enc),
            func,
            step,
            final_,
            user_data,
            drop_user_data,
        )
    } {
        Ok(()) => SQLITE_OK,
        Err(err) => {
            if destroy.is_some() && lookup_udf_user_data(user_data).is_some() {
                destroy_wrapped_udf_user_data(user_data);
            }
            err_code(&err)
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_create_window_function(
    db: *mut sqlite3,
    name: *const c_char,
    n_args: i32,
    enc: i32,
    context: *mut c_void,
    x_step: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
    x_final: Option<extern "C" fn(*mut c_void)>,
    x_value: Option<extern "C" fn(*mut c_void)>,
    x_inverse: Option<extern "C" fn(*mut c_void, i32, *mut *mut c_void)>,
    destroy: Option<extern "C" fn(*mut c_void)>,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || name.is_null() {
        return SQLITE_MISUSE;
    }
    let name = unsafe { cstr_to_str(name) };
    let name = match name {
        Some(name) => name,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    let (user_data, drop_user_data): (*mut c_void, Option<extern "C" fn(*mut c_void)>) =
        match destroy {
            Some(destroy) => {
                let wrapped = wrap_udf_user_data(context, Some(destroy));
                (wrapped, Some(destroy_wrapped_udf_user_data))
            }
            None => (context, None),
        };
    match unsafe {
        state.core.create_window_function(
            db.db,
            name,
            n_args,
            function_flags_from_sqlite(enc),
            x_step,
            x_final,
            x_value,
            x_inverse,
            user_data,
            drop_user_data,
        )
    } {
        Ok(()) => SQLITE_OK,
        Err(err) => {
            if destroy.is_some() && lookup_udf_user_data(user_data).is_some() {
                destroy_wrapped_udf_user_data(user_data);
            }
            err_code(&err)
        }
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_create_collation_v2(
    db: *mut sqlite3,
    name: *const c_char,
    enc: i32,
    context: *mut c_void,
    cmp: Option<extern "C" fn(*mut c_void, i32, *const c_void, i32, *const c_void) -> i32>,
    destroy: Option<extern "C" fn(*mut c_void)>,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || name.is_null() {
        return SQLITE_MISUSE;
    }
    let name = match unsafe { cstr_to_str(name) } {
        Some(name) => name,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    match unsafe {
        state
            .core
            .create_collation_v2(db.db, name, enc, context, cmp, destroy)
    } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_backup_init(
    dest_db: *mut sqlite3,
    dest_name: *const c_char,
    source_db: *mut sqlite3,
    source_name: *const c_char,
) -> *mut c_void {
    let state = match provider() {
        Some(state) => state,
        None => return null_mut(),
    };
    let backup = match state.backup {
        Some(backup) => backup,
        None => return null_mut(),
    };
    if dest_db.is_null() || source_db.is_null() {
        return null_mut();
    }
    let dest_name = unsafe { cstr_to_str(dest_name) }.unwrap_or("main");
    let source_name = unsafe { cstr_to_str(source_name) }.unwrap_or("main");
    let dest_db = unsafe { &*dest_db };
    let source_db = unsafe { &*source_db };
    match unsafe { backup.backup_init(dest_db.db, dest_name, source_db.db, source_name) } {
        Ok(handle) => handle.as_ptr(),
        Err(_) => null_mut(),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_backup_step(backup: *mut c_void, pages: i32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    match unsafe { backup_api.backup_step(backup, pages) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_backup_remaining(backup: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return 0,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return 0,
    };
    unsafe { backup_api.backup_remaining(backup) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_backup_pagecount(backup: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return 0,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return 0,
    };
    unsafe { backup_api.backup_pagecount(backup) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_backup_finish(backup: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let backup_api = match state.backup {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    let backup = match NonNull::new(backup) {
        Some(backup) => backup,
        None => return SQLITE_MISUSE,
    };
    match unsafe { backup_api.backup_finish(backup) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_serialize(
    db: *mut sqlite3,
    schema: *const c_char,
    out: *mut *mut c_void,
    out_bytes: *mut i32,
    flags: u32,
) -> i32 {
    if out.is_null() || out_bytes.is_null() {
        return SQLITE_MISUSE;
    }
    unsafe {
        *out = null_mut();
        *out_bytes = 0;
    }
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let serialize = match state.serialize {
        Some(serialize) => serialize,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    let schema = unsafe { cstr_to_str(schema) };
    let bytes = match unsafe { serialize.serialize(db.db, schema, flags) } {
        Ok(bytes) => bytes,
        Err(err) => return err_code(&err),
    };
    unsafe {
        *out = bytes.ptr.as_ptr() as *mut c_void;
        *out_bytes = clamp_len(bytes.len);
    }
    let key = bytes.ptr.as_ptr() as usize;
    let entry = SerializedEntry {
        provider: serialize,
        len: bytes.len,
    };
    if let Ok(mut map) = serialized_map().lock() {
        map.insert(key, entry);
    } else {
        unsafe { serialize.free(bytes) };
        unsafe {
            *out = null_mut();
            *out_bytes = 0;
        }
        return SQLITE_MISUSE;
    }
    SQLITE_OK
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_deserialize(
    db: *mut sqlite3,
    schema: *const c_char,
    input: *const c_void,
    input_bytes: i32,
    flags: u32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let serialize = match state.serialize {
        Some(serialize) => serialize,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let input = match unsafe { bytes_from_ptr(input, input_bytes) } {
        Some(bytes) => bytes,
        None => return SQLITE_MISUSE,
    };
    let db = unsafe { &*db };
    let schema = unsafe { cstr_to_str(schema) };
    match unsafe { serialize.deserialize(db.db, schema, input, flags) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_blob_open(
    db: *mut sqlite3,
    db_name: *const c_char,
    table_name: *const c_char,
    column_name: *const c_char,
    rowid: i64,
    flags: i32,
    blob_out: *mut *mut c_void,
) -> i32 {
    if blob_out.is_null() {
        return SQLITE_MISUSE;
    }
    unsafe { *blob_out = null_mut() };
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db_name = unsafe { cstr_to_str(db_name) }.unwrap_or("main");
    let table_name = unsafe { cstr_to_str(table_name) }.unwrap_or("");
    let column_name = unsafe { cstr_to_str(column_name) }.unwrap_or("");
    let db = unsafe { &*db };
    match unsafe {
        blob_api.blob_open(db.db, db_name, table_name, column_name, rowid, flags as u32)
    } {
        Ok(handle) => {
            unsafe { *blob_out = handle.as_ptr() };
            SQLITE_OK
        }
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_blob_read(
    blob: *mut c_void,
    data: *mut c_void,
    n: i32,
    offset: i32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let data = match unsafe { bytes_from_ptr_mut(data, n) } {
        Some(data) => data,
        None => return SQLITE_MISUSE,
    };
    match unsafe { blob_api.blob_read(blob, data, offset) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_blob_write(
    blob: *mut c_void,
    data: *const c_void,
    n: i32,
    offset: i32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let data = match unsafe { bytes_from_ptr(data, n) } {
        Some(data) => data,
        None => return SQLITE_MISUSE,
    };
    match unsafe { blob_api.blob_write(blob, data, offset) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_blob_bytes(blob: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return 0,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return 0,
    };
    unsafe { blob_api.blob_bytes(blob) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_blob_close(blob: *mut c_void) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let blob_api = match state.blob {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    let blob = match NonNull::new(blob) {
        Some(blob) => blob,
        None => return SQLITE_MISUSE,
    };
    match unsafe { blob_api.blob_close(blob) } {
        Ok(()) => SQLITE_OK,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_stricmp(a: *const c_char, b: *const c_char) -> i32 {
    if a.is_null() || b.is_null() {
        return if a.is_null() && b.is_null() {
            0
        } else if a.is_null() {
            -1
        } else {
            1
        };
    }
    let a = unsafe { CStr::from_ptr(a).to_bytes() };
    let b = unsafe { CStr::from_ptr(b).to_bytes() };
    let mut i = 0;
    while i < a.len() && i < b.len() {
        let ca = a[i].to_ascii_lowercase();
        let cb = b[i].to_ascii_lowercase();
        if ca != cb {
            return ca as i32 - cb as i32;
        }
        i += 1;
    }
    a.len() as i32 - b.len() as i32
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_expanded_sql(stmt: *mut sqlite3_stmt) -> *mut c_char {
    if stmt.is_null() {
        return null_mut();
    }
    let stmt = unsafe { &*stmt };
    let sql = match stmt.sql.as_ref() {
        Some(sql) => sql,
        None => return null_mut(),
    };
    let bytes = sql.as_bytes_with_nul();
    let out = sqlite3_malloc64(bytes.len() as u64) as *mut c_char;
    if out.is_null() {
        return null_mut();
    }
    unsafe { std::ptr::copy_nonoverlapping(bytes.as_ptr(), out.cast(), bytes.len()) };
    out
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_data_count(stmt: *mut sqlite3_stmt) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    if stmt.is_null() {
        return 0;
    }
    let stmt = unsafe { &*stmt };
    if stmt.last_step != StepResult::Row {
        return 0;
    }
    unsafe { state.core.column_count(stmt.stmt) }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_malloc64(n: u64) -> *mut c_void {
    if n == 0 {
        return null_mut();
    }
    // SQLite's allocation-bearing APIs are bounded by 32-bit lengths in many
    // core paths. Reject oversized requests to avoid truncation/parity bugs.
    if n > i32::MAX as u64 || n > usize::MAX as u64 {
        return null_mut();
    }
    let _ = ensure_default_provider();
    let size = n as usize;
    let (ptr, owner) = match provider() {
        Some(state) => {
            let ptr = unsafe { state.core.malloc(size) };
            if ptr.is_null() {
                return null_mut();
            }
            (ptr, MallocAllocOwner::Provider(state.core))
        }
        None => {
            let ptr = unsafe { libc::malloc(size) };
            if ptr.is_null() {
                return null_mut();
            }
            (ptr, MallocAllocOwner::Libc)
        }
    };
    if register_malloc_alloc(ptr, owner).is_err() {
        unsafe { free_with_owner(owner, ptr) };
        return null_mut();
    }
    ptr
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_free(ptr: *mut c_void) {
    if ptr.is_null() {
        return;
    }
    if let Ok(mut map) = serialized_map().lock()
        && let Some(entry) = map.remove(&(ptr as usize))
    {
        let bytes = sqlite_provider::OwnedBytes {
            ptr: unsafe { NonNull::new_unchecked(ptr as *mut u8) },
            len: entry.len,
        };
        unsafe { entry.provider.free(bytes) };
        return;
    }
    if let Some(owner) = take_malloc_alloc(ptr) {
        unsafe { free_with_owner(owner, ptr) };
        return;
    }
    // Fallback for pointers not tracked by shim-side ownership maps.
    unsafe { libc::free(ptr) };
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_complete(sql: *const c_char) -> i32 {
    let sql = match unsafe { cstr_to_str(sql) } {
        Some(sql) => sql,
        None => return 0,
    };
    let bytes = sql.as_bytes();
    let mut offset = 0usize;
    let mut saw_stmt = false;
    let mut last_terminated = false;
    loop {
        let split = split_first_statement(bytes, offset);
        if !split.complete {
            return 0;
        }
        match split.range {
            Some(_) => {
                saw_stmt = true;
                last_terminated = split.terminated;
                if split.tail <= offset || split.tail > bytes.len() {
                    break;
                }
                offset = split.tail;
            }
            None => {
                if split.terminated {
                    saw_stmt = true;
                    last_terminated = true;
                    if split.tail <= offset || split.tail > bytes.len() {
                        break;
                    }
                    offset = split.tail;
                } else {
                    break;
                }
            }
        }
        if offset >= bytes.len() {
            break;
        }
    }
    if !saw_stmt {
        return 0;
    }
    if last_terminated { 1 } else { 0 }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_threadsafe() -> i32 {
    let _ = ensure_default_provider();
    match provider() {
        Some(state) => state.core.threadsafe(),
        None => 0,
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_libversion() -> *const c_char {
    let _ = ensure_default_provider();
    let state = match provider() {
        Some(state) => state,
        None => return null(),
    };
    state.libversion.as_ptr()
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_libversion_number() -> i32 {
    let _ = ensure_default_provider();
    let state = match provider() {
        Some(state) => state,
        None => return 0,
    };
    state.libversion_number
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_wal_checkpoint(db: *mut sqlite3, db_name: *const c_char) -> i32 {
    sqlite3_wal_checkpoint_v2(
        db,
        db_name,
        SQLITE_CHECKPOINT_PASSIVE,
        null_mut(),
        null_mut(),
    )
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_wal_checkpoint_v2(
    db: *mut sqlite3,
    db_name: *const c_char,
    mode: i32,
    log_size: *mut i32,
    checkpointed: *mut i32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let wal = match state.wal {
        Some(wal) => wal,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    let db_name = unsafe { cstr_to_str(db_name) };
    match unsafe { wal.wal_checkpoint_v2(db.db, db_name, mode) } {
        Ok((log, checkpoint)) => {
            if !log_size.is_null() {
                unsafe { *log_size = log };
            }
            if !checkpointed.is_null() {
                unsafe { *checkpointed = checkpoint };
            }
            SQLITE_OK
        }
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn libsql_wal_frame_count(db: *mut sqlite3, frame_count: *mut u32) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let wal = match state.wal {
        Some(wal) => wal,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || frame_count.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    match unsafe { wal.wal_frame_count(db.db) } {
        Ok(Some(count)) => {
            unsafe { *frame_count = count };
            SQLITE_OK
        }
        Ok(None) => SQLITE_ERROR,
        Err(err) => err_code(&err),
    }
}

#[unsafe(no_mangle)]
pub extern "C" fn sqlite3_table_column_metadata(
    db: *mut sqlite3,
    db_name: *const c_char,
    table: *const c_char,
    column: *const c_char,
    data_type: *mut *const c_char,
    coll_seq: *mut *const c_char,
    not_null: *mut i32,
    primary_key: *mut i32,
    autoinc: *mut i32,
) -> i32 {
    let state = match provider() {
        Some(state) => state,
        None => return SQLITE_MISUSE,
    };
    let metadata = match state.metadata {
        Some(metadata) => metadata,
        None => return SQLITE_MISUSE,
    };
    if db.is_null() || table.is_null() || column.is_null() {
        return SQLITE_MISUSE;
    }
    let db = unsafe { &*db };
    let db_name = unsafe { cstr_to_str(db_name) };
    let table = unsafe { cstr_to_str(table) }.unwrap_or("");
    let column = unsafe { cstr_to_str(column) }.unwrap_or("");
    match unsafe { metadata.table_column_metadata(db.db, db_name, table, column) } {
        Ok(info) => {
            unsafe {
                if !data_type.is_null() {
                    *data_type = info
                        .data_type
                        .map(|raw| raw.ptr as *const c_char)
                        .unwrap_or(null());
                }
                if !coll_seq.is_null() {
                    *coll_seq = info
                        .coll_seq
                        .map(|raw| raw.ptr as *const c_char)
                        .unwrap_or(null());
                }
                if !not_null.is_null() {
                    *not_null = if info.not_null { 1 } else { 0 };
                }
                if !primary_key.is_null() {
                    *primary_key = if info.primary_key { 1 } else { 0 };
                }
                if !autoinc.is_null() {
                    *autoinc = if info.autoinc { 1 } else { 0 };
                }
            }
            SQLITE_OK
        }
        Err(err) => err_code(&err),
    }
}
