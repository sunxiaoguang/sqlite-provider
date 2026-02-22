use super::*;

fn drop_registration_user_data(
    user_data: *mut c_void,
    drop_user_data: Option<extern "C" fn(*mut c_void)>,
) {
    if let Some(drop_user_data) = drop_user_data {
        drop_user_data(user_data);
    }
}

fn registration_name_or_drop(
    name: &str,
    user_data: *mut c_void,
    drop_user_data: Option<extern "C" fn(*mut c_void)>,
    error_message: &'static str,
) -> Result<CString> {
    match CString::new(name) {
        Ok(name) => Ok(name),
        Err(_) => {
            drop_registration_user_data(user_data, drop_user_data);
            Err(Error::with_message(ErrorCode::Misuse, error_message))
        }
    }
}

#[allow(unsafe_op_in_unsafe_fn)]
unsafe impl Sqlite3Api for LibSqlite3 {
    type Db = sqlite3;
    type Stmt = sqlite3_stmt;
    type Value = sqlite3_value;
    type Context = sqlite3_context;
    type VTab = c_void;
    type VTabCursor = c_void;

    fn api_version(&self) -> ApiVersion {
        self.api_version
    }

    fn feature_set(&self) -> FeatureSet {
        self.features
    }

    fn backend_name(&self) -> &'static str {
        "libsqlite3"
    }

    fn backend_version(&self) -> Option<ApiVersion> {
        Some(self.api_version)
    }

    unsafe fn malloc(&self, size: usize) -> *mut c_void {
        if size > i32::MAX as usize {
            return null_mut();
        }
        (self.fns.malloc)(size as c_int)
    }

    unsafe fn free(&self, ptr: *mut c_void) {
        (self.fns.free)(ptr);
    }

    fn threadsafe(&self) -> i32 {
        self.fns.threadsafe.map(|f| unsafe { f() }).unwrap_or(0)
    }

    unsafe fn open(&self, filename: &str, options: OpenOptions<'_>) -> Result<NonNull<Self::Db>> {
        let filename = CString::new(filename)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "filename contains NUL"))?;
        let vfs = match options.vfs {
            Some(vfs) => Some(
                CString::new(vfs)
                    .map_err(|_| Error::with_message(ErrorCode::Misuse, "vfs contains NUL"))?,
            ),
            None => None,
        };
        let mut db = null_mut();
        let flags = map_open_flags(options.flags);
        let vfs_ptr = vfs.as_ref().map(|s| s.as_ptr()).unwrap_or(null());
        let rc = (self.fns.open_v2)(filename.as_ptr(), &mut db, flags, vfs_ptr);
        if rc != SQLITE_OK {
            let err = self.error_from_rc(rc, NonNull::new(db));
            if !db.is_null() {
                let _ = (self.fns.close)(db);
            }
            return Err(err);
        }
        Ok(NonNull::new_unchecked(db))
    }

    unsafe fn close(&self, db: NonNull<Self::Db>) -> Result<()> {
        let rc = (self.fns.close)(db.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn prepare_v2(&self, db: NonNull<Self::Db>, sql: &str) -> Result<NonNull<Self::Stmt>> {
        let mut stmt = null_mut();
        let sql_ptr = sql.as_ptr() as *const c_char;
        let sql_len = clamp_len(sql.len());
        let rc = (self.fns.prepare_v2)(db.as_ptr(), sql_ptr, sql_len, &mut stmt, null_mut());
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, Some(db)));
        }
        Ok(NonNull::new_unchecked(stmt))
    }

    unsafe fn prepare_v3(
        &self,
        db: NonNull<Self::Db>,
        sql: &str,
        flags: u32,
    ) -> Result<NonNull<Self::Stmt>> {
        let prepare = match self.fns.prepare_v3 {
            Some(prepare) => prepare,
            None => return Err(Error::feature_unavailable("prepare_v3 not available")),
        };
        let mut stmt = null_mut();
        let sql_ptr = sql.as_ptr() as *const c_char;
        let sql_len = clamp_len(sql.len());
        let rc = prepare(db.as_ptr(), sql_ptr, sql_len, flags, &mut stmt, null_mut());
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, Some(db)));
        }
        Ok(NonNull::new_unchecked(stmt))
    }

    unsafe fn step(&self, stmt: NonNull<Self::Stmt>) -> Result<StepResult> {
        match (self.fns.step)(stmt.as_ptr()) {
            SQLITE_ROW => Ok(StepResult::Row),
            SQLITE_DONE => Ok(StepResult::Done),
            rc => Err(self.error_from_rc(rc, None)),
        }
    }

    unsafe fn reset(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        let rc = (self.fns.reset)(stmt.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn finalize(&self, stmt: NonNull<Self::Stmt>) -> Result<()> {
        let rc = (self.fns.finalize)(stmt.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_null(&self, stmt: NonNull<Self::Stmt>, idx: i32) -> Result<()> {
        let rc = (self.fns.bind_null)(stmt.as_ptr(), idx);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_int64(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: i64) -> Result<()> {
        let rc = (self.fns.bind_int64)(stmt.as_ptr(), idx, v);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_double(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: f64) -> Result<()> {
        let rc = (self.fns.bind_double)(stmt.as_ptr(), idx, v);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, None))
        }
    }

    unsafe fn bind_text(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &str) -> Result<()> {
        unsafe { self.bind_text_bytes(stmt, idx, v.as_bytes()) }
    }

    unsafe fn bind_text_bytes(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &[u8]) -> Result<()> {
        let (ptr, dtor) = self.alloc_copy(v)?;
        let rc = (self.fns.bind_text)(
            stmt.as_ptr(),
            idx,
            ptr as *const c_char,
            clamp_len(v.len()),
            dtor,
        );
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, None));
        }
        Ok(())
    }

    unsafe fn bind_blob(&self, stmt: NonNull<Self::Stmt>, idx: i32, v: &[u8]) -> Result<()> {
        let (ptr, dtor) = self.alloc_copy(v)?;
        let rc = (self.fns.bind_blob)(stmt.as_ptr(), idx, ptr, clamp_len(v.len()), dtor);
        if rc != SQLITE_OK {
            return Err(self.error_from_rc(rc, None));
        }
        Ok(())
    }

    unsafe fn column_count(&self, stmt: NonNull<Self::Stmt>) -> i32 {
        (self.fns.column_count)(stmt.as_ptr())
    }

    unsafe fn column_type(&self, stmt: NonNull<Self::Stmt>, col: i32) -> ValueType {
        ValueType::from_code((self.fns.column_type)(stmt.as_ptr(), col))
    }

    unsafe fn column_int64(&self, stmt: NonNull<Self::Stmt>, col: i32) -> i64 {
        (self.fns.column_int64)(stmt.as_ptr(), col)
    }

    unsafe fn column_double(&self, stmt: NonNull<Self::Stmt>, col: i32) -> f64 {
        (self.fns.column_double)(stmt.as_ptr(), col)
    }

    unsafe fn column_text(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let ptr = (self.fns.column_text)(stmt.as_ptr(), col);
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.column_bytes)(stmt.as_ptr(), col);
        RawBytes {
            ptr,
            len: len as usize,
        }
    }

    unsafe fn column_blob(&self, stmt: NonNull<Self::Stmt>, col: i32) -> RawBytes {
        let ptr = (self.fns.column_blob)(stmt.as_ptr(), col) as *const u8;
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.column_bytes)(stmt.as_ptr(), col);
        RawBytes {
            ptr,
            len: len as usize,
        }
    }

    unsafe fn errcode(&self, db: NonNull<Self::Db>) -> i32 {
        (self.fns.errcode)(db.as_ptr())
    }

    unsafe fn errmsg(&self, db: NonNull<Self::Db>) -> *const c_char {
        (self.fns.errmsg)(db.as_ptr())
    }

    unsafe fn extended_errcode(&self, db: NonNull<Self::Db>) -> Option<i32> {
        self.fns.extended_errcode.map(|f| f(db.as_ptr()))
    }

    unsafe fn create_function_v2(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_func: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_final: Option<extern "C" fn(*mut Self::Context)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let name = registration_name_or_drop(
            name,
            user_data,
            drop_user_data,
            "function name contains NUL",
        )?;
        let flags = map_function_flags(flags);
        let rc = (self.fns.create_function_v2)(
            db.as_ptr(),
            name.as_ptr(),
            n_args,
            flags,
            user_data,
            x_func,
            x_step,
            x_final,
            drop_user_data,
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn create_window_function(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        n_args: i32,
        flags: FunctionFlags,
        x_step: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        x_final: Option<extern "C" fn(*mut Self::Context)>,
        x_value: Option<extern "C" fn(*mut Self::Context)>,
        x_inverse: Option<extern "C" fn(*mut Self::Context, i32, *mut *mut Self::Value)>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let create = match self.fns.create_window_function {
            Some(create) => create,
            None => {
                drop_registration_user_data(user_data, drop_user_data);
                return Err(Error::feature_unavailable(
                    "create_window_function not available",
                ));
            }
        };
        let name = registration_name_or_drop(
            name,
            user_data,
            drop_user_data,
            "function name contains NUL",
        )?;
        let flags = map_function_flags(flags);
        let rc = create(
            db.as_ptr(),
            name.as_ptr(),
            n_args,
            flags,
            user_data,
            x_step,
            x_final,
            x_value,
            x_inverse,
            drop_user_data,
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn create_collation_v2(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        enc: i32,
        context: *mut c_void,
        cmp: Option<extern "C" fn(*mut c_void, i32, *const c_void, i32, *const c_void) -> i32>,
        destroy: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let create = match self.fns.create_collation_v2 {
            Some(create) => create,
            None => {
                return Err(Error::feature_unavailable(
                    "create_collation_v2 not available",
                ));
            }
        };
        let name = CString::new(name)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "collation name contains NUL"))?;
        let rc = create(db.as_ptr(), name.as_ptr(), enc, context, cmp, destroy);
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn aggregate_context(&self, ctx: NonNull<Self::Context>, bytes: usize) -> *mut c_void {
        (self.fns.aggregate_context)(ctx.as_ptr(), clamp_len(bytes) as c_int)
    }

    unsafe fn result_null(&self, ctx: NonNull<Self::Context>) {
        (self.fns.result_null)(ctx.as_ptr());
    }

    unsafe fn result_int64(&self, ctx: NonNull<Self::Context>, v: i64) {
        (self.fns.result_int64)(ctx.as_ptr(), v);
    }

    unsafe fn result_double(&self, ctx: NonNull<Self::Context>, v: f64) {
        (self.fns.result_double)(ctx.as_ptr(), v);
    }

    unsafe fn result_text(&self, ctx: NonNull<Self::Context>, v: &str) {
        unsafe { self.result_text_bytes(ctx, v.as_bytes()) }
    }

    unsafe fn result_text_bytes(&self, ctx: NonNull<Self::Context>, v: &[u8]) {
        match self.alloc_copy(v) {
            Ok((ptr, dtor)) => {
                (self.fns.result_text)(
                    ctx.as_ptr(),
                    ptr as *const c_char,
                    clamp_len(v.len()),
                    dtor,
                );
            }
            Err(_) => {
                const OOM: &str = "out of memory";
                (self.fns.result_error)(
                    ctx.as_ptr(),
                    OOM.as_ptr() as *const c_char,
                    clamp_len(OOM.len()),
                );
            }
        }
    }

    unsafe fn result_blob(&self, ctx: NonNull<Self::Context>, v: &[u8]) {
        match self.alloc_copy(v) {
            Ok((ptr, dtor)) => {
                (self.fns.result_blob)(ctx.as_ptr(), ptr, clamp_len(v.len()), dtor);
            }
            Err(_) => {
                const OOM: &str = "out of memory";
                (self.fns.result_error)(
                    ctx.as_ptr(),
                    OOM.as_ptr() as *const c_char,
                    clamp_len(OOM.len()),
                );
            }
        }
    }

    unsafe fn result_error(&self, ctx: NonNull<Self::Context>, msg: &str) {
        (self.fns.result_error)(
            ctx.as_ptr(),
            msg.as_ptr() as *const c_char,
            clamp_len(msg.len()),
        );
    }

    unsafe fn user_data(ctx: NonNull<Self::Context>) -> *mut c_void {
        match USER_DATA_FN.get() {
            Some(f) => f(ctx.as_ptr()),
            None => null_mut(),
        }
    }

    unsafe fn value_type(&self, v: NonNull<Self::Value>) -> ValueType {
        ValueType::from_code((self.fns.value_type)(v.as_ptr()))
    }

    unsafe fn value_int64(&self, v: NonNull<Self::Value>) -> i64 {
        (self.fns.value_int64)(v.as_ptr())
    }

    unsafe fn value_double(&self, v: NonNull<Self::Value>) -> f64 {
        (self.fns.value_double)(v.as_ptr())
    }

    unsafe fn value_text(&self, v: NonNull<Self::Value>) -> RawBytes {
        let ptr = (self.fns.value_text)(v.as_ptr());
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.value_bytes)(v.as_ptr());
        RawBytes {
            ptr,
            len: len as usize,
        }
    }

    unsafe fn value_blob(&self, v: NonNull<Self::Value>) -> RawBytes {
        let ptr = (self.fns.value_blob)(v.as_ptr()) as *const u8;
        if ptr.is_null() {
            return RawBytes::empty();
        }
        let len = (self.fns.value_bytes)(v.as_ptr());
        RawBytes {
            ptr,
            len: len as usize,
        }
    }

    unsafe fn declare_vtab(&self, db: NonNull<Self::Db>, schema: &str) -> Result<()> {
        let schema = CString::new(schema)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "schema contains NUL"))?;
        let rc = (self.fns.declare_vtab)(db.as_ptr(), schema.as_ptr());
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }

    unsafe fn create_module_v2(
        &self,
        db: NonNull<Self::Db>,
        name: &str,
        module: &'static sqlite_provider::sqlite3_module<Self>,
        user_data: *mut c_void,
        drop_user_data: Option<extern "C" fn(*mut c_void)>,
    ) -> Result<()> {
        let create = match self.fns.create_module_v2 {
            Some(create) => create,
            None => return Err(Error::feature_unavailable("create_module_v2 not available")),
        };
        let name = CString::new(name)
            .map_err(|_| Error::with_message(ErrorCode::Misuse, "module name contains NUL"))?;
        let rc = create(
            db.as_ptr(),
            name.as_ptr(),
            module as *const _ as *const c_void,
            user_data,
            drop_user_data,
        );
        if rc == SQLITE_OK {
            Ok(())
        } else {
            Err(self.error_from_rc(rc, Some(db)))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{drop_registration_user_data, registration_name_or_drop};
    use sqlite_provider::ErrorCode;
    use std::ffi::c_void;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    static DROP_CALLS: AtomicUsize = AtomicUsize::new(0);
    static DROP_TEST_LOCK: Mutex<()> = Mutex::new(());

    extern "C" fn drop_counting_box(ptr: *mut c_void) {
        DROP_CALLS.fetch_add(1, Ordering::SeqCst);
        if !ptr.is_null() {
            unsafe { drop(Box::from_raw(ptr as *mut usize)) };
        }
    }

    #[test]
    fn registration_name_or_drop_invokes_drop_on_interior_nul() {
        let _guard = DROP_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        DROP_CALLS.store(0, Ordering::SeqCst);
        let user_data = Box::into_raw(Box::new(7usize)) as *mut c_void;
        let result = registration_name_or_drop(
            "bad\0name",
            user_data,
            Some(drop_counting_box),
            "function name contains NUL",
        );
        let err = result.expect_err("interior NUL should fail");
        assert_eq!(err.code, ErrorCode::Misuse);
        assert_eq!(DROP_CALLS.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn registration_name_or_drop_keeps_user_data_on_success() {
        let _guard = DROP_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        DROP_CALLS.store(0, Ordering::SeqCst);
        let user_data = Box::into_raw(Box::new(9usize)) as *mut c_void;
        let name = registration_name_or_drop(
            "ok_name",
            user_data,
            Some(drop_counting_box),
            "function name contains NUL",
        )
        .expect("valid name should pass");
        assert_eq!(name.to_str().unwrap(), "ok_name");
        assert_eq!(DROP_CALLS.load(Ordering::SeqCst), 0);
        drop_registration_user_data(user_data, Some(drop_counting_box));
        assert_eq!(DROP_CALLS.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn drop_registration_user_data_is_noop_without_callback() {
        let _guard = DROP_TEST_LOCK
            .lock()
            .unwrap_or_else(|poison| poison.into_inner());
        drop_registration_user_data(std::ptr::null_mut(), None);
    }
}
