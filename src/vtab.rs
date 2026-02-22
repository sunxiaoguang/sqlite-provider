use core::ffi::{c_char, c_void};
use core::mem::MaybeUninit;
use core::ptr::NonNull;
use std::collections::HashMap;
use std::sync::{Mutex, OnceLock};

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::function::Context;
use crate::provider::{FeatureSet, Sqlite3Api, ValueType, sqlite3_module};
use crate::value::ValueRef;

/// Wrapper for a backend-specific best-index info pointer.
pub struct BestIndexInfo {
    /// Raw backend pointer to `sqlite3_index_info`.
    pub raw: *mut c_void,
}

/// Virtual table implementation.
pub trait VirtualTable<P: Sqlite3Api>: Sized + Send {
    /// Cursor type opened by this table.
    type Cursor: VTabCursor<P>;
    /// Error type mapped into crate `Error`.
    type Error: Into<Error>;

    /// Create/connect a table instance from SQLite module arguments.
    fn connect(args: &[&str]) -> core::result::Result<(Self, String), Self::Error>;
    /// Disconnect and release table resources.
    fn disconnect(self) -> core::result::Result<(), Self::Error>;

    /// Populate best-index constraints/order information.
    fn best_index(&self, _info: &mut BestIndexInfo) -> core::result::Result<(), Self::Error> {
        Ok(())
    }

    /// Open a new cursor over this table.
    fn open(&self) -> core::result::Result<Self::Cursor, Self::Error>;
}

/// Virtual table cursor implementation.
pub trait VTabCursor<P: Sqlite3Api>: Sized + Send {
    /// Error type mapped into crate `Error`.
    type Error: Into<Error>;

    /// Apply constraints and initialize iteration.
    fn filter(
        &mut self,
        idx_num: i32,
        idx_str: Option<&str>,
        args: &[ValueRef<'_>],
    ) -> core::result::Result<(), Self::Error>;
    /// Advance to the next row.
    fn next(&mut self) -> core::result::Result<(), Self::Error>;
    /// Whether iteration reached end-of-input.
    fn eof(&self) -> bool;
    /// Emit one output column into SQLite context.
    fn column(&self, ctx: &Context<'_, P>, col: i32) -> core::result::Result<(), Self::Error>;
    /// Return current rowid.
    fn rowid(&self) -> core::result::Result<i64, Self::Error>;
}

/// Glue wrapper stored in the backend's `sqlite3_vtab` pointer.
#[repr(C)]
pub struct VTab<P: Sqlite3Api, T: VirtualTable<P>> {
    api: *const P,
    table: T,
}

/// Glue wrapper stored in the backend's `sqlite3_vtab_cursor` pointer.
#[repr(C)]
pub struct Cursor<P: Sqlite3Api, C: VTabCursor<P>> {
    api: *const P,
    cursor: C,
}

const INLINE_ARGS: usize = 8;
type ModuleCache = HashMap<usize, usize>;

static MODULE_CACHE: OnceLock<Mutex<ModuleCache>> = OnceLock::new();

struct ArgBuffer<'a> {
    inline: [MaybeUninit<ValueRef<'a>>; INLINE_ARGS],
    len: usize,
    heap: Option<Vec<ValueRef<'a>>>,
}

impl<'a> ArgBuffer<'a> {
    fn new(argc: usize) -> Self {
        // SAFETY: MaybeUninit allows an uninitialized array.
        let inline = unsafe {
            MaybeUninit::<[MaybeUninit<ValueRef<'a>>; INLINE_ARGS]>::uninit().assume_init()
        };
        let heap = if argc > INLINE_ARGS {
            Some(Vec::with_capacity(argc))
        } else {
            None
        };
        Self {
            inline,
            len: 0,
            heap,
        }
    }

    fn push(&mut self, value: ValueRef<'a>) {
        if let Some(heap) = &mut self.heap {
            heap.push(value);
            return;
        }
        let slot = &mut self.inline[self.len];
        slot.write(value);
        self.len += 1;
    }

    fn as_slice(&self) -> &[ValueRef<'a>] {
        if let Some(heap) = &self.heap {
            return heap.as_slice();
        }
        // SAFETY: We only read the initialized prefix `len`.
        unsafe {
            core::slice::from_raw_parts(self.inline.as_ptr() as *const ValueRef<'a>, self.len)
        }
    }
}

fn module_cache() -> &'static Mutex<ModuleCache> {
    MODULE_CACHE.get_or_init(|| Mutex::new(HashMap::new()))
}

fn module_key<P, T>() -> usize
where
    P: Sqlite3Api,
    T: VirtualTable<P>,
{
    x_create::<P, T> as *const () as usize
}

fn cached_module<P, T>() -> &'static sqlite3_module<P>
where
    P: Sqlite3Api<VTab = VTab<P, T>, VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    let key = module_key::<P, T>();
    let mut cache = module_cache()
        .lock()
        .expect("sqlite-provider vtab module cache poisoned");
    if let Some(raw) = cache.get(&key) {
        // SAFETY: `raw` was inserted from a leaked `sqlite3_module<P>` built
        // for this exact monomorphized callback key.
        return unsafe { &*(*raw as *const sqlite3_module<P>) };
    }
    let module = Box::leak(Box::new(module::<P, T>()));
    cache.insert(key, module as *const sqlite3_module<P> as usize);
    module
}

fn err_code(err: &Error) -> i32 {
    err.code.code().unwrap_or(1)
}

fn set_out_err_message<P: Sqlite3Api>(api: &P, out_err: *mut *mut u8, message: &str) {
    if out_err.is_null() {
        return;
    }
    let bytes = message.as_bytes();
    let payload_len = bytes
        .iter()
        .position(|byte| *byte == 0)
        .unwrap_or(bytes.len());
    let alloc_len = payload_len.saturating_add(1);
    let out = unsafe { api.malloc(alloc_len) } as *mut u8;
    if out.is_null() {
        return;
    }
    unsafe {
        if payload_len > 0 {
            core::ptr::copy_nonoverlapping(bytes.as_ptr(), out, payload_len);
        }
        *out.add(payload_len) = 0;
        *out_err = out;
    }
}

fn set_out_err_from_error<P: Sqlite3Api>(api: &P, out_err: *mut *mut u8, err: &Error) {
    if let Some(message) = err.message.as_deref() {
        set_out_err_message(api, out_err, message);
    } else {
        set_out_err_message(api, out_err, &err.to_string());
    }
}

unsafe fn cstr_to_str<'a>(ptr: *const u8) -> Option<&'a str> {
    if ptr.is_null() {
        return None;
    }
    unsafe { core::ffi::CStr::from_ptr(ptr as *const c_char) }
        .to_str()
        .ok()
}

unsafe fn parse_args<'a>(argc: i32, argv: *const *const u8) -> Vec<&'a str> {
    let argc = if argc < 0 { 0 } else { argc as usize };
    let mut out = Vec::with_capacity(argc);
    if argc == 0 || argv.is_null() {
        return out;
    }
    let values = unsafe { core::slice::from_raw_parts(argv, argc) };
    for value in values {
        let value = unsafe { cstr_to_str(*value) }.unwrap_or("");
        out.push(value);
    }
    out
}

unsafe fn value_ref_from_raw<'a, P: Sqlite3Api>(api: &P, value: NonNull<P::Value>) -> ValueRef<'a> {
    match unsafe { api.value_type(value) } {
        ValueType::Null => ValueRef::Null,
        ValueType::Integer => ValueRef::Integer(unsafe { api.value_int64(value) }),
        ValueType::Float => ValueRef::Float(unsafe { api.value_double(value) }),
        ValueType::Text => unsafe { ValueRef::from_raw_text(api.value_text(value)) },
        ValueType::Blob => unsafe { ValueRef::from_raw_blob(api.value_blob(value)) },
    }
}

unsafe fn args_from_values<'a, P: Sqlite3Api>(
    api: &P,
    argc: i32,
    argv: *mut *mut P::Value,
) -> ArgBuffer<'a> {
    let argc = if argc < 0 { 0 } else { argc as usize };
    let mut out = ArgBuffer::new(argc);
    if argc == 0 || argv.is_null() {
        return out;
    }
    let values = unsafe { core::slice::from_raw_parts(argv, argc) };
    for value in values {
        if let Some(ptr) = NonNull::new(*value) {
            out.push(unsafe { value_ref_from_raw(api, ptr) });
        } else {
            out.push(ValueRef::Null);
        }
    }
    out
}

extern "C" fn x_create<P, T>(
    db: *mut P::Db,
    aux: *mut c_void,
    argc: i32,
    argv: *const *const u8,
    out_vtab: *mut *mut P::VTab,
    out_err: *mut *mut u8,
) -> i32
where
    P: Sqlite3Api,
    T: VirtualTable<P>,
{
    if out_vtab.is_null() {
        return 1;
    }
    unsafe {
        if !out_err.is_null() {
            *out_err = core::ptr::null_mut();
        }
    }
    if aux.is_null() || db.is_null() {
        return 1;
    }
    let api = unsafe { &*(aux as *const P) };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let args = unsafe { parse_args(argc, argv) };
        match T::connect(&args) {
            Ok((table, schema)) => {
                if let Err(err) = unsafe { api.declare_vtab(NonNull::new_unchecked(db), &schema) } {
                    set_out_err_from_error(api, out_err, &err);
                    return err_code(&err);
                }
                let vtab = Box::new(VTab {
                    api: api as *const P,
                    table,
                });
                unsafe {
                    *out_vtab = Box::into_raw(vtab) as *mut P::VTab;
                }
                0
            }
            Err(err) => {
                let err: Error = err.into();
                set_out_err_from_error(api, out_err, &err);
                err_code(&err)
            }
        }
    }));
    match out {
        Ok(code) => code,
        Err(_) => {
            set_out_err_message(api, out_err, "panic in virtual table create/connect");
            1
        }
    }
}

extern "C" fn x_connect<P, T>(
    db: *mut P::Db,
    aux: *mut c_void,
    argc: i32,
    argv: *const *const u8,
    out_vtab: *mut *mut P::VTab,
    out_err: *mut *mut u8,
) -> i32
where
    P: Sqlite3Api,
    T: VirtualTable<P>,
{
    x_create::<P, T>(db, aux, argc, argv, out_vtab, out_err)
}

extern "C" fn x_best_index<P, T>(vtab: *mut P::VTab, info: *mut c_void) -> i32
where
    P: Sqlite3Api<VTab = VTab<P, T>>,
    T: VirtualTable<P>,
{
    if vtab.is_null() {
        return 1;
    }
    let vtab: &mut VTab<P, T> = unsafe { &mut *vtab };
    let mut info = BestIndexInfo { raw: info };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        vtab.table.best_index(&mut info)
    }));
    match out {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => err_code(&err.into()),
        Err(_) => 1,
    }
}

extern "C" fn x_disconnect<P, T>(vtab: *mut P::VTab) -> i32
where
    P: Sqlite3Api<VTab = VTab<P, T>>,
    T: VirtualTable<P>,
{
    if vtab.is_null() {
        return 0;
    }
    let vtab: Box<VTab<P, T>> = unsafe { Box::from_raw(vtab) };
    let VTab { table, .. } = *vtab;
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| table.disconnect()));
    match out {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => err_code(&err.into()),
        Err(_) => 1,
    }
}

extern "C" fn x_destroy<P, T>(vtab: *mut P::VTab) -> i32
where
    P: Sqlite3Api<VTab = VTab<P, T>>,
    T: VirtualTable<P>,
{
    x_disconnect::<P, T>(vtab)
}

extern "C" fn x_open<P, T>(vtab: *mut P::VTab, out_cursor: *mut *mut P::VTabCursor) -> i32
where
    P: Sqlite3Api<VTab = VTab<P, T>, VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    if vtab.is_null() || out_cursor.is_null() {
        return 1;
    }
    let vtab: &mut VTab<P, T> = unsafe { &mut *vtab };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| vtab.table.open()));
    match out {
        Ok(Ok(cursor)) => {
            let handle = Box::new(Cursor {
                api: vtab.api,
                cursor,
            });
            unsafe { *out_cursor = Box::into_raw(handle) };
            0
        }
        Ok(Err(err)) => err_code(&err.into()),
        Err(_) => 1,
    }
}

extern "C" fn x_close<P, T>(cursor: *mut P::VTabCursor) -> i32
where
    P: Sqlite3Api<VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    if cursor.is_null() {
        return 0;
    }
    unsafe { drop(Box::from_raw(cursor)) };
    0
}

extern "C" fn x_filter<P, T>(
    cursor: *mut P::VTabCursor,
    idx_num: i32,
    idx_str: *const u8,
    argc: i32,
    argv: *mut *mut P::Value,
) -> i32
where
    P: Sqlite3Api<VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    if cursor.is_null() {
        return 1;
    }
    let cursor: &mut Cursor<P, T::Cursor> = unsafe { &mut *cursor };
    let api = unsafe { &*cursor.api };
    let idx_str = unsafe { cstr_to_str(idx_str) };
    let args = unsafe { args_from_values(api, argc, argv) };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cursor.cursor.filter(idx_num, idx_str, args.as_slice())
    }));
    match out {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => err_code(&err.into()),
        Err(_) => 1,
    }
}

extern "C" fn x_next<P, T>(cursor: *mut P::VTabCursor) -> i32
where
    P: Sqlite3Api<VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    if cursor.is_null() {
        return 1;
    }
    let cursor: &mut Cursor<P, T::Cursor> = unsafe { &mut *cursor };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cursor.cursor.next()));
    match out {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => err_code(&err.into()),
        Err(_) => 1,
    }
}

extern "C" fn x_eof<P, T>(cursor: *mut P::VTabCursor) -> i32
where
    P: Sqlite3Api<VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    if cursor.is_null() {
        return 1;
    }
    let cursor: &mut Cursor<P, T::Cursor> = unsafe { &mut *cursor };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cursor.cursor.eof()));
    match out {
        Ok(true) => 1,
        Ok(false) => 0,
        Err(_) => 1,
    }
}

extern "C" fn x_column<P, T>(cursor: *mut P::VTabCursor, ctx: *mut P::Context, col: i32) -> i32
where
    P: Sqlite3Api<VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    if cursor.is_null() || ctx.is_null() {
        return 1;
    }
    let cursor: &mut Cursor<P, T::Cursor> = unsafe { &mut *cursor };
    let api = unsafe { &*cursor.api };
    let context = Context::new(api, unsafe { NonNull::new_unchecked(ctx) });
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        cursor.cursor.column(&context, col)
    }));
    match out {
        Ok(Ok(())) => 0,
        Ok(Err(err)) => {
            let err: Error = err.into();
            context.result_error(err.message.as_deref().unwrap_or("virtual table error"));
            err_code(&err)
        }
        Err(_) => {
            context.result_error("panic in virtual table column");
            1
        }
    }
}

extern "C" fn x_rowid<P, T>(cursor: *mut P::VTabCursor, rowid: *mut i64) -> i32
where
    P: Sqlite3Api<VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    if cursor.is_null() || rowid.is_null() {
        return 1;
    }
    let cursor: &mut Cursor<P, T::Cursor> = unsafe { &mut *cursor };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| cursor.cursor.rowid()));
    match out {
        Ok(Ok(id)) => {
            unsafe { *rowid = id };
            0
        }
        Ok(Err(err)) => err_code(&err.into()),
        Err(_) => 1,
    }
}

/// Build a `sqlite3_module` backed by `VirtualTable` and `VTabCursor`.
pub fn module<P, T>() -> sqlite3_module<P>
where
    P: Sqlite3Api<VTab = VTab<P, T>, VTabCursor = Cursor<P, T::Cursor>>,
    T: VirtualTable<P>,
{
    sqlite3_module {
        i_version: 1,
        x_create: Some(x_create::<P, T>),
        x_connect: Some(x_connect::<P, T>),
        x_best_index: Some(x_best_index::<P, T>),
        x_disconnect: Some(x_disconnect::<P, T>),
        x_destroy: Some(x_destroy::<P, T>),
        x_open: Some(x_open::<P, T>),
        x_close: Some(x_close::<P, T>),
        x_filter: Some(x_filter::<P, T>),
        x_next: Some(x_next::<P, T>),
        x_eof: Some(x_eof::<P, T>),
        x_column: Some(x_column::<P, T>),
        x_rowid: Some(x_rowid::<P, T>),
    }
}

impl<'p, P: Sqlite3Api> Connection<'p, P> {
    /// Register a virtual table module using the default glue.
    pub fn create_module<T>(&self, name: &str) -> Result<()>
    where
        P: Sqlite3Api<VTab = VTab<P, T>, VTabCursor = Cursor<P, T::Cursor>>,
        T: VirtualTable<P>,
    {
        if !self.api.feature_set().contains(FeatureSet::VIRTUAL_TABLES) {
            return Err(Error::feature_unavailable("virtual tables unsupported"));
        }
        let module = cached_module::<P, T>();
        unsafe {
            self.api.create_module_v2(
                self.db,
                name,
                module,
                self.api as *const P as *mut c_void,
                None,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ArgBuffer, BestIndexInfo};
    use crate::value::ValueRef;

    #[test]
    fn best_index_info_is_pointer() {
        let info = BestIndexInfo {
            raw: core::ptr::null_mut(),
        };
        assert!(info.raw.is_null());
    }

    #[test]
    fn arg_buffer_inline() {
        let mut buf = ArgBuffer::new(2);
        buf.push(ValueRef::Integer(1));
        buf.push(ValueRef::Integer(2));
        assert_eq!(
            buf.as_slice(),
            &[ValueRef::Integer(1), ValueRef::Integer(2)]
        );
    }

    #[test]
    fn arg_buffer_heap() {
        let mut buf = ArgBuffer::new(9);
        for i in 0..9 {
            buf.push(ValueRef::Integer(i));
        }
        assert_eq!(buf.as_slice().len(), 9);
        assert_eq!(buf.as_slice()[0], ValueRef::Integer(0));
    }
}
