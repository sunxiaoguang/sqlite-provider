use core::ffi::c_void;
use core::mem::MaybeUninit;
use core::ptr::NonNull;

use crate::connection::Connection;
use crate::error::{Error, Result};
use crate::provider::{FeatureSet, FunctionFlags, Sqlite3Api, ValueType};
use crate::value::{Value, ValueRef};

/// Context wrapper passed to user-defined functions.
pub struct Context<'p, P: Sqlite3Api> {
    api: &'p P,
    ctx: NonNull<P::Context>,
}

impl<'p, P: Sqlite3Api> Context<'p, P> {
    pub(crate) fn new(api: &'p P, ctx: NonNull<P::Context>) -> Self {
        Self { api, ctx }
    }

    /// Set NULL result.
    pub fn result_null(&self) {
        unsafe { self.api.result_null(self.ctx) }
    }

    /// Set integer result.
    pub fn result_int64(&self, v: i64) {
        unsafe { self.api.result_int64(self.ctx, v) }
    }

    /// Set floating result.
    pub fn result_double(&self, v: f64) {
        unsafe { self.api.result_double(self.ctx, v) }
    }

    /// Set text result (provider must copy or retain the bytes as needed).
    pub fn result_text(&self, v: &str) {
        unsafe { self.api.result_text(self.ctx, v) }
    }

    /// Set blob result (provider must copy or retain the bytes as needed).
    pub fn result_blob(&self, v: &[u8]) {
        unsafe { self.api.result_blob(self.ctx, v) }
    }

    /// Set error result.
    pub fn result_error(&self, msg: &str) {
        unsafe { self.api.result_error(self.ctx, msg) }
    }

    /// Set result from an owned `Value`.
    pub fn result_value(&self, value: Value) {
        match value {
            Value::Null => self.result_null(),
            Value::Integer(v) => self.result_int64(v),
            Value::Float(v) => self.result_double(v),
            Value::Text(v) => self.result_text(&v),
            Value::Blob(v) => self.result_blob(&v),
        }
    }
}

const INLINE_ARGS: usize = 8;

struct ArgBuffer<'a> {
    inline: [MaybeUninit<ValueRef<'a>>; INLINE_ARGS],
    len: usize,
    heap: Option<Vec<ValueRef<'a>>>,
}

impl<'a> ArgBuffer<'a> {
    fn new(argc: usize) -> Self {
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
        unsafe {
            core::slice::from_raw_parts(self.inline.as_ptr() as *const ValueRef<'a>, self.len)
        }
    }
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

fn args_from_raw<'a, P: Sqlite3Api>(api: &P, argc: i32, argv: *mut *mut P::Value) -> ArgBuffer<'a> {
    let argc = if argc < 0 { 0 } else { argc as usize };
    let mut args = ArgBuffer::new(argc);
    if argc == 0 || argv.is_null() {
        return args;
    }
    let values = unsafe { core::slice::from_raw_parts(argv, argc) };
    for value in values {
        if let Some(ptr) = NonNull::new(*value) {
            let arg = unsafe { value_ref_from_raw(api, ptr) };
            args.push(arg);
        } else {
            args.push(ValueRef::Null);
        }
    }
    args
}

fn set_error<P: Sqlite3Api>(ctx: &Context<'_, P>, err: &Error) {
    let msg = err.message.as_deref().unwrap_or("sqlite function error");
    ctx.result_error(msg);
}

struct ScalarState<P: Sqlite3Api, F> {
    api: *const P,
    func: F,
}

extern "C" fn scalar_trampoline<P, F>(ctx: *mut P::Context, argc: i32, argv: *mut *mut P::Value)
where
    P: Sqlite3Api,
    F: for<'a> FnMut(&Context<'a, P>, &[ValueRef<'a>]) -> Result<Value> + Send + 'static,
{
    let ctx = match NonNull::new(ctx) {
        Some(ctx) => ctx,
        None => return,
    };
    let user_data = unsafe { P::user_data(ctx) };
    if user_data.is_null() {
        return;
    }
    let state = unsafe { &mut *(user_data as *mut ScalarState<P, F>) };
    let api = unsafe { &*state.api };
    let context = Context { api, ctx };
    let args = args_from_raw(api, argc, argv);
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (state.func)(&context, args.as_slice())
    }));
    match out {
        Ok(Ok(value)) => context.result_value(value),
        Ok(Err(err)) => set_error(&context, &err),
        Err(_) => context.result_error("panic in sqlite function"),
    }
}

struct AggregateState<P: Sqlite3Api, T, Init, Step, Final> {
    api: *const P,
    init: Init,
    step: Step,
    final_fn: Final,
    _marker: core::marker::PhantomData<T>,
}

type AggStateSlot<T> = *mut T;

unsafe fn get_agg_slot<P: Sqlite3Api, T>(
    api: &P,
    ctx: NonNull<P::Context>,
    allocate: bool,
) -> *mut AggStateSlot<T> {
    let bytes = if allocate {
        core::mem::size_of::<AggStateSlot<T>>()
    } else {
        0
    };
    unsafe { api.aggregate_context(ctx, bytes) as *mut AggStateSlot<T> }
}

extern "C" fn aggregate_step_trampoline<P, T, Init, Step, Final>(
    ctx: *mut P::Context,
    argc: i32,
    argv: *mut *mut P::Value,
) where
    P: Sqlite3Api,
    T: Send + 'static,
    Init: Fn() -> T + Send + 'static,
    Step: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
{
    let ctx = match NonNull::new(ctx) {
        Some(ctx) => ctx,
        None => return,
    };
    let user_data = unsafe { P::user_data(ctx) };
    if user_data.is_null() {
        return;
    }
    let state = unsafe { &mut *(user_data as *mut AggregateState<P, T, Init, Step, Final>) };
    let api = unsafe { &*state.api };
    let context = Context { api, ctx };
    let slot = unsafe { get_agg_slot::<P, T>(api, ctx, true) };
    if slot.is_null() {
        context.result_error("sqlite aggregate no memory");
        return;
    }
    if unsafe { (*slot).is_null() } {
        let init_out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (state.init)()));
        match init_out {
            Ok(value) => {
                unsafe { *slot = Box::into_raw(Box::new(value)) };
            }
            Err(_) => {
                context.result_error("panic in sqlite aggregate init");
                return;
            }
        }
    }
    let args = args_from_raw(api, argc, argv);
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let value = unsafe { &mut **slot };
        (state.step)(&context, value, args.as_slice())
    }));
    match out {
        Ok(Ok(())) => {}
        Ok(Err(err)) => set_error(&context, &err),
        Err(_) => context.result_error("panic in sqlite aggregate"),
    }
}

extern "C" fn aggregate_final_trampoline<P, T, Init, Step, Final>(ctx: *mut P::Context)
where
    P: Sqlite3Api,
    T: Send + 'static,
    Init: Fn() -> T + Send + 'static,
    Step: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
{
    let ctx = match NonNull::new(ctx) {
        Some(ctx) => ctx,
        None => return,
    };
    let user_data = unsafe { P::user_data(ctx) };
    if user_data.is_null() {
        return;
    }
    let state = unsafe { &mut *(user_data as *mut AggregateState<P, T, Init, Step, Final>) };
    let api = unsafe { &*state.api };
    let context = Context { api, ctx };
    let slot = unsafe { get_agg_slot::<P, T>(api, ctx, false) };
    if slot.is_null() {
        context.result_null();
        return;
    }
    let state_ptr = unsafe { *slot };
    if state_ptr.is_null() {
        context.result_null();
        return;
    }
    unsafe { *slot = core::ptr::null_mut() };
    let value = unsafe { *Box::from_raw(state_ptr) };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (state.final_fn)(&context, value)
    }));
    match out {
        Ok(Ok(result)) => context.result_value(result),
        Ok(Err(err)) => set_error(&context, &err),
        Err(_) => context.result_error("panic in sqlite aggregate final"),
    }
}

struct WindowState<P: Sqlite3Api, T, Init, Step, Inverse, ValueFn, Final> {
    api: *const P,
    init: Init,
    step: Step,
    inverse: Inverse,
    value_fn: ValueFn,
    final_fn: Final,
    _marker: core::marker::PhantomData<T>,
}

extern "C" fn window_step_trampoline<P, T, Init, Step, Inverse, ValueFn, Final>(
    ctx: *mut P::Context,
    argc: i32,
    argv: *mut *mut P::Value,
) where
    P: Sqlite3Api,
    T: Send + 'static,
    Init: Fn() -> T + Send + 'static,
    Step: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    Inverse: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    ValueFn: for<'a> FnMut(&Context<'a, P>, &mut T) -> Result<Value> + Send + 'static,
    Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
{
    let ctx = match NonNull::new(ctx) {
        Some(ctx) => ctx,
        None => return,
    };
    let user_data = unsafe { P::user_data(ctx) };
    if user_data.is_null() {
        return;
    }
    let state =
        unsafe { &mut *(user_data as *mut WindowState<P, T, Init, Step, Inverse, ValueFn, Final>) };
    let api = unsafe { &*state.api };
    let context = Context { api, ctx };
    let slot = unsafe { get_agg_slot::<P, T>(api, ctx, true) };
    if slot.is_null() {
        context.result_error("sqlite window no memory");
        return;
    }
    if unsafe { (*slot).is_null() } {
        let init_out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (state.init)()));
        match init_out {
            Ok(value) => {
                unsafe { *slot = Box::into_raw(Box::new(value)) };
            }
            Err(_) => {
                context.result_error("panic in sqlite window init");
                return;
            }
        }
    }
    let args = args_from_raw(api, argc, argv);
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let value = unsafe { &mut **slot };
        (state.step)(&context, value, args.as_slice())
    }));
    match out {
        Ok(Ok(())) => {}
        Ok(Err(err)) => set_error(&context, &err),
        Err(_) => context.result_error("panic in sqlite window step"),
    }
}

extern "C" fn window_inverse_trampoline<P, T, Init, Step, Inverse, ValueFn, Final>(
    ctx: *mut P::Context,
    argc: i32,
    argv: *mut *mut P::Value,
) where
    P: Sqlite3Api,
    T: Send + 'static,
    Init: Fn() -> T + Send + 'static,
    Step: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    Inverse: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    ValueFn: for<'a> FnMut(&Context<'a, P>, &mut T) -> Result<Value> + Send + 'static,
    Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
{
    let ctx = match NonNull::new(ctx) {
        Some(ctx) => ctx,
        None => return,
    };
    let user_data = unsafe { P::user_data(ctx) };
    if user_data.is_null() {
        return;
    }
    let state =
        unsafe { &mut *(user_data as *mut WindowState<P, T, Init, Step, Inverse, ValueFn, Final>) };
    let api = unsafe { &*state.api };
    let context = Context { api, ctx };
    let slot = unsafe { get_agg_slot::<P, T>(api, ctx, true) };
    if slot.is_null() {
        context.result_error("sqlite window no memory");
        return;
    }
    if unsafe { (*slot).is_null() } {
        let init_out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (state.init)()));
        match init_out {
            Ok(value) => {
                unsafe { *slot = Box::into_raw(Box::new(value)) };
            }
            Err(_) => {
                context.result_error("panic in sqlite window init");
                return;
            }
        }
    }
    let args = args_from_raw(api, argc, argv);
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let value = unsafe { &mut **slot };
        (state.inverse)(&context, value, args.as_slice())
    }));
    match out {
        Ok(Ok(())) => {}
        Ok(Err(err)) => set_error(&context, &err),
        Err(_) => context.result_error("panic in sqlite window inverse"),
    }
}

extern "C" fn window_value_trampoline<P, T, Init, Step, Inverse, ValueFn, Final>(
    ctx: *mut P::Context,
) where
    P: Sqlite3Api,
    T: Send + 'static,
    Init: Fn() -> T + Send + 'static,
    Step: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    Inverse: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    ValueFn: for<'a> FnMut(&Context<'a, P>, &mut T) -> Result<Value> + Send + 'static,
    Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
{
    let ctx = match NonNull::new(ctx) {
        Some(ctx) => ctx,
        None => return,
    };
    let user_data = unsafe { P::user_data(ctx) };
    if user_data.is_null() {
        return;
    }
    let state =
        unsafe { &mut *(user_data as *mut WindowState<P, T, Init, Step, Inverse, ValueFn, Final>) };
    let api = unsafe { &*state.api };
    let context = Context { api, ctx };
    let slot = unsafe { get_agg_slot::<P, T>(api, ctx, false) };
    if slot.is_null() {
        context.result_null();
        return;
    }
    if unsafe { (*slot).is_null() } {
        context.result_null();
        return;
    }
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let value = unsafe { &mut **slot };
        (state.value_fn)(&context, value)
    }));
    match out {
        Ok(Ok(result)) => context.result_value(result),
        Ok(Err(err)) => set_error(&context, &err),
        Err(_) => context.result_error("panic in sqlite window value"),
    }
}

extern "C" fn window_final_trampoline<P, T, Init, Step, Inverse, ValueFn, Final>(
    ctx: *mut P::Context,
) where
    P: Sqlite3Api,
    T: Send + 'static,
    Init: Fn() -> T + Send + 'static,
    Step: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    Inverse: for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
    ValueFn: for<'a> FnMut(&Context<'a, P>, &mut T) -> Result<Value> + Send + 'static,
    Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
{
    let ctx = match NonNull::new(ctx) {
        Some(ctx) => ctx,
        None => return,
    };
    let user_data = unsafe { P::user_data(ctx) };
    if user_data.is_null() {
        return;
    }
    let state =
        unsafe { &mut *(user_data as *mut WindowState<P, T, Init, Step, Inverse, ValueFn, Final>) };
    let api = unsafe { &*state.api };
    let context = Context { api, ctx };
    let slot = unsafe { get_agg_slot::<P, T>(api, ctx, false) };
    if slot.is_null() {
        context.result_null();
        return;
    }
    let state_ptr = unsafe { *slot };
    if state_ptr.is_null() {
        context.result_null();
        return;
    }
    unsafe { *slot = core::ptr::null_mut() };
    let value = unsafe { *Box::from_raw(state_ptr) };
    let out = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        (state.final_fn)(&context, value)
    }));
    match out {
        Ok(Ok(result)) => context.result_value(result),
        Ok(Err(err)) => set_error(&context, &err),
        Err(_) => context.result_error("panic in sqlite window final"),
    }
}

extern "C" fn drop_boxed<T>(ptr: *mut c_void) {
    if !ptr.is_null() {
        unsafe { drop(Box::from_raw(ptr as *mut T)) };
    }
}

impl<'p, P: Sqlite3Api> Connection<'p, P> {
    /// Register a scalar function (xFunc).
    pub fn create_scalar_function<F>(&self, name: &str, n_args: i32, func: F) -> Result<()>
    where
        F: for<'a> FnMut(&Context<'a, P>, &[ValueRef<'a>]) -> Result<Value> + Send + 'static,
    {
        if !self
            .api
            .feature_set()
            .contains(FeatureSet::CREATE_FUNCTION_V2)
        {
            return Err(Error::feature_unavailable("create_function_v2 unsupported"));
        }
        let state = Box::new(ScalarState {
            api: self.api as *const P,
            func,
        });
        let user_data = Box::into_raw(state) as *mut c_void;
        unsafe {
            self.api.create_function_v2(
                self.db,
                name,
                n_args,
                FunctionFlags::empty(),
                Some(scalar_trampoline::<P, F>),
                None,
                None,
                user_data,
                Some(drop_boxed::<ScalarState<P, F>>),
            )
        }
    }

    /// Register an aggregate function (xStep/xFinal).
    ///
    /// State is stored out-of-line in a Rust `Box<T>` and the SQLite aggregate
    /// context keeps only a pointer-sized slot, so `T` alignment does not
    /// depend on backend aggregate-context allocation alignment.
    pub fn create_aggregate_function<T, Init, Step, Final>(
        &self,
        name: &str,
        n_args: i32,
        init: Init,
        step: Step,
        final_fn: Final,
    ) -> Result<()>
    where
        T: Send + 'static,
        Init: Fn() -> T + Send + 'static,
        Step:
            for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
        Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
    {
        if !self
            .api
            .feature_set()
            .contains(FeatureSet::CREATE_FUNCTION_V2)
        {
            return Err(Error::feature_unavailable("create_function_v2 unsupported"));
        }
        let state = Box::new(AggregateState::<P, T, Init, Step, Final> {
            api: self.api as *const P,
            init,
            step,
            final_fn,
            _marker: core::marker::PhantomData,
        });
        let user_data = Box::into_raw(state) as *mut c_void;
        unsafe {
            self.api.create_function_v2(
                self.db,
                name,
                n_args,
                FunctionFlags::empty(),
                None,
                Some(aggregate_step_trampoline::<P, T, Init, Step, Final>),
                Some(aggregate_final_trampoline::<P, T, Init, Step, Final>),
                user_data,
                Some(drop_boxed::<AggregateState<P, T, Init, Step, Final>>),
            )
        }
    }

    /// Register a window function (xStep/xInverse/xValue/xFinal).
    ///
    /// State is stored out-of-line in a Rust `Box<T>` and the SQLite aggregate
    /// context keeps only a pointer-sized slot, so `T` alignment does not
    /// depend on backend aggregate-context allocation alignment.
    #[allow(clippy::too_many_arguments)]
    pub fn create_window_function<T, Init, Step, Inverse, ValueFn, Final>(
        &self,
        name: &str,
        n_args: i32,
        init: Init,
        step: Step,
        inverse: Inverse,
        value_fn: ValueFn,
        final_fn: Final,
    ) -> Result<()>
    where
        T: Send + 'static,
        Init: Fn() -> T + Send + 'static,
        Step:
            for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
        Inverse:
            for<'a> FnMut(&Context<'a, P>, &mut T, &[ValueRef<'a>]) -> Result<()> + Send + 'static,
        ValueFn: for<'a> FnMut(&Context<'a, P>, &mut T) -> Result<Value> + Send + 'static,
        Final: for<'a> FnMut(&Context<'a, P>, T) -> Result<Value> + Send + 'static,
    {
        if !self
            .api
            .feature_set()
            .contains(FeatureSet::WINDOW_FUNCTIONS)
        {
            return Err(Error::feature_unavailable("window functions unsupported"));
        }
        let state = Box::new(WindowState::<P, T, Init, Step, Inverse, ValueFn, Final> {
            api: self.api as *const P,
            init,
            step,
            inverse,
            value_fn,
            final_fn,
            _marker: core::marker::PhantomData,
        });
        let user_data = Box::into_raw(state) as *mut c_void;
        unsafe {
            self.api.create_window_function(
                self.db,
                name,
                n_args,
                FunctionFlags::empty(),
                Some(window_step_trampoline::<P, T, Init, Step, Inverse, ValueFn, Final>),
                Some(window_final_trampoline::<P, T, Init, Step, Inverse, ValueFn, Final>),
                Some(window_value_trampoline::<P, T, Init, Step, Inverse, ValueFn, Final>),
                Some(window_inverse_trampoline::<P, T, Init, Step, Inverse, ValueFn, Final>),
                user_data,
                Some(drop_boxed::<WindowState<P, T, Init, Step, Inverse, ValueFn, Final>>),
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ArgBuffer;
    use crate::value::ValueRef;

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
}
