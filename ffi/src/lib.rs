//! C interface to the sia_storage crate, consumed by the Go SDK via cgo.
//!
//! See include/sia_storage.h for the C-side contract. Every extern function
//! is panic-safe: panics are caught and reported as SIA_ERR.

use std::ffi::{CStr, CString, c_char};
use std::future::Future;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::pin::Pin;
use std::sync::{Arc, Mutex, OnceLock};
use std::task::Poll;

use sia_storage::{
    AppApiError, AppKey, AppMetadata, ApprovedState, Builder, BuilderError, DisconnectedState,
    DownloadOptions, Hash256, Object, ObjectsCursor, PackedUpload, RequestingApprovalState, Sdk,
    ShardProgress, UploadOptions,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt, DuplexStream, ReadBuf};
use tokio::runtime::Runtime;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

const SIA_OK: i32 = 0;
const SIA_ERR: i32 = 1;
const SIA_ERR_UNAUTHORIZED: i32 = 2;
const SIA_ERR_USER_REJECTED: i32 = 3;
const SIA_ERR_REQUEST_EXPIRED: i32 = 4;
const SIA_ERR_CANCELLED: i32 = 5;
const SIA_ERR_INVALID_STATE: i32 = 6;

// Sized to keep several slabs' worth of data in flight so upload encoding is
// never starved waiting on the writer.
const UPLOAD_PIPE_CAPACITY: usize = 1 << 24; // 16 MiB

type ProgressFn = unsafe extern "C" fn(usize, *const ShardProgressC);
type LogFn = unsafe extern "C" fn(usize, i32, *const c_char, *const c_char);

#[repr(C)]
pub struct ShardProgressC {
    host_key: [u8; 32],
    shard_size: u64,
    shard_index: u64,
    slab_index: u64,
    elapsed_us: u64,
}

#[repr(C)]
pub struct UploadOptionsC {
    data_shards: u8,
    parity_shards: u8,
    set_redundancy: bool,
    max_buffered_slabs: u64,
    on_shard: Option<ProgressFn>,
    userdata: usize,
}

#[repr(C)]
pub struct DownloadOptionsC {
    offset: u64,
    has_length: bool,
    length: u64,
    max_buffered_chunks: u64,
    on_shard: Option<ProgressFn>,
    userdata: usize,
}

/// A C callback plus its userdata. The Go side guarantees the callback is
/// safe to invoke from any thread.
#[derive(Clone, Copy)]
struct CCallback {
    cb: ProgressFn,
    userdata: usize,
}

unsafe impl Send for CCallback {}
unsafe impl Sync for CCallback {}

impl CCallback {
    fn invoke(&self, progress: ShardProgress) {
        let mut host_key = [0u8; 32];
        host_key.copy_from_slice(progress.host_key.as_ref());
        let c = ShardProgressC {
            host_key,
            shard_size: progress.shard_size as u64,
            shard_index: progress.shard_index as u64,
            slab_index: progress.slab_index as u64,
            elapsed_us: progress.elapsed.as_micros() as u64,
        };
        unsafe { (self.cb)(self.userdata, &c) }
    }
}

struct CLogger {
    cb: LogFn,
    userdata: usize,
}

unsafe impl Send for CLogger {}
unsafe impl Sync for CLogger {}

impl log::Log for CLogger {
    fn enabled(&self, _: &log::Metadata) -> bool {
        true
    }

    fn log(&self, record: &log::Record) {
        let target = CString::new(record.target()).unwrap_or_default();
        let msg = CString::new(record.args().to_string()).unwrap_or_default();
        unsafe { (self.cb)(self.userdata, record.level() as i32, target.as_ptr(), msg.as_ptr()) }
    }

    fn flush(&self) {}
}

enum BuilderState {
    Disconnected(Builder<DisconnectedState>),
    Requesting(Builder<RequestingApprovalState>),
    Approved(Builder<ApprovedState>),
    Consumed,
}

pub struct FfiBuilder(Mutex<BuilderState>);

pub struct FfiUpload {
    writer: Option<DuplexStream>,
    task: Option<JoinHandle<Result<Object, String>>>,
}

pub struct FfiDownload {
    reader: Pin<Box<dyn AsyncRead + Send>>,
    pending_err: Option<std::io::Error>,
}

pub struct FfiPacked {
    inner: Arc<tokio::sync::Mutex<Option<PackedUpload>>>,
    optimal_data_size: u64,
    writer: Option<DuplexStream>,
    add_task: Option<JoinHandle<Result<u64, String>>>,
}

struct FfiEvent {
    id: [u8; 32],
    deleted: bool,
    updated_at_us: i64,
    object: Option<Box<Object>>,
}

pub struct FfiEvents(Vec<FfiEvent>);

#[cfg(feature = "mock")]
pub struct FfiMock {
    hosts: sia_storage::mock::MockHosts,
    app_key: Arc<AppKey>,
}

/// Wraps the read half of a pipe, surfacing the producer task's error (stored
/// in the shared slot before it drops the write half) instead of a bare EOF.
#[cfg(feature = "mock")]
struct PipeReader {
    inner: DuplexStream,
    err: Arc<Mutex<Option<std::io::Error>>>,
}

#[cfg(feature = "mock")]
impl AsyncRead for PipeReader {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buf.filled().len();
        match Pin::new(&mut self.inner).poll_read(cx, buf) {
            Poll::Ready(Ok(())) if buf.filled().len() == before => {
                if let Some(e) = self.err.lock().unwrap().take() {
                    return Poll::Ready(Err(e));
                }
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}

#[derive(serde::Deserialize)]
struct AppMetadataIn {
    #[serde(rename = "appID")]
    id: Hash256,
    name: String,
    description: String,
    #[serde(rename = "serviceURL")]
    service_url: String,
    #[serde(rename = "logoURL")]
    logo_url: Option<String>,
    #[serde(rename = "callbackURL")]
    callback_url: Option<String>,
}

fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("sia-storage-ffi")
            // The default 2 MiB worker stack has been observed to overflow
            // during chunk recovery; overflows on Rust-owned threads kill the
            // process with an untraceable SIGSEGV, so keep this generous.
            .thread_stack_size(8 << 20)
            .build()
            .expect("failed to build tokio runtime")
    })
}

fn set_err(err: *mut *mut c_char, code: i32, msg: impl AsRef<str>) -> i32 {
    if !err.is_null() {
        let s = CString::new(msg.as_ref()).unwrap_or_default();
        unsafe { *err = s.into_raw() }
    }
    code
}

fn set_cancelled(err: *mut *mut c_char) -> i32 {
    set_err(err, SIA_ERR_CANCELLED, "operation cancelled")
}

/// Runs a future to completion on the shared runtime. Returns None if the
/// cancel token fires first.
fn block_on<F: Future>(cancel: *mut CancellationToken, fut: F) -> Option<F::Output> {
    let cancel = if cancel.is_null() {
        None
    } else {
        Some(unsafe { (*cancel).clone() })
    };
    runtime().block_on(async move {
        match cancel {
            Some(tok) => tokio::select! {
                biased;
                _ = tok.cancelled() => None,
                out = fut => Some(out),
            },
            None => Some(fut.await),
        }
    })
}

fn builder_error(err: *mut *mut c_char, e: BuilderError) -> i32 {
    let code = match &e {
        BuilderError::RequestExpired => SIA_ERR_REQUEST_EXPIRED,
        BuilderError::Client(AppApiError::UserRejected) => SIA_ERR_USER_REJECTED,
        _ => SIA_ERR,
    };
    set_err(err, code, e.to_string())
}

fn make_upload_options(c: &UploadOptionsC) -> UploadOptions {
    let mut o = UploadOptions::default();
    if c.set_redundancy {
        o.data_shards = c.data_shards;
        o.parity_shards = c.parity_shards;
    }
    if c.max_buffered_slabs > 0 {
        o.max_buffered_slabs = Some(c.max_buffered_slabs as usize);
    }
    if let Some(cb) = c.on_shard {
        let cb = CCallback {
            cb,
            userdata: c.userdata,
        };
        o = o.on_shard_uploaded(move |p| cb.invoke(p));
    }
    o
}

fn make_download_options(c: &DownloadOptionsC) -> DownloadOptions {
    let mut o = DownloadOptions {
        offset: c.offset,
        ..Default::default()
    };
    if c.has_length {
        o.length = Some(c.length);
    }
    if c.max_buffered_chunks > 0 {
        o.max_buffered_chunks = Some(c.max_buffered_chunks as usize);
    }
    if let Some(cb) = c.on_shard {
        let cb = CCallback {
            cb,
            userdata: c.userdata,
        };
        o = o.on_shard_downloaded(move |p| cb.invoke(p));
    }
    o
}

fn hash_from_ptr(ptr: *const u8) -> Hash256 {
    let mut buf = [0u8; 32];
    buf.copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, 32) });
    Hash256::new(buf)
}

fn app_key_from_ptr(ptr: *const u8) -> AppKey {
    let mut buf = [0u8; 32];
    buf.copy_from_slice(unsafe { std::slice::from_raw_parts(ptr, 32) });
    AppKey::import(buf)
}

fn cstr<'a>(ptr: *const c_char) -> Result<&'a str, std::str::Utf8Error> {
    unsafe { CStr::from_ptr(ptr) }.to_str()
}

/// Wraps an FFI entry point body, converting panics into SIA_ERR.
fn guarded(err: *mut *mut c_char, body: impl FnOnce() -> i32) -> i32 {
    match catch_unwind(AssertUnwindSafe(body)) {
        Ok(code) => code,
        Err(_) => set_err(err, SIA_ERR, "internal panic in sia_storage_ffi"),
    }
}

/// Starts a streaming upload: the returned handle owns the write half of an
/// in-memory pipe and a task driving `upload` with the read half.
fn start_upload<F, Fut>(out: *mut *mut FfiUpload, err: *mut *mut c_char, upload: F) -> i32
where
    F: FnOnce(DuplexStream) -> Fut,
    Fut: Future<Output = Result<Object, String>> + Send + 'static,
{
    let (writer, reader) = tokio::io::duplex(UPLOAD_PIPE_CAPACITY);
    let fut = upload(reader);
    let task = { runtime().spawn(fut) };
    unsafe {
        *out = Box::into_raw(Box::new(FfiUpload {
            writer: Some(writer),
            task: Some(task),
        }));
    }
    let _ = err;
    SIA_OK
}

/// Awaits the upload task's result, mapping abort to SIA_ERR_CANCELLED.
fn finish_upload_task(
    task: JoinHandle<Result<Object, String>>,
    cancel: *mut CancellationToken,
    out: *mut *mut Object,
    err: *mut *mut c_char,
) -> i32 {
    match block_on(cancel, task) {
        None => set_cancelled(err),
        Some(Ok(Ok(obj))) => {
            unsafe { *out = Box::into_raw(Box::new(obj)) }
            SIA_OK
        }
        Some(Ok(Err(msg))) => set_err(err, SIA_ERR, msg),
        Some(Err(join_err)) if join_err.is_cancelled() => set_cancelled(err),
        Some(Err(join_err)) => set_err(err, SIA_ERR, join_err.to_string()),
    }
}

fn start_download(
    reader: Pin<Box<dyn AsyncRead + Send>>,
    out: *mut *mut FfiDownload,
) -> i32 {
    unsafe {
        *out = Box::into_raw(Box::new(FfiDownload {
            reader,
            pending_err: None,
        }));
    }
    SIA_OK
}

fn start_packed(
    packed: PackedUpload,
    out: *mut *mut FfiPacked,
) -> i32 {
    let optimal_data_size = packed.optimal_data_size() as u64;
    unsafe {
        *out = Box::into_raw(Box::new(FfiPacked {
            inner: Arc::new(tokio::sync::Mutex::new(Some(packed))),
            optimal_data_size,
            writer: None,
            add_task: None,
        }));
    }
    SIA_OK
}

// --- memory / util -----------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_string_free(s: *mut c_char) {
    if !s.is_null() {
        drop(unsafe { CString::from_raw(s) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_set_logger(cb: Option<LogFn>, userdata: usize, max_level: i32) {
    let Some(cb) = cb else { return };
    let level = match max_level {
        1 => log::LevelFilter::Error,
        2 => log::LevelFilter::Warn,
        3 => log::LevelFilter::Info,
        4 => log::LevelFilter::Debug,
        _ => log::LevelFilter::Trace,
    };
    if log::set_boxed_logger(Box::new(CLogger { cb, userdata })).is_ok() {
        log::set_max_level(level);
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_generate_recovery_phrase() -> *mut c_char {
    CString::new(sia_storage::generate_recovery_phrase())
        .unwrap_or_default()
        .into_raw()
}

// --- cancellation ------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_cancel_new() -> *mut CancellationToken {
    Box::into_raw(Box::new(CancellationToken::new()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_cancel_cancel(c: *mut CancellationToken) {
    if !c.is_null() {
        unsafe { (*c).cancel() }
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_cancel_free(c: *mut CancellationToken) {
    if !c.is_null() {
        drop(unsafe { Box::from_raw(c) });
    }
}

// --- builder -------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_builder_new(
    indexer_url: *const c_char,
    app_meta_json: *const c_char,
    out: *mut *mut FfiBuilder,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let url = match cstr(indexer_url) {
            Ok(s) => s,
            Err(e) => return set_err(err, SIA_ERR, format!("invalid indexer url: {e}")),
        };
        let meta_json = match cstr(app_meta_json) {
            Ok(s) => s,
            Err(e) => return set_err(err, SIA_ERR, format!("invalid app metadata: {e}")),
        };
        let meta: AppMetadataIn = match serde_json::from_str(meta_json) {
            Ok(m) => m,
            Err(e) => return set_err(err, SIA_ERR, format!("invalid app metadata: {e}")),
        };
        // AppMetadata requires 'static strings; a builder is created once per
        // connection attempt, so the leak is bounded and deliberate.
        let meta = AppMetadata {
            id: meta.id,
            name: Box::leak(meta.name.into_boxed_str()),
            description: Box::leak(meta.description.into_boxed_str()),
            service_url: Box::leak(meta.service_url.into_boxed_str()),
            logo_url: meta.logo_url.map(|s| &*Box::leak(s.into_boxed_str())),
            callback_url: meta.callback_url.map(|s| &*Box::leak(s.into_boxed_str())),
        };
        match Builder::new(url, meta) {
            Ok(b) => {
                unsafe {
                    *out = Box::into_raw(Box::new(FfiBuilder(Mutex::new(
                        BuilderState::Disconnected(b),
                    ))));
                }
                SIA_OK
            }
            Err(e) => builder_error(err, e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_builder_free(b: *mut FfiBuilder) {
    if !b.is_null() {
        drop(unsafe { Box::from_raw(b) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_builder_connect(
    b: *mut FfiBuilder,
    app_key: *const u8,
    cancel: *mut CancellationToken,
    out: *mut *mut Sdk,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let state = unsafe { (*b).0.lock() }.unwrap();
        let builder = match &*state {
            BuilderState::Disconnected(builder) => builder,
            _ => return set_err(err, SIA_ERR_INVALID_STATE, "builder is not disconnected"),
        };
        let key = app_key_from_ptr(app_key);
        match block_on(cancel, builder.connected(&key)) {
            None => set_cancelled(err),
            Some(Ok(Some(sdk))) => {
                unsafe { *out = Box::into_raw(Box::new(sdk)) }
                SIA_OK
            }
            Some(Ok(None)) => set_err(err, SIA_ERR_UNAUTHORIZED, "app key is not authorized"),
            Some(Err(e)) => builder_error(err, e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_builder_request_connection(
    b: *mut FfiBuilder,
    cancel: *mut CancellationToken,
    response_url: *mut *mut c_char,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let mut state = unsafe { (*b).0.lock() }.unwrap();
        let builder = match std::mem::replace(&mut *state, BuilderState::Consumed) {
            BuilderState::Disconnected(builder) => builder,
            other => {
                *state = other;
                return set_err(err, SIA_ERR_INVALID_STATE, "builder is not disconnected");
            }
        };
        match block_on(cancel, builder.request_connection()) {
            None => set_cancelled(err),
            Some(Ok(requesting)) => {
                let url = CString::new(requesting.response_url()).unwrap_or_default();
                *state = BuilderState::Requesting(requesting);
                unsafe { *response_url = url.into_raw() }
                SIA_OK
            }
            Some(Err(e)) => builder_error(err, e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_builder_wait_for_approval(
    b: *mut FfiBuilder,
    cancel: *mut CancellationToken,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let mut state = unsafe { (*b).0.lock() }.unwrap();
        let builder = match std::mem::replace(&mut *state, BuilderState::Consumed) {
            BuilderState::Requesting(builder) => builder,
            other => {
                *state = other;
                return set_err(err, SIA_ERR_INVALID_STATE, "no connection request");
            }
        };
        match block_on(cancel, builder.wait_for_approval()) {
            None => set_cancelled(err),
            Some(Ok(approved)) => {
                *state = BuilderState::Approved(approved);
                SIA_OK
            }
            Some(Err(e)) => builder_error(err, e),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_builder_register(
    b: *mut FfiBuilder,
    mnemonic: *const c_char,
    cancel: *mut CancellationToken,
    out: *mut *mut Sdk,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let phrase = match cstr(mnemonic) {
            Ok(s) => s,
            Err(e) => return set_err(err, SIA_ERR, format!("invalid mnemonic: {e}")),
        };
        let mut state = unsafe { (*b).0.lock() }.unwrap();
        let builder = match std::mem::replace(&mut *state, BuilderState::Consumed) {
            BuilderState::Approved(builder) => builder,
            other => {
                *state = other;
                return set_err(err, SIA_ERR_INVALID_STATE, "connection not approved");
            }
        };
        match block_on(cancel, builder.register(phrase)) {
            None => set_cancelled(err),
            Some(Ok(sdk)) => {
                unsafe { *out = Box::into_raw(Box::new(sdk)) }
                SIA_OK
            }
            Some(Err(e)) => builder_error(err, e),
        }
    })
}

// --- sdk -----------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_free(sdk: *mut Sdk) {
    if !sdk.is_null() {
        drop(unsafe { Box::from_raw(sdk) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_app_key(sdk: *const Sdk, out: *mut u8) {
    let seed = unsafe { &*sdk }.app_key().export();
    unsafe { std::slice::from_raw_parts_mut(out, 32) }.copy_from_slice(&seed);
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_account(
    sdk: *const Sdk,
    cancel: *mut CancellationToken,
    out_json: *mut *mut c_char,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        match block_on(cancel, sdk.account()) {
            None => set_cancelled(err),
            Some(Ok(account)) => match serde_json::to_string(&account) {
                Ok(js) => {
                    unsafe { *out_json = CString::new(js).unwrap_or_default().into_raw() }
                    SIA_OK
                }
                Err(e) => set_err(err, SIA_ERR, e.to_string()),
            },
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_object(
    sdk: *const Sdk,
    id: *const u8,
    cancel: *mut CancellationToken,
    out: *mut *mut Object,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let key = hash_from_ptr(id);
        match block_on(cancel, sdk.object(&key)) {
            None => set_cancelled(err),
            Some(Ok(obj)) => {
                unsafe { *out = Box::into_raw(Box::new(obj)) }
                SIA_OK
            }
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_object_events(
    sdk: *const Sdk,
    has_cursor: bool,
    after_unix_us: i64,
    after_id: *const u8,
    limit: u64,
    cancel: *mut CancellationToken,
    out: *mut *mut FfiEvents,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let cursor = if has_cursor {
            let after = match sia_storage::DateTime::from_timestamp_micros(after_unix_us) {
                Some(t) => t,
                None => return set_err(err, SIA_ERR, "invalid cursor timestamp"),
            };
            Some(ObjectsCursor {
                after,
                id: hash_from_ptr(after_id),
            })
        } else {
            None
        };
        let limit = if limit > 0 { Some(limit as usize) } else { None };
        match block_on(cancel, sdk.object_events(cursor, limit)) {
            None => set_cancelled(err),
            Some(Ok(events)) => {
                let events = events
                    .into_iter()
                    .map(|ev| {
                        let mut id = [0u8; 32];
                        id.copy_from_slice(ev.id.as_ref());
                        FfiEvent {
                            id,
                            deleted: ev.deleted,
                            updated_at_us: ev.updated_at.timestamp_micros(),
                            object: ev.object.map(Box::new),
                        }
                    })
                    .collect();
                unsafe { *out = Box::into_raw(Box::new(FfiEvents(events))) }
                SIA_OK
            }
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_pin_object(
    sdk: *const Sdk,
    obj: *const Object,
    cancel: *mut CancellationToken,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let obj = unsafe { &*obj };
        match block_on(cancel, sdk.pin_object(obj)) {
            None => set_cancelled(err),
            Some(Ok(())) => SIA_OK,
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_update_object_metadata(
    sdk: *const Sdk,
    obj: *const Object,
    cancel: *mut CancellationToken,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let obj = unsafe { &*obj };
        match block_on(cancel, sdk.update_object_metadata(obj)) {
            None => set_cancelled(err),
            Some(Ok(())) => SIA_OK,
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_delete_object(
    sdk: *const Sdk,
    id: *const u8,
    cancel: *mut CancellationToken,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let key = hash_from_ptr(id);
        match block_on(cancel, sdk.delete_object(&key)) {
            None => set_cancelled(err),
            Some(Ok(())) => SIA_OK,
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_prune_slabs(
    sdk: *const Sdk,
    cancel: *mut CancellationToken,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        match block_on(cancel, sdk.prune_slabs()) {
            None => set_cancelled(err),
            Some(Ok(())) => SIA_OK,
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_share_object(
    sdk: *const Sdk,
    obj: *const Object,
    valid_until_unix_us: i64,
    out_url: *mut *mut c_char,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let obj = unsafe { &*obj };
        let valid_until = match sia_storage::DateTime::from_timestamp_micros(valid_until_unix_us) {
            Some(t) => t,
            None => return set_err(err, SIA_ERR, "invalid expiration timestamp"),
        };
        match sdk.share_object(obj, valid_until) {
            Ok(url) => {
                unsafe {
                    *out_url = CString::new(url.as_str()).unwrap_or_default().into_raw();
                }
                SIA_OK
            }
            Err(e) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_sdk_shared_object(
    sdk: *const Sdk,
    share_url: *const c_char,
    cancel: *mut CancellationToken,
    out: *mut *mut Object,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let url = match cstr(share_url) {
            Ok(s) => s,
            Err(e) => return set_err(err, SIA_ERR, format!("invalid share url: {e}")),
        };
        match block_on(cancel, sdk.shared_object(url)) {
            None => set_cancelled(err),
            Some(Ok(obj)) => {
                unsafe { *out = Box::into_raw(Box::new(obj)) }
                SIA_OK
            }
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

// --- object --------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_new() -> *mut Object {
    Box::into_raw(Box::new(Object::default()))
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_free(o: *mut Object) {
    if !o.is_null() {
        drop(unsafe { Box::from_raw(o) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_id(o: *const Object, out: *mut u8) {
    let id = unsafe { &*o }.id();
    unsafe { std::slice::from_raw_parts_mut(out, 32) }.copy_from_slice(id.as_ref());
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_size(o: *const Object) -> u64 {
    unsafe { &*o }.size()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_encoded_size(o: *const Object) -> u64 {
    unsafe { &*o }.encoded_size()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_created_at(o: *const Object) -> i64 {
    unsafe { &*o }.created_at().timestamp_micros()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_updated_at(o: *const Object) -> i64 {
    unsafe { &*o }.updated_at().timestamp_micros()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_metadata(o: *const Object, buf: *mut u8, cap: usize) -> usize {
    let meta = &unsafe { &*o }.metadata;
    if !buf.is_null() && cap >= meta.len() {
        unsafe { std::slice::from_raw_parts_mut(buf, meta.len()) }.copy_from_slice(meta);
    }
    meta.len()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_set_metadata(o: *mut Object, data: *const u8, len: usize) {
    let meta = if data.is_null() || len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    };
    unsafe { &mut *o }.metadata = meta;
}

// --- object events -------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_events_len(evs: *const FfiEvents) -> usize {
    unsafe { &*evs }.0.len()
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_events_at(
    evs: *mut FfiEvents,
    i: usize,
    id_out: *mut u8,
    deleted: *mut bool,
    updated_at_unix_us: *mut i64,
    obj: *mut *mut Object,
) {
    let ev = &mut unsafe { &mut *evs }.0[i];
    unsafe {
        std::slice::from_raw_parts_mut(id_out, 32).copy_from_slice(&ev.id);
        *deleted = ev.deleted;
        *updated_at_unix_us = ev.updated_at_us;
        *obj = match ev.object.take() {
            Some(o) => Box::into_raw(o),
            None => std::ptr::null_mut(),
        };
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_events_free(evs: *mut FfiEvents) {
    if !evs.is_null() {
        drop(unsafe { Box::from_raw(evs) });
    }
}

// --- upload --------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_upload_start(
    sdk: *const Sdk,
    obj: *const Object,
    opts: *const UploadOptionsC,
    out: *mut *mut FfiUpload,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk }.clone();
        let obj = unsafe { &*obj }.clone();
        let options = make_upload_options(unsafe { &*opts });
        if let Err(e) = options.validate() {
            return set_err(err, SIA_ERR, e.to_string());
        }
        start_upload(out, err, move |reader| async move {
            sdk.upload(obj, reader, options)
                .await
                .map_err(|e| e.to_string())
        })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_upload_write(
    up: *mut FfiUpload,
    data: *const u8,
    len: usize,
    cancel: *mut CancellationToken,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let up = unsafe { &mut *up };
        let Some(writer) = up.writer.as_mut() else {
            return set_err(err, SIA_ERR_INVALID_STATE, "upload already finished");
        };
        let buf = unsafe { std::slice::from_raw_parts(data, len) };
        match block_on(cancel, writer.write_all(buf)) {
            None => set_cancelled(err),
            Some(Ok(())) => SIA_OK,
            Some(Err(_)) => {
                // The pipe broke because the upload task ended; report its
                // real error instead of the write failure.
                up.writer = None;
                match up.task.take() {
                    Some(task) => {
                        let mut out = std::ptr::null_mut();
                        let code = finish_upload_task(task, cancel, &mut out, err);
                        if code == SIA_OK {
                            // Upload completed early without consuming all
                            // data; treat as an error to avoid silent loss.
                            unsafe { sia_object_free(out) }
                            return set_err(err, SIA_ERR, "upload ended before all data was written");
                        }
                        code
                    }
                    None => set_err(err, SIA_ERR, "upload task already consumed"),
                }
            }
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_upload_finish(
    up: *mut FfiUpload,
    cancel: *mut CancellationToken,
    out: *mut *mut Object,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let up = unsafe { &mut *up };
        drop(up.writer.take()); // signal EOF
        match up.task.take() {
            Some(task) => finish_upload_task(task, cancel, out, err),
            None => set_err(err, SIA_ERR_INVALID_STATE, "upload already finished"),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_upload_free(up: *mut FfiUpload) {
    if up.is_null() {
        return;
    }
    let up = unsafe { Box::from_raw(up) };
    if let Some(task) = &up.task {
        task.abort();
    }
}

// --- download ------------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_download_start(
    sdk: *const Sdk,
    obj: *const Object,
    opts: *const DownloadOptionsC,
    out: *mut *mut FfiDownload,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let obj = unsafe { &*obj };
        let options = make_download_options(unsafe { &*opts });
        // Download::new spawns tasks; enter the runtime context for the call.
        let _guard = runtime().enter();
        match sdk.download(obj, options) {
            Ok(dl) => start_download(Box::pin(dl), out),
            Err(e) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_download_read(
    dl: *mut FfiDownload,
    buf: *mut u8,
    cap: usize,
    cancel: *mut CancellationToken,
    n: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let dl = unsafe { &mut *dl };
        if let Some(e) = dl.pending_err.take() {
            return set_err(err, SIA_ERR, e.to_string());
        }
        let buf = unsafe { std::slice::from_raw_parts_mut(buf, cap) };
        let reader = &mut dl.reader;
        let result = block_on(cancel, async {
            // Block for the first byte, then drain whatever is immediately
            // available to amortize the FFI crossing over large reads.
            let first = reader.read(buf).await?;
            if first == 0 || first == buf.len() {
                return Ok((first, None));
            }
            let mut total = first;
            let mut pending_err = None;
            std::future::poll_fn(|cx| {
                while total < buf.len() {
                    let mut rb = ReadBuf::new(&mut buf[total..]);
                    match reader.as_mut().poll_read(cx, &mut rb) {
                        Poll::Ready(Ok(())) => {
                            let filled = rb.filled().len();
                            if filled == 0 {
                                break; // EOF; surfaced by the next read call
                            }
                            total += filled;
                        }
                        Poll::Ready(Err(e)) => {
                            pending_err = Some(e);
                            break;
                        }
                        Poll::Pending => break,
                    }
                }
                Poll::Ready(())
            })
            .await;
            Ok::<_, std::io::Error>((total, pending_err))
        });
        match result {
            None => set_cancelled(err),
            Some(Ok((total, pending_err))) => {
                dl.pending_err = pending_err;
                unsafe { *n = total }
                SIA_OK
            }
            Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_download_free(dl: *mut FfiDownload) {
    if !dl.is_null() {
        drop(unsafe { Box::from_raw(dl) });
    }
}

// --- packed upload ---------------------------------------------------------------

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_start(
    sdk: *const Sdk,
    opts: *const UploadOptionsC,
    out: *mut *mut FfiPacked,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let sdk = unsafe { &*sdk };
        let options = make_upload_options(unsafe { &*opts });
        let _guard = runtime().enter();
        match sdk.upload_packed(options) {
            Ok(packed) => start_packed(packed, out),
            Err(e) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_remaining(up: *const FfiPacked) -> u64 {
    let up = unsafe { &*up };
    runtime().block_on(async {
        up.inner
            .lock()
            .await
            .as_ref()
            .map(|p| p.remaining())
            .unwrap_or(0)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_length(up: *const FfiPacked) -> u64 {
    let up = unsafe { &*up };
    runtime().block_on(async {
        up.inner
            .lock()
            .await
            .as_ref()
            .map(|p| p.length())
            .unwrap_or(0)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_optimal_data_size(up: *const FfiPacked) -> u64 {
    unsafe { &*up }.optimal_data_size
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_add_begin(
    up: *mut FfiPacked,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let up = unsafe { &mut *up };
        if up.writer.is_some() || up.add_task.is_some() {
            return set_err(err, SIA_ERR_INVALID_STATE, "an add is already in progress");
        }
        let (writer, reader) = tokio::io::duplex(UPLOAD_PIPE_CAPACITY);
        let inner = up.inner.clone();
        let task = runtime().spawn(async move {
            let mut guard = inner.lock().await;
            let packed = guard
                .as_mut()
                .ok_or_else(|| "upload already finalized".to_string())?;
            packed.add(reader).await.map_err(|e| e.to_string())
        });
        up.writer = Some(writer);
        up.add_task = Some(task);
        SIA_OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_add_write(
    up: *mut FfiPacked,
    data: *const u8,
    len: usize,
    cancel: *mut CancellationToken,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let up = unsafe { &mut *up };
        let Some(writer) = up.writer.as_mut() else {
            return set_err(err, SIA_ERR_INVALID_STATE, "no add in progress");
        };
        let buf = unsafe { std::slice::from_raw_parts(data, len) };
        match block_on(cancel, writer.write_all(buf)) {
            None => set_cancelled(err),
            Some(Ok(())) => SIA_OK,
            Some(Err(_)) => {
                up.writer = None;
                match up.add_task.take() {
                    Some(task) => match block_on(cancel, task) {
                        None => set_cancelled(err),
                        Some(Ok(Ok(_))) => {
                            set_err(err, SIA_ERR, "add ended before all data was written")
                        }
                        Some(Ok(Err(msg))) => set_err(err, SIA_ERR, msg),
                        Some(Err(e)) => set_err(err, SIA_ERR, e.to_string()),
                    },
                    None => set_err(err, SIA_ERR, "add task already consumed"),
                }
            }
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_add_finish(
    up: *mut FfiPacked,
    cancel: *mut CancellationToken,
    written: *mut u64,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let up = unsafe { &mut *up };
        drop(up.writer.take()); // signal EOF for this object
        match up.add_task.take() {
            Some(task) => match block_on(cancel, task) {
                None => set_cancelled(err),
                Some(Ok(Ok(n))) => {
                    unsafe { *written = n }
                    SIA_OK
                }
                Some(Ok(Err(msg))) => set_err(err, SIA_ERR, msg),
                Some(Err(join_err)) if join_err.is_cancelled() => set_cancelled(err),
                Some(Err(join_err)) => set_err(err, SIA_ERR, join_err.to_string()),
            },
            None => set_err(err, SIA_ERR_INVALID_STATE, "no add in progress"),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_finalize(
    up: *mut FfiPacked,
    cancel: *mut CancellationToken,
    out_objs: *mut *mut *mut Object,
    out_len: *mut usize,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let up = unsafe { &mut *up };
        if up.writer.is_some() || up.add_task.is_some() {
            return set_err(err, SIA_ERR_INVALID_STATE, "an add is still in progress");
        }
        let inner = up.inner.clone();
        let result = block_on(cancel, async move {
            let packed = inner
                .lock()
                .await
                .take()
                .ok_or_else(|| "upload already finalized".to_string())?;
            packed.finalize().await.map_err(|e| e.to_string())
        });
        match result {
            None => set_cancelled(err),
            Some(Ok(objects)) => {
                let ptrs: Vec<*mut Object> = objects
                    .into_iter()
                    .map(|o| Box::into_raw(Box::new(o)))
                    .collect();
                let mut ptrs = ptrs.into_boxed_slice();
                unsafe {
                    *out_len = ptrs.len();
                    *out_objs = ptrs.as_mut_ptr();
                }
                std::mem::forget(ptrs);
                SIA_OK
            }
            Some(Err(msg)) => set_err(err, SIA_ERR, msg),
        }
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_object_array_free(objs: *mut *mut Object, len: usize) {
    if !objs.is_null() {
        drop(unsafe { Box::from_raw(std::ptr::slice_from_raw_parts_mut(objs, len)) });
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_packed_upload_free(up: *mut FfiPacked) {
    if up.is_null() {
        return;
    }
    let up = unsafe { Box::from_raw(up) };
    if let Some(task) = &up.add_task {
        task.abort();
    }
}

// --- mock ------------------------------------------------------------------------
//
// Compiled only with the `mock` cargo feature, which swaps sia_storage's host
// transport for an in-memory one crate-wide. Test builds only.

#[cfg(feature = "mock")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_mock_new(num_hosts: usize, app_key: *const u8) -> *mut FfiMock {
    use sia_core::signing::PrivateKey;
    use sia_core::types::v2::NetAddress;

    let hosts = sia_storage::mock::MockHosts::new();
    let host_list: Vec<sia_storage::Host> = (0..num_hosts)
        .map(|i| {
            let mut seed = [0u8; 32];
            seed[..8].copy_from_slice(&(i as u64 + 1).to_le_bytes());
            sia_storage::Host {
                public_key: PrivateKey::from_seed(&seed).public_key(),
                addresses: vec![NetAddress {
                    protocol: sia_storage::Protocol::QUIC,
                    address: "localhost:1234".to_string(),
                }],
                country_code: "US".to_string(),
                latitude: 0.0,
                longitude: 0.0,
                good_for_upload: true,
            }
        })
        .collect();
    hosts.update(host_list, true);
    Box::into_raw(Box::new(FfiMock {
        hosts,
        app_key: Arc::new(app_key_from_ptr(app_key)),
    }))
}

#[cfg(feature = "mock")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_mock_free(m: *mut FfiMock) {
    if !m.is_null() {
        drop(unsafe { Box::from_raw(m) });
    }
}

#[cfg(feature = "mock")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_mock_upload_start(
    m: *const FfiMock,
    obj: *const Object,
    opts: *const UploadOptionsC,
    out: *mut *mut FfiUpload,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let m = unsafe { &*m };
        let uploader = sia_storage::mock::MockUploader::new(m.hosts.clone(), m.app_key.clone());
        let obj = unsafe { &*obj }.clone();
        let options = make_upload_options(unsafe { &*opts });
        if let Err(e) = options.validate() {
            return set_err(err, SIA_ERR, e.to_string());
        }
        start_upload(out, err, move |reader| async move {
            uploader
                .upload(obj, reader, options)
                .await
                .map_err(|e| e.to_string())
        })
    })
}

#[cfg(feature = "mock")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_mock_download_start(
    m: *const FfiMock,
    obj: *const Object,
    opts: *const DownloadOptionsC,
    out: *mut *mut FfiDownload,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let m = unsafe { &*m };
        let hosts = m.hosts.clone();
        let app_key = m.app_key.clone();
        let obj = unsafe { &*obj }.clone();
        let options = make_download_options(unsafe { &*opts });
        // MockDownloader::download's return type captures &self, so it can't
        // be boxed as 'static; run the downloader inside a task that pipes
        // into a duplex. Test-only path, extra copy is fine.
        let (mut writer, inner) = tokio::io::duplex(UPLOAD_PIPE_CAPACITY);
        let err_slot = Arc::new(Mutex::new(None));
        let task_err = err_slot.clone();
        let (started_tx, started_rx) = tokio::sync::oneshot::channel();
        runtime().spawn(async move {
            let downloader = sia_storage::mock::MockDownloader::new(hosts, app_key);
            match downloader.download(&obj, options) {
                Ok(mut dl) => {
                    let _ = started_tx.send(Ok(()));
                    if let Err(e) = tokio::io::copy(&mut dl, &mut writer).await {
                        *task_err.lock().unwrap() = Some(e);
                    }
                }
                Err(e) => {
                    let _ = started_tx.send(Err(e.to_string()));
                }
            }
        });
        match runtime().block_on(started_rx) {
            Ok(Ok(())) => start_download(
                Box::pin(PipeReader {
                    inner,
                    err: err_slot,
                }),
                out,
            ),
            Ok(Err(msg)) => set_err(err, SIA_ERR, msg),
            Err(_) => set_err(err, SIA_ERR, "mock download task died"),
        }
    })
}

#[cfg(feature = "mock")]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sia_mock_packed_upload_start(
    m: *const FfiMock,
    opts: *const UploadOptionsC,
    out: *mut *mut FfiPacked,
    err: *mut *mut c_char,
) -> i32 {
    guarded(err, || {
        let m = unsafe { &*m };
        let uploader = sia_storage::mock::MockUploader::new(m.hosts.clone(), m.app_key.clone());
        let options = make_upload_options(unsafe { &*opts });
        let _guard = runtime().enter();
        match uploader.upload_packed(options) {
            Ok(packed) => start_packed(packed, out),
            Err(e) => set_err(err, SIA_ERR, e.to_string()),
        }
    })
}
