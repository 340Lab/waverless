pub mod container;
pub mod zip;

use std::{
    fmt::Debug,
    future::Future,
    ops::{Deref, DerefMut, Drop, Range},
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

use parking_lot::MutexGuard;
use rand::{thread_rng, Rng};
#[cfg(test)]
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, Layer};

#[cfg(test)]
pub fn test_tracing_start() {
    let my_filter = tracing_subscriber::filter::filter_fn(|v| {
        // println!("{}", v.module_path().unwrap());
        // println!("{}", v.name());
        // if v.module_path().unwrap().contains("quinn_proto") {
        //     return false;
        // }

        // if v.module_path().unwrap().contains("qp2p::wire_msg") {
        //     return false;
        // }

        // println!("{}", v.target());
        if let Some(mp) = v.module_path() {
            if mp.contains("async_raft") {
                return false;
            }
            if mp.contains("hyper") {
                return false;
            }
        }

        // if v.module_path().unwrap().contains("less::network::p2p") {
        //     return false;
        // }

        // v.level() == &tracing::Level::ERROR
        //     || v.level() == &tracing::Level::WARN
        //     || v.level() == &tracing::Level::INFO
        v.level() != &tracing::Level::TRACE
        // v.level() == &tracing::Level::INFO
        // true
    });
    let my_layer = tracing_subscriber::fmt::layer();
    let _ = tracing_subscriber::registry()
        .with(my_layer.with_filter(my_filter))
        .try_init();
}

pub enum JoinHandleWrapper {
    Task(Option<tokio::task::JoinHandle<()>>),
    Thread(Option<std::thread::JoinHandle<()>>),
}

impl From<tokio::task::JoinHandle<()>> for JoinHandleWrapper {
    fn from(handle: tokio::task::JoinHandle<()>) -> Self {
        Self::Task(handle.into())
    }
}

impl From<std::thread::JoinHandle<()>> for JoinHandleWrapper {
    fn from(handle: std::thread::JoinHandle<()>) -> Self {
        Self::Thread(handle.into())
    }
}

// TODO: remove unwrap
impl JoinHandleWrapper {
    pub async fn join(&mut self) {
        match self {
            Self::Task(handle) => handle.take().unwrap().await.unwrap(),
            Self::Thread(handle) => {
                let handle = handle.take().unwrap();
                tokio::task::spawn_blocking(|| handle.join().unwrap())
                    .await
                    .unwrap()
            }
        }
    }
}

pub enum WithBind<'a, T> {
    MutexGuard(MutexGuard<'a, T>),
    MutexGuardOpt(MutexGuard<'a, Option<T>>),
}

impl<T> WithBind<'_, T> {
    pub fn option_mut(&mut self) -> &mut Option<T> {
        match self {
            Self::MutexGuard(_) => unreachable!(),
            Self::MutexGuardOpt(g) => &mut *g,
        }
    }
}

impl<T> Deref for WithBind<'_, T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::MutexGuard(g) => g,
            Self::MutexGuardOpt(g) => g.as_ref().unwrap(),
        }
    }
}

impl<T> DerefMut for WithBind<'_, T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        match self {
            Self::MutexGuard(g) => g,
            Self::MutexGuardOpt(g) => g.as_mut().unwrap(),
        }
    }
}

pub struct StrUnsafeRef(usize, usize);

impl StrUnsafeRef {
    pub fn new(str: &str) -> StrUnsafeRef {
        StrUnsafeRef(str.as_ptr() as usize, str.len())
    }
    pub fn str<'a>(&self) -> &'a str {
        #[cfg(feature = "unsafe-log")]
        tracing::debug!("unsafe str ref");

        let res =
            std::str::from_utf8(unsafe { std::slice::from_raw_parts(self.0 as *const u8, self.1) })
                .unwrap();
        #[cfg(feature = "unsafe-log")]
        tracing::debug!("unsafe str ref get");
        res
    }
}

pub struct TryUtf8VecU8(pub Vec<u8>);

impl Debug for TryUtf8VecU8 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let res = std::str::from_utf8(&self.0);
        match res {
            Ok(str) => write!(f, "{}", str),
            Err(_) => write!(f, "{:?}", &self.0),
        }
    }
}

pub unsafe fn unsafe_mut<T>(arc: &T) -> &mut T {
    #[cfg(feature = "unsafe-log")]
    tracing::debug!("unsafe_mut begin");
    let res = unsafe { &mut *(arc as *const T as *mut T) };
    #[cfg(feature = "unsafe-log")]
    tracing::debug!("unsafe_mut end");
    res
}

struct FutureWrapper<F: Future> {
    fut: Pin<Box<F>>,
}

impl<F> std::future::Future for FutureWrapper<F>
where
    F: Future,
{
    type Output = F::Output;

    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.fut.as_mut().poll(cx)
    }
}

unsafe impl<F> Send for FutureWrapper<F> where F: Future {}

pub struct SendNonNull<T>(pub NonNull<T>);
unsafe impl<T> Send for SendNonNull<T> {}

pub fn call_async_from_sync<Fut>(fut: Fut) -> Fut::Output
where
    Fut: std::future::Future + 'static,
    Fut::Output: Send + 'static,
{
    let (tx, rx) = tokio::sync::oneshot::channel();
    let fut = async move {
        match tx.send(fut.await) {
            Ok(res) => res,
            Err(_) => {
                panic!("oneshot channel closed which should not happen")
            }
        }
    };
    let _ = tokio::task::spawn(FutureWrapper { fut: Box::pin(fut) });

    rx.blocking_recv().unwrap()
}

pub unsafe fn non_null<T>(v: &T) -> NonNull<T> {
    #[cfg(feature = "unsafe-log")]
    tracing::debug!("non_null");
    let ptr = v as *const T as *mut T;
    let non_null = NonNull::new_unchecked(ptr);
    non_null
}

pub struct DropDebug<T> {
    tag: String,
    rand: i32,
    pub _t: T,
}

impl<T> DropDebug<T> {
    pub fn new(tag: String, t: T) -> Self {
        let rand = thread_rng().gen_range(0..100000000);
        tracing::debug!("tracked new {} [{}]", tag, rand);
        Self { tag, _t: t, rand }
    }
}

impl<T> Drop for DropDebug<T> {
    fn drop(&mut self) {
        tracing::debug!("tracked drop {} [{}]", self.tag, self.rand);
    }
}

pub enum VecOrSlice<'a, T> {
    Vec(Vec<T>),
    Slice(&'a [T]),
}

impl<T> From<Vec<T>> for VecOrSlice<'_, T> {
    fn from(v: Vec<T>) -> Self {
        Self::Vec(v)
    }
}

impl<'a, T> From<&'a [T]> for VecOrSlice<'a, T> {
    fn from(v: &'a [T]) -> Self {
        Self::Slice(v)
    }
}

pub trait VecExt<T> {
    fn limit_range_debug(&self, range: Range<usize>) -> String;
}

impl<T: Debug> VecExt<T> for Vec<T> {
    fn limit_range_debug(&self, range: Range<usize>) -> String {
        if self.len() >= range.end {
            format!("{:?}", &self[range])
        } else {
            format!(
                "{:?}, hide len:{}",
                &self[range.start..],
                self.len() - range.end
            )
        }
    }
}
