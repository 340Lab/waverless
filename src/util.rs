use std::{
    fmt::Debug,
    future::Future,
    pin::Pin,
    ptr::NonNull,
    task::{Context, Poll},
};

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

pub struct StrUnsafeRef(usize, usize);

impl StrUnsafeRef {
    pub fn new(str: &str) -> StrUnsafeRef {
        StrUnsafeRef(str.as_ptr() as usize, str.len())
    }
    pub fn str<'a>(&self) -> &'a str {
        // tracing::info!("unsafe str ref");
        let res =
            std::str::from_utf8(unsafe { std::slice::from_raw_parts(self.0 as *const u8, self.1) })
                .unwrap();
        // tracing::info!("unsafe str ref get");
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
    unsafe { &mut *(arc as *const T as *mut T) }
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
