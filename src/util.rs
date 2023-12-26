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
