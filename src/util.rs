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
