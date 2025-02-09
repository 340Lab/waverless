use std::hash::Hash;
use std::sync::Arc;
use std::ops::Deref;
use dashmap::DashMap;
use tokio::sync::broadcast;
use thiserror::Error;

/// AsyncInitMap 的错误类型
#[derive(Debug, Error)]
pub enum AsyncInitError {
    /// 等待初始化完成时发生错误
    #[error("等待初始化完成时发生错误: {0}")]
    WaitError(broadcast::error::RecvError),
}

/// Map 值的状态
#[derive(Clone)]
enum ValueState<V> {
    /// 正在初始化，包含一个通知 channel
    Initializing(broadcast::Sender<V>),
    /// 初始化完成，包含实际值
    Ready(V),
}

impl<V> ValueState<V> {
    /// 获取就绪值的引用
    fn as_ready(&self) -> Option<&V> {
        match self {
            Self::Ready(v) => Some(v),
            _ => None,
        }
    }

    /// 获取初始化中的 sender
    fn as_initializing(&self) -> Option<&broadcast::Sender<V>> {
        match self {
            Self::Initializing(tx) => Some(tx),
            _ => None,
        }
    }

    /// 是否已经就绪
    fn is_ready(&self) -> bool {
        matches!(self, Self::Ready(_))
    }

    /// 是否正在初始化
    fn is_initializing(&self) -> bool {
        matches!(self, Self::Initializing(_))
    }
}

/// 支持异步初始化的并发 Map
pub struct AsyncInitMap<K, V> 
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync+'static,
{
    inner: Arc<DashMap<K, ValueState<V>>>,
}

impl<K, V> AsyncInitMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync+'static,
{
    /// 创建新的异步初始化 Map
    pub fn new() -> Self {
        Self {
            inner: Arc::new(DashMap::new()),
        }
    }

    /// 获取或初始化一个值
    /// 
    /// # 参数
    /// * `key` - 键
    /// * `init_fut` - 初始化 Future
    /// 
    /// # 返回
    /// 返回初始化完成的值，如果初始化失败则返回错误
    pub async fn get_or_init<Fut>(&self, key: K, init_fut: Fut) -> Result<V, AsyncInitError>
    where
        Fut: std::future::Future<Output = Result<V, AsyncInitError>> + Send + 'static,
    {
        // 先尝试只读获取
        if let Some(entry) = self.inner.get(&key) {
            match entry.value() {
                ValueState::Ready(v) => return Ok(v.clone()),
                ValueState::Initializing(tx) => {
                    let mut rx = tx.subscribe();
                    drop(entry);
                    return Ok(rx.recv().await.map_err(AsyncInitError::WaitError)?);
                }
            }
        }
        
        // 使用 or_insert_with 进行原子操作并获取 rx
        let mut rx = {
            let entry = self.inner.entry(key.clone()).or_insert_with(|| {
                let (tx, _) = broadcast::channel(1);
                let tx_clone = tx.clone();
                
                let inner = self.inner.clone();
                let key = key.clone();
                
                let _ = tokio::spawn(async move {
                    match init_fut.await {
                        Ok(value) => {
                            // 先通过 channel 发送值
                            let _ = tx.send(value.clone());
                            // 然后更新状态
                            inner.insert(key, ValueState::Ready(value));
                        }
                        Err(e) => {
                            inner.remove(&key);
                            tracing::error!("初始化失败: {:?}", e);
                            drop(tx); // 关闭 channel 通知错误
                        }
                    }
                });
                
                ValueState::Initializing(tx_clone)
            });

            entry.value().as_initializing()
                .expect("刚插入的值必定处于初始化状态")
                .subscribe()
        };
        
        // 等待值通过 channel 传递
        Ok(rx.recv().await.map_err(AsyncInitError::WaitError)?)
    }
}

impl<K, V> Default for AsyncInitMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync+'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<K, V> Clone for AsyncInitMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync+'static,
{
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<K, V> Deref for AsyncInitMap<K, V>
where
    K: Eq + Hash + Clone + Send + Sync + 'static,
    V: Clone + Send + Sync+'static,
{
    type Target = DashMap<K, ValueState<V>>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
