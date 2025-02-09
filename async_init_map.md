# AsyncInitConcurrentMap 封装（基于dashmap）

## 设计动机

在 Rust 异步编程中，我们经常遇到这样的场景：需要一个并发 Map，同时要支持异步初始化。

### 现有方案的问题

1. **DashMap 的 or_insert 限制**：
```rust
// DashMap 的 or_insert_with 是同步的
map.entry(key).or_insert_with(|| {
    // 这里不能直接用 async 函数
    // 如果在这里调用 block_on 会导致严重问题
});
```

2. **同步调用异步的问题**：
   - 如果在同步上下文中调用异步函数（如使用 block_on）
   - 当前线程会被阻塞
   - 导致其他异步任务无法调度
   - 可能引发死锁

### 解决方案

我们的方案是将异步初始化逻辑从 entry 的回调中分离出来：

```rust
// 不在 or_insert_with 中执行异步初始化
let entry = map.entry(key).or_insert_with(|| {
    // 只创建初始状态
    ValueState::Initializing(tx)
});

// 在单独的异步任务中执行初始化
tokio::spawn(async move {
    // 这里可以安全地执行异步操作
    match init_fut.await {
        Ok(value) => {
            let _ = tx.send(value.clone());  // 先发送值
            inner.insert(key, ValueState::Ready(value));  // 再更新状态
        }
        Err(e) => {
            inner.remove(&key);
            drop(tx);  // 通知错误
        }
    }
});
```

## 核心实现

### 状态管理

**设计原因**：
- 使用枚举保证状态转换的类型安全
- 将通知 channel 绑定到初始化状态，确保生命周期正确
- 避免使用额外的标志位，保持内存效率

```rust
enum ValueState<V> {
    Initializing(broadcast::Sender<V>),  // channel 直接传递值
    Ready(V),
}
```

**关键细节**：
- `Initializing` 持有 `broadcast::Sender` 而不是 `oneshot`，支持多个等待者
- `Ready` 直接持有值，避免额外的引用计数
- 枚举设计使得状态检查在编译时完成

### 读写分离设计

**设计原因**：
- 读操作应该尽可能快速且无阻塞
- 写操作需要保证原子性，但要最小化锁持有时间
- 异步等待不能持有任何锁

1. **快速路径（读）**：
```rust
if let Some(entry) = self.inner.get(&key) {  // 只获取读锁
    match entry.value() {
        ValueState::Ready(v) => return Ok(v.clone()),
        ValueState::Initializing(tx) => {
            let mut rx = tx.subscribe();
            drop(entry);  // 立即释放读锁
            return Ok(rx.recv().await?);
        }
    }
}
```

**关键细节**：
- 使用 `get()` 而不是 `entry()`，避免不必要的写锁
- 获取 subscriber 后立即释放锁，允许其他读者访问
- 值的克隆在锁外进行，最小化锁持有时间

2. **初始化路径（写）**：
```rust
let mut rx = {  // 使用代码块控制写锁范围
    let entry = self.inner.entry(key.clone()).or_insert_with(|| {
        let (tx, _) = broadcast::channel(1);
        // 启动异步初始化...
        ValueState::Initializing(tx_clone)
    });
    entry.value().as_initializing()
        .expect("刚插入的值必定处于初始化状态")
        .subscribe()
};  // 写锁在这里释放
```

**关键细节**：
- 使用代码块限制 entry 的生命周期，确保写锁及时释放
- `or_insert_with` 保证检查和插入的原子性
- 初始化任务在获取 subscriber 后启动，避免竞态条件

### 通过 Channel 传递值

**设计原因**：
- 直接通过 channel 传递值，避免等待者重新查询 map
- broadcast channel 支持多个等待者同时等待初始化结果
- 错误处理更简单，关闭 channel 即可通知所有等待者

```rust
// 优化后的设计
enum ValueState<V> {
    Initializing(broadcast::Sender<V>),  // channel 直接传递值
    Ready(V),
}

// 初始化完成时
match init_fut.await {
    Ok(value) => {
        let _ = tx.send(value.clone());  // 先发送值
        inner.insert(key, ValueState::Ready(value));  // 再更新状态
    }
    // ...
}

// 等待初始化时
let mut rx = tx.subscribe();
drop(entry);
return Ok(rx.recv().await?);  // 直接从 channel 获取值，无需再查询 map
```

**关键细节**：
- 等待者直接从 channel 接收值，无需再次获取锁查询 map
- 使用 broadcast channel 支持多个等待者，而不是 oneshot
- channel 容量为 1 即可，因为只需要传递一次初始化结果
- 初始化失败时，直接关闭 channel 通知所有等待者，简化错误处理
