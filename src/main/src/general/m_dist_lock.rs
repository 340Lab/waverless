use std::collections::HashMap;
use std::collections::HashSet;
use std::fmt;
use std::fmt::Debug;
use std::hash::DefaultHasher;
use std::hash::Hash;
use std::hash::Hasher;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use crate::sys::LogicalModule;
use crate::sys::LogicalModulesRef;
use crate::util::DropDebug;
use crate::{
    logical_module_view_impl, result::WSResult, sys::LogicalModuleNewArgs, util::JoinHandleWrapper,
};
use axum::async_trait;
use parking_lot::Mutex;
use rand::thread_rng;
use rand::Rng;
use tokio::sync::Notify;
use tokio::sync::OwnedRwLockReadGuard;
use tokio::sync::OwnedRwLockWriteGuard;
use tokio::sync::RwLock;
use ws_derive::LogicalModule;

use super::network::m_p2p::P2PModule;
use super::network::m_p2p::RPCCaller;
use super::network::m_p2p::RPCHandler;
use super::network::m_p2p::RPCResponsor;
use super::network::proto;

logical_module_view_impl!(View);
logical_module_view_impl!(View, p2p, P2PModule);
logical_module_view_impl!(View, dist_lock, DistLock);

type LockReleaseId = u32;

/// https://fvd360f8oos.feishu.cn/wiki/ZUPNwpKLEiRs6Ukzf3ncVa9FnHe
/// 这个是对于某个key的锁的状态记录，包括读写锁的引用计数，以及等待释放的notify
/// 对于写锁，只有第一个人竞争往map里插入能拿到锁，后续的都得等notify，然后竞争往map里插入
/// 对于读锁，只要自增的初始值>=1即可，初始值为0意味着当前锁已经处于释放过程中（cnt减到0但可能还在map里）得等到map中的值被删掉
///    如何等map中的值被删掉：等到map中的值被删掉，再把wait_for_delete中的notify全部notify
///    有没有在删掉后，全部notify过了，又有用户往notify队列里插入，（使用Mutex<Option>来保证插入时，一定还没有notify）
struct LockState<Payload> {
    read_or_write: bool,
    cnt: AtomicUsize,
    pub payload: Mutex<Payload>,
    wait_for_delete: Mutex<Option<Arc<Notify>>>,
}
#[derive(Clone)]
struct LockStateShared<Payload>(Arc<LockState<Payload>>);
impl<Payload> LockStateShared<Payload> {
    fn new(read_or_write: bool, payload: Payload) -> LockStateShared<Payload> {
        LockStateShared(Arc::new(LockState {
            read_or_write,
            cnt: AtomicUsize::new(1),
            wait_for_delete: Mutex::new(Some(Arc::new(Notify::new()))),
            payload: Mutex::new(payload),
        }))
    }

    fn fetch_add(&self) -> bool {
        loop {
            let cnt = self.0.cnt.load(Ordering::SeqCst);
            if cnt == 0 {
                return false;
                // let _ = self.0.cnt.fetch_add(1, Ordering::SeqCst);
            }
            if self
                .0
                .cnt
                .compare_exchange(cnt, cnt + 1, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                return true;
            }
            continue;
        }
    }

    // return false means need to delete lock key
    fn fetch_sub(&self) -> bool {
        let bfsub = self.0.cnt.fetch_sub(1, Ordering::SeqCst);
        assert!(bfsub >= 1);
        bfsub > 1
    }
}

#[derive(LogicalModule)]
/// The distributed lock
pub struct DistLock {
    view: View,

    rpc_caller_kv_lock: RPCCaller<proto::kv::KvLockRequest>,
    // rpc_caller_kv_lock_wait_acquire_notify_request:
    //     RPCCaller<proto::kv::KvLockWaitAcquireNotifyRequest>,
    rpc_handler_kv_lock: RPCHandler<proto::kv::KvLockRequest>,
    // rpc_handler_kv_lock_wait_acquire_notify_request:
    //     RPCHandler<proto::kv::KvLockWaitAcquireNotifyRequest>,
    locks: RwLock<HashMap<Vec<u8>, LockStateShared<HashSet<LockReleaseId>>>>,
}

#[async_trait]
impl LogicalModule for DistLock {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        let view = View::new(args.logical_modules_ref.clone());
        Self {
            rpc_caller_kv_lock: RPCCaller::new(),
            rpc_handler_kv_lock: RPCHandler::new(),

            // rpc_caller_kv_lock_wait_acquire_notify_request: RPCCaller::new(),
            // rpc_handler_kv_lock_wait_acquire_notify_request: RPCHandler::new(),
            locks: RwLock::new(HashMap::new()), //dashmap::DashMap::new(),

            view,
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        // register rpc caller
        {
            self.rpc_caller_kv_lock.regist(self.view.p2p());
            // self.rpc_caller_kv_lock_wait_acquire_notify_request
            //     .regist(self.view.p2p());
        }

        // register rpc handler
        {
            let view = self.view.clone();
            self.rpc_handler_kv_lock.regist(
                self.view.p2p(),
                move |responser: RPCResponsor<proto::kv::KvLockRequest>,
                      req: proto::kv::KvLockRequest| {
                    tracing::debug!("handle kv lock once");
                    let view = view.clone();
                    let _ = tokio::spawn(async move {
                        if let Err(err) = view.handle_kv_lock_request(responser, req).await {
                            tracing::warn!("handle kv lock request failed: {:?}", err);
                        }
                    });
                    Ok(())
                },
            );
            // let view = self.view.clone();
            // self.rpc_handler_kv_lock_wait_acquire_notify_request.regist(
            //     self.view.p2p(),
            //     move |responser: RPCResponsor<proto::kv::KvLockWaitAcquireNotifyRequest>,
            //           req: proto::kv::KvLockWaitAcquireNotifyRequest| {
            //         let view = view.clone();
            //         tokio::spawn(async move {
            //             view.handle_kv_lock_wait_acquire_notify(responser, req)
            //                 .await;
            //         });
            //         Ok(())
            //     },
            // );
        }

        Ok(vec![])
    }
}

impl View {
    async fn wait_for_lock(
        &self,
        req: &proto::kv::KvLockRequest,
        read_or_write_tag: &str,
    ) -> LockStateShared<HashSet<LockReleaseId>> {
        loop {
            let mut insert_new = false;
            let lock: LockStateShared<HashSet<LockReleaseId>> = {
                let mut distlock_wr = DropDebug::new(
                    format!("prepare stage {} lock map", read_or_write_tag),
                    self.dist_lock().locks.write().await,
                );
                let entry = distlock_wr._t.entry(req.key.clone()).or_insert_with(|| {
                    insert_new = true;
                    LockStateShared::new(req.read_0_write_1_unlock_2 == 0, HashSet::new())
                });
                entry.clone()
            };

            async fn wait_for_next(lock: &LockStateShared<HashSet<LockReleaseId>>) {
                let adding_wait_opt = lock.0.wait_for_delete.lock().as_ref().map(|v| v.clone());
                if let Some(notify) = adding_wait_opt {
                    notify.notified().await;
                } else {
                    // already deleted, break loop and compete for next lock
                    return;
                }
                // if let Some(adding_wait_opt) = adding_wait_opt.as_mut() {
                //     let notify = adding_wait_opt.notified();
                // }
            }
            if !insert_new {
                // conflict lock type
                if (req.read_0_write_1_unlock_2 == 0 && !lock.0.read_or_write)
                    || (req.read_0_write_1_unlock_2 == 1 && lock.0.read_or_write)
                {
                    wait_for_next(&lock).await
                } else if req.read_0_write_1_unlock_2 == 0 {
                    if lock.fetch_add() {
                        // get the lock
                        return lock;
                    } else {
                        wait_for_next(&lock).await
                    }
                } else {
                    wait_for_next(&lock).await
                }
                tracing::debug!("wait_for_lock - wait for next lock");
            } else {
                tracing::debug!("wait_for_lock - insert new lock");
                return lock;
            }
        }
    }
    pub async fn handle_kv_lock_request(
        &self,
        responser: RPCResponsor<proto::kv::KvLockRequest>,
        req: proto::kv::KvLockRequest,
    ) -> WSResult<()> {
        match req.read_0_write_1_unlock_2 {
            // 0 => {
            //     tracing::debug!(
            //         "handle_kv_lock_request - read lock, handling node: {}, key: '{:?}'",
            //         self.p2p().nodes_config.this_node(),
            //         std::str::from_utf8(&req.key)
            //     );
            //     let lock = {
            //         let mut distlock_wr = DropDebug::new(
            //             "prepare stage read lock map".to_owned(),
            //             self.dist_lock().locks.write().await,
            //         );
            //         let entry = distlock_wr
            //             ._t
            //             .entry(req.key.clone())
            //             .or_insert((Arc::new(RwLock::new(())), None));
            //         entry.0.clone()
            //     };

            //     tracing::debug!("handle_kv_lock_request - read lock acquiring");
            //     let read_guard = lock.read_owned().await;
            //     tracing::debug!("handle_kv_lock_request - read lock acquired");

            //     // Here we have got the lock,
            //     let release_id = {
            //         tracing::debug!(
            //             "handle_kv_lock_request read - getting lock map to hold read guard"
            //         );
            //         let mut distlock_wr = DropDebug::new(
            //             "update stage read lock map".to_owned(),
            //             self.dist_lock().locks.write().await,
            //         );
            //         // let mut distlock_wr = self.dist_lock().locks.write();
            //         let lock = distlock_wr._t.get_mut(&req.key).unwrap();
            //         tracing::debug!(
            //             "handle_kv_lock_request read - g。ot lock map to hold read guard"
            //         );
            //         let release_id = thread_rng().gen_range(1..u32::MAX);
            //         let a = lock
            //             .1
            //             .replace((ReadOrWriteGuard::ReadGuard(read_guard), release_id));
            //         tracing::debug!("handle_kv_lock_request read - holded read guard");
            //         assert!(a.is_none());

            //         release_id
            //     };

            //     responser
            //         .send_resp(proto::kv::KvLockResponse {
            //             success: true,
            //             context: "locked".to_owned(),
            //             release_id,
            //         })
            //         .await?;
            //     tracing::debug!("handle_kv_lock_request read lock request returned");
            // }
            0 | 1 => {
                let read_or_write_tag = if req.read_0_write_1_unlock_2 == 0 {
                    "read"
                } else {
                    "write"
                };
                tracing::debug!(
                    "handle_kv_lock_request {} lock acquiring, handling node: {}, key: '{:?}'",
                    read_or_write_tag,
                    self.p2p().nodes_config.this_node(),
                    std::str::from_utf8(&req.key)
                );

                let lock = self.wait_for_lock(&req, read_or_write_tag).await;

                // Here we have got the lock, just regist for a new release id

                fn insert_release_id(
                    lock: &LockStateShared<HashSet<LockReleaseId>>,
                ) -> LockReleaseId {
                    // tracing::debug!("handle_kv_lock_request - inserting release id");
                    loop {
                        let release_id = thread_rng().gen_range(1..u32::MAX);
                        let mut locked_release_ids = lock.0.payload.lock();
                        if locked_release_ids.contains(&release_id) {
                            // tracing::debug!("handle_kv_lock_request - inserting release id retry");
                            continue;
                        }
                        let _ = locked_release_ids.insert(release_id);
                        // tracing::debug!("handle_kv_lock_request - inserted release id");
                        return release_id;
                    }
                }
                let release_id = insert_release_id(&lock);

                responser
                    .send_resp(proto::kv::KvLockResponse {
                        success: true,
                        context: "locked".to_owned(),
                        release_id,
                    })
                    .await?;
                tracing::debug!("handle_kv_lock_request write lock request returned");
            }
            2 => {
                tracing::debug!("handle_kv_lock_request unlocking");
                let mut fail_reason = None;
                {
                    let distlock_rd = DropDebug::new(
                        "prepare stage unlock map".to_owned(),
                        self.dist_lock().locks.read().await,
                    );
                    if let Some(lock_state) = distlock_rd._t.get(&req.key).cloned() {
                        let mut locked_release_ids = lock_state.0.payload.lock();
                        if locked_release_ids.contains(&req.release_id) {
                            assert!(locked_release_ids.remove(&req.release_id));
                        } else {
                            fail_reason =
                                Some("invalid release id or unlock multiple times".to_owned());
                        }
                    } else {
                        fail_reason = Some("not locked".to_owned());
                    }
                }

                if let Some(fail_reason) = fail_reason {
                    responser
                        .send_resp(proto::kv::KvLockResponse {
                            success: false,
                            context: fail_reason,
                            release_id: 0,
                        })
                        .await?;
                    tracing::debug!("handle_kv_lock_request unlocking returned");
                } else {
                    // 这里进行fetch sub
                    let mut need_delete = false;
                    {
                        let distlock_rd = DropDebug::new(
                            "fetch_sub stage unlock map".to_owned(),
                            self.dist_lock().locks.read().await,
                        );

                        if let Some(lock_state) = distlock_rd._t.get(&req.key).cloned() {
                            need_delete = !lock_state.fetch_sub();
                        }
                    }

                    if need_delete {
                        let mut removed = self.dist_lock().locks.write().await.remove(&req.key);
                        assert!(removed.is_some());

                        let wait_for_delete_took = removed
                            .as_mut()
                            .unwrap()
                            .0
                            .wait_for_delete
                            .lock()
                            .take()
                            .unwrap(); // only one can unlock and take it
                        wait_for_delete_took.notify_waiters();
                    }
                    responser
                        .send_resp(proto::kv::KvLockResponse {
                            success: true,
                            context: "unlocked".to_owned(),
                            release_id: 0,
                        })
                        .await?;
                    tracing::debug!("handle_kv_lock_request unlocking returned");
                }

                // let mut lock = self.dist_lock().locks.remove(&req.key);
            }
            _ => {
                let errmsg = "invalid request type, should be verified in msg pack precheck";
                responser
                    .send_resp(proto::kv::KvLockResponse {
                        success: false,
                        context: errmsg.to_owned(),
                        release_id: 0,
                    })
                    .await?;
                panic!("{}", errmsg);
            }
        }
        Ok(())
    }
    // pub async fn handle_kv_lock_wait_acquire_notify(
    //     &self,
    //     responser: RPCResponsor<proto::kv::KvLockWaitAcquireNotifyRequest>,
    //     req: proto::kv::KvLockWaitAcquireNotifyRequest,
    // ) {
    // }
}

impl DistLock {
    pub async fn lock(
        &self,
        lock: proto::kv::KvLockRequest,
    ) -> WSResult<proto::kv::KvLockResponse> {
        // hash target node
        let node_cnt = self.view.p2p().nodes_config.node_cnt();
        let mut hasher = DefaultHasher::new();
        lock.key.hash(&mut hasher);
        let node_id = hasher.finish() as usize % node_cnt;
        tracing::debug!("requested to node {}", node_id);
        self.rpc_caller_kv_lock
            .call(
                self.view.p2p(),
                node_id as u32,
                lock,
                Some(Duration::from_secs(10000)),
            )
            .await
            .map_err(|err| {
                tracing::error!("rpc call failed: {}", err);
                err
            })
    }
}

#[cfg(test)]
mod test {
    use std::{sync::Arc, time::Duration};

    use super::View;
    use crate::general::{network::proto, test_utils};

    #[tokio::test(flavor = "multi_thread")]
    async fn test_dist_lock() {
        let (sys1, sys2) = test_utils::get_test_sys().await;
        tokio::time::sleep(Duration::from_secs(3)).await;

        assert!(sys1.inner.upgrade().is_some());
        assert!(sys2.inner.upgrade().is_some());
        let view1 = View::new(sys1);
        let view2 = View::new(sys2);

        async fn test_common_lock_unlock(view: &View) {
            for keyi in 0..10 {
                let key = format!("{}", keyi);
                tracing::debug!("test_common_lock_unlock once");
                let resp = view
                    .dist_lock()
                    .lock(proto::kv::KvLockRequest {
                        key: key.as_bytes().to_owned(),
                        read_0_write_1_unlock_2: 0,
                        release_id: 0,
                    })
                    .await
                    .unwrap();
                tracing::debug!("test_common_lock_unlock locked");
                assert!(
                    view.dist_lock()
                        .lock(proto::kv::KvLockRequest {
                            key: key.as_bytes().to_owned(),
                            read_0_write_1_unlock_2: 2,
                            release_id: resp.release_id,
                        })
                        .await
                        .unwrap()
                        .success
                );
                tracing::debug!("test_common_lock_unlock unlocked");
            }
        }
        test_common_lock_unlock(&view1).await;
        test_common_lock_unlock(&view2).await;

        {
            //test lock wait
            tracing::debug!("outter locking");
            let resp = view1
                .dist_lock()
                .lock(proto::kv::KvLockRequest {
                    key: "key".as_bytes().to_owned(),
                    read_0_write_1_unlock_2: 1, // outter get the write lock, inner will wait
                    release_id: 0,
                })
                .await
                .unwrap();
            tracing::debug!("outter locked");
            let begin = std::time::Instant::now();
            let view2 = view2.clone();
            // view 2 will wait a while for the lock
            let _t = tokio::spawn(async move {
                tracing::debug!("inner locking");
                let resp = view2
                    .dist_lock()
                    .lock(proto::kv::KvLockRequest {
                        key: "key".as_bytes().to_owned(),
                        read_0_write_1_unlock_2: 0,
                        release_id: 0,
                    })
                    .await
                    .unwrap();
                tracing::debug!("inner locked");
                let elapsed = begin.elapsed();
                tracing::debug!("wait unlock time {} ms", elapsed.as_millis());
                assert!(elapsed.as_millis() >= 5000);
                println!("lock acquire time {} ms", elapsed.as_millis());
                assert!(
                    view2
                        .dist_lock()
                        .lock(proto::kv::KvLockRequest {
                            key: "key".as_bytes().to_owned(),
                            read_0_write_1_unlock_2: 2,
                            release_id: resp.release_id,
                        })
                        .await
                        .unwrap()
                        .success
                );
            });
            tokio::time::sleep(Duration::from_secs(5)).await;
            tracing::debug!("outter unlocking");
            assert!(
                view1
                    .dist_lock()
                    .lock(proto::kv::KvLockRequest {
                        key: "key".as_bytes().to_owned(),
                        read_0_write_1_unlock_2: 2,
                        release_id: resp.release_id,
                    })
                    .await
                    .unwrap()
                    .success
            );
            tracing::debug!("outter unlocked");
            let _ = _t.await.unwrap();
        }
    }
}
