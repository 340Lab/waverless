use crate::{
    kv::dist_kv::SetOptions,
    network::proto::kv::{KeyRange, KvPair},
    new_test_systems, start_tracing,
    sys::LogicalModulesRef,
};
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_meta_kv() {
    start_tracing();
    let systems = new_test_systems();
    let mut join = vec![];
    let modules = LogicalModulesRef::new(systems[0].logical_modules.clone());
    for (i, mut s) in systems.into_iter().enumerate() {
        join.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_secs((i * 5) as u64)).await;
            s.wait_for_end().await;
        }));
    }
    tokio::time::sleep(Duration::from_secs(5)).await;
    // test meta kv
    if modules.p2p().nodes_config.this.0 == 1 {
        let view = modules.clone();

        if let Some(meta) = view.meta_kv() {
            let mut last_set = 0;
            loop {
                if meta.ready().await {
                    tracing::info!("try test once");
                    let value = format!("test value: {}", last_set).as_bytes().to_owned();
                    let _ = meta
                        .set(
                            vec![KvPair {
                                key: "test".as_bytes().to_owned(),
                                value: value.clone(),
                            }],
                            SetOptions { consistent: true },
                        )
                        .await
                        .unwrap();
                    tokio::time::sleep(Duration::from_millis(200)).await;
                    let res = meta
                        .get(KeyRange {
                            start: "test".as_bytes().to_owned(),
                            end: vec![],
                        })
                        .await
                        .unwrap();
                    assert!(res.len() == 1);
                    assert!(res[0].value == value);

                    tracing::info!(
                        "test once success with result: {}",
                        String::from_utf8_lossy(&*res[0].value)
                    );
                    last_set += 1;
                    if last_set == 5 {
                        return;
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }

    // for j in join {
    //     j.await.unwrap();
    // }
}
