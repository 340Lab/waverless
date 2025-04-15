use std::{collections::HashMap, sync::OnceLock};

use lazy_static::lazy_static;
use tokio::sync::Mutex;

use crate::{
    config::{NodeConfig, NodesConfig},
    start_tracing,
    sys::{LogicalModulesRef, Sys},
};

lazy_static! {
    static ref TEST_SYS1_SYS2: Mutex<Option<((Sys, LogicalModulesRef), (Sys, LogicalModulesRef))>> =
        Mutex::new(None);
}

pub async fn get_test_sys() -> (LogicalModulesRef, LogicalModulesRef) {
    let mut locked = TEST_SYS1_SYS2.lock().await;
    if locked.is_none() {
        *locked = Some(start_2_node().await);
    }
    let locked = locked.as_ref().unwrap();
    (locked.0 .1.clone(), locked.1 .1.clone())
}

async fn start_2_node() -> ((Sys, LogicalModulesRef), (Sys, LogicalModulesRef)) {
    start_tracing();

    let node0: NodeConfig = serde_yaml::from_str(
        r#"
addr: 127.0.0.1:2303
spec: [meta,master]
"#,
    )
    .unwrap();

    let node1: NodeConfig = serde_yaml::from_str(
        r#"
addr: 127.0.0.1:2307
spec: [meta,worker]
"#,
    )
    .unwrap();

    let sys1 = Sys::new(NodesConfig {
        peers: {
            let mut temp = HashMap::new();
            let _ = temp.insert(0, node0.clone());
            temp
        },
        this: (1, node1.clone()),
        file_dir: "test_temp_dir2".into(),
    });

    let sys0 = Sys::new(NodesConfig {
        peers: {
            let mut temp = HashMap::new();
            let _ = temp.insert(1, node1.clone());
            temp
        },
        this: (0, node0.clone()),
        file_dir: "test_temp_dir1".into(),
    });

    tracing::info!("starting sys1");
    let sys0_handle = sys0.test_start_all().await;
    tracing::info!("starting sys2");
    let sys1_handle = sys1.test_start_all().await;

    ((sys0, sys0_handle), (sys1, sys1_handle))
}
