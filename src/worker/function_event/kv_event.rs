use std::time::Duration;

use crate::{
    general::{
        kv_interface::{KVInterface, KvOps, SetOptions},
        network::proto::{self},
    },
    util,
    worker::{
        app_meta::{AppMetaManager, KeyPattern, KvMeta},
        executor::FunctionCtx,
        kv_user_client::KvUserClient,
    },
};

async fn handle_set_event(
    kv: proto::kv::KvPair,
    kv_client: &KvUserClient,
    prev_fn_ctx: &FunctionCtx,
    triggers: Vec<(&String, &String, &KvMeta)>,
) {
    let worker = kv_client.view.worker();
    let p2p = kv_client.view.instance_manager().view.p2p();
    let schecmd = match worker
        .rpc_caller_make_sche
        .call(
            p2p,
            p2p.nodes_config.get_master_node(),
            proto::sche::MakeSchePlanReq {
                app_fns: triggers
                    .iter()
                    .map(|(app, func, _)| proto::sche::make_sche_plan_req::AppFn {
                        app: app.to_string(),
                        func: func.to_string(),
                    })
                    .collect::<Vec<_>>(),
                trigger_type: proto::sche::make_sche_plan_req::TriggerType::SetKv as i32,
            },
            None,
        )
        .await
    {
        Ok(schecmd) => schecmd,
        Err(e) => {
            tracing::error!("rpc call error: {:?}", e);
            return;
        }
    };
    tracing::info!("got sche plan from master: {:?}", schecmd);
    // 1. sync set kv
    // TODO: use schecmd.data_target_node
    let key = kv.key.clone();
    if let Ok(_res) = kv_client.set(vec![kv], SetOptions::new()).await {
        for (&target_node, (app, func, _)) in schecmd.sche_target_node.iter().zip(triggers) {
            let view = kv_client.view.clone();
            let app = app.to_owned();
            let func = func.to_owned();
            let key = key.clone();

            let remote_sche_task = tokio::spawn(async move {
                let sub_task = view.executor().register_sub_task();
                if let Err(err) = view
                    .worker()
                    .rpc_caller_distribute_task
                    .call(
                        view.p2p(),
                        target_node,
                        proto::sche::DistributeTaskReq {
                            app,
                            func,
                            task_id: sub_task,
                            trigger: Some(proto::sche::distribute_task_req::Trigger::KvKeySet(key)),
                        },
                        // max wait time
                        Some(Duration::from_secs(60 * 30)),
                    )
                    .await
                {
                    tracing::error!("sche sub fn failed with err: {}", err);
                }
            });
            unsafe {
                util::unsafe_mut(prev_fn_ctx)
                    .sub_waiters
                    .push(remote_sche_task)
            }
        }
    }
}

pub async fn check_and_handle_event(
    // record triggerd events
    fn_ctx: &FunctionCtx,
    // tigger pattern
    pattern: &KeyPattern,
    // kv operation
    kv_client: &KvUserClient,
    // app meta to get trigger infos
    app_meta_man: &AppMetaManager,
    // may trigger op
    op: KvOps,
    // kv to set
    kv: proto::kv::KvPair,
) {
    match op {
        KvOps::Get | KvOps::Delete => {
            tracing::warn!("kv event not support get/delete");
            return;
        }
        KvOps::Set => {}
    }
    tracing::info!("event trigger kv ope matched");
    let triggers = if let Some(triggers) = app_meta_man.get_pattern_triggers(&*pattern.0) {
        if triggers.is_empty() {
            return;
        }
        triggers
    } else {
        return;
    };
    tracing::info!("kv pattern has potential triggers");
    // collect must consume triggers
    let triggers: Vec<(&String, &String, &KvMeta)> = triggers
        .iter()
        .filter_map(|(app, func)| {
            let maytrigger_fnmeta = app_meta_man
                .get_app_meta(app)
                .unwrap()
                .get_fn_meta(func)
                .unwrap();

            if let Some(kvmeta) = maytrigger_fnmeta.find_will_trigger_kv_event(pattern, op) {
                Some((app, func, kvmeta))
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    tracing::info!("kv pattern has {} triggers", triggers.len());
    handle_set_event(kv, kv_client, fn_ctx, triggers).await;
}
