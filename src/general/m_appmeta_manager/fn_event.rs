use crate::general::{
    kv_interface::KvOps,
    network::proto::sche::distribute_task_req::{Trigger, TriggerKvSet},
};

use super::{
    super::network::proto::{self, kv::KvRequest},
    AppMetas,
};

pub struct EventTriggerInfo {
    pub trigger_appfns: Vec<(String, String)>,
    pub kvreq: KvRequest,
}
impl EventTriggerInfo {
    pub fn to_trigger(&self, opeid: u32) -> Trigger {
        match self.kvreq.op.as_ref().unwrap() {
            proto::kv::kv_request::Op::Set(set) => {
                let kv = set.kv.as_ref().unwrap();
                Trigger::KvSet(TriggerKvSet {
                    key: kv.key.clone(),
                    opeid,
                })
            }
            _ => unimplemented!(),
        }
    }
}

// impl Into<Trigger> for EventTriggerInfo {
//     fn into(self) -> Trigger {
//         match self.kvreq.op.unwrap() {
//             proto::kv::kv_request::Op::Set(set) => Trigger::KvSet(TriggerKvSet{
//                 key:
//             }),
//             _ => unimplemented!(),
//         }
//     }
// }

// pub async fn try_match_kv_event(
//     app_metas: &AppMetas,
//     req: &KvRequest,
//     source_app: &str,
//     source_fn: &str,
// ) -> Option<EventTriggerInfo> {
//     // find source app
//     let Some(appmeta) = app_metas.get_app_meta(source_app).await else {
//         tracing::warn!("source app:{} not found", source_app);
//         return None;
//     };
//     // find source func
//     let Some(fnmeta) = appmeta.get_fn_meta(source_fn) else {
//         tracing::warn!("app {} source func:{} not found", source_app, source_fn);
//         return None;
//     };

//     match req.op.as_ref().unwrap() {
//         proto::kv::kv_request::Op::Set(set) => {
//             let kv = set.kv.as_ref().unwrap();
//             // match kv pattern
//             let Some(pattern) = fnmeta.match_key(&kv.key, KvOps::Set) else {
//                 return None;
//             };
//             // find trigger func
//             app_metas
//                 .pattern_2_app_fn
//                 .get(&pattern.0)
//                 .map(|triggers| EventTriggerInfo {
//                     trigger_appfns: triggers.clone(),
//                     kvreq: req.clone(),
//                 })
//         }
//         proto::kv::kv_request::Op::Get(_) => None,
//         proto::kv::kv_request::Op::Delete(_) => None,
//         proto::kv::kv_request::Op::Lock(_) => None,
//     }
// }
