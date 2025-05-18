use crate::general::network::proto;

use super::process_rpc::proc_proto;

impl From<proc_proto::FnTaskId> for proto::FnTaskId {
    fn from(taskid: proc_proto::FnTaskId) -> Self {
        proto::FnTaskId {
            call_node_id: taskid.call_node_id,
            task_id: taskid.task_id,
        }
    }
}

impl From<proto::kv::KvResponse> for proc_proto::KvResponse {
    fn from(response: proto::kv::KvResponse) -> Self {
        proc_proto::KvResponse {
            resp: Some(match response.resp.unwrap() {
                proto::kv::kv_response::Resp::Get(get) => {
                    proc_proto::kv_response::Resp::Get(proc_proto::kv_response::KvGetResponse {
                        idxs: get.idxs,
                        values: get.values,
                    })
                }
                proto::kv::kv_response::Resp::PutOrDel(put_or_del) => {
                    let kv = put_or_del.kv.unwrap();
                    proc_proto::kv_response::Resp::PutOrDel(
                        proc_proto::kv_response::KvPutOrDelResponse {
                            kv: Some(proc_proto::KvPair {
                                key: kv.key,
                                values: kv.values,
                            }),
                        },
                    )
                }
                proto::kv::kv_response::Resp::LockId(lock_id) => {
                    proc_proto::kv_response::Resp::LockId(lock_id)
                }
            }),
        }
    }
}

pub trait ProcRpcReqExt {
    fn app_fn(&self) -> &str;
    fn app_name(&self) -> &str {
        self.app_fn().split("/").next().unwrap()
    }
    fn func_name(&self) -> &str {
        self.app_fn().split("/").nth(1).unwrap()
    }
    fn fn_taskid(&self) -> proto::FnTaskId;
}

impl ProcRpcReqExt for proc_proto::KvRequest {
    fn app_fn(&self) -> &str {
        match &self.op {
            Some(proc_proto::kv_request::Op::Set(set)) => set.app_fn.as_str(),
            Some(proc_proto::kv_request::Op::Get(get)) => get.app_fn.as_str(),
            Some(proc_proto::kv_request::Op::Delete(delete)) => delete.app_fn.as_str(),
            None => panic!("no app_fn in kv request"),
        }
    }
    fn fn_taskid(&self) -> proto::FnTaskId {
        match &self.op {
            Some(proc_proto::kv_request::Op::Set(set)) => {
                proto::FnTaskId::from(set.src_task_id.clone().unwrap())
            }
            Some(proc_proto::kv_request::Op::Get(get)) => {
                proto::FnTaskId::from(get.src_task_id.clone().unwrap())
            }
            Some(proc_proto::kv_request::Op::Delete(delete)) => {
                proto::FnTaskId::from(delete.src_task_id.clone().unwrap())
            }
            None => panic!("no fn_taskid in kv request"),
        }
    }
}

pub trait ProcRpcExtKvReq {
    fn to_proto_kvrequests(self) -> proto::kv::KvRequests;
}

impl ProcRpcExtKvReq for proc_proto::KvRequest {
    fn to_proto_kvrequests(self) -> proto::kv::KvRequests {
        let app = self.app_name().to_string();
        let func = self.func_name().to_string();

        match self.op {
            Some(proc_proto::kv_request::Op::Set(set)) => {
                let kv = set.kv.unwrap();
                proto::kv::KvRequests {
                    app,
                    func,
                    requests: vec![proto::kv::KvRequest {
                        op: Some(proto::kv::kv_request::Op::Set(
                            proto::kv::kv_request::KvPutRequest {
                                kv: Some(proto::kv::KvPair {
                                    key: kv.key,
                                    values: kv.values,
                                }),
                            },
                        )),
                    }],
                    prev_kv_opeid: 0,
                }
            }
            Some(proc_proto::kv_request::Op::Get(get)) => {
                let range = get.range.unwrap();
                proto::kv::KvRequests {
                    app,
                    func,
                    requests: vec![proto::kv::KvRequest {
                        op: Some(proto::kv::kv_request::Op::Get(
                            proto::kv::kv_request::KvGetRequest {
                                range: Some(proto::kv::KeyRange {
                                    start: range.start,
                                    end: range.end,
                                }),
                                idxs: get.idxs,
                            },
                        )),
                    }],
                    prev_kv_opeid: 0,
                }
            }
            Some(proc_proto::kv_request::Op::Delete(delete)) => {
                let range = delete.range.unwrap();
                proto::kv::KvRequests {
                    app,
                    func,
                    requests: vec![proto::kv::KvRequest {
                        op: Some(proto::kv::kv_request::Op::Delete(
                            proto::kv::kv_request::KvDeleteRequest {
                                range: Some(proto::kv::KeyRange {
                                    start: range.start,
                                    end: range.end,
                                }),
                            },
                        )),
                    }],
                    prev_kv_opeid: 0,
                }
            }
            None => panic!("no op in kv request"),
        }
    }
}
