use crate::{general::m_dist_lock::DistLockOpe, util::VecOrSlice};

use super::proto::{self, kv::KvResponse, DataItem, FileData};

use std::ops::Range;

pub trait ProtoExtDataItem {
    fn data_sz_bytes(&self) -> usize;
    fn clone_split_range(&self, range: Range<usize>) -> Self;
}

impl ProtoExtDataItem for proto::DataItem {
    fn data_sz_bytes(&self) -> usize {
        match self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(file_data) => file_data.file_content.len(),
            proto::data_item::DataItemDispatch::RawBytes(vec) => vec.len(),
            // proto::write_one_data_request::DataItem::Data(d) => d.data.len(),
            // proto::write_one_data_request::DataItem::DataVersion(d) => d.data.len(),
        }
    }

    fn clone_split_range(&self, range: Range<usize>) -> Self {
        Self {
            data_item_dispatch: Some(match &self.data_item_dispatch.as_ref().unwrap() {
                proto::data_item::DataItemDispatch::File(file_data) => {
                    proto::data_item::DataItemDispatch::File(proto::FileData {
                        file_name_opt: String::new(),
                        is_dir_opt: file_data.is_dir_opt,
                        file_content: file_data.file_content[range.clone()].to_owned(),
                    })
                }
                proto::data_item::DataItemDispatch::RawBytes(vec) => {
                    proto::data_item::DataItemDispatch::RawBytes(vec[range.clone()].to_owned())
                }
            }),
        }
    }
}

impl AsRef<[u8]> for proto::DataItem {
    fn as_ref(&self) -> &[u8] {
        match &self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(file_data) => &file_data.file_content,
            proto::data_item::DataItemDispatch::RawBytes(vec) => vec,
        }
    }
}

pub trait ProtoExtKvResponse {
    fn new_lock(lock_id: u32) -> KvResponse;
    fn new_common(kvs: Vec<proto::kv::KvPair>) -> KvResponse;
    fn lock_id(&self) -> Option<u32>;
    fn common_kvs(&self) -> Option<&Vec<proto::kv::KvPair>>;
}

impl ProtoExtKvResponse for KvResponse {
    fn new_common(kvs: Vec<proto::kv::KvPair>) -> KvResponse {
        KvResponse {
            resp: Some(proto::kv::kv_response::Resp::CommonResp(
                proto::kv::kv_response::KvResponse { kvs },
            )),
        }
    }
    fn new_lock(lock_id: u32) -> KvResponse {
        KvResponse {
            resp: Some(proto::kv::kv_response::Resp::LockId(lock_id)),
        }
    }
    fn lock_id(&self) -> Option<u32> {
        match self.resp.as_ref().unwrap() {
            proto::kv::kv_response::Resp::CommonResp(_) => None,
            proto::kv::kv_response::Resp::LockId(id) => Some(*id),
        }
    }
    fn common_kvs(&self) -> Option<&Vec<proto::kv::KvPair>> {
        match self.resp.as_ref().unwrap() {
            proto::kv::kv_response::Resp::CommonResp(resp) => Some(&resp.kvs),
            proto::kv::kv_response::Resp::LockId(_) => None,
        }
    }
}

pub trait KvRequestExt {
    fn new_set(kv: proto::kv::KvPair) -> Self;
    fn new_get(key: Vec<u8>) -> Self;
    fn new_delete(key: Vec<u8>) -> Self;
    fn new_lock(ope: DistLockOpe, key: Vec<u8>) -> Self;
}

impl KvRequestExt for proto::kv::KvRequest {
    fn new_set(kv: proto::kv::KvPair) -> Self {
        proto::kv::KvRequest {
            op: Some(proto::kv::kv_request::Op::Set(
                proto::kv::kv_request::KvPutRequest { kv: Some(kv) },
            )),
        }
    }
    fn new_get(key: Vec<u8>) -> Self {
        proto::kv::KvRequest {
            op: Some(proto::kv::kv_request::Op::Get(
                proto::kv::kv_request::KvGetRequest {
                    range: Some(proto::kv::KeyRange {
                        start: key,
                        end: vec![],
                    }),
                },
            )),
        }
    }
    fn new_delete(key: Vec<u8>) -> Self {
        proto::kv::KvRequest {
            op: Some(proto::kv::kv_request::Op::Delete(
                proto::kv::kv_request::KvDeleteRequest {
                    range: Some(proto::kv::KeyRange {
                        start: key,
                        end: vec![],
                    }),
                },
            )),
        }
    }
    fn new_lock(ope: DistLockOpe, key: Vec<u8>) -> Self {
        proto::kv::KvRequest {
            op: Some(proto::kv::kv_request::Op::Lock(
                proto::kv::kv_request::KvLockRequest {
                    read_or_write: ope.is_read(),
                    release_id: if let DistLockOpe::Unlock(release_id) = ope {
                        vec![release_id]
                    } else {
                        vec![]
                    },
                    range: Some(proto::kv::KeyRange {
                        start: key,
                        end: vec![],
                    }),
                },
            )),
        }
    }
}

pub trait DataItemExt {
    fn decode_persist(data: Vec<u8>) -> Self;
    fn encode_persist<'a>(&'a self) -> Vec<u8>;
}

impl DataItemExt for proto::DataItem {
    fn decode_persist(data: Vec<u8>) -> Self {
        let data_item_dispatch = match data[0] {
            0 => proto::data_item::DataItemDispatch::File(FileData {
                file_name_opt: String::new(),
                is_dir_opt: false,
                file_content: data[1..].to_owned(),
            }),
            1 => proto::data_item::DataItemDispatch::RawBytes(data[1..].to_owned()),
            _ => {
                panic!("unknown data type")
            }
        };
        Self {
            data_item_dispatch: Some(data_item_dispatch),
        }
    }
    fn encode_persist<'a>(&'a self) -> Vec<u8> {
        match self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(f) => {
                let mut ret = vec![0];
                ret.extend_from_slice(&f.file_content);
                ret
            }
            proto::data_item::DataItemDispatch::RawBytes(bytes) => {
                // tracing::debug!("writing data part{} bytes", idx);
                // VecOrSlice::from(&bytes)
                let mut ret = vec![1];
                ret.extend_from_slice(bytes);
                ret
            }
        }
    }
}
