use super::proto::{self, kv::KvResponse};

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
