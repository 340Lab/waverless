use crate::general::app::DataEventTrigger;
use crate::general::data::m_data_general::dataitem::DataItemSource;
use crate::general::data::m_dist_lock::DistLockOpe;
use crate::general::network::proto::sche::distribute_task_req::{
    DataEventTriggerNew, DataEventTriggerWrite, Trigger,
};

use super::proto::{self, kv::KvResponse, FileData};

use std::{ops::Range, path::Path};
use crate::result::{WSResult, WSError, WsDataError};

pub trait ProtoExtDataItem {
    fn data_sz_bytes(&self) -> usize;
    fn clone_split_range(&self, range: Range<usize>) -> Self;
    fn to_string(&self) -> String;
    fn new_raw_bytes(rawbytes: impl Into<Vec<u8>>) -> Self;
    fn as_raw_bytes<'a>(&'a self) -> Option<&'a [u8]>;
    fn new_file_data(filepath: impl AsRef<Path>, is_dir: bool) -> Self;
    fn as_file_data(&self) -> Option<&proto::FileData>;
    fn to_data_item_source(&self) -> DataItemSource;
}

impl ProtoExtDataItem for proto::DataItem {
    fn new_raw_bytes(rawbytes: impl Into<Vec<u8>>) -> Self {
        proto::DataItem {
            data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(
                rawbytes.into(),
            )),
        }
    }
    fn new_file_data(filepath: impl AsRef<Path>, is_dir: bool) -> Self {
        let file_content = std::fs::read(filepath.as_ref()).unwrap();
        Self {
            data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(FileData {
                file_name_opt: filepath.as_ref().to_string_lossy().to_string(),
                is_dir_opt: is_dir,
                file_content,
            })),
        }
    }

    fn data_sz_bytes(&self) -> usize {
        match self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(file_data) => file_data.file_content.len(),
            proto::data_item::DataItemDispatch::RawBytes(vec) => vec.len(),
            // proto::write_one_data_request::DataItem::Data(d) => d.data.len(),
            // proto::write_one_data_request::DataItem::DataVersion(d) => d.data.len(),
        }
    }

    fn clone_split_range(&self, range: Range<usize>) -> Self {
        // let data_length = match &self.data_item_dispatch.as_ref().unwrap() {
        //     proto::data_item::DataItemDispatch::File(file_data) => file_data.file_content.len(),
        //     proto::data_item::DataItemDispatch::RawBytes(vec) => vec.len(),
        // };

        // if range.start >= data_length || range.end > data_length {
        //     panic!("range out of bounds: {:?}", range);
        // }

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

    fn as_raw_bytes<'a>(&'a self) -> Option<&'a [u8]> {
        match &self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::RawBytes(vec) => Some(vec),
            _ => None,
        }
    }
    fn to_string(&self) -> String {
        match &self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(file_data) => {
                format!("file: {}", file_data.file_name_opt.clone())
            }
            proto::data_item::DataItemDispatch::RawBytes(vec) => {
                format!("raw bytes: {:?}", &vec[0..vec.len().min(100)])
            }
        }
    }

    fn as_file_data(&self) -> Option<&proto::FileData> {
        match &self.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::File(file_data)) => Some(file_data),
            _ => None,
        }
    }

    fn to_data_item_source(&self) -> DataItemSource {
        match &self.data_item_dispatch {
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => DataItemSource::Memory {
                data: bytes.clone(),
            },
            Some(proto::data_item::DataItemDispatch::File(file_data)) => DataItemSource::File {
                path: file_data.file_name_opt.clone().into(),
            },
            _ => DataItemSource::Memory {
                data: Vec::new(),
            },
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
    fn decode_persist(data: Vec<u8>) -> WSResult<Self> where Self: Sized;
    fn encode_persist<'a>(&'a self) -> Vec<u8>;
}

impl DataItemExt for proto::DataItem {
    fn decode_persist(data: Vec<u8>) -> WSResult<Self> where Self: Sized {
        if data.is_empty() {
            return Err(WSError::WsDataError(WsDataError::DataDecodeError {
                reason: "Empty data".to_string(),
                data_type: "proto::DataItem".to_string(),
            }));
        }
        let data_item_dispatch = match data[0] {
            0 => {
                let path_str = String::from_utf8(data[1..].to_vec()).map_err(|e| {
                    WSError::WsDataError(WsDataError::DataDecodeError {
                        reason: format!("Failed to decode path string: {}", e),
                        data_type: "proto::DataItem::File".to_string(),
                    })
                })?;
                proto::data_item::DataItemDispatch::File(FileData {
                    file_name_opt: path_str,
                    is_dir_opt: false,
                    file_content: Vec::new(),
                })
            },
            1 => proto::data_item::DataItemDispatch::RawBytes(data[1..].to_vec()),
            _ => {
                return Err(WSError::WsDataError(WsDataError::DataDecodeError {
                    reason: format!("Unknown data item type id: {}", data[0]),
                    data_type: "proto::DataItem".to_string(),
                }));
            }
        };
        Ok(Self {
            data_item_dispatch: Some(data_item_dispatch),
        })
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

pub trait ProtoExtDataEventTrigger {
    fn into_proto_trigger(self, key: Vec<u8>, opeid: u32) -> Trigger;
}

impl ProtoExtDataEventTrigger for DataEventTrigger {
    fn into_proto_trigger(self, key: Vec<u8>, opeid: u32) -> Trigger {
        match self {
            DataEventTrigger::Write | DataEventTrigger::WriteWithCondition { .. } => {
                Trigger::EventWrite(DataEventTriggerWrite { key, opeid })
            }
            DataEventTrigger::New | DataEventTrigger::NewWithCondition { .. } => {
                Trigger::EventNew(DataEventTriggerNew { key, opeid })
            }
        }
    }
}

// Example usage in tests
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_data_event_trigger_conversion() {
        let key = b"test_key".to_vec();
        let opeid = 1;

        // Test Write
        let write_trigger = DataEventTrigger::Write.into_proto_trigger(key.clone(), opeid);
        if let Trigger::EventWrite(trigger) = write_trigger {
            assert_eq!(trigger.key, key);
            assert_eq!(trigger.opeid, opeid);
        } else {
            panic!("Expected EventWrite trigger");
        }

        // Test WriteWithCondition (should produce same proto as Write)
        let write_cond_trigger = DataEventTrigger::WriteWithCondition {
            condition: "test_condition".to_string(),
        }
        .into_proto_trigger(key.clone(), opeid);
        if let Trigger::EventWrite(trigger) = write_cond_trigger {
            assert_eq!(trigger.key, key);
            assert_eq!(trigger.opeid, opeid);
        } else {
            panic!("Expected EventWrite trigger");
        }

        // Test New
        let new_trigger = DataEventTrigger::New.into_proto_trigger(key.clone(), opeid);
        if let Trigger::EventNew(trigger) = new_trigger {
            assert_eq!(trigger.key, key);
            assert_eq!(trigger.opeid, opeid);
        } else {
            panic!("Expected EventNew trigger");
        }

        // Test NewWithCondition (should produce same proto as New)
        let new_cond_trigger = DataEventTrigger::NewWithCondition {
            condition: "test_condition".to_string(),
        }
        .into_proto_trigger(key.clone(), opeid);
        if let Trigger::EventNew(trigger) = new_cond_trigger {
            assert_eq!(trigger.key, key);
            assert_eq!(trigger.opeid, opeid);
        } else {
            panic!("Expected EventNew trigger");
        }
    }
}
