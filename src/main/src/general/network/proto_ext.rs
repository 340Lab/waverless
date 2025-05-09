use crate::general::app::DataEventTrigger;
use crate::general::data::m_data_general::dataitem::DataItemSource;
use crate::general::data::m_data_general::DataItemIdx;
use crate::general::data::m_dist_lock::DistLockOpe;
use crate::general::network::proto::sche::distribute_task_req::{
    DataEventTriggerNew, DataEventTriggerWrite, Trigger,
};

use super::proto::{self, kv::KvResponse, FileData};

use crate::result::{WSError, WSResult, WsDataError};
use std::fs::File;
use std::path::PathBuf;
use std::{ops::Range, path::Path};
use tokio;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

pub enum NewPartialFileDataArg {
    FilePath {
        basepath: PathBuf,
        path: PathBuf,
        zip_path: Option<PathBuf>,
    },
    FileContent {
        path: PathBuf,
        content: Vec<u8>,
    },
    File {
        path: PathBuf,
        file: File,
    },
}

pub trait ProtoExtDataItem: Sized {
    fn data_sz_bytes(&self) -> usize;
    fn clone_split_range(&self, range: Range<usize>) -> Self;
    fn to_string(&self) -> String;
    fn as_raw_bytes<'a>(&'a self) -> Option<&'a [u8]>;
    fn as_file_data(&self) -> Option<&proto::FileData>;
    fn to_data_item_source(&self) -> DataItemSource;
    async fn new_partial_file_data(
        arg: NewPartialFileDataArg,
        range: Range<usize>,
    ) -> WSResult<Self>;
    fn new_partial_raw_bytes(rawbytes: impl Into<Vec<u8>>, range: Range<usize>) -> WSResult<Self>;
}

impl ProtoExtDataItem for proto::DataItem {
    fn new_partial_raw_bytes(rawbytes: impl Into<Vec<u8>>, range: Range<usize>) -> WSResult<Self> {
        let bytes = rawbytes.into();
        if range.end > bytes.len() {
            return Err(WSError::WsDataError(WsDataError::SizeMismatch {
                expected: range.end,
                actual: bytes.len(),
            }));
        }

        Ok(Self {
            data_item_dispatch: Some(proto::data_item::DataItemDispatch::RawBytes(
                bytes[range].to_vec(),
            )),
        })
    }

    async fn new_partial_file_data(
        arg: NewPartialFileDataArg,
        range: Range<usize>,
    ) -> WSResult<Self> {
        let mut file_data = proto::FileData::default();

        // 从文件读取指定范围的数据
        async fn read_file_range(
            path: &Path,
            file: tokio::fs::File,
            range: Range<usize>,
        ) -> WSResult<Vec<u8>> {
            let mut file = tokio::io::BufReader::new(file);
            // file.seek(std::io::SeekFrom::Start(range.start as u64))       曾俊 没对正常返回值做处理
            let _ = file
                .seek(std::io::SeekFrom::Start(range.start as u64))
                .await
                .map_err(|e| {
                    WSError::WsDataError(WsDataError::FileSeekErr {
                        path: path.to_path_buf(),
                        err: e,
                    })
                })?;

            let mut buffer = vec![0; range.end - range.start];
            // file.read_exact(&mut buffer)
            let n = file.read_exact(&mut buffer).await.map_err(|e| {
                WSError::WsDataError(WsDataError::FileReadErr {
                    path: path.to_path_buf(),
                    err: e,
                })
            })?;

            assert_eq!(n, buffer.len(), "read file range len mismatch");

            tracing::debug!(
                "read file range len ({}), partial ({:?})",
                buffer.len(),
                &buffer[0..30]
            );

            Ok(buffer)
        }

        match arg {
            NewPartialFileDataArg::FilePath {
                basepath,
                path: path_,
                zip_path,
            } => {
                let path = basepath.join(&path_);
                file_data.file_name_opt = path_.to_string_lossy().to_string();
                file_data.is_dir_opt = path.is_dir();

                // 如果是目录，使用zip文件
                let actual_path = if path.is_dir() {
                    zip_path.as_ref().ok_or_else(|| {
                        WSError::WsDataError(WsDataError::BatchTransferFailed {
                            // node: 0,
                            // batch: 0,
                            request_id: proto::BatchRequestId {
                                node_id: 0,
                                sequence: 0,
                            },
                            reason: "Directory must have zip_path".to_string(),
                        })
                    })?
                } else {
                    &path
                };

                let file = tokio::fs::File::open(actual_path).await.map_err(|e| {
                    WSError::WsDataError(WsDataError::FileOpenErr {
                        path: actual_path.to_path_buf(),
                        err: e,
                    })
                })?;

                file_data.file_content = read_file_range(actual_path, file, range).await?;
            }
            NewPartialFileDataArg::FileContent { path, content } => {
                if range.end > content.len() {
                    return Err(WSError::WsDataError(WsDataError::SizeMismatch {
                        expected: range.end,
                        actual: content.len(),
                    }));
                }
                file_data.file_name_opt = path.to_string_lossy().to_string();
                file_data.is_dir_opt = path.is_dir();
                file_data.file_content = content[range].to_vec();
            }
            NewPartialFileDataArg::File { path, file } => {
                file_data.file_name_opt = path.to_string_lossy().to_string();
                file_data.is_dir_opt = path.is_dir();

                let file = tokio::fs::File::from_std(file);
                file_data.file_content = read_file_range(&path, file, range).await?;
            }
        }

        Ok(Self {
            data_item_dispatch: Some(proto::data_item::DataItemDispatch::File(file_data)),
        })
    }

    fn data_sz_bytes(&self) -> usize {
        match self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(file_data) => file_data.file_content.len(),
            proto::data_item::DataItemDispatch::RawBytes(vec) => vec.len(),
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
            _ => DataItemSource::Memory { data: Vec::new() },
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

// pub trait DataItemExt {
//     fn decode_persist(data: Vec<u8>) -> WSResult<Self>
//     where
//         Self: Sized;
//     fn encode_persist<'a>(&'a self) -> Vec<u8>;
//     fn get_data_type(&self) -> proto::data_item::DataItemDispatch;
//     fn into_data_bytes(self) -> Vec<u8>;
// }

pub trait DataItemExt {
    fn decode_persist(data: Vec<u8>) -> WSResult<Self>
    where
        Self: Sized;
    fn encode_persist<'a>(&'a self) -> Vec<u8>;
    fn get_data_type(&self) -> proto::data_item::DataItemDispatch;
    fn into_data_bytes(self) -> Vec<u8>;
    fn inmem_size(&self) -> usize;
}

impl DataItemExt for proto::DataItem {
    fn decode_persist(mut data: Vec<u8>) -> WSResult<Self>
    where
        Self: Sized,
    {
        // if data.is_empty() {
        //     return Err(WSError::WsDataError(WsDataError::DataDecodeError {
        //         reason: "Empty data".to_string(),
        //         data_type: "proto::DataItem".to_string(),
        //     }));
        // }
        let data_item_dispatch = match data[data.len() - 1] {
            0 => {
                // filename length
                let is_dir = data[data.len() - 2] == 1;
                let filename_len = data[data.len() - 3] as usize;
                let filename =
                    String::from_utf8(data[data.len() - 3 - filename_len..data.len() - 3].to_vec())
                        .map_err(|e| {
                            WSError::WsDataError(WsDataError::DataDecodeError {
                                reason: format!("Failed to decode path string: {}", e),
                                data_type: "proto::DataItem::File".to_string(),
                            })
                        })?;
                //         data_type: "proto::DataItem::File".to_string(),
                //     })
                // })?;
                // proto::data_item::DataItemDispatch::File(FileData {
                //     file_name_opt: path_str,
                //     is_dir_opt: false,
                //     file_content: Vec::new(),
                // })
                data.truncate(data.len() - 3 - filename_len);
                proto::data_item::DataItemDispatch::File(FileData {
                    file_name_opt: filename,
                    is_dir_opt: is_dir, // ignore is_dir
                    file_content: data, // [0..data.len() - 3 - filename_len].to_vec(),
                })
            }
            1 => proto::data_item::DataItemDispatch::RawBytes(data[0..data.len() - 1].to_vec()),
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
                let mut ret = vec![];
                tracing::debug!("encoding file data, size: {}", f.file_content.len());
                ret.extend_from_slice(&f.file_content);
                // append file name on the end of the file content
                ret.extend_from_slice(f.file_name_opt.as_bytes());
                // append file name length on the end of the file content
                ret.push(f.file_name_opt.len() as u8);
                // append is dir on the end of the file content
                ret.push(if f.is_dir_opt { 1 } else { 0 });
                // append data item type id on the end of the file content
                ret.push(0);
                ret
            }
            proto::data_item::DataItemDispatch::RawBytes(bytes) => {
                let mut ret = vec![];
                tracing::debug!("encoding raw bytes data, size: {}", bytes.len());
                ret.extend_from_slice(bytes);
                // append data item type id on the end of the file content
                ret.push(1);
                ret
            }
        }
    }
    fn get_data_type(&self) -> proto::data_item::DataItemDispatch {
        match &self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(d) => {
                proto::data_item::DataItemDispatch::File(FileData {
                    file_name_opt: d.file_name_opt.clone(),
                    is_dir_opt: d.is_dir_opt,
                    file_content: Vec::new(),
                })
            }
            proto::data_item::DataItemDispatch::RawBytes(_) => {
                proto::data_item::DataItemDispatch::RawBytes(Vec::new())
            }
        }
    }

    fn into_data_bytes(mut self) -> Vec<u8> {
        match self.data_item_dispatch.take() {
            Some(proto::data_item::DataItemDispatch::File(f)) => f.file_content,
            Some(proto::data_item::DataItemDispatch::RawBytes(bytes)) => bytes,
            None => panic!("DataItem is empty"),
        }
    }

    fn inmem_size(&self) -> usize {
        match &self.data_item_dispatch.as_ref().unwrap() {
            proto::data_item::DataItemDispatch::File(f) => f.file_content.len(),
            proto::data_item::DataItemDispatch::RawBytes(bytes) => bytes.len(),
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

pub trait ProtoExtDataScheduleContext {
    fn dataitem_cnt(&self) -> DataItemIdx;
}

impl ProtoExtDataScheduleContext for proto::DataScheduleContext {
    fn dataitem_cnt(&self) -> DataItemIdx {
        self.each_data_sz_bytes.len() as DataItemIdx
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
