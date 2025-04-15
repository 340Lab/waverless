use std::os::unix::net::SocketAddr;

use async_raft::{InitializeError, RaftError};
use camelpaste::paste;
use prost::{DecodeError, Message};
use qp2p::{EndpointError, SendError};
use thiserror::Error;
use wasmedge_sdk::error::WasmEdgeError;
use zip_extract::ZipExtractError;

use crate::{
    general::{m_appmeta_manager::FnMeta, network::rpc_model::HashValue},
    sys::NodeID,
    util::TryUtf8VecU8,
};

pub type WSResult<T> = Result<T, WSError>;

#[derive(Debug)]
pub enum NotMatchNodeErr {
    NotRaft(String),
}

#[derive(Debug)]
pub enum WsNetworkLogicErr {
    DecodeError(DecodeError),
    MsgIdNotDispatchable(u32),
    InvaidNodeID(NodeID),
}

#[derive(Debug)]
pub enum WsNetworkConnErr {
    EndPointError(EndpointError),
    SendError(SendError),
    ConnectionNotEstablished(NodeID),
    RPCTimout(NodeID),
    ConnectionExpired(NodeID),
}

#[derive(Debug)]
pub enum WsRpcErr {
    ConnectionNotEstablished(HashValue),
    RPCTimout(HashValue),
    InvalidMsgData { msg: Box<dyn Message> },
    UnknownPeer { peer: SocketAddr },
}

#[derive(Debug)]
pub enum WsIoErr {
    Io(std::io::Error),
    Io2(walkdir::Error),
    Zip(ZipExtractError),
    Zip2(zip::result::ZipError),
}

impl From<std::io::Error> for WSError {
    fn from(e: std::io::Error) -> Self {
        WSError::WsIoErr(WsIoErr::Io(e))
    }
}

impl From<walkdir::Error> for WSError {
    fn from(e: walkdir::Error) -> Self {
        WSError::WsIoErr(WsIoErr::Io2(e))
    }
}

impl From<zip::result::ZipError> for WSError {
    fn from(e: zip::result::ZipError) -> Self {
        WSError::WsIoErr(WsIoErr::Zip2(e))
    }
}

impl From<ZipExtractError> for WSError {
    fn from(e: ZipExtractError) -> Self {
        WSError::WsIoErr(WsIoErr::Zip(e))
    }
}

#[derive(Debug)]
pub enum WsRaftErr {
    InitializeError(async_raft::error::InitializeError),
    RaftError(RaftError),
}

#[derive(Debug)]
pub enum WsSerialErr {
    BincodeErr(Box<bincode::ErrorKind>),
    AppMetaKvKeyIndexOutOfBound {
        app: String,
        func: String,
        index: usize,
        kvs_len: Option<usize>,
    },
}

#[derive(Error, Debug)]
pub enum WsFormatErr {
    #[error("KeyPatternFormatErr: {key_pattern}, '{{' '}}' should appear in pair, only '_' is allowed to appear as symbol")]
    KeyPatternFormatErr { key_pattern: String },
}

#[derive(Debug)]
pub enum WsPermissionErr {
    AccessKeyPermissionDenied {
        app: String,
        func: String,
        access_key: TryUtf8VecU8,
    },
}

#[derive(Debug)]
pub enum WsFuncError {
    WasmError(wasmedge_sdk::error::WasmEdgeError),
    AppNotFound {
        app: String,
    },
    FuncNotFound {
        app: String,
        func: String,
    },
    InvalidHttpUrl(String),
    FuncHttpNotSupported {
        fname: String,
        fmeta: FnMeta,
    },
    FuncBackendHttpNotSupported {
        fname: String,
    },
    FuncHttpFail {
        app: String,
        func: String,
        http_err: reqwest::Error,
    },
    AppPackFailedZip(ZipExtractError),
    AppPackNoExe,
    AppPackExeName(String),
    AppPackConfReadInvalid(std::io::Error),
    AppPackConfDecodeErr(serde_yaml::Error),
    AppPackRemoveFailed(std::io::Error),
    AppPackTmp2NewFailed(std::io::Error),
    FuncSnapshotFailed {
        detail: String,
    },
    InstanceNotFound(String),
    InstanceTypeNotMatch {
        app: String,
        want: String,
    },
    InstanceJavaPidNotFound(String),
    InstanceProcessStartFailed(std::io::Error),
}

#[derive(Debug)]
pub enum WsDataError {
    SetExpiredDataVersion {
        target_version: u64,
        cur_version: u64,
        data_id: String,
    },
}

#[derive(Error, Debug)]
pub enum WSError {
    #[error("Io error: {0:?}")]
    WsIoErr(WsIoErr),

    #[error("Network logic error: {0:?}")]
    WsNetworkLogicErr(WsNetworkLogicErr),

    #[error("Network connection error: {0:?}")]
    WsNetworkConnErr(WsNetworkConnErr),

    #[error("Raft error: {0:?}")]
    WsRaftErr(WsRaftErr),

    #[error("Permission error: {0:?}")]
    WsPermissionErr(WsPermissionErr),

    #[error("Serial error: {0:?}")]
    WsSerialErr(WsSerialErr),

    #[error("Format error: {0:?}")]
    WsFormatErr(WsFormatErr),

    #[error("Func error: {0:?}")]
    WsFuncError(WsFuncError),

    #[error("Rpc error: {0:?}")]
    WsRpcErr(WsRpcErr),

    #[error("Data error: {0:?}")]
    WsDataError(WsDataError),

    #[error("Not Implemented")]
    NotImplemented,
}

impl From<WsNetworkLogicErr> for WSError {
    fn from(e: WsNetworkLogicErr) -> Self {
        WSError::WsNetworkLogicErr(e)
    }
}

impl From<WsNetworkConnErr> for WSError {
    fn from(e: WsNetworkConnErr) -> Self {
        WSError::WsNetworkConnErr(e)
    }
}

impl From<WsRaftErr> for WSError {
    fn from(e: WsRaftErr) -> Self {
        WSError::WsRaftErr(e)
    }
}

impl From<WsSerialErr> for WSError {
    fn from(e: WsSerialErr) -> Self {
        WSError::WsSerialErr(e)
    }
}

impl From<WsIoErr> for WSError {
    fn from(e: WsIoErr) -> Self {
        WSError::WsIoErr(e)
    }
}

impl From<WsPermissionErr> for WSError {
    fn from(e: WsPermissionErr) -> Self {
        WSError::WsPermissionErr(e)
    }
}

impl From<WsFuncError> for WSError {
    fn from(e: WsFuncError) -> Self {
        WSError::WsFuncError(e)
    }
}

impl From<WasmEdgeError> for WSError {
    fn from(e: WasmEdgeError) -> Self {
        WSError::WsFuncError(WsFuncError::WasmError(e))
    }
}

impl From<WsRpcErr> for WSError {
    fn from(e: WsRpcErr) -> Self {
        WSError::WsRpcErr(e)
    }
}

impl From<WsDataError> for WSError {
    fn from(value: WsDataError) -> Self {
        WSError::WsDataError(value)
    }
}

pub struct ErrCvt<T>(pub T);

macro_rules! impl_err_convertor {
    ($t:ty,$sub_t:ty,$sub_tt:ty) => {
        paste! {
            impl ErrCvt<$t> {
                pub fn [<to_ $sub_t:snake>](self) -> WSError {
                    WSError::$sub_t($sub_t::$sub_tt(self.0))
                }
            }
        }
    };
}

impl_err_convertor!(DecodeError, WsNetworkLogicErr, DecodeError);
impl_err_convertor!(EndpointError, WsNetworkConnErr, EndPointError);
impl_err_convertor!(SendError, WsNetworkConnErr, SendError);
impl_err_convertor!(InitializeError, WsRaftErr, InitializeError);
impl_err_convertor!(RaftError, WsRaftErr, RaftError);
impl_err_convertor!(std::io::Error, WsIoErr, Io);
