use prost::DecodeError;
use qp2p::EndpointError;
use thiserror::Error;

pub type WSResult<T> = Result<T, WSError>;

#[derive(Debug)]
pub enum NotMatchNodeErr {
    NotRaft(String),
}

#[derive(Debug)]
pub enum WSNetworkErr {
    Str(String),
    QP2PEndPointError(EndpointError, String),
    NotMatchNode(NotMatchNodeErr),
}

#[derive(Error, Debug)]
pub enum WSError {
    #[error("Network error: {0:?}")]
    NetworkError(WSNetworkErr),
}

impl From<WSNetworkErr> for WSError {
    fn from(e: WSNetworkErr) -> Self {
        WSError::NetworkError(e)
    }
}

impl From<EndpointError> for WSError {
    fn from(e: EndpointError) -> Self {
        WSError::NetworkError(WSNetworkErr::QP2PEndPointError(
            e,
            "QP2P endpoint error".to_owned(),
        ))
    }
}

impl From<DecodeError> for WSError {
    fn from(e: DecodeError) -> Self {
        WSError::NetworkError(WSNetworkErr::Str(format!("Decode error: {:?}", e)))
    }
}

impl From<NotMatchNodeErr> for WSError {
    fn from(e: NotMatchNodeErr) -> Self {
        WSError::NetworkError(WSNetworkErr::NotMatchNode(e))
    }
}
