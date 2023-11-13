use async_trait::async_trait;

use crate::{kv::dist_kv::SetOptions, result::WSResult};

// use super::dist_kv::SetOptions;

#[async_trait]
pub trait LocalKVRaw: Send + Sync + 'static {
    async fn get(&self, key: &[u8], end: Option<&[u8]>) -> WSResult<Vec<(Vec<u8>, Vec<u8>)>>;
    async fn set(
        &self,
        kvs: &[(&[u8], &[u8])],
        opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>>;
}
