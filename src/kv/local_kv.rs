use async_trait::async_trait;

use crate::result::WSResult;

use super::dist_kv::SetOptions;

#[async_trait]
pub trait LocalKV {
    async fn get(&self, keys: &[&[u8]]) -> WSResult<Option<Vec<u8>>>;
    async fn set(
        &self,
        kvs: &[(&[u8], &[u8])],
        opts: SetOptions,
    ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>>;
}
