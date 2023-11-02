// use async_trait::async_trait;

// use crate::result::WSResult;

// use super::dist_kv::{DistKV, SetOptions};

// pub struct RSCodeDistKV {}

// #[async_trait]
// impl DistKV for RSCodeDistKV {
//     async fn get(&self, keys: &[&[u8]]) -> WSResult<Option<Vec<u8>>> {
//         Ok(None)
//     }
//     async fn set(
//         &self,
//         kvs: &[(&[u8], &[u8])],
//         opts: SetOptions,
//     ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
//         Ok(None)
//     }
// }
