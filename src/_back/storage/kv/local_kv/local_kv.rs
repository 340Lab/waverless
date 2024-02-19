use std::collections::BTreeMap;

// use super::dist_kv::SetOptions;
use crate::{
    network::proto::kv::{kv_response::KvPairOpt, KeyRange, KvPair},
    result::WSResult,
    storage::kv::kv_interface::{KvInterface, KvNode, SetOptions},
    sys::{LogicalModule, LogicalModuleNewArgs},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use tokio::sync::RwLock;
use ws_derive::LogicalModule;

// pub struct LocalKv<K: LocalKvRaw> {
//     k: K,
// }

#[derive(LogicalModule)]
pub struct LocalKvNode {
    map: RwLock<BTreeMap<Vec<u8>, Vec<u8>>>,
}

#[async_trait]
impl KvInterface for LocalKvNode {
    async fn get(&self, key_range: KeyRange) -> WSResult<Vec<KvPair>> {
        let mut res = vec![];
        if key_range.end.len() > 0 {
        } else {
            let map = self.map.read().await;
            if let Some(kv) = map.get_key_value(&key_range.start) {
                res.push(KvPair {
                    key: kv.0.clone(),
                    value: kv.1.clone(),
                });
            }
        }
        Ok(res)
    }
    async fn set(&self, kvs: Vec<KvPair>, _opts: SetOptions) -> WSResult<Vec<KvPairOpt>> {
        let mut map = self.map.write().await;
        for kv in kvs {
            let _ = map.insert(kv.key, kv.value);
        }
        Ok(vec![])
    }
}

#[async_trait]
impl KvNode for LocalKvNode {
    async fn ready(&self) -> bool {
        true
    }
}

#[async_trait]
impl LogicalModule for LocalKvNode {
    fn inner_new(_args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        LocalKvNode {
            map: RwLock::new(BTreeMap::new()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];

        Ok(all)
    }
}

// #[async_trait]
// impl<K: LocalKvRaw> LocalKvRaw for LocalKv<K> {
//     #[inline]
//     async fn get(&self, key: &[u8], end: Option<&[u8]>) -> WSResult<Vec<(Vec<u8>, Vec<u8>)>> {
//         self.k.get(key, end).await
//     }

//     #[inline]
//     async fn set(
//         &self,
//         kvs: &[(&[u8], &[u8])],
//         opts: SetOptions,
//     ) -> WSResult<Option<Vec<(Vec<u8>, Vec<u8>)>>> {
//         self.k.set(kvs, opts).await
//     }
// }

// impl<K: LocalKvRaw> LocalKv<K> {
//     pub async fn get_spec(&self, k: &K, end: Option<&K>) -> WSResult<Vec<(K, K::Value)>>
//     where
//         K: LocalKey,
//     {
//         let key = bincode::serialize(k).unwrap();
//         let end = end.map(|v| bincode::serialize(&v).unwrap());

//         let res = self
//             .k
//             .get(key.as_slice(), end.as_ref().map(|v| v.as_slice()))
//             .await?;
//         let mut res_kv = vec![];
//         for (k_vec, v_vec) in res {
//             let des_k = bincode::deserialize::<K>(k_vec.as_slice())
//                 .map_err(|err| WsSerialErr::BincodeErr(err))?;
//             let des_v = bincode::deserialize::<K::Value>(v_vec.as_slice())
//                 .map_err(|err| WsSerialErr::BincodeErr(err))?;
//             let pair = (des_k, des_v);
//             res_kv.push(pair);
//         }

//         Ok(res_kv)
//     }
//     // pub async fn set_spec(
//     //     &self,
//     //     kvs: &[(K, K::Value)],
//     //     opts: SetOptions,
//     // ) -> WSResult<Option<Vec<(K, K::Value)>>>
//     // where
//     //     K: LocalKey,
//     // {
//     //     let mut kvs_vec = vec![];
//     //     for (k, v) in kvs {
//     //         let k_vec = bincode::serialize(k).unwrap();
//     //         let v_vec = bincode::serialize(v).unwrap();
//     //         kvs_vec.push((k_vec, v_vec));
//     //     }

//     //     let res = self.k.set(kvs_vec.as_slice(), opts).await?;
//     //     let mut res_kv = vec![];
//     //     if let Some(res) = res {
//     //         for (k_vec, v_vec) in res {
//     //             let des_k = bincode::deserialize::<K>(k_vec.as_slice())
//     //                 .map_err(|err| WsSerialErr::BincodeErr(err))?;
//     //             let des_v = bincode::deserialize::<K::Value>(v_vec.as_slice())
//     //                 .map_err(|err| WsSerialErr::BincodeErr(err))?;
//     //             let pair = (des_k, des_v);
//     //             res_kv.push(pair);
//     //         }
//     //     }

//     //     Ok(Some(res_kv))
//     // }
// }

// pub trait LocalKey: DeserializeOwned + Serialize {
//     type Value: DeserializeOwned + Serialize;
// }

// pub trait KvPair {
//     type Key: DeserializeOwned + Serialize;
//     type Value: DeserializeOwned + Serialize;
// }
