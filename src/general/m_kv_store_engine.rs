// pub struct KvStorage {
//     // testmap: SkipMap<Vec<u8>, Vec<u8>>,
//     pub view: KvStorageView,
// }

use super::{m_data_general::DataSetMeta, m_os::OperatingSystem, network::m_p2p::P2PModule};
use crate::{
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef, NodeID},
    util::JoinHandleWrapper,
};
use axum::async_trait;
use bincode::serialize;
use bincode::serialize_into;
use serde::Serialize;
use serde::{de::DeserializeOwned, ser::SerializeTuple};
use std::sync::OnceLock;
use ws_derive::LogicalModule;

logical_module_view_impl!(View);
logical_module_view_impl!(View, os, OperatingSystem);
logical_module_view_impl!(View, p2p, P2PModule);

#[derive(LogicalModule)]
pub struct KvStoreEngine {
    db: OnceLock<sled::Db>,
    view: View,
}

#[async_trait]
impl LogicalModule for KvStoreEngine {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            db: OnceLock::new(),
            view: View::new(args.logical_modules_ref.clone()),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let db_path = self.view.os().file_path.join(format!(
            "kv_store_engine_{}",
            self.view.p2p().nodes_config.this_node()
        ));
        let _ = self.db.get_or_init(|| {
            let db = sled::Config::default()
                .path(&db_path)
                .create_new(true)
                .open()
                .map_or_else(
                    |_e| sled::Config::default().path(db_path).open().unwrap(),
                    |v| v,
                );
            db
        });
        Ok(vec![])
    }
}

impl KvStoreEngine {
    pub fn set<K>(&self, key: K, value: &K::Value)
    where
        K: KeyType,
    {
        let key = key.make_key();
        let _ = self
            .db
            .get()
            .unwrap()
            .insert(key, serialize(value).unwrap())
            .unwrap();
    }
    pub fn get<'a, K>(&self, key: K) -> Option<K::Value>
    where
        K: KeyType,
    {
        let key = key.make_key();
        self.db.get().unwrap().get(key).map_or_else(
            |e| {
                tracing::error!("get kv error: {:?}", e);
                None
            },
            |v| v.map(|v| bincode::deserialize_from(v.as_ref()).unwrap()),
        )
    }
    pub fn del<K>(&self, key: K)
    where
        K: KeyType,
    {
        let key = key.make_key();
        let _ = self.db.get().unwrap().remove(key).unwrap();
    }
    pub fn flush(&self) {
        let _ = self.db.get().unwrap().flush().unwrap();
    }
}

pub trait KeyType: Serialize {
    type Value: Serialize + DeserializeOwned;
    fn id(&self) -> u8;
    fn make_key(&self) -> Vec<u8> {
        let mut key = Vec::with_capacity(1 + bincode::serialized_size(self).unwrap() as usize);
        key.push(self.id());
        serialize_into(&mut key, self).unwrap();
        key
    }
}

pub struct KeyTypeKv<'a>(pub &'a [u8]);

pub struct KeyTypeKvPosition<'a>(pub &'a [u8]);

pub struct KeyTypeServiceMeta<'a>(pub &'a [u8]);

pub struct KeyTypeServiceList;

pub struct KeyTypeDataSetMeta<'a>(pub &'a [u8]);

pub struct KeyTypeDataSetItem<'a> {
    pub uid: &'a [u8],
    pub idx: u8,
}

impl KeyType for KeyTypeKvPosition<'_> {
    type Value = NodeID;
    fn id(&self) -> u8 {
        0
    }
}
impl KeyType for KeyTypeKv<'_> {
    type Value = Vec<u8>;
    fn id(&self) -> u8 {
        1
    }
}
impl KeyType for KeyTypeServiceMeta<'_> {
    type Value = Vec<u8>;
    fn id(&self) -> u8 {
        2
    }
}
impl KeyType for KeyTypeServiceList {
    type Value = Vec<u8>;
    fn id(&self) -> u8 {
        3
    }
}

impl KeyType for KeyTypeDataSetMeta<'_> {
    type Value = DataSetMeta;
    fn id(&self) -> u8 {
        4
    }
}

impl KeyType for KeyTypeDataSetItem<'_> {
    type Value = Vec<u8>;
    fn id(&self) -> u8 {
        5
    }
}

impl Serialize for KeyTypeKvPosition<'_> {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl Serialize for KeyTypeKv<'_> {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl Serialize for KeyTypeServiceMeta<'_> {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl Serialize for KeyTypeServiceList {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_unit()
    }
}

impl Serialize for KeyTypeDataSetMeta<'_> {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
    }
}

impl Serialize for KeyTypeDataSetItem<'_> {
    fn serialize<S: serde::ser::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let mut tup = serializer.serialize_tuple(2)?;
        tup.serialize_element(self.uid)?;
        tup.serialize_element(&self.idx)?;
        tup.end()
    }
}
