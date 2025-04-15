// pub struct KvStorage {
//     // testmap: SkipMap<Vec<u8>, Vec<u8>>,
//     pub view: KvStorageView,
// }
use axum::async_trait;
use camelpaste::paste;
use dashmap::DashMap;
use enum_as_inner::EnumAsInner;

use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use serde::Serialize;
use serde::{de::DeserializeOwned, ser::SerializeTuple};

use std::io::Cursor;
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Duration;

use super::{m_data_general::DataSetMetaV1, m_os::OperatingSystem, network::m_p2p::P2PModule};
use crate::general::m_data_general::DataSetMetaV2;

use crate::{
    logical_module_view_impl,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef, NodeID},
    util::JoinHandleWrapper,
};
use ws_derive::LogicalModule;

logical_module_view_impl!(View);
logical_module_view_impl!(View, os, OperatingSystem);
logical_module_view_impl!(View, p2p, P2PModule);

pub type KvVersion = usize;

/// attention: non-reentrant
pub struct KeyLock {
    lock: Arc<RwLock<()>>,
}

impl KeyLock {
    pub fn new(lock: Arc<RwLock<()>>) -> Self {
        Self { lock }
    }
    pub fn read<'a>(&'a self) -> RwLockReadGuard<'a, ()> {
        self.lock.read()
    }
    pub fn write<'a>(&'a self) -> RwLockWriteGuard<'a, ()> {
        self.lock.write()
    }
}
// impl KeyLock {
//     pub fn read(self) -> (Self, RwLockReadGuard<'_, ()>) {

//     }
// }

#[derive(LogicalModule)]
pub struct KvStoreEngine {
    key_waitings: DashMap<Vec<u8>, Mutex<Vec<tokio::sync::oneshot::Sender<(KvVersion, KvValue)>>>>,

    /// lock should be free when there is no read or write operation on the key
    ///  let's use cache to replace the map
    /// keyserialed -> lock
    key_locks: moka::sync::Cache<Vec<u8>, Arc<RwLock<()>>>, //RwLock<HashMap<Vec<u8>, KeyLock>>,
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
            key_locks: moka::sync::CacheBuilder::new(100)
                .time_to_live(Duration::from_secs(60)) // lock won't be hold too long
                .build(),
            key_waitings: DashMap::new(),
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

pub enum KvAdditionalRes {
    // SerialedValue(Arc<[u8]>),
}

pub struct KvAdditionalConf {
    // pub with_serialed_value: bool,
}

impl Default for KvAdditionalConf {
    fn default() -> Self {
        Self {
            // with_serialed_value: false,
        }
    }
}

impl KvStoreEngine {
    pub async fn wait_for_new(&self, key: &[u8]) -> (KvVersion, KvValue) {
        let (wait_tx, wait_rx) = tokio::sync::oneshot::channel();
        let _ = self
            .key_waitings
            .entry(key.to_owned())
            .and_modify(|v| {
                v.lock().push(tokio::sync::oneshot::channel().0);
            })
            .or_insert_with(|| Mutex::new(vec![wait_tx]));
        wait_rx.await.unwrap()
    }

    // make sure some operation is atomic
    pub fn with_rwlock<'a>(&'a self, key: &[u8]) -> KeyLock {
        KeyLock::new(
            self.key_locks
                .get_with(key.to_owned(), || Arc::new(RwLock::new(())))
                // .write()
                // .entry(key.to_owned())
                .clone(),
        )
    }

    pub fn set_raw(
        &self,
        key: &[u8],
        value: Vec<u8>,
        locked: bool,
    ) -> WSResult<(KvVersion, Vec<KvAdditionalRes>)> {
        let additinal_res = Vec::new();
        let keybytes = key.to_owned();

        let hold_lock = if locked {
            None
        } else {
            Some(self.with_rwlock(&keybytes))
        };
        let _hold_lock_guard = hold_lock.as_ref().map(|lock| lock.write());

        let db = self.db.get().unwrap();

        // get old version
        let old = self.get_raw(key, true);
        let kvversion = if let Some((old_version, _)) = old {
            old_version + 1
        } else {
            // new version
            1
        };
        // let
        let mut vec_writer = Cursor::new(vec![0; 8 + value.len()]);
        // assert_eq!(bincode::serialized_size(&kvversion).unwrap(), 8);

        bincode::serialize_into(&mut vec_writer, &kvversion);
        let mut vec = vec_writer.into_inner();
        vec.extend(value);

        let _ = db.insert(keybytes, vec).unwrap();

        Ok((kvversion, additinal_res))
    }

    pub fn set<K>(
        &self,
        key: K,
        value: &K::Value,
        locked: bool,
        // additional: KvAdditionalConf,
    ) -> WSResult<(KvVersion, Vec<KvAdditionalRes>)>
    where
        K: KeyType,
    {
        let additinal_res = Vec::new();
        let keybytes = key.make_key();

        let hold_lock = if locked {
            None
        } else {
            Some(self.with_rwlock(&keybytes))
        };
        let _hold_lock_guard = hold_lock.as_ref().map(|lock| lock.write());

        let db = self.db.get().unwrap();

        // get old version
        let old = self.get(&key, true, KvAdditionalConf::default());
        let kvversion = if let Some((old_version, _)) = old {
            old_version + 1
        } else {
            // new version
            1
        };
        // let
        let mut vec_writer = Cursor::new(vec![
            0;
            8 + bincode::serialized_size(&kvversion).unwrap()
                as usize
        ]);
        assert_eq!(bincode::serialized_size(&kvversion).unwrap(), 8);

        bincode::serialize_into(&mut vec_writer, &kvversion);
        bincode::serialize_into(&mut vec_writer, value);

        let _ = db.insert(keybytes, vec_writer.into_inner()).unwrap();

        Ok((kvversion, additinal_res))
    }

    pub fn get_raw(&self, key: &[u8], locked: bool) -> Option<(KvVersion, Vec<u8>)> {
        let hold_lock = if locked {
            None
        } else {
            Some(self.with_rwlock(key))
        };
        let _hold_lock_guard = hold_lock.as_ref().map(|lock| lock.read());

        let res = self.db.get().unwrap().get(key).unwrap();
        res.map(|v| {
            let kvversion = bincode::deserialize::<u64>(&v.as_ref()[0..8]).unwrap() as usize;
            // let value: K::Value = key
            //     .deserialize_from(&v.as_ref()[8..])
            //     .unwrap_or_else(|| panic!("deserialize failed"));
            (kvversion, v.to_vec().drain(0..8).collect())
        })
    }

    pub fn get<'a, K>(
        &self,
        key_: &K,
        locked: bool,
        _additional: KvAdditionalConf,
    ) -> Option<(KvVersion, K::Value)>
    where
        K: KeyType,
    {
        let keybytes = key_.make_key();

        let hold_lock = if locked {
            None
        } else {
            Some(self.with_rwlock(&keybytes))
        };
        let _hold_lock_guard = hold_lock.as_ref().map(|lock| lock.read());

        self.db.get().unwrap().get(keybytes).map_or_else(
            |e| {
                tracing::error!("get kv error: {:?}", e);
                None
            },
            |v| {
                v.map(|v| {
                    // support old no kv version storage format
                    if let Some(value) = key_.deserialize_from(v.as_ref()) {
                        (0, value)
                    } else {
                        let kvversion =
                            bincode::deserialize::<u64>(&v.as_ref()[0..8]).unwrap() as usize;
                        let value: K::Value = key_
                            .deserialize_from(&v.as_ref()[8..])
                            .unwrap_or_else(|| panic!("deserialize failed"));
                        (kvversion, value)
                    }
                })
            },
        )
    }

    pub fn del_raw(&self, key: &[u8], locked: bool) -> WSResult<Option<(KvVersion, Vec<u8>)>> {
        let hold_lock = if locked {
            None
        } else {
            Some(self.with_rwlock(&key))
        };
        let _hold_lock_guard = hold_lock.as_ref().map(|lock| lock.write());

        let res = self.db.get().unwrap().remove(key).unwrap();
        Ok(res.map(|v| {
            let kvversion = bincode::deserialize::<u64>(&v.as_ref()[0..8]).unwrap() as usize;
            // let value: K::Value = key
            //     .deserialize_from(&v.as_ref()[8..])
            //     .unwrap_or_else(|| panic!("deserialize failed"));
            (kvversion, v.to_vec().drain(0..8).collect())
        }))
    }

    pub fn del<K>(&self, key: K, locked: bool) -> WSResult<Option<(KvVersion, K::Value)>>
    where
        K: KeyType,
    {
        let keybytes = key.make_key();

        let hold_lock = if locked {
            None
        } else {
            Some(self.with_rwlock(&keybytes))
        };
        let _hold_lock_guard = hold_lock.as_ref().map(|lock| lock.write());

        let res = self.db.get().unwrap().remove(keybytes).unwrap();
        Ok(res.map(|v| {
            // support old no kv version storage format
            if let Some(value) = key.deserialize_from(v.as_ref()) {
                (0, value)
            } else {
                let kvversion = bincode::deserialize::<u64>(&v.as_ref()[0..8]).unwrap() as usize;
                let value: K::Value = key
                    .deserialize_from(&v.as_ref()[8..])
                    .unwrap_or_else(|| panic!("deserialize failed"));
                (kvversion, value)
            }
        }))
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
        bincode::serialize_into(&mut key, self).unwrap();
        key
    }

    fn deserialize_from(&self, bytes: &[u8]) -> Option<Self::Value>; //-> Result<T>
}

macro_rules! generate_key_struct_content {
    ($id:expr, $latest:ty, [$($old:ty),+]) => {
        type Value= $latest;
        fn id(&self) -> u8 {
            $id
        }
        fn deserialize_from(&self, bytes:&[u8]) -> Option<$latest>//-> Result<T>
        {
            // 尝试最新版本的反序列化
            if let Ok(val) = bincode::deserialize::<$latest>(bytes) {
                return Some(val);
            }

            // 尝试旧版本的反序列化
            $(
                if let Ok(old_val) = bincode::deserialize::<$old>(bytes) {
                    // 如果旧版本反序列化成功，尝试转换为最新版本
                    return Some(<$latest>::from(old_val));
                }
            )*

            None
        }
    };
    ($id:expr, $latest:ty) => {
        type Value= $latest;
        fn id(&self) -> u8 {
            $id
        }
        fn deserialize_from(&self, bytes:&[u8]) -> Option<$latest>//-> Result<T>
        {
            // 尝试最新版本的反序列化
            if let Ok(val) = bincode::deserialize::<$latest>(bytes) {
                return Some(val);
            }
            None
        }
    };
}

macro_rules! generate_key_struct {
    ([$name:ident], $id:expr, $latest:ty, [$($old:ty),+]) => {
        paste! {
            impl KeyType for $name {
                generate_key_struct_content!( $id, $latest, [$($old),+]);
            }
        }
    };
    ([$name:ident], $id:expr, $latest:ty) => {
        paste! {
            impl KeyType for $name {
                generate_key_struct_content!( $id, $latest);
            }
        }
    };
    ([$name:ident,$lifetime:lifetime], $id:expr, $latest:ty, [$($old:ty),+]) => {
        paste! {
            impl KeyType for $name<$lifetime> {
                generate_key_struct_content!( $id, $latest, [$($old),+]);
            }
        }
    };
    ([$name:ident,$lifetime:lifetime], $id:expr, $latest:ty) => {
        paste! {
            impl KeyType for $name<$lifetime> {
                generate_key_struct_content!( $id, $latest);
            }
        }
    };
}

#[derive(EnumAsInner, Debug)]
pub enum KvValue {
    Kv(Vec<u8>),
    KvPosition(NodeID),
    ServiceMeta(Vec<u8>),
    ServiceList(Vec<u8>),
    DataSetMeta(DataSetMetaV2),
    DataSetItem(Vec<u8>),
}

pub struct KeyTypeKv<'a>(pub &'a [u8]);
generate_key_struct!([KeyTypeKv,'_], 1, Vec<u8>);

pub struct KeyTypeKvPosition<'a>(pub &'a [u8]);
generate_key_struct!([KeyTypeKvPosition,'_], 0, NodeID);

pub struct KeyTypeServiceMeta<'a>(pub &'a [u8]);
generate_key_struct!([KeyTypeServiceMeta,'_], 2, Vec<u8>);

pub struct KeyTypeServiceList;
generate_key_struct!([KeyTypeServiceList], 3, Vec<u8>);

pub struct KeyTypeDataSetMeta<'a>(pub &'a [u8]);
generate_key_struct!([KeyTypeDataSetMeta,'_], 4, DataSetMetaV2, [DataSetMetaV1]);

pub struct KeyTypeDataSetItem<'a> {
    pub uid: &'a [u8],
    pub idx: u8,
}
generate_key_struct!([KeyTypeDataSetItem,'_], 5, Vec<u8>);

// impl KeyType for KeyTypeKvPosition<'_> {
//     type Value = NodeID;
//     fn id(&self) -> u8 {
//         0
//     }
// }
// impl KeyType for KeyTypeKv<'_> {
//     type Value = Vec<u8>;
//     fn id(&self) -> u8 {
//         1
//     }
// }
// impl KeyType for KeyTypeServiceMeta<'_> {
//     type Value = Vec<u8>;
//     fn id(&self) -> u8 {
//         2
//     }
// }
// impl KeyType for KeyTypeServiceList {
//     type Value = Vec<u8>;
//     fn id(&self) -> u8 {
//         3
//     }
// }

// impl KeyType for KeyTypeDataSetMeta<'_> {
//     type Value = DataSetMetaV1;
//     fn id(&self) -> u8 {
//         4
//     }
// }

// impl KeyType for KeyTypeDataSetItem<'_> {
//     type Value = Vec<u8>;
//     fn id(&self) -> u8 {
//         5
//     }
// }

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
