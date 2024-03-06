use parking_lot::Mutex;
use std::collections::BTreeMap;
use std::mem::ManuallyDrop;
use std::vec::Vec;
pub use wasmedge_bindgen::*;
// use externref::externref;
// #[allow(unused_imports)]
// use wasmedge_bindgen::*;
// use wasmedge_bindgen_macro::*;

#[cfg(not(feature = "test"))]
extern "C" {
    // fn kv_set(kptr: *const u8, klen: i32, v: *const u8, vlen: i32);
    // fn kv_get_len(kptr: *const u8, klen: i32, vlen: &mut i32, id: &mut i32);
    // fn kv_get(id: i32, vptr: *const u8);
    fn kv_batch_ope(ope_ptr: *const i32, ope_len: i32, ope_id: &mut i32);
    fn kv_batch_res(ope_id: i32, args_ptr: *const i32, args_len: i32);
    fn open_file(fname: *const u8, fnamelen: i32, fd: &mut i32);
    fn read_file_at(fd: i32, buf: *const u8, buflen: i32, offset: i32, readlen: &mut i32);
    pub fn write_result(res_ptr: *const u8, res_len: i32);
}

#[cfg(not(feature = "test"))]
pub trait KvResTrans {
    fn res_str(&self) -> Option<&str>;
}

#[cfg(not(feature = "test"))]
impl KvResTrans for Vec<KvResult> {
    fn res_str(&self) -> Option<&str> {
        if let KvResult::Get(Some(s)) = &self[0] {
            return std::str::from_utf8(s).map_or_else(
                |err| {
                    println!("err when get kv res str:{}", err);
                    None
                },
                |v| Some(v),
            );
        }
        None
    }
}

// pub enum KvOpe {
//     Set(&[u8], &[u8]),
//     Get(&[u8]),
//     Delete(&[u8]),
//     Lock(&[u8]),
//     Unlock(u32),
// }

#[cfg(not(feature = "test"))]
const SET_ID: i32 = 1;
#[cfg(not(feature = "test"))]
const GET_ID: i32 = 2;
#[cfg(not(feature = "test"))]
const LOCK_ID: i32 = 3;
#[cfg(not(feature = "test"))]
const DELETE_ID: i32 = 4;

pub enum KvResult {
    Set,
    GetLen(i32),
    Get(Option<Vec<u8>>),
    Delete,
    Lock(u32),
    Unlock,
}

impl KvResult {
    #[cfg(not(feature = "test"))]
    fn one_ptr(&self) -> Option<i32> {
        match self {
            KvResult::Set => None,
            KvResult::GetLen(len) => Some(len as *const i32 as i32),
            KvResult::Get(vec) => vec.as_ref().map(|v| v.as_ptr() as i32),
            KvResult::Delete => None,
            KvResult::Lock(lockid) => Some(lockid as *const u32 as i32),
            KvResult::Unlock => None,
        }
    }
}

#[cfg(feature = "test")]
lazy_static::lazy_static! {
    static ref KV_BATCH: Mutex<KvBatchInner> = Mutex::new(KvBatchInner::new());
}

#[cfg(feature = "test")]
struct KvBatchInner {
    map: BTreeMap<Vec<u8>, Vec<u8>>,
}

#[cfg(feature = "test")]
impl KvBatchInner {
    pub fn new() -> Self {
        Self {
            map: BTreeMap::new(),
        }
    }
}

#[cfg(feature = "test")]
pub struct KvBatch {
    res: Vec<KvResult>,
}

// moke impl
#[cfg(feature = "test")]
impl KvBatch {
    pub fn new() -> Self {
        Self { res: Vec::new() }
    }
    pub fn reset(mut self) -> Self {
        self.res.clear();
        self
    }
    pub fn then_set(mut self, key: &[u8], value: &[u8]) -> Self {
        let mut k = KV_BATCH.lock();
        k.map.insert(key.to_vec(), value.to_vec());
        self.res.push(KvResult::Set);
        self
    }
    pub fn then_get(mut self, key: &[u8]) -> Self {
        let k = KV_BATCH.lock();
        self.res.push(KvResult::Get(k.map.get(key).cloned()));
        self
    }
    pub fn then_delete(mut self, key: &[u8]) -> Self {
        let mut k = KV_BATCH.lock();
        k.map.remove(key);
        self.res.push(KvResult::Delete);
        self
    }
    pub fn then_lock(mut self, _key: &[u8]) -> Self {
        tracing::warn!("mock lock not implemented");

        self.res.push(KvResult::Lock(0));
        self
    }
    pub fn then_unlock(self, _key: &[u8], _id: u32) -> Self {
        tracing::warn!("mock unlock not implemented");
        self
    }
    pub fn finally_call(self) -> Vec<KvResult> {
        self.res
    }
}

#[cfg(not(feature = "test"))]
pub struct KvBatch {
    batch_args: Vec<i32>,
    results: Vec<KvResult>,
}

#[cfg(not(feature = "test"))]
impl KvBatch {
    pub fn new() -> Self {
        Self {
            batch_args: vec![0],
            results: Vec::new(),
        }
    }
    pub fn reset(mut self) -> Self {
        self.batch_args.clear();
        self.batch_args.push(0);
        self
    }
    pub fn then_set(mut self, key: &[u8], value: &[u8]) -> Self {
        self.batch_args.push(SET_ID as i32);
        self.batch_args.push(key.as_ptr() as i32);
        self.batch_args.push(key.len() as i32);
        self.batch_args.push(value.as_ptr() as i32);
        self.batch_args.push(value.len() as i32);
        self.results.push(KvResult::Set);
        self
    }
    pub fn then_get(mut self, key: &[u8]) -> Self {
        self.batch_args.push(GET_ID as i32);
        self.batch_args.push(key.as_ptr() as i32);
        self.batch_args.push(key.len() as i32);
        self.results.push(KvResult::GetLen(0));
        self.batch_args
            .push(self.results.iter().rev().next().unwrap().one_ptr().unwrap());

        self
    }
    pub fn then_delete(mut self, key: &[u8]) -> Self {
        self.batch_args.push(DELETE_ID as i32);
        self.batch_args.push(key.as_ptr() as i32);
        self.batch_args.push(key.len() as i32);
        self.results.push(KvResult::Delete);

        self
    }
    pub fn then_lock(mut self, key: &[u8]) -> Self {
        self.batch_args.push(LOCK_ID as i32);
        self.batch_args.push(key.as_ptr() as i32);
        self.batch_args.push(key.len() as i32);
        self.batch_args.push(-1);
        self.results.push(KvResult::Lock(0));
        self.batch_args
            .push(self.results.iter().rev().next().unwrap().one_ptr().unwrap());

        self
    }
    pub fn then_unlock(mut self, key: &[u8], id: u32) -> Self {
        self.batch_args.push(LOCK_ID as i32);
        self.batch_args.push(key.as_ptr() as i32);
        self.batch_args.push(key.len() as i32);
        self.batch_args.push(id as i32);
        self.results.push(KvResult::Unlock);

        self
    }
    pub fn finally_call(mut self) -> Vec<KvResult> {
        self.batch_args[0] = self.results.len() as i32;
        println!("batch args: {:?}", self.batch_args);
        let mut id = 0;
        unsafe {
            kv_batch_ope(
                self.batch_args.as_ptr(),
                self.batch_args.len() as i32,
                &mut id,
            )
        };
        self.batch_args.clear();
        for (ope_idx, res) in self.results.iter_mut().enumerate() {
            let mut is_get_len = None;
            match res {
                KvResult::GetLen(len) => {
                    is_get_len = Some(*len);
                }
                _ => {}
            }
            if let Some(len) = is_get_len {
                if len >= 0 {
                    let vec = vec![0; len as usize];
                    *res = KvResult::Get(Some(vec));
                    self.batch_args.push(ope_idx as i32);
                    self.batch_args.push(res.one_ptr().unwrap());
                } else {
                    *res = KvResult::Get(None);
                }
            }
        }
        unsafe { kv_batch_res(id, self.batch_args.as_ptr(), self.batch_args.len() as i32) };
        self.results
    }
}

// pub fn kv_set_wrapper(key: &[u8], value: &[u8]) {
//     unsafe {
//         kv_set(
//             key.as_ptr(),
//             key.len() as i32,
//             value.as_ptr(),
//             value.len() as i32,
//         )
//     };
// }

// pub fn kv_get_wrapper(key: &[u8]) -> Vec<u8> {
//     unsafe {
//         let mut veclen: i32 = 0;
//         let mut id: i32 = 0;
//         kv_get_len(key.as_ptr(), key.len() as i32, &mut veclen, &mut id);

//         let mut vec = Vec::new();
//         if veclen > 0 {
//             vec.resize(veclen as usize, 0);
//             kv_get(id, vec.as_ptr());
//         }

//         vec
//     }
// }

#[cfg(not(feature = "test"))]
pub struct HostFile {
    fd: i32,
}
#[cfg(not(feature = "test"))]
impl HostFile {
    pub fn open(fname: &str) -> Self {
        let mut fd = 0;
        unsafe {
            open_file(fname.as_ptr(), fname.len() as i32, &mut fd);
        }
        Self { fd }
    }

    // data will be append to `buf` until the file is read to the end or `buf` is full to capacity
    pub fn read_at(&self, offset: usize, buf: &mut Vec<u8>) -> usize {
        let mut readlen = 0;
        let buf_old_len = buf.len();
        unsafe {
            read_file_at(
                self.fd,
                (buf.as_ptr() as usize + buf.len()) as *const u8,
                (buf.capacity() - buf.len()) as i32,
                offset as i32,
                &mut readlen,
            );
            buf.set_len(buf_old_len + readlen as usize);
        }
        readlen as usize
    }
}
