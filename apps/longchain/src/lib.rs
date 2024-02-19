use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::mem::ManuallyDrop;
use std::str::from_utf8;
use wasm_serverless_lib::*;
#[allow(unused_imports)]
use wasmedge_bindgen::*;
use wasmedge_bindgen_macro::*;
use wasmedge_wasi_helper::wasmedge_wasi_helper::_initialize;
// extern "C" {
//     fn kv_set(kptr: *const u8, klen: i32, v: *const u8, vlen: i32);
//     fn kv_get_len(kptr: *const u8, klen: i32, vlen: &mut i32, id: &mut i32);
//     fn kv_get(id: i32, vptr: *const u8);
// }

// fn kv_set_wrapper(key: &[u8], value: &[u8]) {
//     unsafe {
//         kv_set(
//             key.as_ptr(),
//             key.len() as i32,
//             value.as_ptr(),
//             value.len() as i32,
//         )
//     };
// }

// fn kv_get_wrapper(key: &[u8]) -> Vec<u8> {
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

// #[no_mangle]
// pub fn allocate(size: usize) -> *mut u8 {
//     unsafe {
//         let mut buffer = Vec::with_capacity(size);
//         let pointer = buffer.as_mut_ptr();
//         std::mem::forget(buffer);

//         pointer
//     }
// }

#[no_mangle]
pub fn chain_begin() {
    let res = KvBatch::new()
        .then_lock("chain_lock".as_bytes())
        .then_get("chain_count".as_bytes())
        .finally_call();
    let lock_id = if let KvResult::Lock(lock_id) = res[0] {
        lock_id
    } else {
        panic!("chain_begin lock failed");
    };
    let mut batch = KvBatch::new();
    if let KvResult::Get(Some(count)) = &res[1] {
        let count_str = from_utf8(count).unwrap();
        println!("chain count: {}", count_str);
        let count = count_str.parse::<u32>().unwrap();
        if count < 100 {
            batch = batch.then_set(
                "chain_count".as_bytes(),
                format!("{}", count + 1).as_bytes(),
            );
        } else {
            batch = batch.then_set("chain_count".as_bytes(), format!("{}", 0).as_bytes());
        }
    } else {
        batch = batch.then_set("chain_count".as_bytes(), "1".as_bytes());
    }
    batch
        .then_unlock("chain_lock".as_bytes(), lock_id)
        .finally_call();
}

// #[no_mangle]
// pub fn chain_loop(key: *mut u8, key_len: u32) {
//     let res = KvBatch::new().then_get("chain_count").finally_call();
//     match res[0] {
//         KvResult::Get(value) => {
//             if let Some(value) = value {
//                 let mut count = value.parse::<u32>().unwrap();
//                 count += 1;
//                 if count < 10 {
//                     let key = unsafe { std::slice::from_raw_parts(key, key_len as usize) };
//                     let key = std::str::from_utf8(key).unwrap();
//                     KvBatch::new()
//                         .then_set(key, &value)
//                         .then_set("chain_count", &format!("{}", count))
//                         .finally_call();
//                 } else {
//                     KvBatch::new().then_unlock("chain_lock").finally_call();
//                 }
//             }
//         }
//         _ => {
//             panic!("chain_loop get chain_count failed")
//         }
//     }
// }
