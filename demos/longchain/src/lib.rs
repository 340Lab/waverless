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

const LOOP_TIME: u32 = 100;

#[no_mangle]
pub fn chain_begin() {
    let res = KvBatch::new()
        .then_set("chain_count".as_bytes(), "1".as_bytes())
        .finally_call();
}

fn get_count() -> u32 {
    let res = KvBatch::new()
        .then_get("chain_count".as_bytes())
        .finally_call();
    match &res[0] {
        KvResult::Get(value) => {
            if let Some(value) = value.as_ref().map(|v| {
                std::str::from_utf8(v.as_slice())
                    .map_err(|e| {
                        panic!("get_count get chain_count failed: {:?}", e);
                    })
                    .unwrap()
            }) {
                value.parse::<u32>().unwrap()
            } else {
                panic!("get_count get chain_count failed")
            }
        }
        _ => {
            panic!("get_count get chain_count failed")
        }
    }
}

#[no_mangle]
pub fn chain_loop(key: *mut u8, key_len: u32) {
    let key = unsafe { std::slice::from_raw_parts(key, key_len as usize) };
    let key = std::str::from_utf8(key).unwrap();
    assert_eq!(key, "chain_count");

    let res = KvBatch::new()
        .then_get("chain_count".as_bytes())
        .finally_call();
    match &res[0] {
        KvResult::Get(value) => {
            if let Some(value) = value.as_ref().map(|v| {
                std::str::from_utf8(v.as_slice())
                    .map_err(|e| {
                        panic!("chain_loop get chain_count failed: {:?}", e);
                    })
                    .unwrap()
            }) {
                let mut count = value.parse::<u32>().unwrap();
                count += 1;
                println!("chain_loop count: {}", count - 1);
                if count - 1 < LOOP_TIME {
                    KvBatch::new()
                        .then_set("chain_count".as_bytes(), format!("{}", count).as_bytes())
                        .finally_call();
                }
            }
        }
        _ => {
            panic!("chain_loop get chain_count failed")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chain_begin() {
        chain_begin();
    }

    #[test]
    fn test_chain_loop() {
        chain_begin();
        for _ in 0..LOOP_TIME - 1 {
            let bfcnt = get_count();
            chain_loop(
                "chain_count".as_bytes().as_ptr() as *mut u8,
                "chain_count".len() as u32,
            );
            let afcnt = get_count();
            assert_eq!(afcnt, bfcnt + 1);
        }

        // the last time don't update the count
        let bfcnt = get_count();
        chain_loop(
            "chain_count".as_bytes().as_ptr() as *mut u8,
            "chain_count".len() as u32,
        );
        let afcnt = get_count();
        assert_eq!(afcnt, bfcnt);
    }
}
