use std::mem::ManuallyDrop;

use wasm_serverless_lib::*;
// use externref::externref;
// #[allow(unused_imports)]
// use wasmedge_bindgen::*;
// use wasmedge_bindgen_macro::*;

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

#[no_mangle]
pub fn fn2() {
    kv_set_wrapper("sss".as_bytes(), "hello".as_bytes());
    let v = kv_get_wrapper("sss".as_bytes());
    println!("kv get res {}", std::str::from_utf8(&*v).unwrap())
}
