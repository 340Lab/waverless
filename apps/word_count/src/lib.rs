use std::fs::File;
use std::io::BufReader;
use std::io::Read;
use std::mem::ManuallyDrop;
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
pub fn split_file() {
    _initialize();
    let file_path = "files/random_words.txt";
    println!("split file start");
    // read file
    let mut file = HostFile::open(file_path);
    let mut slice_id = 0;
    let mut offset: usize = 0;
    // read 1 mb slice, align to \n, write to kv store

    // 读取 1 MB 的数据
    let mut buffer = Vec::with_capacity(1024 * 1024);
    loop {
        println!("buffer begin len {}", buffer.len());
        let len = file.read_at(offset, &mut buffer);
        offset += len;

        println!("split file one slice with len {} {}", buffer.len(), len);
        if buffer.len() == 0 {
            break;
        }
        // find last \n
        if let Some(last) = buffer
            .iter()
            .enumerate()
            .rev()
            .find(|(_, &b)| b == b'\n')
            .map(|(i, _)| i)
        {
            println!("split file one slice with last \\n at{}", last);
            kv_set_wrapper(
                format!("wordcount_slice_{}", slice_id).as_bytes(),
                &buffer[..last],
            );
            slice_id += 1;
            if last + 1 < buffer.len() {
                for i in last + 1..buffer.len() {
                    buffer[i - last - 1] = buffer[i];
                }
                buffer.truncate(buffer.len() - last - 1);
            } else {
                buffer.clear();
            }

            println!("buffer end len {}", buffer.len());
        } else {
            break;
        }
    }
}

#[no_mangle]
pub fn handle_one_slice(key: *mut u8, key_len: u32) {
    let key = unsafe { Vec::from_raw_parts(key, key_len as usize, key_len as usize) };
    // let val = kv_get_wrapper(&key);
    println!(
        "handle_one_slice k {}",
        std::str::from_utf8(&key).unwrap(),
        // std::str::from_utf8(&val).unwrap(),
    );
}
