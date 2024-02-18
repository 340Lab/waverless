use std::mem::ManuallyDrop;
use std::vec::Vec;

// use externref::externref;
// #[allow(unused_imports)]
// use wasmedge_bindgen::*;
// use wasmedge_bindgen_macro::*;

extern "C" {
    fn kv_set(kptr: *const u8, klen: i32, v: *const u8, vlen: i32);
    fn kv_get_len(kptr: *const u8, klen: i32, vlen: &mut i32, id: &mut i32);
    fn kv_get(id: i32, vptr: *const u8);
    fn open_file(fname: *const u8, fnamelen: i32, fd: &mut i32);
    fn read_file_at(fd: i32, buf: *const u8, buflen: i32, offset: i32, readlen: &mut i32);
}

pub fn kv_set_wrapper(key: &[u8], value: &[u8]) {
    unsafe {
        kv_set(
            key.as_ptr(),
            key.len() as i32,
            value.as_ptr(),
            value.len() as i32,
        )
    };
}

pub fn kv_get_wrapper(key: &[u8]) -> Vec<u8> {
    unsafe {
        let mut veclen: i32 = 0;
        let mut id: i32 = 0;
        kv_get_len(key.as_ptr(), key.len() as i32, &mut veclen, &mut id);

        let mut vec = Vec::new();
        if veclen > 0 {
            vec.resize(veclen as usize, 0);
            kv_get(id, vec.as_ptr());
        }

        vec
    }
}

pub struct HostFile {
    fd: i32,
}

impl HostFile {
    pub fn open(fname: &str) -> Self {
        let mut fd = 0;
        unsafe {
            open_file(fname.as_ptr(), fname.len() as i32, &mut fd);
        }
        Self { fd }
    }

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
