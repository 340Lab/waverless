use serde_json;
use serde_json::Value;
use std::mem::ManuallyDrop;
use wasm_serverless_lib::*;
mod r#impl;
mod r#struct;
use r#struct::*;

trait AuthApi {
    fn login(&self, req: LoginReq) -> LoginResp;
    fn verify_token(&self, req: VerifyTokenReq) -> VerifyTokenResp;
}

pub struct Impl;

#[no_mangle]
fn login(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe {
        String::from_raw_parts(
            http_json_ptr as *mut u8,
            http_json_len as usize,
            http_json_len as usize,
        )
    };
    if let Ok(req) = serde_json::from_str::<LoginReq>(&json_str) {
        let resp = Impl.login(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn verify_token(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe {
        String::from_raw_parts(
            http_json_ptr as *mut u8,
            http_json_len as usize,
            http_json_len as usize,
        )
    };
    if let Ok(req) = serde_json::from_str::<VerifyTokenReq>(&json_str) {
        let resp = Impl.verify_token(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
    println!("verify_token done");
}
