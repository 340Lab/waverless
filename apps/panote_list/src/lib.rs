use serde_json;
use serde_json::Value;
use std::mem::ManuallyDrop;
use wasm_serverless_lib::*;
mod r#struct;
mod r#impl;
use r#struct::*;

trait ListApi {
fn get_notes_mata(&self,req: GetNotesMataReq) -> GetNotesMataResp;
fn create_new_note(&self,req: CreateNewNoteReq) -> CreateNewNoteResp;
fn rename_note(&self,req: RenameNoteReq) -> RenameNoteResp;
}

pub struct Impl;

#[no_mangle]
fn get_notes_mata(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<GetNotesMataReq>(&json_str) {
        let resp = Impl.get_notes_mata(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn create_new_note(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<CreateNewNoteReq>(&json_str) {
        let resp = Impl.create_new_note(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn rename_note(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<RenameNoteReq>(&json_str) {
        let resp = Impl.rename_note(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
