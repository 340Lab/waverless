use serde_json;
use serde_json::Value;
use std::mem::ManuallyDrop;
use wasm_serverless_lib::*;
mod r#struct;
mod r#impl;
use r#struct::*;

trait ContentApi {
fn get_note_mata(&self,req: GetNoteMataReq) -> GetNoteMataResp;
fn get_chunk_note_ids(&self,req: GetChunkNoteIdsReq) -> GetChunkNoteIdsResp;
fn get_note_bar_info(&self,req: GetNoteBarInfoReq) -> GetNoteBarInfoResp;
fn create_new_bar(&self,req: CreateNewBarReq) -> CreateNewBarResp;
fn update_bar_content(&self,req: UpdateBarContentReq) -> UpdateBarContentResp;
fn update_bar_transform(&self,req: UpdateBarTransformReq) -> UpdateBarTransformResp;
fn redo(&self,req: RedoReq) -> RedoResp;
fn add_path(&self,req: AddPathReq) -> AddPathResp;
fn get_path_info(&self,req: GetPathInfoReq) -> GetPathInfoResp;
fn set_path_info(&self,req: SetPathInfoReq) -> SetPathInfoResp;
fn remove_path(&self,req: RemovePathReq) -> RemovePathResp;
fn delete_bar(&self,req: DeleteBarReq) -> DeleteBarResp;
fn article_binder(&self,req: ArticleBinderReq) -> ArticleBinderResp;
fn article_list(&self,req: ArticleListReq) -> ArticleListResp;
fn fetch_all_note_bars_epoch(&self,req: FetchAllNoteBarsEpochReq) -> FetchAllNoteBarsEpochResp;
}

pub struct Impl;

#[no_mangle]
fn get_note_mata(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<GetNoteMataReq>(&json_str) {
        let resp = Impl.get_note_mata(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn get_chunk_note_ids(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<GetChunkNoteIdsReq>(&json_str) {
        let resp = Impl.get_chunk_note_ids(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn get_note_bar_info(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<GetNoteBarInfoReq>(&json_str) {
        let resp = Impl.get_note_bar_info(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn create_new_bar(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<CreateNewBarReq>(&json_str) {
        let resp = Impl.create_new_bar(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn update_bar_content(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<UpdateBarContentReq>(&json_str) {
        let resp = Impl.update_bar_content(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn update_bar_transform(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<UpdateBarTransformReq>(&json_str) {
        let resp = Impl.update_bar_transform(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn redo(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<RedoReq>(&json_str) {
        let resp = Impl.redo(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn add_path(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<AddPathReq>(&json_str) {
        let resp = Impl.add_path(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn get_path_info(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<GetPathInfoReq>(&json_str) {
        let resp = Impl.get_path_info(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn set_path_info(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<SetPathInfoReq>(&json_str) {
        let resp = Impl.set_path_info(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn remove_path(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<RemovePathReq>(&json_str) {
        let resp = Impl.remove_path(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn delete_bar(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<DeleteBarReq>(&json_str) {
        let resp = Impl.delete_bar(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn article_binder(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<ArticleBinderReq>(&json_str) {
        let resp = Impl.article_binder(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn article_list(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<ArticleListReq>(&json_str) {
        let resp = Impl.article_list(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
#[no_mangle]
fn fetch_all_note_bars_epoch(http_json_ptr: i32, http_json_len: i32) {
    let json_str = unsafe { String::from_raw_parts(http_json_ptr as *mut u8, http_json_len as usize, http_json_len as usize) };
    if let Ok(req) = serde_json::from_str::<FetchAllNoteBarsEpochReq>(&json_str) {
        let resp = Impl.fetch_all_note_bars_epoch(req);
        let resp_str = serde_json::to_string(&resp).unwrap();
        unsafe { write_result(resp_str.as_ptr(), resp_str.len() as i32) }
    }
}
