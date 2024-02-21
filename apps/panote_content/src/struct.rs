use serde::{Serialize, Deserialize};
// Requests
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetNoteMataReq { pub noteid: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetChunkNoteIdsReq { pub noteid: String, pub chunkx: i32, pub chunky: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetNoteBarInfoReq { pub noteid: String, pub notebarid: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateNewBarReq { pub noteid: String, pub x: f32, pub y: f32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct UpdateBarContentReq { pub noteid: String, pub barid: String, pub text: String, pub formatted: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct UpdateBarTransformReq { pub noteid: String, pub barid: String, pub x: f32, pub y: f32, pub w: f32, pub h: f32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RedoReq { pub noteid: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AddPathReq { pub noteid: String, pub from: String, pub to: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetPathInfoReq { pub noteid: String, pub pathid_with_line: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SetPathInfoReq { pub noteid: String, pub pathid_with_line: String, pub type_: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RemovePathReq { pub noteid: String, pub pathid_with_line: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DeleteBarReq { pub noteid: String, pub barid: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ArticleBinderReq { pub bind_unbind_rename: String, pub article_name: String, pub barid: String, pub noteid: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ArticleListReq { pub noteid: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FetchAllNoteBarsEpochReq { pub noteid: String }

// Responses
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetNoteMataResp { pub next_noteid: i32, pub max_chunkx: i32, pub max_chunky: i32, pub min_chunkx: i32, pub min_chunky: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetChunkNoteIdsResp { pub noteids: Vec<serde_json::Value> }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetNoteBarInfoResp { pub x: f32, pub y: f32, pub w: f32, pub h: f32, pub text: String, pub formatted: String, pub connected: Vec<serde_json::Value>, pub epoch: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateNewBarResp { pub chunkx: i32, pub chunky: i32, pub noteids: Vec<serde_json::Value> }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct UpdateBarContentResp { pub new_epoch: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct UpdateBarTransformResp { pub new_epoch: i32, pub chunk_maxx: i32, pub chunk_minx: i32, pub chunk_maxy: i32, pub chunk_miny: i32, pub chunk_change: Vec<serde_json::Value> }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RedoResp { pub redotype: String, pub redovalue: serde_json::Value }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct AddPathResp { pub _1succ_0fail: i32, pub new_epoch_from: i32, pub new_epoch_to: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetPathInfoResp { pub type_: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct SetPathInfoResp {  }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RemovePathResp { pub new_epoch_from: i32, pub new_epoch_to: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct DeleteBarResp { pub chunk_maxx: i32, pub chunk_minx: i32, pub chunk_maxy: i32, pub chunk_miny: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ArticleBinderResp { pub if_success: i32 }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct ArticleListResp { pub if_success: i32, pub list: Vec<serde_json::Value> }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct FetchAllNoteBarsEpochResp { pub bars_id_and_epoch: Vec<serde_json::Value> }
