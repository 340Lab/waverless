use serde::{Serialize, Deserialize};
// Requests
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetNotesMataReq {  }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateNewNoteReq {  }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RenameNoteReq { pub noteid: String, pub name: String }

// Responses
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct GetNotesMataResp { pub node_id_name_list: Vec<serde_json::Value> }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct CreateNewNoteResp {  }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct RenameNoteResp {  }
