use serde::{Serialize, Deserialize};
// Requests
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct LoginReq { pub id: String, pub pw: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct VerifyTokenReq { pub token: String }

// Responses
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct LoginResp { pub if_success: i32, pub token: String }
#[derive(Debug, Serialize, Deserialize, Default)]
pub struct VerifyTokenResp { pub if_success: i32, pub new_token: String }
