use crate::general::network::http_handler::ApiHandlerImpl;
use async_trait::async_trait;
use axum::{http::StatusCode, routing::post, Json, Router};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use crate::metrics::metrics_handler;
use axum::routing::get;

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeBasic {
    pub name: String,
    pub online: bool,
    pub ip: String,
    pub ssh_port: String,
    pub cpu_sum: f64,
    pub cpu_cur: f64,
    pub mem_sum: f64,
    pub mem_cur: f64,
    pub passwd: String,
    pub system: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Action {
    pub name: String,
    pub cmd: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ServiceBasic {
    pub name: String,
    pub node: String,
    pub dir: String,
    pub actions: Vec<Action>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum AddServiceResp {
    Succ {},
    Template { nodes: Vec<String> },
    Fail { msg: String },
}

impl AddServiceResp {
    fn id(&self) -> u32 {
        match self {
            AddServiceResp::Succ { .. } => 1,
            AddServiceResp::Template { .. } => 2,
            AddServiceResp::Fail { .. } => 3,
        }
    }
    pub fn serialize(&self) -> Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct AddServiceReq {
    pub service: ServiceBasic,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum DeleteServiceResp {
    Succ {},
    Fail { msg: String },
}

impl DeleteServiceResp {
    fn id(&self) -> u32 {
        match self {
            DeleteServiceResp::Succ { .. } => 1,
            DeleteServiceResp::Fail { .. } => 2,
        }
    }
    pub fn serialize(&self) -> Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DeleteServiceReq {
    pub service: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GetServiceListResp {
    Exist { services: Vec<ServiceBasic> },
}

impl GetServiceListResp {
    fn id(&self) -> u32 {
        match self {
            GetServiceListResp::Exist { .. } => 1,
        }
    }
    pub fn serialize(&self) -> Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RunServiceActionResp {
    Succ { output: String },
    Fail { msg: String },
}

impl RunServiceActionResp {
    fn id(&self) -> u32 {
        match self {
            RunServiceActionResp::Succ { .. } => 1,
            RunServiceActionResp::Fail { .. } => 2,
        }
    }
    pub fn serialize(&self) -> Value {
        json!({
            "id": self.id(),
            "kernel": serde_json::to_value(self).unwrap(),
        })
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct RunServiceActionReq {
    pub service: String,
    pub action_cmd: String,
    pub sync: bool,
}

#[async_trait]
pub trait ApiHandler {
    async fn handle_add_service(&self, req: AddServiceReq) -> AddServiceResp;

    async fn handle_delete_service(&self, req: DeleteServiceReq) -> DeleteServiceResp;

    async fn handle_get_service_list(&self) -> GetServiceListResp;

    async fn handle_run_service_action(&self, req: RunServiceActionReq) -> RunServiceActionResp;
}

pub fn add_routers(mut router: Router) -> Router {
    async fn add_service(Json(req): Json<AddServiceReq>) -> (StatusCode, Json<Value>) {
        (
            StatusCode::OK,
            Json(ApiHandlerImpl.handle_add_service(req).await.serialize()),
        )
    }
    router = router.route("/add_service", post(add_service));

    async fn delete_service(Json(req): Json<DeleteServiceReq>) -> (StatusCode, Json<Value>) {
        (
            StatusCode::OK,
            Json(ApiHandlerImpl.handle_delete_service(req).await.serialize()),
        )
    }
    router = router.route("/delete_service", post(delete_service));

    async fn get_service_list() -> (StatusCode, Json<Value>) {
        (
            StatusCode::OK,
            Json(ApiHandlerImpl.handle_get_service_list().await.serialize()),
        )
    }
    router = router.route("/get_service_list", post(get_service_list));

    async fn run_service_action(Json(req): Json<RunServiceActionReq>) -> (StatusCode, Json<Value>) {
        (
            StatusCode::OK,
            Json(
                ApiHandlerImpl
                    .handle_run_service_action(req)
                    .await
                    .serialize(),
            ),
        )
    }
    router = router.route("/run_service_action", post(run_service_action));
    router
}
