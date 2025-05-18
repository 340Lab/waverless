use super::{DataGeneral, DataGeneralView};
use crate::general::data::m_data_general::dataitem::DataItemArgWrapper;
use crate::general::data::m_data_general::new_data_unique_id_fn_kv;
use crate::general::network::proto;
use crate::general::network::proto_ext::data_ope_role::ProtoExtDataOpeRole;
use crate::general::network::proto_ext::ProtoExtDataItem;
use crate::result::WSResultExt;
use crate::with_option;
use crate::{result::WSResult, util::syntactic_discipline::with_option};
use async_raft::State as RaftState;
use axum::extract::{Multipart, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use serde::Serialize;
use std::io;

impl DataGeneral {
    pub fn register_http(&self) -> WSResult<()> {
        let mut router_holder = self.view.http_handler().building_router();

        with_option!(router_holder.option_mut(), router => {
            // router.route("/upload_data", post(handle_write_data))
            router.merge(Router::new().route("/upload_data", post(handle_upload_data).with_state(self.view.clone())))
        });
        Ok(())
    }
}

#[derive(Debug, Serialize)]
struct UploadDataResponse {
    err_msg: String,
}
// impl UploadDataResponse {
//     fn is_err(&self) -> bool {
//         self.err_msg.len() > 0
//     }
// }

// #[derive(Debug, Serialize)]
// struct UploadDataResponses {
//     responses: Vec<UploadDataResponse>,
// }

// impl UploadDataResponses {
//     fn contains_err(&self) -> bool {
//         self.responses.iter().any(|r| r.is_err())
//     }
// }

async fn handle_upload_data(
    State(view): State<DataGeneralView>,
    mut multipart: Multipart,
) -> Response {
    // let mut responses = UploadDataResponses {
    //     responses: Vec::new(),
    // };
    let mut data_items = Vec::new();
    let mut first_field = true;
    let mut unique_id: Option<String> = None;
    while let Ok(Some(field)) = multipart.next_field().await {
        if first_field {
            first_field = false;
            // require name for first field as unique id
            let Some(name) = field.name() else {
                // responses.responses.push(UploadDataResponse {
                //     err_msg: "field name is None".to_string(),
                // });
                return (
                    StatusCode::BAD_REQUEST,
                    serde_json::to_string(&UploadDataResponse {
                        err_msg: "field name is None".to_string(),
                    })
                    .unwrap(),
                )
                    .into_response();
            };
            unique_id = Some(name.to_string());
        }

        let data = match field
            .bytes()
            .await
            .map_err(|e| format!("Failed to read file data: {}", e))
        {
            Ok(data) => data,
            Err(err) => {
                return (
                    StatusCode::BAD_REQUEST,
                    serde_json::to_string(&UploadDataResponse { err_msg: err }).unwrap(),
                )
                    .into_response();
            }
        };

        data_items.push(DataItemArgWrapper::new(proto::DataItem::new_mem_data(
            data.to_vec(),
        )));
        // let name = field.name().unwrap_or("").to_string();

        // if name == "app_name" {
        //     app_name = field.text().await.map_err(|e| {
        //         (StatusCode::BAD_REQUEST, format!("Failed to read app name: {}", e))
        //     })?;
        // } else if name == "file" {
        //     file_data = Some(field.bytes().await.map_err(|e| {
        //         (StatusCode::BAD_REQUEST, format!("Failed to read file data: {}", e))
        //     })?);
        // }
    }
    let Some(unique_id) = unique_id else {
        return (
            StatusCode::BAD_REQUEST,
            serde_json::to_string(&UploadDataResponse {
                err_msg: "unique id is not specified".to_string(),
            })
            .unwrap(),
        )
            .into_response();
    };

    tracing::debug!("data received: {}, start writing to system", &unique_id);
    let taskid = view.executor().register_sub_task();
    let taskid_value = taskid.task_id;
    let _ = view
        .data_general()
        .write_data(
            new_data_unique_id_fn_kv(unique_id.as_bytes()),
            // vec![DataItemArgWrapper::new(proto::DataItem::new_mem_data(
            //     data.to_vec(),
            // ))],
            data_items,
            Some((
                view.p2p().nodes_config.this_node(),
                proto::DataOpeType::Write,
                proto::data_schedule_context::OpeRole::new_upload_data(),
                taskid,
            )),
        )
        .await
        .todo_handle("write data failed when upload data");

    view.executor().wait_for_subtasks(&taskid_value).await;

    // no sub trigger result collection

    (StatusCode::OK).into_response()
}
