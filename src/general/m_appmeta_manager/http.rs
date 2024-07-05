use std::time::{SystemTime, UNIX_EPOCH};

use axum::extract::{DefaultBodyLimit, Multipart, Path};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::{routing::post, Router};
use lazy_static::lazy_static;

use crate::master::m_master::ScheduleWorkload;
use crate::util;

lazy_static! {
    static ref VIEW: Option<super::View> = None;
}
fn view() -> &'static super::View {
    unsafe { util::non_null(&*VIEW).as_ref().as_ref().unwrap() }
}

pub(super) fn binds(router: Router, view: super::View) -> Router {
    unsafe {
        let _ = util::non_null(&*VIEW).as_mut().replace(view);
    }
    tracing::debug!("binds appmeta_manager http");
    router
        .route("/appmgmt/upload_app", post(upload_app))
        .layer(DefaultBodyLimit::disable())
        .route("/:app/:fn", post(call_app_fn))
    // .layer(RequestBodyLimitLayer::new(
    //     250 * 1024 * 1024, /* 250mb */
    // ))
}

async fn call_app_fn(Path((app, func)): Path<(String, String)>, body: String) -> Response {
    if view().p2p().nodes_config.this.1.is_master() {
        view()
            .http_handler()
            .handle_request(&format!("{app}/{func}"), body)
            .await
    } else if !view()
        .appmeta_manager()
        .app_available(&app)
        .await
        .map_or_else(
            |e| {
                tracing::debug!("failed to get app available, e:{:?}", e);
                false
            },
            |v| v,
        )
    {
        // # check app valid
        StatusCode::BAD_REQUEST.into_response()
    } else {
        // # call instance run
        let req_arrive_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as u64;
        let res = view()
            .executor()
            .handle_http_task(&format!("{app}/{func}"), body)
            // .execute_http_app(FunctionCtxBuilder::new(
            //     app.to_owned(),
            //     self.local_req_id_allocator.alloc(),
            //     self.request_handler_view.p2p().nodes_config.this.0,
            // ))
            .await;
        // inject `req_arrive_time`
        let res = res.map(|v| {
            v.map(|v| {
                let mut res: serde_json::Value = serde_json::from_str(&v).unwrap();
                let _ = res.as_object_mut().unwrap().insert(
                    "req_arrive_time".to_owned(),
                    serde_json::Value::from(req_arrive_time),
                );
                serde_json::to_string(&res).unwrap()
            })
        });
        match res {
            Ok(Some(res)) => (StatusCode::OK, res).into_response(),
            Ok(None) => StatusCode::OK.into_response(),
            Err(e) => (StatusCode::BAD_REQUEST, format!("err: {:?}", e)).into_response(),
        }
    }
}

async fn upload_app(mut multipart: Multipart) -> Response {
    tracing::debug!("upload_app called");
    // only worker can upload app
    if view().p2p().nodes_config.this.1.is_master() {
        let tar = view().master().schedule(ScheduleWorkload::JavaAppConstruct);

        tracing::debug!("redirect 2 worker");
        return tar
            .http_redirect(&view().p2p().nodes_config)
            .into_response();
    }

    let mut tasks = vec![];
    while let Some(field) = multipart.next_field().await.unwrap() {
        let name = field.name().unwrap().to_string();
        // let file_name = field.file_name().unwrap().to_string();
        // let content_type = field.content_type().unwrap().to_string();
        let data = field.bytes().await.unwrap();

        let name2 = name.clone();
        let task =
            tokio::spawn(async move { view().appmeta_manager().app_uploaded(name2, data).await });

        tasks.push((task, name));
    }
    for (t, app) in tasks {
        let res = t.await.unwrap();
        match res {
            Err(e) => {
                let errmsg = format!("Failed to upload app {}: {}", app, e);
                tracing::warn!(errmsg);
                return (StatusCode::INTERNAL_SERVER_ERROR, errmsg).into_response();
            }
            Ok(_) => {}
        }
    }
    (StatusCode::OK).into_response()
}
