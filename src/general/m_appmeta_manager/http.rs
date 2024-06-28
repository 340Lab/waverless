use axum::extract::{DefaultBodyLimit, Multipart};
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
    // .layer(RequestBodyLimitLayer::new(
    //     250 * 1024 * 1024, /* 250mb */
    // ))
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
