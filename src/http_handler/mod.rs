use axum::{extract::Path, routing::post, Router};
use std::{net::SocketAddr, str::FromStr};
use wasmedge_sdk::{
    config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions},
    wasi::r#async::{AsyncState, WasiContext},
    VmBuilder,
};

pub async fn start_http_handler() {
    let app = Router::new().route("/:route", post(handler));

    axum::Server::bind(&SocketAddr::from_str("127.0.0.1:3001").unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handler(route: Path<String>, post: String) {
    tracing::info!("route {:?}, post: {:?}", route, post);
    let config = ConfigBuilder::new(CommonConfigOptions::default())
        // .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
        .build()
        .expect("failed to create config");
    assert!(config.wasi_enabled());

    // create WasiContext
    let wasi_ctx = WasiContext::new(None, Some(vec![("ENV", "VAL")]), Some(vec![(".", ".")]));

    // create a Vm
    let mut vm = VmBuilder::new()
        .with_config(config)
        .with_wasi_context(wasi_ctx)
        .build()
        .expect("failed to create vm");

    // run the wasm function from a specified wasm file
    let async_state = AsyncState::new();
    let _ = vm
        .run_func_from_file_async(
            &async_state,
            format!("/apps/imgs/{}.wasm", route),
            "_start",
            [],
        )
        .await
        .expect("failed to run func from file");
}
