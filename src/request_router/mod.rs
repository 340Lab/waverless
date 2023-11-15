use axum::{extract::Path, routing::post, Router};
use std::{
    net::SocketAddr,
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};
use wasmedge_sdk::config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions};

pub async fn start_http_handler() {
    let app = Router::new().route("/:route", post(handler));

    axum::Server::bind(&SocketAddr::from_str("127.0.0.1:3001").unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handler(route: Path<String>, post: String) {
    tracing::info!("route {:?}, post: {:?}", route, post);

    let _cold_start_begin = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    let _config = ConfigBuilder::new(CommonConfigOptions::default())
        .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
        .build()
        .expect("failed to create config");
    // tracing::info!("assert {}", config.wasi_enabled());
    // assert!(config.wasi_enabled());

    // create WasiContext

    // // create a Vm
    // let vm = VmBuilder::new()
    //     .with_config(config)
    //     .build()
    //     .expect("failed to create vm")
    //     .register_module_from_file(route.as_str(), format!("apps/imgs/{}.wasm", route.as_str()))
    //     .unwrap_or_else(|err| {
    //         tracing::error!("register module failed {}", err);
    //         panic!("register module failed {}", err);
    //     });

    // // tracing::info!("vm new");

    // let cold_start_end = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
    // tracing::info!(
    //     "coldstart load module: {} ms",
    //     (cold_start_end - cold_start_begin).as_millis()
    // );

    // // run the wasm function from a specified wasm file
    // // let async_state = AsyncState::new();

    // let _ = tokio::task::spawn_blocking(move || {
    //     vm.run_func(route.as_str().into(), "dummy_fn_name", vec![]);
    // }).await;

    // vm.run_func_from_file(file, func_name, args)
    // let _ = vm
    //     .run_func_from_file_async(
    //         &async_state,
    //         ,
    //         "_start",
    //         [],
    //     )
    //     .await
    //     .expect("failed to run func from file");
    tracing::info!("handle request end");
}
