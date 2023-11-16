use axum::{extract::Path, routing::get, Router};
// use lazy_static::lazy_static;
use lazy_static::lazy_static;
use std::{
    net::SocketAddr,
    str::FromStr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use wasmedge_sdk::{
    config::{CommonConfigOptions, ConfigBuilder, HostRegistrationConfigOptions},
    Vm, VmBuilder,
};

pub async fn start_http_handler() {
    let app = Router::new().route("/:route", get(handler));

    axum::Server::bind(&SocketAddr::from_str("0.0.0.0:3001").unwrap())
        .serve(app.into_make_service())
        .await
        .unwrap();
}

struct VmPool {
    total_count: AtomicUsize,
    mpmc_tx: async_channel::Sender<Box<Vm>>,
    mpmc_rx: async_channel::Receiver<Box<Vm>>,
}

impl VmPool {
    fn new() -> Self {
        let (mpmc_tx, mpmc_rx) = async_channel::bounded(100);
        Self {
            total_count: AtomicUsize::new(0),
            mpmc_tx,
            mpmc_rx,
        }
    }
    async fn get_vm(&self) -> Box<Vm> {
        // create WasiContext

        // // create a Vm
        if self.total_count.fetch_add(1, Ordering::SeqCst) < 1 {
            let config = ConfigBuilder::new(CommonConfigOptions::default())
                .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
                .build()
                .expect("failed to create config");
            return Box::new(
                VmBuilder::new()
                    .with_config(config)
                    .build()
                    .expect("failed to create vm")
                    .register_module_from_file("fn2", format!("apps/imgs/{}.wasm", "fn2"))
                    .unwrap_or_else(|err| {
                        tracing::error!("register module failed {}", err);
                        panic!("register module failed {}", err);
                    }),
            );
        }

        let vm = self.mpmc_rx.recv().await.unwrap();

        // let config = ConfigBuilder::new(CommonConfigOptions::default())
        //     .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
        //     .build()
        //     .expect("failed to create config");
        // // tracing::info!("assert {}", config.wasi_enabled());
        // // assert!(config.wasi_enabled());
        // let vm = Box::new(
        //     VmBuilder::new()
        //         .with_config(config)
        //         .build()
        //         .expect("failed to create vm")
        //         .register_module_from_file("fn2", format!("apps/imgs/{}.wasm", "fn2"))
        //         .unwrap_or_else(|err| {
        //             tracing::error!("register module failed {}", err);
        //             panic!("register module failed {}", err);
        //         }),
        // );
        vm
    }
}

lazy_static! {
//     /// This is an example for using doc comment attributes
    static ref VM_POOL: Arc<VmPool> = Arc::new(VmPool::new());
//         let config = ConfigBuilder::new(CommonConfigOptions::default())
//             .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
//             .build()
//             .expect("failed to create config");
//         // tracing::info!("assert {}", config.wasi_enabled());
//         // assert!(config.wasi_enabled());

//         // create WasiContext

//         // // create a Vm
//         let vm = VmBuilder::new()
//             .with_config(config)
//             .build()
//             .expect("failed to create vm")
//             .register_module_from_file("fn2", format!("apps/imgs/{}.wasm", "fn2"))
//             .unwrap_or_else(|err| {
//                 tracing::error!("register module failed {}", err);
//                 panic!("register module failed {}", err);
//             });
//         Arc::new(vm)
//     };
}

async fn handler(_route: Path<String> /* _post: String*/) {
    // tracing::info!("route {:?}, post: {:?}", route, post);

    let _cold_start_begin = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();

    // let config = ConfigBuilder::new(CommonConfigOptions::default())
    //     .with_host_registration_config(HostRegistrationConfigOptions::default().wasi(true))
    //     .build()
    //     .expect("failed to create config");
    // // tracing::info!("assert {}", config.wasi_enabled());
    // // assert!(config.wasi_enabled());

    // // create WasiContext

    // // // create a Vm
    // let vm = VmBuilder::new()
    //     .with_config(config)
    //     .build()
    //     .expect("failed to create vm")
    //     .register_module_from_file("fn2", format!("apps/imgs/{}.wasm", "fn2"))
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

    // let vm = VM.clone();
    let vm = VM_POOL.get_vm().await;
    let vm = tokio::task::spawn_blocking(move || {
        let _ = vm
            .run_func("fn2".into(), "fn2", vec![])
            .unwrap_or_else(|err| {
                tracing::error!("run func failed {}", err);
                panic!("run func failed {}", err);
            });

        vm
    })
    .await
    .unwrap();
    VM_POOL.mpmc_tx.send(vm).await.unwrap();

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
