// use crate::{
//     config::{NodeConfig, NodesConfig},
//     general::app::View,
//     util::command::CommandDebugStdio,
// };
// use axum::body::Bytes;
// use core::panic;
// use path_absolutize::Absolutize;
// use reqwest;
// use serde_json;
// use tokio::process::Command;
// // use std::process::{Command, Stdio};
// use std::{collections::HashMap, env, fs, path::PathBuf, process::Stdio};

// // #[cfg(test)]
// use crate::general::test_utils;

// #[tokio::test(flavor = "multi_thread")]
// async fn test_app_upload() -> Result<(), Box<dyn std::error::Error>> {
//     // install java related by scripts/install/2.3install_java_related.py
//     // run real time output command
//     let (stdout_task, stderr_task, mut child) = Command::new("bash")
//         .arg("-c")
//         .arg("python3 scripts/install/2.3install_java_related.py")
//         .current_dir("../../../../../middlewares/waverless/waverless")
//         .stdout(Stdio::piped())
//         .stderr(Stdio::piped())
//         .spawn_debug()
//         .await
//         .unwrap();
//     let status = child.wait().await.unwrap();
//     if !status.success() {
//         panic!(
//             "install java related failed, stderr: {}, stdout: {}",
//             stderr_task.await.unwrap(),
//             stdout_task.await.unwrap()
//         );
//     }

//     // 使用 get_test_sys 新建两个系统模块（一个 master，一个 worker）
//     let (
//         _sys_guard,              // 互斥锁守卫
//         _master_logical_modules, // 系统 0 (Master) 的逻辑模块引用
//         worker_logical_modules,  // 系统 1 (Worker) 的逻辑模块引用
//     ) = test_utils::get_test_sys().await;

//     // 延迟等待连接稳定
//     tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

//     //调用 bencher 的 prepare 模式触发应用上传
//     tracing::debug!("test_app_upload uploading app");

//     // 创建临时配置文件
//     let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
//     let config_path = temp_dir.path().join("cluster_config.yml");

//     // 写入配置内容
//     let config_content = format!(
//         r#"
// master:
//   ip: 127.0.0.1:{}
//   is_master: 
// worker: 
//   ip: 127.0.0.1:{}
// "#,
//         test_utils::TEST_SYS1_PORT + 1,
//         test_utils::TEST_SYS2_PORT + 1,
//     );
//     std::fs::write(&config_path, config_content).expect("Failed to write config file");

//     //sleep 10s
//     tracing::debug!("test_app_upload sleep 4s for system to be ready");
//     tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

//     async fn upload_app(config_path_str: &str) {
//         let command_str = format!(
//             "echo $PWD && \
//             cargo run -- \
//             simple_demo/simple \
//             --with-wl \
//             --prepare \
//             --config {}",
//             config_path_str
//         );

//         let (stdout_task, stderr_task, mut child) = Command::new("bash")
//             .arg("-c")
//             .arg(command_str)
//             .stdout(Stdio::piped())
//             .stderr(Stdio::piped())
//             .current_dir("../../../../../bencher") // 设置工作目录
//             .spawn_debug()
//             .await
//             .unwrap_or_else(|err| {
//                 // 确保路径是绝对路径
//                 let absolute_path = env::current_dir();

//                 panic!("Command failed to execute: {:?}  {:?}", absolute_path, err,)
//             });

//         let status = child.wait().await.unwrap();
//         if !status.success() {
//             panic!(
//                 "Command failed to execute: {:?}\nstdout: {}\nstderr: {}",
//                 status,
//                 stdout_task.await.unwrap(),
//                 stderr_task.await.unwrap()
//             );
//         }

//         tracing::debug!(
//             "test_app_upload app uploaded",
//             // stdout_task.await.unwrap()
//         );
//     }

//     upload_app(config_path.to_str().unwrap()).await;
//     // // 增加延迟等待上传完成
//     // tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

//     // 应用名称
//     let appname = "simple_demo";
//     // 读取本地 ZIP 文件的内容
//     let zip_path = format!("../../../../../middlewares/waverless/{}.zip", appname);
//     let zip_content = tokio::fs::read(zip_path)
//         .await /*  */
//         .unwrap_or_else(|err| {
//             panic!(
//                 "test read app zip failed {:?}, current path is {:?}",
//                 err,
//                 std::env::current_dir().unwrap().absolutize().unwrap()
//             )
//         });

//     let zip_bytes = Bytes::from(zip_content);

//     // 获取当前视图的逻辑
//     let view2 = View::new(worker_logical_modules.clone()); // 直接使用 master_logical_modules
//     let app_meta_manager2 = view2.appmeta_manager();

//     // 通过状态标志位校验应用上传 http 接口是否正常
//     let test_http_app_uploaded = {
//         let test_http_app_uploaded_guard = app_meta_manager2.test_http_app_uploaded.lock();
//         test_http_app_uploaded_guard.clone()
//     };
//     // 检查标志位是否为空
//     if test_http_app_uploaded.is_empty() {
//         panic!("应用上传失败：未接收到上传数据");
//     }

//     tracing::debug!("test_app_upload verifying app uploaded bytes");
//     assert!(test_http_app_uploaded == zip_bytes, "应用上传失败");

//     // 调用数据接口校验应用是否上传完成
//     tracing::debug!("test_app_upload verifying app meta");
//     let app_meta = app_meta_manager2.get_app_meta("simple_demo").await;
//     assert!(app_meta.is_ok(), "Failed to get app meta");
//     let app_meta = app_meta.unwrap();
//     assert!(app_meta.is_some(), "App meta data not found");

//     // 再次上传
//     tracing::debug!("test_app_upload uploading app again");
//     upload_app(config_path.to_str().unwrap()).await;

//     // wait for checkpoint
//     tracing::debug!("test_app_upload wait 10s for checkpoint");
//     for i in 0..10 {
//         tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
//         tracing::debug!("test_app_upload waited {}s", i + 1);
//     }
//     // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

//     // 发起对函数的 http 请求校验应用是否运行
//     tracing::debug!("test_app_upload try calling test app");
//     let client = reqwest::Client::new();
//     let response = client
//         .post(&format!(
//             "http://localhost:{}/simple_demo/simple",
//             test_utils::TEST_SYS1_PORT + 1
//         ))
//         .body("{}")
//         .send()
//         .await
//         .expect("Failed to send HTTP request");

//     let status = response.status().as_u16();

//     let resptext = response
//         .text()
//         .await
//         .unwrap_or_else(|err| panic!("receive bytes failed with error {}", err));
//     // let respmaybestr = std::str::from_utf8(&respbytes);
//     tracing::debug!("test_app_upload call app resp with {} {}", status, resptext);
//     // 验证响应状态码

//     if status != 200 {
//         panic!("call application failed");
//     }
//     // 解析响应
//     // let response_text = response.text().await.expect("Failed to read response text");
//     let res: serde_json::Value =
//         serde_json::from_str(&resptext).expect("Failed to parse response as JSON");

//     // 验证响应中包含必要的时间戳字段
//     assert!(
//         res.get("req_arrive_time").is_some(),
//         "Missing req_arrive_time"
//     );
//     assert!(res.get("bf_exec_time").is_some(), "Missing bf_exec_time");
//     assert!(
//         res.get("recover_begin_time").is_some(),
//         "Missing recover_begin_time"
//     );
//     assert!(res.get("fn_start_time").is_some(), "Missing fn_start_time");
//     assert!(res.get("fn_end_time").is_some(), "Missing fn_end_time");

//     Ok(()) // 返回 Ok(()) 表示成功
// }


use crate::{
    config::{NodeConfig, NodesConfig},
    general::app::View,
    util::command::CommandDebugStdio,
};
use axum::body::Bytes;
use core::panic;
use path_absolutize::Absolutize;
use reqwest;
use serde_json;
use tokio::process::Command;
// use std::process::{Command, Stdio};
use std::{collections::HashMap, env, fs, path::PathBuf, process::Stdio, sync::Arc};

// #[cfg(test)]
use crate::general::test_utils;

// 抽取的应用上传逻辑
async fn setup_test_environment() -> (Arc<test_utils::TestSystemGuard>, Arc<test_utils::LogicalModules>, Arc<test_utils::LogicalModules>, PathBuf) {
    // install java related by scripts/install/2.3install_java_related.py
    let (stdout_task, stderr_task, mut child) = Command::new("bash")
        .arg("-c")
        .arg("python3 scripts/install/2.3install_java_related.py")
        .current_dir("../../../../../middlewares/waverless/waverless")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn_debug()
        .await
        .unwrap();
    let status = child.wait().await.unwrap();
    if !status.success() {
        panic!(
            "install java related failed, stderr: {}, stdout: {}",
            stderr_task.await.unwrap(),
            stdout_task.await.unwrap()
        );
    }

    // 使用 get_test_sys 新建两个系统模块（一个 master，一个 worker）
    let (
        _sys_guard,              // 互斥锁守卫
        _master_logical_modules, // 系统 0 (Master) 的逻辑模块引用
        worker_logical_modules,  // 系统 1 (Worker) 的逻辑模块引用
    ) = test_utils::get_test_sys().await;

    // 延迟等待连接稳定
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // 创建临时配置文件
    let temp_dir = tempfile::tempdir().expect("Failed to create temp directory");
    let config_path = temp_dir.path().join("cluster_config.yml");

    // 写入配置内容
    let config_content = format!(
        r#"
master:
  ip: 127.0.0.1:{}
  is_master: 
worker: 
  ip: 127.0.0.1:{}
"#,
        test_utils::TEST_SYS1_PORT + 1,
        test_utils::TEST_SYS2_PORT + 1,
    );
    std::fs::write(&config_path, config_content).expect("Failed to write config file");

    // 等待系统就绪
    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    (sys_guard, master_logical_modules, worker_logical_modules, config_path)
}

async fn upload_app(config_path: &PathBuf, app_name: &str) {
    let command_str = format!(
        "echo $PWD && \
        cargo run -- \
        {} \
        --with-wl \
        --prepare \
        --config {}",
        app_name,
        config_path.to_str().unwrap()
    );

    let (stdout_task, stderr_task, mut child) = Command::new("bash")
        .arg("-c")
        .arg(command_str)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .current_dir("../../../../../bencher")
        .spawn_debug()
        .await
        .unwrap_or_else(|err| {
            let absolute_path = env::current_dir();
            panic!("Command failed to execute: {:?}  {:?}", absolute_path, err)
        });

    let status = child.wait().await.unwrap();
    if !status.success() {
        panic!(
            "Command failed to execute: {:?}\nstdout: {}\nstderr: {}",
            status,
            stdout_task.await.unwrap(),
            stderr_task.await.unwrap()
        );
    }

    tracing::debug!("App uploaded successfully");
}

#[tokio::test(flavor = "multi_thread")]
async fn test_app_upload() -> Result<(), Box<dyn std::error::Error>> {
    let (sys_guard, master_logical_modules, worker_logical_modules, config_path) = setup_test_environment().await;
    
    // 应用名称
    let appname = "simple_demo";
    upload_app(&config_path, &format!("simple_demo/simple")).await;

    // 读取本地 ZIP 文件的内容
    let zip_path = format!("../../../../../middlewares/waverless/{}.zip", appname);
    let zip_content = tokio::fs::read(zip_path)
        .await
        .unwrap_or_else(|err| {
            panic!(
                "test read app zip failed {:?}, current path is {:?}",
                err,
                std::env::current_dir().unwrap().absolutize().unwrap()
            )
        });

    let zip_bytes = Bytes::from(zip_content);

    // 获取当前视图的逻辑
    let view2 = View::new(worker_logical_modules.clone());
    let app_meta_manager2 = view2.appmeta_manager();

    // 通过状态标志位校验应用上传 http 接口是否正常
    let test_http_app_uploaded = {
        let test_http_app_uploaded_guard = app_meta_manager2.test_http_app_uploaded.lock();
        test_http_app_uploaded_guard.clone()
    };
    // 检查标志位是否为空
    if test_http_app_uploaded.is_empty() {
        panic!("应用上传失败：未接收到上传数据");
    }

    tracing::debug!("test_app_upload verifying app uploaded bytes");
    assert!(test_http_app_uploaded == zip_bytes, "应用上传失败");

    // 调用数据接口校验应用是否上传完成
    tracing::debug!("test_app_upload verifying app meta");
    let app_meta = app_meta_manager2.get_app_meta("simple_demo").await;
    assert!(app_meta.is_ok(), "Failed to get app meta");
    let app_meta = app_meta.unwrap();
    assert!(app_meta.is_some(), "App meta data not found");

    // 再次上传
    tracing::debug!("test_app_upload uploading app again");
    upload_app(&config_path, "simple_demo/simple").await;

    // wait for checkpoint
    tracing::debug!("test_app_upload wait 10s for checkpoint");
    for i in 0..10 {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        tracing::debug!("test_app_upload waited {}s", i + 1);
    }
    // tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;

    // 发起对函数的 http 请求校验应用是否运行
    tracing::debug!("test_app_upload try calling test app");
    let client = reqwest::Client::new();
    let response = client
        .post(&format!(
            "http://localhost:{}/simple_demo/simple",
            test_utils::TEST_SYS1_PORT + 1
        ))
        .body("{}")
        .send()
        .await
        .expect("Failed to send HTTP request");

    let status = response.status().as_u16();
    let resptext = response
        .text()
        .await
        .unwrap_or_else(|err| panic!("receive bytes failed with error {}", err));
    // let respmaybestr = std::str::from_utf8(&respbytes);
    tracing::debug!("test_app_upload call app resp with {} {}", status, resptext);
    // 验证响应状态码

    if status != 200 {
        panic!("call application failed");
    }
    // 解析响应
    // let response_text = response.text().await.expect("Failed to read response text");
    let res: serde_json::Value =
        serde_json::from_str(&resptext).expect("Failed to parse response as JSON");

    // 验证响应中包含必要的时间戳字段
    assert!(res.get("req_arrive_time").is_some(), "Missing req_arrive_time");
    assert!(res.get("bf_exec_time").is_some(), "Missing bf_exec_time");
    assert!(res.get("recover_begin_time").is_some(), "Missing recover_begin_time");
    assert!(res.get("fn_start_time").is_some(), "Missing fn_start_time");
    assert!(res.get("fn_end_time").is_some(), "Missing fn_end_time");

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_data_upload() -> Result<(), Box<dyn std::error::Error>> {
    // 使用抽取的环境设置函数
    let (sys_guard, master_logical_modules, worker_logical_modules, config_path) = setup_test_environment().await;
    
    // 上传 img_resize demo 应用
    upload_app(&config_path, "img_resize/demo").await;

    // 生成测试数据（这里使用一个简单的图片数据）
    let test_data = vec![0u8; 1024]; // 1KB 的测试数据
    let test_data_bytes = Bytes::from(test_data);

    // 上传数据
    let client = reqwest::Client::new();
    let response = client
        .post(&format!(
            "http://localhost:{}/data/upload",
            test_utils::TEST_SYS1_PORT + 1
        ))
        .body(test_data_bytes.clone())
        .send()
        .await
        .expect("Failed to send data upload request");

    assert_eq!(response.status(), 200, "Data upload failed");

    // 读取上传的数据进行验证
    let view = View::new(worker_logical_modules.clone());
    let data_manager = view.data_manager();
    
    // 等待数据处理完成
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // 使用 data_manager 验证数据是否正确存储
    let data_id = response.text().await.expect("Failed to get data ID");
    let stored_data = data_manager.get_data(&data_id).await;
    assert!(stored_data.is_ok(), "Failed to get stored data");
    let stored_data = stored_data.unwrap();
    assert!(stored_data.is_some(), "Data not found in storage");
    assert_eq!(stored_data.unwrap(), test_data_bytes, "Stored data doesn't match uploaded data");

    // 验证数据是否触发了关联应用
    let app_meta_manager = view.appmeta_manager();
    let app_meta = app_meta_manager.get_app_meta("img_resize").await;
    assert!(app_meta.is_ok(), "Failed to get app meta");
    let app_meta = app_meta.unwrap();
    assert!(app_meta.is_some(), "App meta not found");

    // 验证数据处理结果
    let client = reqwest::Client::new();
    let response = client
        .get(&format!(
            "http://localhost:{}/img_resize/demo",
            test_utils::TEST_SYS1_PORT + 1
        ))
        .send()
        .await
        .expect("Failed to send request to img_resize demo");

    assert_eq!(response.status(), 200, "Image resize demo failed to process data");
    
    // 验证处理结果的内容
    let processed_data = response.bytes().await.expect("Failed to get processed data");
    assert!(!processed_data.is_empty(), "Processed data is empty");
    // 这里可以添加更多的验证逻辑，比如检查图片格式、大小等

    Ok(())
}
