use core::panic;
use std::{collections::HashMap, env, fs, path::PathBuf};
use bytes::Bytes;
use std::process::Command;
use reqwest;
use serde_json;
use crate::{
    config::{NodeConfig, NodesConfig}, general::app::View
};

// #[cfg(test)]
use crate::general::test_utils;

#[tokio::test(flavor = "multi_thread")]
async fn test_app_upload()  -> Result<(), Box<dyn std::error::Error>> {
    


    // 使用 get_test_sys 新建两个系统模块（一个 master，一个 worker）
    let (
        _sys_guard, // 互斥锁守卫
        master_logical_modules, // 系统 0 (Master) 的逻辑模块引用
        _worker_logical_modules, // 系统 1 (Worker) 的逻辑模块引用
    ) = test_utils::get_test_sys().await;

    // 延迟等待连接稳定
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    //调用 bencher 的 prepare 模式触发应用上传
    let output = Command::new("sh")
        .arg("-c")
        .arg("echo")
        .arg("$PWD")
        .arg("&&")
        .arg("cargo")
        .arg("run")
        .arg("--")
        .arg("simple_demo/simple")
        .arg("--with-wl")
        .arg("--prepare")
        .arg("--config")
        .arg("../middlewares/cluster_config.yml")
        // .arg("--appname simple_demo")
        // .arg("--rename uploaded_demo")
        .current_dir("../../../../../bencher") // 设置工作目录
        .output()
        .unwrap_or_else(|err|{
            // 确保路径是绝对路径
            let absolute_path = env::current_dir();
            

            panic!(
                "Command failed to execute: {:?}  {:?}",
                absolute_path,
                err,
            )
        });
    
    
    if !output.status.success() {
        panic!(
            "Command failed to execute: {:?}\nstdout: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout)
        );
    }

    // 增加延迟等待上传完成
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    

    // 应用名称
    let appname = "simple_demo";
    // 读取本地 ZIP 文件的内容
    let zip_path = format!("/root/ygy/middlewares/waverless/{}.zip", appname);
    let zip_content = tokio::fs::read(zip_path).await?;
    let zip_bytes = Bytes::from(zip_content);

    // 获取当前视图的逻辑
    let view = View::new(master_logical_modules.clone()); // 直接使用 master_logical_modules
    let app_meta_manager = view.appmeta_manager();
    

    // 通过状态标志位校验应用上传 http 接口是否正常
    let test_http_app_uploaded = {
        let test_http_app_uploaded_guard = app_meta_manager.test_http_app_uploaded.lock().unwrap();
        test_http_app_uploaded_guard.clone()
    };
    // 检查标志位是否为空
    if test_http_app_uploaded.is_empty() {
        panic!("应用上传失败：未接收到上传数据");
    }

    assert!(test_http_app_uploaded == zip_bytes, "应用上传失败");

    // 调用数据接口校验应用是否上传完成
    let app_meta = app_meta_manager.get_app_meta("simple_demo").await;
    assert!(app_meta.is_ok(), "Failed to get app meta");
    let app_meta = app_meta.unwrap();
    assert!(app_meta.is_some(), "App meta data not found");

    // 发起对函数的 http 请求校验应用是否运行
    let client = reqwest::Client::new();
    let response = client
        .post("http://localhost:2606/simple_demo/simple")
        .body("{}")
        .send()
        .await
        .expect("Failed to send HTTP request");

    // 验证响应状态码
    assert_eq!(response.status().as_u16(), 200); // 使用 as_u16() 比较状态码

    // 解析响应
    let response_text = response.text().await.expect("Failed to read response text");
    let res: serde_json::Value = serde_json::from_str(&response_text)
        .expect("Failed to parse response as JSON");

    // 验证响应中包含必要的时间戳字段
    assert!(res.get("req_arrive_time").is_some(), "Missing req_arrive_time");
    assert!(res.get("bf_exec_time").is_some(), "Missing bf_exec_time");
    assert!(res.get("recover_begin_time").is_some(), "Missing recover_begin_time");
    assert!(res.get("fn_start_time").is_some(), "Missing fn_start_time");
    assert!(res.get("fn_end_time").is_some(), "Missing fn_end_time");

    Ok(()) // 返回 Ok(()) 表示成功
}