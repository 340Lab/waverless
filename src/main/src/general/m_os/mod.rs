pub mod zip;

use crate::general::{
    app::AppMetaManager,
    network::{
        m_p2p::{P2PModule, RPCCaller, RPCHandler, RPCResponsor},
        proto::remote_sys::{
            get_dir_content_resp::{self, GetDirContentRespFail},
            GetDirContentReq, GetDirContentResp, RunCmdReq, RunCmdResp,
        },
    },
};
use crate::{
    general::network::proto,
    logical_module_view_impl,
    result::{ErrCvt, WSError, WSResult, WSResultExt, WsIoErr},
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModulesRef},
    util::JoinHandleWrapper,
};
use async_trait::async_trait;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::{
    fs::{self, File},
    io::{Read, Seek, Write},
    os::fd::AsRawFd,
    path::{Path, PathBuf},
    process::Stdio,
    sync::Arc,
    time::SystemTime,
};
use tokio::process::{self, Command};
use ws_derive::LogicalModule;

logical_module_view_impl!(OperatingSystemView);
logical_module_view_impl!(OperatingSystemView, p2p, P2PModule);
logical_module_view_impl!(OperatingSystemView, os, OperatingSystem);
logical_module_view_impl!(OperatingSystemView, appmeta_manager, AppMetaManager);


pub const APPS_REL_DIR: &str = "apps";

#[derive(LogicalModule)]
pub struct OperatingSystem {
    view: OperatingSystemView,
    fd_files: SkipMap<i32, Arc<Mutex<File>>>,
    pub file_path: PathBuf,

    // pub remote_run_cmd_caller: RPCCaller<proto::remote_sys::RunCmdReq>,
    pub remote_get_dir_content_caller: RPCCaller<proto::remote_sys::GetDirContentReq>,

    // remote_run_cmd_handler: RPCHandler<proto::remote_sys::RunCmdReq>,
    remote_get_dir_content_handler: RPCHandler<proto::remote_sys::GetDirContentReq>,

    pub remote_run_cmd_caller: RPCCaller<proto::remote_sys::RunCmdReq>,

    remote_run_cmd_handler: RPCHandler<proto::remote_sys::RunCmdReq>,
}

#[async_trait]
impl LogicalModule for OperatingSystem {
    fn inner_new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {
            view: OperatingSystemView::new(args.logical_modules_ref.clone()),
            fd_files: SkipMap::new(),
            file_path: args.nodes_config.file_dir.clone(),

            remote_get_dir_content_caller: RPCCaller::new(),
            remote_get_dir_content_handler: RPCHandler::new(),

            remote_run_cmd_caller: RPCCaller::new(),
            remote_run_cmd_handler: RPCHandler::new(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        let all = vec![];

        let view = self.view.clone();
        self.remote_get_dir_content_handler
            .regist(self.view.p2p(), move |responser, msg| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.os()
                        .remote_get_dir_content_handler(responser, msg)
                        .await;
                });
                Ok(())
            });
        self.remote_get_dir_content_caller.regist(self.view.p2p());

        let view = self.view.clone();
        self.remote_run_cmd_handler
            .regist(self.view.p2p(), move |responser, msg| {
                let view = view.clone();
                let _ = tokio::spawn(async move {
                    view.os().remote_run_cmd_handler(responser, msg).await;
                });
                Ok(())
            });
        self.remote_run_cmd_caller.regist(self.view.p2p());
        Ok(all)
    }
}

pub enum OsProcessType {
    JavaApp(String),
    JavaCheckpoints(String),
}

impl OperatingSystem {
    pub fn abs_file_path(&self, p: PathBuf) -> PathBuf {
        if p.is_absolute() {
            p
        } else {
            self.file_path.join(p)
        }
    }
    pub fn app_path(&self, app: &str) -> PathBuf {
        self.view.appmeta_manager().fs_layer.concat_app_dir(app)
    }

    pub fn app_rootdir(&self) -> PathBuf {
        self.view.appmeta_manager().fs_layer.app_dir()
    }

    pub fn start_process(&self, p: OsProcessType) -> process::Child {
        let (mut binding, log_file) = match p {
            OsProcessType::JavaApp(app) => {
                // let crac_config_path = self.view.appmeta_manager().fs_layer.crac_file_path();
                let appdir = self.view.appmeta_manager().fs_layer.concat_app_dir(&app);
                // 打开或创建日志文件
                let log_file_path = appdir.join(format!("app{:?}.log", SystemTime::now()));
                // 打开或创建日志文件
                let log_file = File::create(log_file_path).expect("Failed to create log file");

                // check dir contains checkpoint-dir
                if std::fs::read_dir(appdir.join("checkpoint-dir")).is_ok() {
                    tracing::debug!("start process with checkpoint");
                    let mut binding = Command::new("java");
                    let _ = binding
                        // .arg("-Djava.net.preferIPv4Stack=true")
                        .arg("-XX:CRaCRestoreFrom=checkpoint-dir")
                        .current_dir(appdir);
                    (binding, log_file)
                } else {
                    tracing::debug!("start process app without checkpoint {}", app);

                    let mut binding = Command::new("java");
                    let _ = binding
                        // .arg("-Djava.net.preferIPv4Stack=true")
                        .arg(format!("-Djdk.crac.resource-policies=../crac_config"))
                        .arg("-XX:CRaCCheckpointTo=checkpoint-dir")
                        .arg("-jar")
                        .arg("app.jar")
                        .arg("--agentSock=../../agent.sock")
                        .arg(format!("--appName={}", app))
                        .current_dir(appdir);
                    (binding, log_file)
                }
            }
            OsProcessType::JavaCheckpoints(app) => {
                let appdir = self.view.appmeta_manager().fs_layer.concat_app_dir(&app);
                // create checkpoint-dir
                let _ = std::fs::create_dir(appdir.join("checkpoint-dir"));

                // 打开或创建日志文件
                let log_file_path = appdir.join("checkpoint.log");
                // 打开或创建日志文件
                let log_file = File::create(log_file_path).expect("Failed to create log file");

                let mut binding = Command::new("jcmd");
                let _ = binding
                    .arg("app.jar")
                    .arg("JDK.checkpoint")
                    .current_dir(appdir);
                (binding, log_file)
            }
        };
        binding
            .stdout(Stdio::from(
                log_file.try_clone().expect("Failed to clone log file"),
            )) // 重定向stdout到日志文件
            .stderr(Stdio::from(log_file)) // 重定向stderr到日志文件
            // binding.stdout(
            //     // out put to terminal
            //     // std::process::Stdio::inherit()
            //     // hide output
            //     std::process::Stdio::piped(),
            // )
            .spawn()
            .expect("Failed to start child process")
    }

    // pub async fn run_cmd_local(&self, cmd: OsCmd) {}

    async fn remote_run_cmd_handler(&self, responser: RPCResponsor<RunCmdReq>, msg: RunCmdReq) {
        let res = tokio::task::spawn_blocking(move || {
            // temp run use random name
            let tmpsh = std::env::temp_dir().join(format!("tmpsh-{}.sh", uuid::Uuid::new_v4()));

            let mut file = match File::create(tmpsh.clone()) {
                Ok(file) => file,
                Err(e) => {
                    return RunCmdResp {
                        dispatch: Some(proto::remote_sys::run_cmd_resp::Dispatch::Err(
                            proto::remote_sys::run_cmd_resp::RunCmdRespErr {
                                error: format!("err in remote_run_cmd_handler1({:?}): {}", &msg, e),
                            },
                        )),
                    };
                }
            };

            // 将内容写入文件
            tracing::debug!("will run cmd: {}", &msg.cmd);
            match file.write_all(format!("cd {}\n{}\n", &msg.workdir, &msg.cmd).as_bytes()) {
                Ok(()) => (),
                Err(e) => {
                    return RunCmdResp {
                        dispatch: Some(proto::remote_sys::run_cmd_resp::Dispatch::Err(
                            proto::remote_sys::run_cmd_resp::RunCmdRespErr {
                                error: format!("err in remote_run_cmd_handler2({:?}): {}", &msg, e),
                            },
                        )),
                    };
                }
            }

            let output = std::process::Command::new("bash")
                .arg(tmpsh)
                // .current_dir(&msg.workdir)
                .output();
            match output {
                Ok(output) => {
                    let output = String::from_utf8_lossy(&output.stdout).to_string();
                    tracing::debug!("remote_run_cmd_handler output: {}", output);
                    RunCmdResp {
                        dispatch: Some(proto::remote_sys::run_cmd_resp::Dispatch::Ok(
                            proto::remote_sys::run_cmd_resp::RunCmdRespOk { output },
                        )),
                    }
                }
                Err(e) => RunCmdResp {
                    dispatch: Some(proto::remote_sys::run_cmd_resp::Dispatch::Err(
                        proto::remote_sys::run_cmd_resp::RunCmdRespErr {
                            error: format!("err in remote_run_cmd_handler({:?}): {}", &msg, e),
                        },
                    )),
                },
            }
        })
        .await
        .unwrap();
        responser.send_resp(res).await.todo_handle("This part of the code needs to be implemented.");      //返回结果未处理    曾俊
    }

    async fn remote_get_dir_content_handler(
        &self,
        responser: RPCResponsor<GetDirContentReq>,
        msg: GetDirContentReq,
    ) {
        let res = tokio::task::spawn_blocking(move || {
            let path = Path::new(&msg.path);
            if path.exists() && path.is_dir() {
                if let Ok(entries) = fs::read_dir(path) {
                    let files = entries
                        .filter_map(|entry| {
                            if let Ok(entry) = entry {
                                if let Ok(file_name) = entry.file_name().into_string() {
                                    if let Ok(file_type) = entry.file_type() {
                                        Some((file_name, file_type))
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    let dirs = files
                        .iter()
                        .filter(|(_, t)| t.is_dir())
                        .map(|(n, _)| n.clone())
                        .collect();
                    let files = files
                        .iter()
                        .filter(|(_, t)| t.is_file())
                        .map(|(n, _)| n.clone())
                        .collect();
                    GetDirContentResp {
                        dispatch: Some(get_dir_content_resp::Dispatch::Ok(
                            get_dir_content_resp::GetDirContentRespOk { files, dirs },
                        )),
                    }
                    // 在这里使用 responser 将 dir_contents 发送回调用方
                } else {
                    // 发生读取目录错误，可以选择使用 responser 发送错误消息
                    GetDirContentResp {
                        dispatch: Some(get_dir_content_resp::Dispatch::Fail(
                            GetDirContentRespFail {
                                error: "read dir error".to_string(),
                            },
                        )),
                    }
                }
            } else {
                GetDirContentResp {
                    dispatch: Some(get_dir_content_resp::Dispatch::Fail(
                        GetDirContentRespFail {
                            error: "path not exists or not a dir".to_string(),
                        },
                    )),
                }
            }
        })
        .await
        .unwrap();
        responser.send_resp(res).await.todo_handle("This part of the code needs to be implemented.");     //返回结果未处理  曾俊
    }

    pub fn open_file(&self, fname: &str) -> WSResult<i32> {
        let fp = self.file_path.join("files").join(fname);
        tracing::debug!("openning file {:?}", fp);
        let f = File::open(fp).map_err(|e| ErrCvt(e).to_ws_io_err())?;
        let fd = f.as_raw_fd();
        let _ = self.fd_files.insert(fd, Arc::new(Mutex::new(f)));
        Ok(fd)
    }
    pub fn close_file(&self, fd: i32) -> WSResult<()> {
        if let Some(_) = self.fd_files.remove(&fd) {}
        Ok(())
    }
    pub fn read_file_at(&self, fd: i32, offset: i32, buf: &mut [u8]) -> WSResult<usize> {
        if let Some(f) = self.fd_files.get(&fd).map(|v| v.value().clone()) {
            let mut f = f.lock();
            let _ = f
                .seek(std::io::SeekFrom::Start(offset as u64))
                .map_err(|e| ErrCvt(e).to_ws_io_err())?;
            // Read into the buffer
            let bytes_read = f.read(buf).map_err(|e| ErrCvt(e).to_ws_io_err())?;

            Ok(bytes_read)
        } else {
            tracing::error!("function read_file_at: invalid fd {}", fd);

            Ok(0)
        }
    }

    pub fn cover_data_2_path(&self, p: impl AsRef<Path>, data: Vec<u8>) -> WSResult<()> {
        let mut f = match File::create(p) {
            Err(e) => {
                return Err(ErrCvt(e).to_ws_io_err());
            }
            Ok(f) => f,
        };
        f.write_all(&data)
            .map_err(|e| WSError::from(WsIoErr::Io(e)))?;
        Ok(())
    }
}
