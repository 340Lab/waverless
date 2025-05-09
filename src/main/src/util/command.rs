use tokio::{
    io::{AsyncBufReadExt, BufReader},
    process::{Child, Command},
};

use crate::result::WSResult;

pub trait CommandDebugStdio {
    async fn spawn_debug(
        &mut self,
    ) -> WSResult<(
        tokio::task::JoinHandle<String>,
        tokio::task::JoinHandle<String>,
        Child,
    )>;
}

impl CommandDebugStdio for Command {
    async fn spawn_debug(
        &mut self,
    ) -> WSResult<(
        tokio::task::JoinHandle<String>,
        tokio::task::JoinHandle<String>,
        Child,
    )> {
        let mut child = self.spawn()?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();

        // 分别处理 stdout 和 stderr
        let mut stdout_reader = BufReader::new(stdout).lines();
        let mut stderr_reader = BufReader::new(stderr).lines();

        let stdout_task = tokio::spawn(async move {
            let mut all = String::new();
            while let Ok(Some(line)) = stdout_reader.next_line().await {
                println!("[STDOUT] {}", line);
                all += &format!("[STDOUT] {}\n", line);
            }
            all
        });

        let stderr_task = tokio::spawn(async move {
            let mut all = String::new();
            while let Ok(Some(line)) = stderr_reader.next_line().await {
                eprintln!("[STDERR] {}", line);
                all += &format!("[STDERR] {}\n", line);
            }
            all
        });

        Ok((stdout_task, stderr_task, child))
    }
}
