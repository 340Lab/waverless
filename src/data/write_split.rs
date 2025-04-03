/// A waiter for tracking completion of all write split data tasks
pub struct WriteSplitDataWaiter {
    rx: broadcast::Receiver<()>,
    total_tasks: usize,
}

impl WriteSplitDataWaiter {
    /// Wait for all tasks to complete
    pub async fn wait(mut self) -> WSResult<()> {
        let mut completed = 0;
        while completed < self.total_tasks {
            self.rx.recv().await.map_err(|e| {
                WsDataError::WaitTaskError { 
                    reason: format!("Failed to receive task completion: {}", e) 
                }
            })?;
            completed += 1;
        }
        Ok(())
    }
}

impl Handle {
    /// Gets a waiter that will complete when all tasks are finished
    pub fn get_all_tasks_waiter(&self) -> WriteSplitDataWaiter {
        WriteSplitDataWaiter {
            rx: self.task_complete_tx.subscribe(),
            total_tasks: self.tasks.lock().unwrap().len(),
        }
    }
}

// 需要在 errors.rs 中添加新的错误类型
#[derive(Debug)]
pub enum WsDataError {
    // ... existing errors ...
    WaitTaskError {
        reason: String,
    },
} 