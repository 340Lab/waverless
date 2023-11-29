use std::path::Path;

use super::container_manager::ContainerManager;

pub struct Executor {
    container_manager: ContainerManager,
}

impl Executor {
    pub fn new(file_dir: impl AsRef<Path>) -> Self {
        Self {
            container_manager: ContainerManager::new(file_dir),
        }
    }
    pub async fn execute(&self, req_fn: &str) {
        let (vm, guard) = self.container_manager.load_container(req_fn).await;

        let _ = vm.run_func(Some(req_fn), "fn2", None).unwrap();

        self.container_manager.finish_using(req_fn, vm, guard).await
    }
}
