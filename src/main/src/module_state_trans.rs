

#[derive(Clone, Debug)]
pub enum ModuleSignal {
    // reason with string
    Preparing(String),
    Running,
}

pub struct LogicalModuleWaiter {
    ok: bool,
    rx: Vec<tokio::sync::broadcast::Receiver<ModuleSignal>>,
}

impl LogicalModuleWaiter {
    pub fn new(rx: Vec<tokio::sync::broadcast::Receiver<ModuleSignal>>) -> Self {
        LogicalModuleWaiter { ok: false, rx }
    }
    pub fn ok(&self) -> bool {
        self.ok
    }
    pub fn stop(&self) -> bool {
        false
    }
    pub async fn wait_for_ok(&mut self) {
        // for rx in self.rx.iter_mut() {
        //     rx.recv().await;
        // }
    }
    pub async fn wait_for_wrong(&self) {
        // for rx in self.rx.iter_mut() {
        //     rx.recv().await;
        // }
    }
    pub fn select_wrong(&mut self) -> bool {
        for r in &mut self.rx {
            if let Ok(msg) = r.try_recv() {
                match msg {
                    ModuleSignal::Preparing(reason) => {
                        tracing::info!(
                            "thread received preparing signal, reason: {}, pause running",
                            reason
                        );
                        self.ok = false;
                        return true;
                    }
                    ModuleSignal::Running => {
                        continue;
                    }
                }
            }
        }
        false
    }
    pub fn sync_wait_for_ok(&mut self) {
        for rx in self.rx.iter_mut() {
            loop {
                match rx.blocking_recv() {
                    Ok(res) => match res {
                        ModuleSignal::Preparing(reason) => {
                            tracing::info!("thread received preparing signal, reason: {}, continue waiting for ok", reason);
                            continue;
                        }
                        ModuleSignal::Running => {
                            break;
                        }
                    },
                    Err(err) => panic!("tx should not be dropped, err: {:?}", err),
                }
            }
        }
        self.ok = true;
    }
}

// pub trait TickStateMachine {
//     fn tick(&mut self);
// }

// fn run_with_sync_waiter_wrapper<T: TickStateMachine>(mut t: T, mut waiter: LogicalModuleWaiter) {
//     loop {
//         if waiter.ok() {
//             // user code begin
//             loop {
//                 t.tick();
//                 if waiter.select_wrong() {
//                     break;
//                 }
//             }
//             // user code end
//         } else if waiter.stop() {
//             break;
//         } else {
//             waiter.sync_wait_for_ok();
//         }
//     }
// }

// async mod
// async fn test_waiter() {
//     let waiter=LogicalModuleWaiter{true,vec![]};
//     loop{
//         if waiter.ok(){
//             select!{
//                 // user code
//                 _=module_run(){},
//                 _=waiter.wait_for_wrong(){},
//             }
//         }else if waiter.stop(){
//             break;
//         }else{
//             waiter.wait_for_ok().await;
//         }
//     }
// }

#[macro_export]
macro_rules! sync_loop {
    ($module_name:expr,$waiter:ident,$tick_do:block) => {
        // // let waiter=LogicalModuleWaiter{true,vec![]};
        loop{
            if $waiter.ok(){
                tracing::info!("thread {} into running",$module_name);
                // user code begin
                loop{
                    $tick_do
                    if $waiter.select_wrong(){
                        break;
                    }
                }
                // user code end
            }else if $waiter.stop(){
                tracing::info!("thread {} stopped",$module_name);
                break;
            }else{
                tracing::info!("thread {} into preparing",$module_name);
                $waiter.sync_wait_for_ok();
            }
        }
    };
}

// // sync mod
// fn test_waiter_() {
//     let waiter=LogicalModuleWaiter{true,vec![]};
//     loop{
//         if waiter.ok(){
//             // user code begin
//             loop{
//                 module_tick(){}
//                 if waiter.select_wrong(){
//                     break;
//                 }
//             }
//             // user code end
//         }else if waiter.stop(){
//             break;
//         }else{
//             waiter.wait_for_ok();
//         }
//     }
// }
