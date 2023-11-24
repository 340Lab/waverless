use parking_lot::Mutex;

use crate::sys::NodeID;

#[derive(Clone)]
pub enum Event {
    Connected(NodeID),
}

pub struct EventDispatcher {
    listeners: Mutex<Vec<async_channel::Sender<Event>>>,
}

impl EventDispatcher {
    pub fn new() -> Self {
        Self {
            listeners: Vec::new().into(),
        }
    }
    pub fn add_listener(&mut self, listener: async_channel::Sender<Event>) {
        self.listeners.lock().push(listener);
    }
    pub async fn dispatch(&self, event: Event) {
        let l = self.listeners.lock();
        for listener in l.iter() {
            // don't care if the listener is closed, because the dispatcher close at last
            let _ = listener.send(event.clone()).await;
        }
    }
}
