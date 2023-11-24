pub mod p2p;

pub struct EventReceiver<E> {
    pub receiver: async_channel::Receiver<E>,
}
