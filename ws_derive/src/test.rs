struct MetaKvClientView {
    inner: Option<Weak<LogicalModules>>,
}
impl MetaKvClientView {
    fn local_kv_client(&self) -> &Box<dyn KvClient> {
        self.inner.upgrade().unwrap().local_kv_client
    }
}
struct LocalKvClientView {
    inner: Weak<LogicalModules>,
}
impl LocalKvClientView {
    fn local_kv(&self) -> &Option<Box<dyn KvNode>> {
        self.inner.upgrade().unwrap().local_kv
    }
}
struct LocalKvView {
    inner: Weak<LogicalModules>,
}
impl LocalKvView {}
