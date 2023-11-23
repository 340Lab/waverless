//! Continuous monitoring of the executor node

pub struct NodeObserved {
    node_id: NodeId,
    total_cpu: f64,
    cpu: f64,
    total_memory: f64,
    memory: f64,
}

pub struct Monitor {
    each_node_state: HashMap<NodeId, HashSet<String>>,
}

impl Monitor {
    pub fn new() -> Self {
        Self {}
    }
}
