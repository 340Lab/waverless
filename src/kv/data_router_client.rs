use std::{collections::BTreeMap, sync::Arc};

use async_trait::async_trait;
use tokio::task::JoinHandle;

use crate::{
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, LogicalModules, NodeID},
};

use super::KeyRange;

// Manage the key routing node information
pub struct DataRouterClient {}

impl DataRouterClient {
    pub fn get_route_of_key_range(&self, key: KeyRange) -> WSResult<BTreeMap<KeyRange, NodeID>> {
        Ok(BTreeMap::new())
    }
}

#[async_trait]
impl LogicalModule for DataRouterClient {
    fn new(args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        Self {}
    }
    fn start(&self) -> WSResult<Vec<JoinHandle<()>>> {
        Ok(vec![])
    }
}
