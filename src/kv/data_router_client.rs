use std::{collections::BTreeMap};

use async_trait::async_trait;


use crate::{
    module_iter::*,
    result::WSResult,
    sys::{LogicalModule, LogicalModuleNewArgs, NodeID},
    util::JoinHandleWrapper,
};

use super::KeyRange;

// Manage the key routing node information
#[derive(LogicalModuleParent, LogicalModule)]
pub struct DataRouterClient {
    name: String,
}

impl DataRouterClient {
    pub fn get_route_of_key_range(&self, _key: KeyRange) -> WSResult<BTreeMap<KeyRange, NodeID>> {
        Ok(BTreeMap::new())
    }
}

#[async_trait]
impl LogicalModule for DataRouterClient {
    fn inner_new(mut args: LogicalModuleNewArgs) -> Self
    where
        Self: Sized,
    {
        args.expand_parent_name(Self::self_name());
        Self {
            name: args.parent_name.clone(),
        }
    }
    async fn start(&self) -> WSResult<Vec<JoinHandleWrapper>> {
        Ok(vec![])
    }
    fn name(&self) -> &str {
        &self.name
    }
}
