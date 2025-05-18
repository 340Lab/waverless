use crate::{general::network::proto, sys::NodeID};

pub trait ProtoExtDataOpeRole {
    fn new_upload_data() -> proto::data_schedule_context::OpeRole;
    fn new_fn_call(
        app_name: &str,
        func_name: &str,
        thisnode: NodeID,
    ) -> proto::data_schedule_context::OpeRole;
}

impl ProtoExtDataOpeRole for proto::data_schedule_context::OpeRole {
    fn new_upload_data() -> proto::data_schedule_context::OpeRole {
        proto::data_schedule_context::OpeRole::UploadData(proto::DataOpeRoleUploadData {})
    }
    fn new_fn_call(
        app_name: &str,
        func_name: &str,
        thisnode: NodeID,
    ) -> proto::data_schedule_context::OpeRole {
        proto::data_schedule_context::OpeRole::FuncCall(proto::DataOpeRoleFuncCall {
            app_func: format!("{}/{}", app_name, func_name),
            node_id: thisnode,
        })
    }
}
