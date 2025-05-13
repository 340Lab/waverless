use crate::general::network::proto;

pub trait ProtoExtDataOpeRole {
    fn new_upload_data() -> proto::data_schedule_context::OpeRole;
}

impl ProtoExtDataOpeRole for proto::data_schedule_context::OpeRole {
    fn new_upload_data() -> proto::data_schedule_context::OpeRole {
        proto::data_schedule_context::OpeRole::UploadData(proto::DataOpeRoleUploadData {})
    }
}
