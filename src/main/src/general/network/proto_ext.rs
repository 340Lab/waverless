use super::proto;

pub trait ProtoExtDataItem {
    fn data_sz_bytes(&self);
}

impl ProtoExtDataItem for proto::write_one_data_request::DataItem {
    fn data_sz_bytes(&self) {
        match self {
            proto::write_one_data_request::DataItem::Data(d) => d.data.len(),
            proto::write_one_data_request::DataItem::DataVersion(d) => d.data.len(),
        }
    }
}
