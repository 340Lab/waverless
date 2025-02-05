use std::io::Result;
fn main() -> Result<()> {
    let mut config = prost_build::Config::new();
    config
        .type_attribute("BatchRequestId", "#[derive(Eq, Hash)]");
    config.compile_protos(
        &[
            "src/general/network/proto_src/kv.proto",
            "src/general/network/proto_src/raft.proto",
            "src/general/network/proto_src/sche.proto",
            "src/general/network/proto_src/metric.proto",
            "src/general/network/proto_src/remote_sys.proto",
            "src/general/network/proto_src/data.proto",
            "src/general/app/app_shared/process_rpc_proto.proto",
        ],
        &["src/"],
    )?;
    Ok(())
}
