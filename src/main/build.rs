use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "src/general/network/proto_src/kv.proto",
            "src/general/network/proto_src/raft.proto",
            "src/general/network/proto_src/sche.proto",
            "src/general/network/proto_src/metric.proto",
            "src/general/network/proto_src/remote_sys.proto",
            "src/general/network/proto_src/data.proto",
            "src/worker/func/shared/process_rpc_proto.proto",
        ],
        &["src/"],
    )?;
    Ok(())
}
