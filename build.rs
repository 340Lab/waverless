use std::io::Result;
fn main() -> Result<()> {
    prost_build::compile_protos(
        &[
            "src/network/proto_src/kv.proto",
            "src/network/proto_src/raft.proto",
            "src/network/proto_src/sche.proto",
            "src/network/proto_src/metric.proto",
        ],
        &["src/"],
    )?;
    Ok(())
}
