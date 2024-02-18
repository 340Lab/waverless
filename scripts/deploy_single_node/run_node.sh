# cargo build --release
# get command arg
# 1: node_id
export LANG=C.UTF-8
export RUST_BACKTRACE=1
NODE_ID=$1
target/release/wasm_serverless $NODE_ID scripts/deploy_single_node/test_dir