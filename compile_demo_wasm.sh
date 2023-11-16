cd apps/fn2
cargo build --target wasm32-wasi --release
cd ../..
cp apps/fn2/target/wasm32-wasi/release/fn2.wasm apps/imgs

python3 prepare_fns.py