# copy apps/imgs/fn2.wasm to fn1-fn10
import os

# os.system("cargo build --target wasm32-wasi --release")

os.system("mkdir apps/imgs")

for i in range(10):
    os.system(
        "cp apps/fn2/target/wasm32-wasi/release/fn2.wasm apps/imgs/fn{}.wasm".format(i+1))
