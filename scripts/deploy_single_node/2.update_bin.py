### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

os.system("cargo build --release")
os.system("cp ../../target/release/wasm_serverless ./pack")
