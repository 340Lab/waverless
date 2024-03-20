
#     NODE_ID=$1
#     wasm_serverless $NODE_ID test_dir

import os
import sys


CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
os.chdir(CUR_FDIR)


# export RUST_BACKTRACE=1
os.environ['RUST_BACKTRACE'] = '1'
# export RUST_LOG=info,wasm_serverless=debug
os.environ['RUST_LOG'] = 'info,wasm_serverless=debug'


#     NODE_ID=$1
NODE_ID = sys.argv[1]
#     wasm_serverless $NODE_ID test_dir
os.system(f'./wasm_serverless {NODE_ID} test_dir')