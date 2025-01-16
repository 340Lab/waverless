#!/usr/bin/python3

#     NODE_ID=$1
#     wasm_serverless $NODE_ID test_dir

import os
import sys


CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
os.chdir(CUR_FDIR)



### utils
def os_system_sure(command):
    print(f"执行命令：{command}")
    result = os.system(command)
    if result != 0:
        print(f"命令执行失败：{command}")
        exit(1)
    print(f"命令执行成功：{command}")



CRAC_INSTALL_DIR = "/usr/jdk_crac"
bins=[
        "java",
        "javac",
        "jcmd"
    ]
# swicth back to openjdk
for bin in bins:
    os_system_sure(f"update-alternatives --install /usr/bin/{bin} {bin} {CRAC_INSTALL_DIR}/bin/{bin} 100")
    os_system_sure(f"update-alternatives --set {bin} {CRAC_INSTALL_DIR}/bin/{bin}")




# export RUST_BACKTRACE=1
os.environ['RUST_BACKTRACE'] = '1'
# export RUST_LOG=info,wasm_serverless=debug
os.environ['RUST_LOG'] = 'info,wasm_serverless=debug'
# use crac jdk (See install/inner/install_crac.py)
os.environ['JAVA_HOME'] = CRAC_INSTALL_DIR
# wasmedge
ld_library_path = os.environ.get('LD_LIBRARY_PATH', '')
new_path = '/root/.wasmedge/lib/'
os.environ['LD_LIBRARY_PATH'] = new_path + ':' + ld_library_path

#     NODE_ID=$1
NODE_ID = sys.argv[1]
#     wasm_serverless $NODE_ID test_dir
os.system(f'./wasm_serverless {NODE_ID} test_dir')