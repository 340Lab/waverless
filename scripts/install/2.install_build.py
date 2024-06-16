
### chdir
import os
import subprocess
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

# os.system('ansible-playbook -vv 2.ans_install_build.yml -i ../local_ansible_conf.ini')
### utils
def os_system_sure(command):
    print(f"Run：{command}\n\n")
    result = os.system(command)
    if result != 0:
        print(f"Fail：{command}\n\n")
        exit(1)
    print(f"Succ：{command}\n\n")


# result.returncode
# result.stdout
def run_cmd_return(cmd):
    print(f"Run：{cmd}\n\n")
    result = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    print(f"Stdout：{result.stdout}")
    return result

#################################################################################################

apps=[
    # "clang",
    "lldb",
    "lld",
    "build-essential",
    "curl",
    "protobuf-compiler",
    "pkg-config",
    "libssl-dev",
    "snap"
]
os_system_sure(f"apt install -y {' '.join(apps)}")


os_system_sure("python3 2.2install_clang.py")


res=run_cmd_return("$HOME/.cargo/bin/rustc --version")
if res.returncode != 0:
    print("Rust not installed, installing now...")
    os_system_sure("curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y")
    # os_system_sure("source $HOME/.cargo/env")
    os_system_sure("$HOME/.cargo/bin/rustup target add wasm32-wasi")
    # os_system_sure("export PATH=$HOME/.cargo/bin:$PATH")



os_system_sure("python3 2.1install_wasmedge.py")

# - name: Install WasmEdge
#   include_tasks: 2.1_ans_install_wasmedge.yml

