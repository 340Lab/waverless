### chdir
import os
import sys

CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

# os.system('ansible-playbook -vv 2.ans_install_build.yml -i ../local_ansible_conf.ini')
### utils
def os_system_sure(command):
    print(f"Run：{command}\n")
    result = os.system(command)
    if result != 0:
        print(f"\nFail：{command}\n\n")
        exit(1)
    print(f"\nSucc：{command}\n\n")


# result.returncode
# result.stdout
def run_cmd_return(cmd):
    print(f"Run：{cmd}\n")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(f"\nStdout：{result.stdout}\n\n")
    return result


def print_title(title):
    print(f"\n\n>>> {title}")
#################################################################################################


print_title("build core")
os_system_sure("rm -rf pack")
os_system_sure("mkdir -p pack/waverless_backend")
BACKEND_PATH=os.path.abspath("pack/waverless_backend")
os.chdir("../../")
os_system_sure("rustup default 1.79.0")
os_system_sure("$HOME/.cargo/bin/cargo build --release")
os_system_sure(f"cp target/release/wasm_serverless {BACKEND_PATH}")
