
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


print_title("preparing pkg")
VERSION="0.13.3"
TARGZ_FILE = f"WasmEdge-{VERSION}-manylinux2014_x86_64.tar.gz"
if not os.path.exists(f"/tmp/install/{TARGZ_FILE}"):
    os_system_sure("mkdir -p /tmp/install/")
    os_system_sure(f"wget https://github.com/WasmEdge/WasmEdge/releases/download/{VERSION}/{TARGZ_FILE}")
    os_system_sure(f"mv {TARGZ_FILE} /tmp/install/")


print_title("installing wasmedge")
os_system_sure(f"python3 ../install/inner/wasm_edge.py -v {VERSION}")


print_title("debug wasmedge")
os_system_sure("ls /root/.wasmedge/bin/")
os_system_sure("/root/.wasmedge/bin/wasmedge --version")
# - name: Install WasmEdge
#   become: true
#   shell: |
#     cat > /tmp/install_wasmedge.sh <<'END'
#       #!/bin/bash

#       mkdir -p /tmp/install
#       cp inner/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz /tmp/install/WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz
#       python3 inner/wasm_edge.py -v 0.13.3
#     END

#     bash /tmp/install_wasmedge.sh
#     rm -f /tmp/install_wasmedge.sh
#   args:
#     creates: "/usr/local/bin/wasmedge" # Skip if WasmEdge is already installed

# - name: Debug WasmEdge version
#   become: true
#   shell: |
#     cat > /tmp/debug_wasmedge.sh <<'END'
#       #!/bin/bash
#       ls /root/.wasmedge/bin/
#       export PATH="/root/.wasmedge/bin/":$PATH
#       # source ~/.bashrc
#       whereis wasmedge
#       wasmedge --version
#     END

#     bash /tmp/debug_wasmedge.sh
#     rm -f /tmp/debug_wasmedge.sh
  


