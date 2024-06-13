### chdir
import os
import sys
import yaml
import zipfile

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



import subprocess
import os
def is_clang_installed():
    try:
        subprocess.run(["clang", "--version"], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False
    except FileNotFoundError:
        return False
if not is_clang_installed():
    # wget https://apt.llvm.org/llvm.sh
    # chmod +x llvm.sh
    # sudo ./llvm.sh 12

    os_system_sure("wget https://apt.llvm.org/llvm.sh")
    os_system_sure("chmod +x llvm.sh")
    os_system_sure("./llvm.sh 12")
    os_system_sure("apt-get install clang-12")
    os_system_sure("ln -sf /usr/bin/clang-12 /usr/bin/clang")