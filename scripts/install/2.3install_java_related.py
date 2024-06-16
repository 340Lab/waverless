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
    print(f">>> Run：{command}")
    result = os.system(command)
    if result != 0:
        print(f">>> Fail：{command}\n\n")
        exit(1)
    print(f">>> Succ：{command}\n\n")


# result.returncode
# result.stdout
def run_cmd_return(cmd):
    print(f"Run：{cmd}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    print(f"Stdout：{result.stdout}\n\n")
    return result


def print_title(title):
    print(f"\n\n>>> {title}")
#################################################################################################

os_system_sure("python3 inner/install_maven.py")

CRAC_INSTALL_DIR = "/usr/jdk_crac"
if not os.path.exists(CRAC_INSTALL_DIR):
    os_system_sure("python3 inner/install_crac.py")

os_system_sure("python3 inner/switch_jdk_17crac.py")