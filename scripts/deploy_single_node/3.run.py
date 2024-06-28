### chdir
import os
import subprocess
import sys
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

def print_title(title):
    print(f"\n\n{'#'*20} {title} {'#'*20}\n\n")
#################################################################################################ß

args=sys.argv

print_title("update config")
os_system_sure(f"cp node_config.yaml pack/waverless_backend/test_dir{args[1]}/files/node_config.yaml")


print_title("run")
os_system_sure(f"python3 pack/waverless_backend/run_node{args[1]}.py")