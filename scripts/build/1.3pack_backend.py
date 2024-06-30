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


def assert_msg(condition,msg):
    if not condition:
        print(msg)
        exit(1)
#################################################################################################




assert_msg(os.path.exists("pack/waverless_backend"),"please run 1.1build_core.py first")
# assert_msg(os.path.exists("pack/apps"),"please run 1.2build_apps.py first")




os_system_sure("rm -rf pack/waverless_backend/test_dir")
APPDIR="pack/waverless_backend/test_dir/apps"
os_system_sure(f"rm -rf {APPDIR}")
os_system_sure(f"mkdir -p {APPDIR}")
# crac_config
with open(f"{APPDIR}/crac_config", "w") as f:
    f.write("""type: FILE
action: ignore
---
type: SOCKET
action: close""")
os_system_sure("mkdir -p pack/waverless_backend/test_dir/files")
# os_system_sure("mv pack/apps pack/waverless_backend/test_dir")
os_system_sure("cp template/install_service.py pack/waverless_backend/")
os_system_sure("cp template/run_node.py pack/waverless_backend/")