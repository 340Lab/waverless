### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)


### utils
def os_system_sure(command):
    print(f"执行命令：{command}")
    result = os.system(command)
    if result != 0:
        print(f"命令执行失败：{command}")
        exit(1)
    print(f"命令执行成功：{command}\n\n")

def os_system(command):
    print(f"执行命令：{command}")
    result = os.system(command)
    print("\n\n")

def cp_except(src, dest, exclude_list):
    contents = os.listdir(src)
    for item in contents:
        if item not in exclude_list:
            os_system_sure(f"cp -r {src}/{item} {dest}")

### workflow
# build backend
if not os.path.exists("pack"):
    print("\n!!! should run pack before repack\n")
    exit(1)

os_system_sure("cp template/install_service.py pack/waverless_backend/install_service.py")
os_system_sure("cp template/run_node.py pack/waverless_backend/run_node.py")
os_system_sure("cp template/_gen_app_need_data.py pack/waverless_backend/_gen_app_need_data.py")

# build frontend (docker)
os_system_sure("rm -rf pack/waverless_ui")
os_system_sure("mkdir -p waverless_ui")
cp_except("../../waverless_ui", "waverless_ui", ["dist", "node_modules"])
os_system_sure("mv waverless_ui pack/")

