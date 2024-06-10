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
# os_system_sure("ansible-playbook -vv 1.ans_pack_core_and_ui.yml -i ../local_ansible_conf.ini")
os_system_sure("python3 1.1build_core.py")
os_system_sure("python3 1.2build_apps.py")
os_system_sure("python3 1.3pack_backend.py")
os_system_sure("python3 1.4pack_with_ui.py")


