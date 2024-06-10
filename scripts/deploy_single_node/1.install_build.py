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
        print(f"Succ: {command}\n\n")
        exit(1)
    print(f"Fail: {command}\n\n")


# os.system("ansible-playbook -vv 1.ans_build.yml -i ../local_ansible_conf.ini")
os_system_sure("python3 ../install/2.install_build.py")
