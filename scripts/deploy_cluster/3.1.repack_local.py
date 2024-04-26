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


os_system_sure("python3 ../build/2.repack_core_and_ui.py")

# copy node_config.yaml
os_system_sure("cp node_config.yaml ../build/pack/waverless_backend/test_dir/files/")

# tar
os_system_sure("rm -rf ../build/pack.tar.gz")
os_system_sure("tar -czvf ../build/pack.tar.gz -C ../build/pack .")