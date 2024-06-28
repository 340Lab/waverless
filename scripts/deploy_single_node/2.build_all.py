### chdir
import os
import subprocess
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

#################################################################################################ß


# os.system("ansible-playbook -vv 1.ans_build.yml -i ../local_ansible_conf.ini")
os_system_sure("python3 ../build/1.pack_core_and_ui.py")
os_system_sure("rm -rf pack")
os_system_sure("mv ../build/pack pack")
os_system_sure("cp -r pack/waverless_backend/test_dir pack/waverless_backend/test_dir1")
os_system_sure("cp -r pack/waverless_backend/test_dir pack/waverless_backend/test_dir2")
os_system_sure("rm -rf pack/waverless_backend/test_dir")

def new_run_script(nodeid):
    with open("pack/waverless_backend/run_node.py", "r") as f:
        content = f.read()
        content = content.replace("test_dir", f"test_dir{nodeid}")
        content = content.replace("NODE_ID = sys.argv[1]", f"NODE_ID = {nodeid}")
    with open(f"pack/waverless_backend/run_node{nodeid}.py", "w") as f:
        f.write(content)
    
new_run_script(1)
new_run_script(2)
os_system_sure("rm -rf pack/waverless_backend/run_node.py")


