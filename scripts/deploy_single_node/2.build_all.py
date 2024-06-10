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
