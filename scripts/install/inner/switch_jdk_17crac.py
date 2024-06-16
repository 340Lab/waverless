
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
    print(f"命令执行成功：{command}")


CRAC_INSTALL_DIR = "/usr/jdk_crac"


bins=[
        "java",
        "javac",
        "jcmd"
    ]
# swicth back to openjdk
for bin in bins:
    os_system_sure(f"update-alternatives --install /usr/bin/{bin} {bin} {CRAC_INSTALL_DIR}/bin/{bin} 100")
    os_system_sure(f"update-alternatives --set {bin} {CRAC_INSTALL_DIR}/bin/{bin}")




# Check and update JAVA_HOME in /etc/environment
with open("/root/.bashrc", "r") as env_file:
    lines = env_file.readlines()

java_home_set = False
for line in lines:
    if line.startswith("export JAVA_HOME="):
        line=f"export JAVA_HOME={CRAC_INSTALL_DIR}\n"
        java_home_set = True
if not java_home_set:
    lines.append(f"export JAVA_HOME={CRAC_INSTALL_DIR}\n")
print("env lines: ",lines)

with open("/root/.bashrc", "w") as env_file:    
    env_file.writelines(lines)
