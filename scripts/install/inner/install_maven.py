
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



if os.path.exists("apache-maven-3.9.6-bin.tar.gz"):
    print("maven已存在，无需再次下载")
else:
    os_system_sure("wget https://dlcdn.apache.org/maven/maven-3/3.9.6/binaries/apache-maven-3.9.6-bin.tar.gz")



if os.path.exists("apache-maven-3.9.6"):
    print("maven已解压，无需再次解压")
else:
    os_system_sure("tar -zxvf apache-maven-3.9.6-bin.tar.gz")



MAVEN="/usr/lib/mvn"
os_system_sure("mkdir -p "+MAVEN)
os_system_sure("cp -r apache-maven-3.9.6 "+MAVEN)



bins=[
    "mvn",
    "mvnDebug",
    "mvnyjp",
]



for bin in bins:
    os_system_sure(f"update-alternatives --install /usr/bin/{bin} {bin} {MAVEN}/apache-maven-3.9.6/bin/{bin} 1")
    os_system_sure(f"update-alternatives --set {bin} {MAVEN}/apache-maven-3.9.6/bin/{bin}")

