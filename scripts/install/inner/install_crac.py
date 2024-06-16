
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



def compile_crac():
    os_system_sure("apt install build-essential autoconf openjdk-17-jdk -y")
    os.system("git clone https://github.com/ActivePeter/crac")
    os.chdir("crac")
    os_system_sure("git checkout sofacrac17")
    installs=[
        'libfontconfig1-dev',
        'libx11-dev libxrender-dev libxrandr-dev libxtst-dev libxt-dev',
        'zip unzip',
        'file',
        'build-essential',
        'libasound2-dev',
        'libcups2-dev'
    ]
    
    os_system_sure("apt update")
    os_system_sure("apt install {} -y".format(" ".join(installs)))

    bins=[
        "java",
        "javac",
        "jcmd"
    ]
    OPENJDK="/usr/lib/jvm/java-17-openjdk-amd64/"
    # swicth back to openjdk
    for bin in bins:
        # os_system_sure(f"update-alternatives --install /usr/bin/{bin} {bin} {OPENJDK}bin/{bin} 1")
        os_system_sure(f"update-alternatives --set {bin} {OPENJDK}bin/{bin}")

    os_system_sure("bash configure")
    os_system_sure("make images")
    CRIU_PATH = "build/linux-x86_64-server-release/images/jdk/lib/criu"
    if not os.path.exists(CRIU_PATH):
        os_system_sure("wget https://github.com/CRaC/criu/releases/download/release-crac/criu-dist.tar.gz")
        os_system_sure("tar -zxvf criu-dist.tar.gz")
        os_system_sure(f"cp criu-dist/sbin/criu {CRIU_PATH}")

    os_system_sure("rm -rf {}".format(CRAC_INSTALL_DIR))
    os_system_sure(f"cp -r build/linux-x86_64-server-release/images/jdk/ {CRAC_INSTALL_DIR}")
    # switch to crac jdk
    
    for bin in bins:
        os_system_sure(f"update-alternatives --install /usr/bin/{bin} {bin} {CRAC_INSTALL_DIR}/bin/{bin} 100")
        os_system_sure(f"update-alternatives --set {bin} {CRAC_INSTALL_DIR}/bin/{bin}")




def download_crac_bin():
    if not os.path.exists("./bellsoft-jdk17.0.10+14-linux-amd64-crac.deb"):
        os_system_sure("wget https://download.bell-sw.com/java/17.0.10+14/bellsoft-jdk17.0.10+14-linux-amd64-crac.deb")

    os_system_sure("dpkg -i bellsoft-jdk17.0.10+14-linux-amd64-crac.deb")

compile_crac()