import os,urllib.request,sys


install_dir="/teledeploy_secret/bin_crac"
crac_pack="jdk_crac.tar.gz"

def chdir(dir):
    print("chdir:",dir)
    os.chdir(dir)

def os_system(cmd):
    print("os_system:",cmd)
    os.system(cmd)

def download(url,file):
    file=os.path.abspath(file)
    dir=os.path.dirname(file)
    os_system(f"mkdir -p {dir}")
    print(f"downloading {url} to {file}")
    urllib.request.urlretrieve(url,file)

### utils
def os_system_sure(command):
    print(f"执行命令：{command}")
    result = os.system(command)
    if result != 0:
        print(f"命令执行失败：{command}")
        exit(1)
    print(f"命令执行成功：{command}")

if len(sys.argv)!=3:
    print("usage: python3 install_crac.py <bin_prj> <main_node_ip>")
    exit(1)
BIN_PRJ=sys.argv[1]
MAIN_NODE_IP=sys.argv[2]

os_system(f"mkdir -p {install_dir}")
chdir(install_dir)

url=f"http://{MAIN_NODE_IP}:8003/{BIN_PRJ}/{crac_pack}"
download(url,crac_pack)
    
# extract jdk_crac.tar.gz to 
os_system("tar -xvf jdk_crac.tar.gz")

# copy jdk_crac to /usr/jdk_crac
os_system_sure("rm -rf /usr/jdk_crac && cp -r jdk_crac /usr/jdk_crac")

# switch to jdk crac 17
def switch_to_jdk_crac():
    CRAC_INSTALL_DIR = "/usr/jdk_crac"
    bins=[
            "java",
            "javac",
            "jcmd"
        ]
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
    print("\nsuccess switch to jdk_crac")
switch_to_jdk_crac()