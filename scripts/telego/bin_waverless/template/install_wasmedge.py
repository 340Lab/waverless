import os,urllib.request,sys


install_dir="/teledeploy_secret/bin_wasmedge"
files=[
    ["install_wasmedge_inner.py","./"],
    ["WasmEdge-0.13.3-manylinux2014_x86_64.tar.gz","/tmp/install/"]
]

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

if len(sys.argv)!=3:
    print("usage: python3 install_wasmedge.py <bin_prj> <main_node_ip>")
    exit(1)
BIN_PRJ=sys.argv[1]
MAIN_NODE_IP=sys.argv[2]

os_system(f"mkdir -p {install_dir}")
chdir(install_dir)

for file in files:
    url=f"http://{MAIN_NODE_IP}:8003/{BIN_PRJ}/{file[0]}"
    file=os.path.join(file[1],file[0])
    download(url,file)

os_system("python3 wasmedge_local_install.py")