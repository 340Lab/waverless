import os

PRJ_DIR=os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DOWNLOAD_CACHE_DIR=os.path.join(PRJ_DIR,"prepare_cache")
WAVERLESS_PATH="/root/serverless_benchmark_plus/middlewares/waverless/waverless"

#########

waverless_benchmark_path=os.path.abspath(os.path.join(WAVERLESS_PATH,"../../.."))

# cmd means the necessary command to prepare the resource
# rsc means the resource to be prepared
rscs=[
    [   # binary 
        {"cmd":"python3 "+os.path.join(WAVERLESS_PATH,"scripts/build/1.1build_core.py")},
        {"rsc":os.path.join(WAVERLESS_PATH,"scripts/build/pack/waverless_backend/wasm_serverless")},
    ],
    [   # entry script
        {"rsc":os.path.join(WAVERLESS_PATH,"scripts/build/template/run_node.py")},
    ],
    [   # wasmedge installer
        {"rsc":os.path.join(WAVERLESS_PATH,"scripts/install/inner/wasm_edge.py")},
    ],
    [   # crac
        {"cmd":"python3 "+os.path.join(WAVERLESS_PATH,"scripts/install/inner/install_crac.py && "+
            "mkdir -p /teledeploy_secret/waverless && "
            "rm -f /teledeploy_secret/waverless/jdk_crac.tar.gz && "+
            "tar -czvf /teledeploy_secret/waverless/jdk_crac.tar.gz -C /usr jdk_crac")},
        {"rsc":"/teledeploy_secret/waverless/jdk_crac.tar.gz"}
    ]
]


def chdir(dir):
    print("chdir:",dir)
    os.chdir(dir)

def os_system(cmd):
    print("os_system:",cmd)
    os.system(cmd)

def os_system_sure(cmd):
    print("os_system_sure:",cmd)
    res=os.system(cmd)
    if res!=0:
        raise Exception(f"os_system_sure failed: {cmd}")

for rsc_ in rscs:
    cmd=""
    rsc=""
    copy=""

    for item in rsc_:
        if "rsc" in item:
            rsc=item["rsc"]

    for item in rsc_:
        if rsc=="":
            # 没有目标资源绑定，每次都执行脚本
            if "cmd" in item:
                cmd=item["cmd"]
                os_system_sure(cmd)
        else:
            rsc_file=rsc.split("/")[-1]
            cache_rsc=os.path.join(DOWNLOAD_CACHE_DIR,rsc_file)
            
            # 有目标资源绑定，只有资源不存在时（缓存被删除），才执行脚本，并更新资源
            if not os.path.exists(cache_rsc):
                if "cmd" in item:
                    cmd=item["cmd"]
                    os_system_sure(cmd)
                # copy to prepare_cache dir
                os_system_sure(f"mkdir -p {DOWNLOAD_CACHE_DIR}")
                os_system_sure(f"cp -r {rsc} {DOWNLOAD_CACHE_DIR}")

print("pack waverless related done!")