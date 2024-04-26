import os
import sys


CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
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



# require for nodeid
if len(sys.argv)!=2:
    print("Usage: python3 install_service.py <node_id>")
    exit(1)

# stop service
os_system_sure("systemctl stop waverless")

# Update /waverless_deploy/waverless_backend files
INSTALL_DIR="/waverless_deploy/"

# back up /waverless_backend/test_dir/*
os_system_sure(f"mkdir -p {INSTALL_DIR}/install_backup_test_dir")
os_system_sure(f"ls {INSTALL_DIR}/install_backup_test_dir/")
os_system_sure(f"ls {INSTALL_DIR}/waverless_backend/test_dir")
os_system_sure(f"rm -rf {INSTALL_DIR}/waverless_backend/test_dir/apps")
os_system_sure(f"rm -rf {INSTALL_DIR}/waverless_backend/test_dir/files")
os_system(f"mv {INSTALL_DIR}/waverless_backend/test_dir/* {INSTALL_DIR}/install_backup_test_dir/")
os_system_sure(f"ls {INSTALL_DIR}/install_backup_test_dir/")

# remove {INSTALL_DIR}/waverless_backend
os_system_sure(f"rm -rf {INSTALL_DIR}/waverless_backend")

# copy current to DEPLOY_DIR
os_system_sure(f"cp -r ../waverless_backend {INSTALL_DIR}")

# restore /waverless_backend/test_dir/files
os_system(f"mv {INSTALL_DIR}/install_backup_test_dir/* {INSTALL_DIR}/waverless_backend/test_dir")



# system service
SERVICE=f"""[Unit]
Description=Waverless Backend Service
After=network.target

[Service]
Type=simple
ExecStart={sys.executable} /waverless_deploy/waverless_backend/run_node.py {sys.argv[1]}
Restart=always
User=root
Group=root

[Install]
WantedBy=multi-user.target
"""

with open('/etc/systemd/system/waverless.service', 'w') as f:
    f.write(SERVICE)

os_system_sure("systemctl daemon-reload")
os_system_sure("systemctl enable waverless")
os_system_sure("systemctl start waverless")