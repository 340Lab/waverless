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


# make ../install tar.gz
os_system_sure("rm -rf install.tar.gz")
CRAC_INSTALL_DIR = "/usr/jdk_crac"

# 判断一下 ../install/inner/jdk_crac 存不存在，如果不存在则 cp， 存在则跳过
dst = "../install/inner/jdk_crac"
if not os.path.exists(dst):
    os_system(f"cp -r {CRAC_INSTALL_DIR} {dst}")
else:
    print(f"{dst} 已存在，跳过复制。")

# os_system_sure(f"cp -r {CRAC_INSTALL_DIR} ../install/inner/jdk_crac")
os_system_sure("tar -czvf install.tar.gz -C ../install .")

def deploy_to_nodes():
    import multiprocessing
    import subprocess
    import yaml
    def deploy_to_node(ip, username, port,output_file):
        try:
            print("Start deploy, check logs in deploy_cluster/log/")
            # SCP file transfer
            subprocess.run(f'ssh -p {port} {username}@{ip} "rm -rf /tmp/waverless_tmp/install"', shell=True, check=True, stdout=output_file, stderr=subprocess.STDOUT)
            subprocess.run(f'ssh -p {port} {username}@{ip} "mkdir -p /tmp/waverless_tmp/install"', shell=True, check=True, stdout=output_file, stderr=subprocess.STDOUT)
            subprocess.run(f'scp install.tar.gz {username}@{ip}:/tmp/waverless_tmp/install', shell=True, check=True, stdout=output_file, stderr=subprocess.STDOUT)
            
            remote_cmd="""
            ls /tmp/waverless_tmp/install
            cd /tmp/waverless_tmp/install
            tar -zxvf install.tar.gz
            bash 1.install_basic.sh
            python3 2.install_build.py
            """
            
            # export shell script to remote server and execute
            subprocess.run(f'ssh -p {port} {username}@{ip} "{remote_cmd}"', shell=True, check=True, stdout=output_file, stderr=subprocess.STDOUT)
            print(f"Deployment to {ip} successful!")

        except subprocess.CalledProcessError as e:
            print(f"Error deploying to {ip}: {e}")
            exit(1)

    processes = []

    # Read YAML configuration file
    with open('deploy_config.yml', 'r') as file:
        config = yaml.safe_load(file)
    for ip, credentials in config.items():
        username = credentials['user']
        port=ip.split(":")[1]
        ip=ip.split(":")[0]

        os_system_sure("mkdir -p log")
        with open(f"log/deploy_install_{ip}.log", "w") as output_file:
            process = multiprocessing.Process(target=deploy_to_node, args=(ip, username, port,output_file))
            process.start()
            processes.append(process)
            

    # Wait for all processes to finish
    for process in processes:
        process.join()
deploy_to_nodes()


