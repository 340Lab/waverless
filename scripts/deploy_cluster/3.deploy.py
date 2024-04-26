### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)

import multiprocessing
import subprocess
import yaml

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

def run_batch(batch):
    for cmd in batch:
        if len(cmd)==1:
            # default is os_system_sure
            os_system_sure(cmd[0])
        else:
            print("\n\n!!! not supported batch type\n\n")
            exit(1)

def run_batch_2_file(batch,output_file):
    for cmd in batch:
        try:
            subprocess.run(cmd, shell=True, check=True, stdout=output_file, stderr=subprocess.STDOUT)            
            # print(f"Run batch one!")

        except subprocess.CalledProcessError as e:
            print(f"Error Run Batch")
            exit(1)

    print(f"Run batch all done!")



run_batch([
            ["python3 3.1.repack_local.py"],
            ["mkdir -p log"],
            ["rm -rf log/*"]
        ])


def deploy_to_nodes():
    # def comment_for_cmds(cmds):
    #     # add echo for each line
    #     cmds=cmds.split("\n")
    #     cmds=[f'echo "\\n\\nRunning command: {cmd}"\n{cmd}' for cmd in cmds]
    #     return "\n".join(cmds)
    def deploy_to_node(ip, username, port,output_file,node_id):
        def wrap_ssh(cmd):
            return f"ssh -p {port} {username}@{ip} \"{cmd}\""
        def wrap_scp(src,dst):
            return f"scp -P {port} {src} {username}@{ip}:{dst}"
        run_batch_2_file(
            [
                wrap_ssh("rm -rf /tmp/waverless_tmp/"),
                wrap_ssh("mkdir -p /tmp/waverless_tmp/"),
                wrap_scp("../build/pack.tar.gz","/tmp/waverless_tmp/"),

                wrap_ssh("mkdir -p /tmp/waverless_tmp/install_pack/"),
                wrap_ssh("tar -zxvf /tmp/waverless_tmp/pack.tar.gz -C /tmp/waverless_tmp/install_pack/"),
                wrap_ssh(f"python3 /tmp/waverless_tmp/install_pack/waverless_backend/install_service.py {node_id}"),
                "sleep 5",
                wrap_ssh("systemctl status waverless")
            ],output_file
        )
        print(f"Deployment to {ip} successful!")

    processes = []

    # Read YAML configuration file
    with open('deploy_config.yml', 'r') as file:
        config = yaml.safe_load(file)

    def find_ip_node_id(ip):
        with open('node_config.yaml', 'r') as file:
            node_config = yaml.safe_load(file)
            # map node_id to ip
            for key, value in node_config['nodes'].items():
                if value['addr'].split(":")[0]==ip:
                    return key
        print(f"Error: node_id not found for ip {ip}")
        exit(1)

    for ip, credentials in config.items():
        username = credentials['user']
        port=ip.split(":")[1]
        ip=ip.split(":")[0]

        
        with open(f"log/deploy_service_{ip}.log", "w") as output_file:
            process = multiprocessing.Process(target=deploy_to_node, args=(ip, username, port,output_file,find_ip_node_id(ip)))
            process.start()
            processes.append(process)
            

    # Wait for all processes to finish
    for process in processes:
        process.join()
deploy_to_nodes()