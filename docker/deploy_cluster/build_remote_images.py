import os
import yaml
import argparse
import sys
import pexpect

PASSWORD="123456"

def run_cmd(cmd):
    print("> "+cmd)
    # if cmd.startswith("ssh") or cmd.startswith("scp"):
    #     # 创建spawn对象
    #     child = pexpect.spawn(cmd, encoding='utf-8',logfile=sys.stdout)

    #     # 匹配密码提示，然后发送密码
    #     child.expect('password:')
    #     child.sendline(PASSWORD)

    #     # 在这里可以继续与SSH会话进行交互
    #     # 例如，可以发送其他命令

    #     # 等待命令执行完成
    #     try:
    #         child.expect(pexpect.EOF)
    #     except:
    #         pass
    #     child.close()
    #     # 打印输出
    #     # print(child.before)
    # else:
    os.system(cmd)

def run_cmd_with_result(cmd):
    print("> "+cmd)
    return os.popen(cmd).read()

def read_yaml(f):
    # parse
    import ruamel.yaml
    yaml = ruamel.yaml.YAML(typ='rt')
    parsed_data = yaml.load(f)

    return parsed_data

def entry(args):
    # read cluster-nodes.yml
    with open('node_config.yaml', 'r') as f:
        cluster_nodes = read_yaml(f)
        for nid in cluster_nodes["nodes"]:
            node=cluster_nodes["nodes"][nid]
            ip=node["addr"].split(":")[0]
            port=node["addr"].split(":")[1]
            id=node["id"]

            # mkdir remote
            run_cmd("ssh root@{} 'rm -rf /root/wasm_serverless_deploy && mkdir -p /root/wasm_serverless_deploy/target/release && ls /root/wasm_serverless_deploy'".format(ip))
            if args.update_binary:
                # cp ../../target to remote /root/wasm_serverless_deploy/target
                run_cmd("scp ../../target/release/wasm_serverless root@{}:/root/wasm_serverless_deploy/target/release && scp -r ../../docker root@{}:/root/wasm_serverless_deploy/docker".format(ip,ip))
            
            job="cd /root/wasm_serverless_deploy && ls && chmod -R 775 . && ./docker/WasmServerless/build_image.sh"
            if args.update_env:
                job="cd /root/wasm_serverless_deploy && ls && chmod -R 775 . && ./docker/WasmEdge/build_image.sh && ./docker/WasmServerless/build_image.sh"
            # store to remote tmp.sh
            run_cmd("ssh root@{} 'echo \"{}\" > /root/wasm_serverless_deploy/tmp.sh'".format(ip, job))
            run_cmd("ssh root@{} 'chmod +x /root/wasm_serverless_deploy/tmp.sh && /root/wasm_serverless_deploy/tmp.sh'".format(ip))

            # if args.update_env:
            #     # check if docker image exists: wasm_serverless_env:v1
            #     check=run_cmd_with_result("ssh root@{} docker images".format(ip))
            #     if "wasm_serverless_env " not in check:
            #         run_cmd("cd /root/wasm_serverless_deploy && ./docker/WasmEdge/build_image.sh")
            
            # run_cmd("ssh root@{} cd /root/wasm_serverless_deploy && ls && chmod -R 775 . && ./docker/WasmServerless/build_image.sh ".format(ip))

            

# parse arg
# - update env
# - update binary
# password
parser = argparse.ArgumentParser(description='deploy wasm_serverless to cluster')
# parser.add_argument('--update-binary', action='store_false',  help='update binary',default=False)
parser.add_argument('--update-binary', action='store_true',  help='update binary',default=False)
# parser.add_argument('--update-env', action='store_false', help='update env',default=False)
parser.add_argument('--update-env', action='store_true', help='update env',default=False)
parser.add_argument('--password', action='store', help='password',default="123456")
args=parser.parse_args()
PASSWORD=args.password
print(args)

entry(args)

