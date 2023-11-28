import os
import yaml
import argparse
import sys
import pexpect

PASSWORD="aaaaa"

def run_cmd(cmd):
    print("> "+cmd)
    if cmd.startswith("ssh") or cmd.startswith("scp"):
        # 创建spawn对象
        child = pexpect.spawn(cmd, encoding='utf-8',logfile=sys.stdout)

        # 匹配密码提示，然后发送密码
        child.expect('password:')
        child.sendline(PASSWORD)

        # 在这里可以继续与SSH会话进行交互
        # 例如，可以发送其他命令

        # 等待命令执行完成
        try:
            child.expect(pexpect.EOF)
        except:
            pass
        child.close()
        # 打印输出
        # print(child.before)
    else:
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

def entry():
    # check base image prepared
    check=run_cmd_with_result("docker images")
    if "wasm_serverless " not in check:
        print("Error: base image not prepared")
        exit(1)

    run_cmd("rm -f docker-stack.yml")
    new_docker_stack={
        # fix(can't be 3): https://github.com/docker/cli/issues/1073#issuecomment-395466027
        'version':"3.5",
        'services':{},
        "networks":{
            "default":{
                "external":{
                    "name": "host"
                }
            }
        }
    }
    def add_sevice(dockerid,nodeid, port,ip):
        def portstr(port):
            return {
                "target": int(port),
                "published": int(port),
                "mode": "host"
            }
        new_docker_stack["services"]["node{}".format(nodeid)]={
            "image":"wasm_serverless:v1",
            "deploy":{
                "mode": "replicated",
                "replicas": 1,
                "placement":{
                    "constraints":["node.id == {}".format(dockerid)]
                }
            },
            "ports":[portstr(port), portstr(int(port)+1)],
            "volumes":['/etc/wasm_serverless/config:/etc/wasm_serverless/config'],
            'environment': {
                "WASM_SERVERLESS_NODEID": nodeid
            },
            # 'networks':['host']
            # 'network_mode': 'host'
        }

    run_cmd("docker node ls")

    # read cluster-nodes.yml
    with open('node_config.yaml', 'r') as f:
        cluster_nodes = read_yaml(f)
        for nid in cluster_nodes["nodes"]:
            node=cluster_nodes["nodes"][nid]
            ip=node["addr"].split(":")[0]
            port=node["addr"].split(":")[1]
            id=node["id"]

            # mkdir remote
            run_cmd("ssh root@{} mkdir -p /etc/wasm_serverless/config".format(ip))
            # send node.config to each node scp
            run_cmd("scp node_config.yaml root@{}:/etc/wasm_serverless/config/node_config.yaml".format(ip))
            # check remote /root/wasm_serverless_deploy exist, if not scp send
            # exist = run_cmd_with_result("ssh root@{} ls /root/wasm_serverless_deploy".format(ip))

            # run_cmd("docker node update --label-add nodeid={} {}".format(nid,id))
            # run_cmd("docker node update --label-add baseport={} {}".format(port,id))

            # show labels
            # run_cmd("docker node inspect --format '{{ .Spec.Labels }}' "+(id))

            # build image
            # generate entrypoint.sh
            # with open('entrypoint.sh', 'w') as f:
            #     content='#!/bin/bash \n\
            #     # 在脚本中获取节点标签的值并设置为环境变量\n\
            #     export WASM_SERVERLESS_NODEID={} \n\
            #     # 运行实际的命令 \n\
            #     exec "$@"'.format(nid)

            #     f.write(content)
            
            # run_cmd("docker build -t wasm_serverless_node{}:v1 . --no-cache".format(nid))
            # run_cmd("rm -f entrypoint.sh")

            add_sevice(id,nid, port, ip)
        
        # write docker-stack.yml
        with open('docker-stack.yml', 'w') as f:
            yaml.dump(new_docker_stack, f)



# parse password
parser = argparse.ArgumentParser()
parser.add_argument('--password', help='password for ssh', default="aaaaa")
args=parser.parse_args()
PASSWORD=args.password
entry()