import yaml
import os

# Get the directory of the current script
DEPLOY_CLUSTER_DIR = os.path.dirname(os.path.abspath(__file__))
NODE_CONFIG = os.path.join(DEPLOY_CLUSTER_DIR, 'node_config.yaml')


def read_yaml(file_path):
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)
    return data


def generate_docker_compose(ip, nodes):
    services = {}
    
    for key, node in nodes.items():
        service_name = f"node{key}"

        external_port1 = int(node['addr'].split(':')[-1])
        external_port2 = external_port1 + 1
        external_ip = node['addr'].split(':')[0]

        services[service_name] = {
            'image': 'wasm_serverless:v1',
            'ports': [f"{external_ip}:{external_port1}:{external_port1}/udp", f"{external_port2}:{external_port2}"],
            'deploy':{
                'resources':{
                    'limits':{
                        'memory': '6G'
                    }
                }
            },
                
            'volumes': ['/root/wasm_serverless_deploy/files:/etc/wasm_serverless/files'],
            'environment': {
                'WASM_SERVERLESS_NODEID': key
            },
            'privileged': True # for tc control
        }

    compose_data = {'version': '3', 'services': services}
    compose_file_name = os.path.join(DEPLOY_CLUSTER_DIR, f"compose_{ip}.yml")

    with open(compose_file_name, 'w') as file:
        yaml.dump(compose_data, file, default_flow_style=False)


def main():
    data = read_yaml(NODE_CONFIG)

    grouped_nodes = {}
    for key, node in data['nodes'].items():
        ip = node['addr'].split(':')[0]
        if ip not in grouped_nodes:
            grouped_nodes[ip] = {}
        grouped_nodes[ip][key] = node

    for ip, nodes in grouped_nodes.items():
        generate_docker_compose(ip, nodes)


if __name__ == "__main__":
    main()
