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
    services =yaml.safe_load('''
        promtail:
            image: "grafana/promtail"
            volumes:
                - /var/lib/docker/containers:/var/lib/docker/containers:ro
                - /var/run/docker.sock:/var/run/docker.sock
                - /root/wasm_serverless_deploy/promtail.yaml:/etc/promtail/promtail.yaml
            command: -config.file=/etc/promtail/promtail.yaml
        ''')

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
            'volumes': [
                '/root/wasm_serverless_deploy:/etc/wasm_serverless',
            ],
            'environment': {
                'WASM_SERVERLESS_NODEID': key
            },
            'privileged': True, # for tc control,
            'logging': {
                # 设置日志驱动程序和选项
                'driver': "json-file",
                'options':{
                    'max-size': "10m",
                    'max-file': "5"
                }
            },
            'labels':["log_promtail"]
        }
    

    compose_data = {'version': '3', 'services': services}
    compose_file_name = os.path.join(DEPLOY_CLUSTER_DIR, f"compose_{ip}.yml")

    with open(compose_file_name, 'w') as file:
        yaml.dump(compose_data, file, default_flow_style=False)

def promtail_config(lokiaddr):
    PROMTAIL_CONFIG = f'''
server:
  http_listen_port: 9080
  grpc_listen_port: 0

positions:
  filename: /tmp/positions.yaml

clients:
  - url: http://{lokiaddr}/loki/api/v1/push

scrape_configs:
  - job_name: flog_scrape
    docker_sd_configs:
      - host: unix:///var/run/docker.sock
        refresh_interval: 5s
        filters:
          - name: label
            values: ["log_promtail"]
    relabel_configs:
      - source_labels: ['__meta_docker_container_name']
        regex: '/(.*)'
        target_label: 'container'
      - source_labels: ['__meta_docker_container_log_stream']
        target_label: 'logstream'
      - source_labels: ['__meta_docker_container_label_logging_jobname']
        target_label: 'job'
    '''
    path=os.path.join(DEPLOY_CLUSTER_DIR, 'promtail.yaml')
    with open(path, 'w') as f:
        f.write(PROMTAIL_CONFIG)



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
    
    promtail_config(data['loki']['addr'])


if __name__ == "__main__":
    main()
