### Multi Nodes

1. First clone this project on master node

2. Ansible install with `scripts/install/install_ansible.sh` (need python)

3. Docker swarm init, join others to master (TODO: make auto)

4. Config the `scripts/deploy_cluster/node_config.yaml` each service

5. Set up ssh interflow and ansible node info `python scripts/deploy_cluster/1.ansible_setup.py`

6. Redploy `2.redeploy.sh`

### Single Node
follow the ci