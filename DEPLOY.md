## Multi Nodes

1. First clone this project on master node

2. Ansible install with `scripts/install/install_ansible.sh` (need python)

3. Docker swarm init, join others to master (TODO: make auto)

4. Config the `scripts/deploy_cluster/node_config.yaml` each service

5. Set up ssh interflow and ansible node info `python scripts/deploy_cluster/1.ansible_setup.py`

6. Redploy `2.redeploy.sh`

### Deployment File Distribution

- /root/wasm_serverless_deploy/
    - apps/
    - {app_name}/
        - app.wasm
        - app.yaml
    - ...
    - docker/
    - (contents of the Docker directory, after unzipping)
    - scripts/
    - (contents of the scripts directory, after unzipping)
    - docker.zip
    - scripts.zip
    - files/
        - node_config.yaml
        - app needed data
    - target/
    - release/
        - wasm_serverless
    - compose_{ip}.yml (copied from the deploy_cluster directory)
    - docker-compose.yml
    - install/
    - _ans_install.yml
    - _ans_install_docker.yml


### Docker Container File Distribution
- /usr/local/bin/
    - wasm_serverless (binary file copied from target/release/ during the build)
- /etc/wasm_serverless/
    - apps/
        - {app_name}
            - app.* (files copied from apps/fn2/ during the build)
        - ...
    - wasm_serverless_entrypoint.sh (script copied during the build)
    - node_config.yaml (uncomment the corresponding line to copy it during the build if needed)
    - files/ (volume to /root/wasm_serverless_deploy/files)


## Single Node
follow the ci