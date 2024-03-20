export LANG=C.UTF-8
source $HOME/.cargo/env
ansible-playbook -vv scripts/deploy_single_node/1.ans_build.yml -i scripts/local_ansible_conf.ini