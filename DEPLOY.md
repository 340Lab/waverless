### We use multi-computer to build up a cluster.

1. First clone this project on master node

2. Docker swarm

    1. docker swarm init

    2. (TODO: make this auto) run `docker swarm join xxx` on each node.

    3. Registry 镜像

    docker pull registry:latest

    docker run -d -p 5000:5000 --restart=always --name registry --network ingress registry:latest

    docker tag wasm_serverless:v1 localhost:5000/wasm_serverless:v1

    docker push localhost:5000/wasm_serverless:v1

    4. `docker service ls`
3. 
