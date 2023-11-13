# wasm_serverless
An distributed serverless framework based on wasm runtime.

## Feishu doc
[https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink](https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink)

## Roadmap
### System
- RPC communication between nodes.
- Metadata management.
    - IP of each nodes
    - location of each nodes
    - status of each nodes (cpu, mem, task)
    - status of each task
    - all deployed application
        - img location
        - function computing graph
- Basic scaler and scheduler
- Different application type
- Propagation delay simulation
- Metric (work with the metadata system)
- General storage api
- Cache for storage api

### Frontend
- Deploy
- Metric
- Experiment config

## 底层技术选型

### Wasm
- [WasmEdge](https://wasmedge.org/)

  `curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s -- -v 0.13.2`

### KV for route
- async-raft

### KV for global
- Router cluster with raft for load balancing.
- Erasure coding for reliability.

## Dev Guide
- Code counter

  `tokei src`


## Notes
- `source /Users/hello/.zshenv`