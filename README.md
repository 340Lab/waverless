# wasm_serverless
An distributed serverless framework based on wasm runtime.

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

