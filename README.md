# wasm_serverless
An distributed serverless framework based on wasm runtime.

# Feishu doc
[https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink](https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink)

# Roadmap of wasm_serverless
### System
- (paused) Integrated KV storage, different design for meta and serverless app.

- x Basic P2P and RPC based on quic

- x Basic scheduler single master - multiple worker

- x Metric, collecting infos of each nodes

- x Cluster deployment with docker and ansible

- Bench test, comprehensive tests are necessary for writing paper.

  - x Goose stress test
  
  - x Network bandwidth and latency limit with tc

  - Different load pattern

- Scheduling algorithm

  - x random

  - x hash



- Different application type

- General storage api

- Cache for storage api

### Frontend
- Deploy
- Metric
- Experiment config

# 底层技术选型

### Wasm
- [WasmEdge](https://wasmedge.org/)

### Network
- P2P

### KV for route
- async-raft

### KV for global
- Router cluster with raft for load balancing.
- Erasure coding for reliability.

# Tools
- Code counter

  `tokei src`

# Try it
follow the [DEPLOY.md](./DEPLOY.md)