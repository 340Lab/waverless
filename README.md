# wasm_serverless
An distributed serverless framework based on wasm runtime.

## Feishu doc
[https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink](https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink)

## Roadmap of wasm_serverless
### System
- (paused) Integrated KV storage, different design for meta and serverless app.

- x Basic P2P and RPC based on quic

- x Basic scheduler single master - multiple worker

- x Metric, collecting infos of each nodes

- Bench test, comprehensive tests are necessary for writing paper.

- Different application type

- Propagation delay simulation

- General storage api

- Cache for storage api

### Frontend
- Deploy
- Metric
- Experiment config

## 底层技术选型

### Wasm
- [WasmEdge](https://wasmedge.org/)

  `curl -sSf https://raw.githubusercontent.com/WasmEdge/WasmEdge/master/utils/install.sh | bash -s -- -v 0.13.3`

  `rustup target add wasm32-wasi`

  `cargo build -p --target wasm32-wasi --release`



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
- `source /home/pa/.bashrc`

- proxy
  ```
  export https_proxy=http://192.168.232.1:23333
  export http_proxy=http://192.168.232.1:23333
  ```