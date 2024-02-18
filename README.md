# waverless

![](figs/waverless.png)
An distributed serverless framework based on wasm runtime.

# Feishu doc
[https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink](https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f?from=from_copylink)

# Roadmap of wasm_serverless
### System
- Overall Module Organization Structure
  - 【done】[global sync modules struct](https://mp.weixin.qq.com/s?__biz=MzkwOTYzODYyMw==&amp;mid=2247483654&amp;idx=1&amp;sn=5b07690f39d6eecfe826690fc5638874&amp;chksm=c136efb6f64166a05c1bce1ceee74a1121cd5d4fa2ad70e56d24e853c4709b0c9cf4389ae3fb&token=12476630&lang=zh_CN#rd)
- Basic P2P and RPC based on quic and protobuf
  - 【done】[p2p（quic+protobuf](http://mp.weixin.qq.com/s?__biz=MzkwOTYzODYyMw==&amp;mid=2247483663&amp;idx=1&amp;sn=672664a02e62d031a2716c5e45e76cce&amp;chksm=c136efbff64166a936bb219da89179a2b28e612de62ab248c958b2f9a88dc37ceb81bbc98826&token=12476630&lang=zh_CN#rd)
  - 【done】rpc
- Metric, collecting infos of each nodes
  - 【done】basic
  - Support prometheus
- Basic scheduler single master - multiple worker
  - 【done】wasmedge对接，虚拟机缓存机制，
  - 【done】请求的路由调度
  - DataEvent-based scheduler
  - Wasmedge 升级
  - Third party db？
- Integrated KV storage
  - 【done】Wasm host function, Kv event
- Cluster deployment with docker and ansible
  - 【done】basic
- Bench test, comprehensive tests are necessary for writing paper.
  - 【done】Goose stress test
  - 【done】Network bandwidth and latency limit with tc
  - Different load pattern
- Scheduling algorithm
  - 【done】random
  - 【done】hash
  - 【done】straw2



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

  Wasm，

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

``` yaml
fns:
  split_file:
    # http请求访问对应app名称的api就会触发入口函数
    event:
    - http_app: 
    args: [http_text,kv_key(0)]
    kvs:
      wordcount_slice_{}_{}: [set]

  handle_one_slice:
    event:
    - kv_set: wordcount_slice_{}
    # 用于表征数据消费关系，决策时直接将数据存到目标执行位置
    args: [kv_key(0)]
    kvs: 
      wordcount_slice_{}_{}: [delete]
      wordcount_{}_{}: [set]
```