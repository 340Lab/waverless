---
structs:
  - WriteSplitDataTaskGroup: 管理数据分片写入任务组
  - SharedMemHolder: 共享内存数据访问管理
  - SharedMemOwnedAccess: 共享内存的所有权访问控制

task_group_functions:
  - 任务组管理
  - 分片合并优化
  - 状态同步

mem_holder_functions:
  - 高效的内存访问
  - 资源自动管理

functions:
  - new_shared_mem: 创建共享内存数据结构
  - write_split_data: 写入分片数据
---

# 数据项处理 (dataitem.rs)

数据项处理模块负责管理单个数据项的处理流程，包括数据分片和共享内存访问。

## 核心数据结构 ^dataitem-structs

### WriteSplitDataTaskGroup ^dataitem-task-group
- 管理数据分片写入任务组
- 为 batch 和 get 操作提供高效的分片合并封装
- 主要功能：
  - 任务组管理
  - 分片合并优化
  - 状态同步

### SharedMemHolder ^dataitem-mem-holder
- 共享内存数据访问管理
- 提供安全的内存共享机制
- 特点：
  - 高效的内存访问
  - 资源自动管理

### SharedMemOwnedAccess ^dataitem-mem-access
- 共享内存的所有权访问控制
- 确保内存访问的安全性和独占性

## 核心功能 ^dataitem-functions

### new_shared_mem ^dataitem-new-mem
- 创建共享内存数据结构
- 初始化内存访问控制

### write_split_data ^dataitem-write-split
- 写入分片数据
- 功能特点：
  - 支持数据分片
  - 并发写入控制
  - 数据完整性校验
